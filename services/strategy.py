# services/strategy.py
from __future__ import annotations
import asyncio, collections, time
from dataclasses import dataclass
from typing import Dict, Deque
import numpy as np

from core.bus import BUS
from core.schemas import Tick, Signal

# ---- knobs you can tune ------------------------------------------------------
FAST = 12
SLOW = 36
MIN_MOVE_BPS = 0.8          # micro-momentum threshold per tick
COOLDOWN_SEC = 0.8          # min seconds between signals per symbol
HEARTBEAT_SEC = 30.0         # if nothing fired in this long, send a “poke” trade
TRADE_SIZES = {             # in units of the asset (BTC, ETH)
    "BTC-USD": 0.003,
    "ETH-USD": 0.03,
    "*": 0.01,
}
# -----------------------------------------------------------------------------

def _size(sym: str) -> float:
    return TRADE_SIZES.get(sym, TRADE_SIZES["*"])

@dataclass
class Engine:
    symbol: str
    prices: Deque[float]
    last_ts: float
    last_emit: float
    last_px: float | None
    last_side: str | None   # for heartbeat alternation

    def want_signal(self) -> str | None:
        """Return 'buy'/'sell'/None from SMA cross + micro momentum."""
        if len(self.prices) < max(FAST, SLOW):
            return None
        arr = np.asarray(self.prices, dtype=float)
        fast = arr[-FAST:].mean()
        slow = arr[-SLOW:].mean()

        # 1) crossover (zero buffer so it actually trades)
        if fast > slow and (self.last_side != "buy"):
            return "buy"
        if fast < slow and (self.last_side != "sell"):
            return "sell"

        # 2) micro momentum on recent tick-to-tick move
        if self.last_px is not None:
            move_bps = (arr[-1] - self.last_px) / self.last_px * 1e4
            if move_bps >= MIN_MOVE_BPS:
                return "buy"
            if move_bps <= -MIN_MOVE_BPS:
                return "sell"

        return None

async def run_strategy(symbols):
    # subscribe for ticks
    q = asyncio.Queue()
    BUS.subscribe("tick", q)

    engines: Dict[str, Engine] = {
        s: Engine(symbol=s,
                  prices=collections.deque(maxlen=max(120, SLOW*3)),
                  last_ts=time.time(),
                  last_emit=0.0,
                  last_px=None,
                  last_side=None)
        for s in symbols
    }

    async def heartbeat():
        while True:
            now = time.time()
            for s, eng in engines.items():
                # if no signal emitted for a while, poke the book
                if now - eng.last_emit >= HEARTBEAT_SEC and len(eng.prices) >= 2:
                    # alternate side each heartbeat
                    side = "sell" if eng.last_side == "buy" else "buy"
                    sig = Signal(ts=None, symbol=s, side=side, size=_size(s),
                                 reason=f"heartbeat/{HEARTBEAT_SEC:.0f}s")
                    await BUS.publish("signal", sig)
                    eng.last_emit = now
                    eng.last_side = side
            await asyncio.sleep(1.0)

    asyncio.create_task(heartbeat())

    while True:
        tick: Tick = await q.get()
        eng = engines.get(tick.symbol)
        if eng is None:
            # initialize ad-hoc engine if a new symbol sneaks in
            eng = engines[tick.symbol] = Engine(
                symbol=tick.symbol,
                prices=collections.deque(maxlen=max(120, SLOW*3)),
                last_ts=time.time(),
                last_emit=0.0,
                last_px=None,
                last_side=None
            )

        eng.prices.append(float(tick.price))
        now = time.time()

        # cooldown guard
        if now - eng.last_emit < COOLDOWN_SEC:
            eng.last_px = float(tick.price)
            eng.last_ts = now
            continue

        side = eng.want_signal()
        if side:
            sig = Signal(
                ts=tick.ts,
                symbol=tick.symbol,
                side=side,
                size=_size(tick.symbol),
                reason=f"SMA{FAST}/{SLOW}+mm{MIN_MOVE_BPS}bps"
            )
            await BUS.publish("signal", sig)
            eng.last_emit = now
            eng.last_side = side

        eng.last_px = float(tick.price)
        eng.last_ts = now
