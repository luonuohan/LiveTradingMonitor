# services/strategy_stateful.py
import asyncio, collections, time
from dataclasses import dataclass
import numpy as np

from core.bus import BUS
from core.schemas import Tick, Signal
from core.state import STORE

# ------------------ knobs (more active defaults) ------------------
WINDOW_FAST = 14           # was 20
WINDOW_SLOW = 70           # was 120
BAND_BPS    = 3            # was 8  (narrower band => more entries)
CONFIRM_TICKS = 2          # was 5  (quicker confirm)
COOLDOWN_SEC  = 2          # was 5
MIN_HOLD_SEC  = 6          # was 10
STOP_LOSS_PCT = 0.006      # 0.6% hard stop
TRAIL_PCT     = 0.004      # 0.4% trailing stop once in profit

# micro-breakout window; if price makes a local high/low it can boost entry
BRK_WIN = 18               # ticks for rolling high/low breakout

DEFAULT_QTY   = {"BTC-USD": 0.002, "ETH-USD": 0.02}
USE_VOL_SIZING = False
RISK_PER_TRADE_USD = 50.0

VOL_WIN = 50               # shorter vol lookback (ticks)

# slope sensitivity (per-tick change of fast-slow); helps fire when diff is growing
SLOPE_MIN = 1e-4           # tune: 1e-4 ~ “noticeable” drift on crypto last
# -----------------------------------------------------------------

@dataclass
class PositionState:
    regime: str = "FLAT"               # FLAT/LONG/SHORT
    entry_px: float | None = None
    trail_px: float | None = None
    last_flip_ts: float = 0.0
    entry_ts: float = 0.0
    confirm_ctr: int = 0
    last_signal_side: str | None = None
    last_diff: float | None = None     # for slope

class SlowChurnSMA:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.prices = collections.deque(maxlen=max(WINDOW_SLOW, VOL_WIN, BRK_WIN) + 5)
        self.state = PositionState()

    # ---------- statistic helpers ----------
    def _stats(self):
        arr = np.asarray(self.prices, dtype=float)
        n = arr.size
        if n < WINDOW_SLOW:
            return None
        fast = float(arr[-WINDOW_FAST:].mean())
        slow = float(arr[-WINDOW_SLOW:].mean())

        vol = None
        if n >= VOL_WIN + 1:
            r = np.diff(np.log(arr[-(VOL_WIN + 1):]))
            vol = float(np.std(r))

        # rolling breakout
        brk_up = brk_dn = False
        if n >= BRK_WIN:
            win = arr[-BRK_WIN:]
            cur = float(arr[-1])
            brk_up = cur >= win.max()
            brk_dn = cur <= win.min()

        return fast, slow, vol, brk_up, brk_dn

    def _band_trigger(self, fast: float, slow: float) -> str | None:
        diff = fast - slow
        band = (BAND_BPS / 1e4) * slow
        # band with margin
        if diff > band:
            return "buy"
        if diff < -band:
            return "sell"
        return None

    def _slope_bias(self, fast: float, slow: float) -> str | None:
        """Momentum confirm: if (fast-slow) slope is growing, bias toward that side."""
        diff = fast - slow
        last = self.state.last_diff
        self.state.last_diff = diff
        if last is None:
            return None
        slope = diff - last
        if slope > SLOPE_MIN:
            return "buy"
        if slope < -SLOPE_MIN:
            return "sell"
        return None

    def _qty(self, px: float, vol: float | None):
        if not USE_VOL_SIZING or vol is None or vol <= 0:
            return float(DEFAULT_QTY.get(self.symbol, 0.001))
        adverse_move = max(3.0 * vol, 1e-5)
        return float(RISK_PER_TRADE_USD / (px * adverse_move))

    def _should_cooldown(self) -> bool:
        return (time.time() - self.state.last_flip_ts) < COOLDOWN_SEC

    def _min_hold_done(self) -> bool:
        return (time.time() - self.state.entry_ts) >= MIN_HOLD_SEC

    def _check_stops(self, px: float) -> bool:
        st = self.state
        if st.regime == "FLAT" or st.entry_px is None:
            return False
        if st.regime == "LONG":
            if px <= st.entry_px * (1.0 - STOP_LOSS_PCT):
                return True
            if st.trail_px is None:
                st.trail_px = st.entry_px * (1.0 + TRAIL_PCT)
            else:
                st.trail_px = max(st.trail_px, px * (1.0 - TRAIL_PCT))
            if px <= st.trail_px and self._min_hold_done():
                return True
        else:  # SHORT
            if px >= st.entry_px * (1.0 + STOP_LOSS_PCT):
                return True
            if st.trail_px is None:
                st.trail_px = st.entry_px * (1.0 - TRAIL_PCT)
            else:
                st.trail_px = min(st.trail_px, px * (1.0 + TRAIL_PCT))
            if px >= st.trail_px and self._min_hold_done():
                return True
        return False

    # ---------- main event ----------
    async def on_tick(self, tick: Tick):
        if tick.symbol != self.symbol:
            return
        px = float(tick.price)
        self.prices.append(px)

        s = self._stats()
        if s is None:
            return
        fast, slow, vol, brk_up, brk_dn = s

        # Exit logic first (stops/trailed)
        if self._check_stops(px):
            await self._flatten(tick, reason="stop/trailed")
            return

        # Core band signal + confirmations
        side_band = self._band_trigger(fast, slow)
        side_slope = self._slope_bias(fast, slow)  # optional bias
        side = side_band or side_slope

        # Micro-breakout can reinforce marginal signals near the band
        if not side_band:
            if brk_up:
                side = side or "buy"
            elif brk_dn:
                side = side or "sell"

        # Confirmation counter
        if side == self.state.last_signal_side and side is not None:
            self.state.confirm_ctr += 1
        else:
            self.state.confirm_ctr = 1 if side else 0
            self.state.last_signal_side = side

        # Use *actual* position from shared state to prevent drift
        shared = STORE.read()
        cur_qty = float(shared.positions.get(self.symbol, 0.0))

        # State transitions
        if self.state.regime == "FLAT":
            if side and not self._should_cooldown() and self.state.confirm_ctr >= CONFIRM_TICKS:
                q = self._qty(px, vol)
                if side == "sell":
                    q = -q
                await self._enter(tick, q, reason=f"SMA({WINDOW_FAST}/{WINDOW_SLOW}) band={BAND_BPS}bps")
        elif self.state.regime == "LONG":
            # exit only on genuine reversal into sell side + min hold
            if side == "sell" and self.state.confirm_ctr >= CONFIRM_TICKS and self._min_hold_done():
                await self._flatten(tick, reason="reversal")
        elif self.state.regime == "SHORT":
            if side == "buy" and self.state.confirm_ctr >= CONFIRM_TICKS and self._min_hold_done():
                await self._flatten(tick, reason="reversal")

    # ---------- order plumbing ----------
    async def _enter(self, tick: Tick, qty: float, reason: str):
        side = "buy" if qty > 0 else "sell"
        sig = Signal(ts=tick.ts, symbol=self.symbol, side=side, size=abs(qty), reason=reason)
        await BUS.publish("signal", sig)

        self.state.regime = "LONG" if qty > 0 else "SHORT"
        self.state.entry_px = float(tick.price)
        self.state.trail_px = None
        self.state.entry_ts = time.time()

    async def _flatten(self, tick: Tick, reason: str):
        shared = STORE.read()
        cur = float(shared.positions.get(self.symbol, 0.0))
        if abs(cur) < 1e-12:
            self._reset_to_flat()
            return
        side = "sell" if cur > 0 else "buy"
        sig = Signal(ts=tick.ts, symbol=self.symbol, side=side, size=abs(cur), reason=f"exit:{reason}")
        await BUS.publish("signal", sig)
        self._reset_to_flat()

    def _reset_to_flat(self):
        self.state.regime = "FLAT"
        self.state.entry_px = None
        self.state.trail_px = None
        self.state.last_flip_ts = time.time()
        self.state.entry_ts = 0.0
        self.state.confirm_ctr = 0
        self.state.last_signal_side = None
        self.state.last_diff = None


async def run_strategy(symbols: list[str]):
    q = asyncio.Queue()
    BUS.subscribe("tick", q)
    engines = {s: SlowChurnSMA(s) for s in symbols}
    while True:
        tick = await q.get()
        eng = engines.get(tick.symbol)
        if eng:
            await eng.on_tick(tick)
