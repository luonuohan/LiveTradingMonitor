# services/strategy_rigorous.py
import asyncio, time, math, numpy as np
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from core.bus import BUS
from core.schemas import Tick, Signal

SYMA = "BTC-USD"
SYMB = "ETH-USD"

RET_WIN     = 300          # rolling window
Z_ENTRY     = 2.0          # enter when |z| > 2
Z_EXIT      = 0.6          # exit when |z| < 0.6
COOLDOWN_S  = 8
MAX_HOLD_S  = 600
TARGET_USD  = 200.0        # per spread leg target notional
STOP_PCT    = 0.008        # 0.8% spread stop
TAKE_PCT    = 0.008        # 0.8% take
ROUNDTRIP_BPS = 10.0       # crude cost guard (both legs + roundtrip)
MIN_VOL     = 1e-6

@dataclass
class Book:
    last_ts: float = 0.0
    pos: int = 0       # -1 short spread (short BTC, long ETH^β), +1 long spread
    entry_spread: float | None = None

class Roll:
    def __init__(self): 
        self.p: dict[str, deque] = {SYMA: deque(maxlen=RET_WIN+1), SYMB: deque(maxlen=RET_WIN+1)}

    def add(self, s, px: float):
        self.p[s].append(float(px))

    def ready(self): 
        return all(len(d)>=RET_WIN//2 for d in self.p.values())

    def rets(self, s):
        d = np.array(self.p[s], dtype=float)
        if len(d) < 2: return None
        r = np.diff(np.log(d))
        return r

    def beta(self):
        ra, rb = self.rets(SYMA), self.rets(SYMB)
        L = min(len(ra), len(rb))
        if L < 30: return None
        y, x = ra[-L:], rb[-L:]
        X = np.vstack([np.ones(L), x]).T
        a, b = np.linalg.lstsq(X, y, rcond=None)[0]
        resid = y - (a + b*x)
        mu, sd = float(np.mean(resid)), float(np.std(resid, ddof=1))
        return float(b), resid, mu, sd

ROLL = Roll()
BOOK = Book()

def _usd_to_qty(price, usd): 
    return round(usd / max(price, 1e-9), 6)

def _edge_okay(sd_resid: float) -> bool:
    # expected half-life reversion (very crude) vs round-trip cost
    # require sd * 1.0 > costs proxy
    cost = ROUNDTRIP_BPS / 1e4
    return sd_resid > cost + MIN_VOL

async def run_strategy(symbols=None):
    # subscribe to ticks
    q = asyncio.Queue()
    BUS.subscribe("tick", q)

    # price cache for USD sizing
    last_px = {SYMA: None, SYMB: None}

    async def maybe_signal(side_spread: int, reason: str):
        """side_spread: +1 long spread (long A, short β*B), -1 short spread"""
        now = time.time()
        if now - BOOK.last_ts < COOLDOWN_S:
            return
        if last_px[SYMA] is None or last_px[SYMB] is None:
            return

        b_beta, resid, mu, sd = ROLL.beta()
        if b_beta is None or sd < MIN_VOL:
            return
        if not _edge_okay(sd):
            return

        # sizes in USD, hedge by beta
        qa = _usd_to_qty(last_px[SYMA], TARGET_USD)
        qb = _usd_to_qty(last_px[SYMB], TARGET_USD * abs(b_beta))
        # long spread: +A, -βB. short spread: -A, +βB
        if side_spread == +1:
            sa, sb = "buy", "sell"
        else:
            sa, sb = "sell", "buy"

        # fire both legs
        ts = datetime.now(timezone.utc)
        sigA = Signal(ts=ts, symbol=SYMA, side=sa, size=qa, reason=f"Pairs z>{Z_ENTRY} β={b_beta:.2f}")
        sigB = Signal(ts=ts, symbol=SYMB, side=sb, size=qb, reason=f"Pairs hedge β={b_beta:.2f}")
        await BUS.publish("signal", sigA)
        await BUS.publish("signal", sigB)
        BOOK.pos = side_spread
        BOOK.entry_spread = resid[-1] - mu
        BOOK.last_ts = now
        print("[pairs] enter", "LONG" if side_spread==1 else "SHORT", 
              f"z={((resid[-1]-mu)/max(sd,1e-9)):.2f}", f"β={b_beta:.2f}")

    while True:
        t: Tick = await q.get()
        if t.symbol not in (SYMA, SYMB):
            continue
        last_px[t.symbol] = float(t.price)
        ROLL.add(t.symbol, float(t.price))
        if not ROLL.ready():
            continue

        b_beta, resid, mu, sd = ROLL.beta()
        if b_beta is None or sd < MIN_VOL:
            continue
        z = (resid[-1] - mu) / max(sd, 1e-9)

        # exits
        if BOOK.pos != 0:
            elapsed = time.time() - BOOK.last_ts
            # stop / take on the spread (absolute, not z)
            if abs(resid[-1] - BOOK.entry_spread) > STOP_PCT:
                BOOK.pos = 0; BOOK.entry_spread = None
                BOOK.last_ts = time.time()
                # flatten both legs
                qa = _usd_to_qty(last_px[SYMA], TARGET_USD)
                qb = _usd_to_qty(last_px[SYMB], TARGET_USD * abs(b_beta))
                sa = "sell" if t.symbol==SYMA else "sell"
                # just publish a neutralizing pair opposite to entry
                opp = -1  # flip
                await maybe_signal(opp, "stop")
                continue
            if abs(z) < Z_EXIT or elapsed > MAX_HOLD_S:
                BOOK.pos = 0; BOOK.entry_spread = None
                BOOK.last_ts = time.time()
                await maybe_signal(-1 if z>0 else +1, "exit")  # neutralize with opposite legs
                continue

        # entries
        if BOOK.pos == 0:
            if z > Z_ENTRY:
                await maybe_signal(-1, "enter_short_spread")
            elif z < -Z_ENTRY:
                await maybe_signal(+1, "enter_long_spread")
