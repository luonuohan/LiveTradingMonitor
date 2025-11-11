# services/execution.py
import asyncio, time
from datetime import datetime, timezone
from typing import Dict

from core.config import CFG
from core.bus import BUS
from core.schemas import Signal, OrderAck, Tick
from core.state import update_position, STORE, SharedState

# Track last prices for fill/marking
_last_price: Dict[str, float] = {}

# Simple average-cost book: symbol -> {"qty": float, "avg_px": float}
_BOOK: Dict[str, Dict[str, float]] = {}

def _apply_fill(symbol: str, side: str, qty: float, px: float) -> float:
    """
    Update avg-cost book with a new fill.
    Returns realized P&L from the portion that closes.
    """
    pos = _BOOK.get(symbol, {"qty": 0.0, "avg_px": 0.0})
    q0, ap = float(pos["qty"]), float(pos["avg_px"])

    q = qty if side == "buy" else -qty
    realized = 0.0

    # closing part if direction differs
    if q0 != 0.0 and (q0 > 0) != (q > 0):
        close_qty = min(abs(q0), abs(q))
        # PnL is (fill - avg) * signed closed qty
        realized += (px - ap) * (close_qty if q0 > 0 else -close_qty)

    q1 = q0 + q
    if q1 == 0.0:
        ap1 = 0.0
    elif (q0 > 0) == (q1 > 0):
        # same direction: weighted average
        ap1 = (ap * abs(q0) + px * abs(q)) / abs(q1)
    else:
        # crossed and flipped: new avg at fill price
        ap1 = px

    _BOOK[symbol] = {"qty": q1, "avg_px": ap1}
    return realized

def _mark_unrealized(last_prices: Dict[str, float]) -> float:
    unrl = 0.0
    for s, pos in _BOOK.items():
        lp = last_prices.get(s)
        if lp is None:  # no mark yet
            continue
        unrl += (lp - pos["avg_px"]) * pos["qty"]
    return unrl

async def run_execution():
    """
    Listens to:
      - 'tick'   -> keep last prices updated
      - 'signal' -> simulate market fill at last price, update book/state
    """
    q_tick = asyncio.Queue()
    q_sig  = asyncio.Queue()
    BUS.subscribe("tick", q_tick)
    BUS.subscribe("signal", q_sig)

    async def tick_updater():
        while True:
            t: Tick = await q_tick.get()
            _last_price[t.symbol] = float(t.price)

    asyncio.create_task(tick_updater())

    while True:
        sig: Signal = await q_sig.get()

        px = _last_price.get(sig.symbol)
        if px is None:
            # if first signal arrives before first tick, just skip updating book price
            px = 0.0

        realized = _apply_fill(sig.symbol, sig.side, float(sig.size), px)

        # write positions & PnL to shared state (this is what the UI reads)
        mult = 1.0 if sig.side == "buy" else -1.0
        update_position(sig.symbol, mult * float(sig.size), realized_pnl=realized)

        # recompute unrealized from current book and marks
        st: SharedState = STORE.read()
        st.unrealized_pnl = _mark_unrealized(st.last_prices)
        STORE.write(st)

        ack = OrderAck(
            ts=datetime.now(timezone.utc),
            symbol=sig.symbol,
            side=sig.side,
            size=sig.size,
            price=px,
            status="filled",          # simulated instant fill
            venue="sim",
            client_id=f"sim-{int(time.time())}",
            note=f"avg_px={_BOOK[sig.symbol]['avg_px']:.2f}, realized={realized:.2f}",
        )

        print("[exec] fill", sig.symbol, sig.side, float(sig.size), "@", f"{px:.2f}",
              "| avg", f"{_BOOK[sig.symbol]['avg_px']:.2f}",
              "| real", f"{realized:.2f}")
        await BUS.publish("order_ack", ack)
