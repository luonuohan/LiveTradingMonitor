# services/execution_with_costs.py
import asyncio, time, math
from datetime import datetime, timezone

from core.bus import BUS
from core.schemas import Signal, OrderAck
from core.state import STORE, SharedState, append_order   # add append_order import


# ------- venue / symbol cost model (tune as you like) -------
FEE_BPS = {            # taker commission (bps of notional)
    "BTC-USD": 4.0,    # 4 bps
    "ETH-USD": 5.0,
    "*": 5.0,
}
SPREAD_BPS = {         # typical full spread (bps); we charge half per side
    "BTC-USD": 2.0,    # 2 bps
    "ETH-USD": 3.0,
    "*": 3.0,
}
BASE_SLIP_BPS = {      # baseline market impact/slippage (bps)
    "BTC-USD": 1.0,
    "ETH-USD": 1.5,
    "*": 1.0,
}
VOL_SLIP_K = 50.0      # additional slip bps per 1% recent vol (heuristic)
MIN_FEE = 0.01         # minimum commission dollars per fill

# Keep an internal book of average cost to compute realized P&L properly
_positions = {}   # symbol -> qty
_avg_cost = {}    # symbol -> $ average entry price

_last_price = {}  # updated by tick stream listener below

def _bps(sym: str, table: dict[str, float]) -> float:
    return table.get(sym, table["*"])

def _slippage_bps(sym: str) -> float:
    """Baseline slippage + a bump for recent volatility (approx from last 30 returns in state if present)."""
    st = STORE.read()
    # crude volatility proxy from last 60 seconds of UI sparkline not stored backend;
    # as a backend approximation use 0.5% if nothing is known.
    vol_pct = 0.5  # %
    base = BASE_SLIP_BPS.get(sym, BASE_SLIP_BPS["*"])
    return base + VOL_SLIP_K * (vol_pct / 100.0)

def _cost_components(sym: str, px: float, qty: float) -> tuple[float, float, float, float]:
    """
    Returns (commission$, half_spread$, slippage$, total$).
    qty is signed in asset units (e.g., BTC).
    """
    notional = abs(qty) * px
    fee = max(MIN_FEE, notional * (_bps(sym, FEE_BPS) / 1e4))
    half_spread = notional * (_bps(sym, SPREAD_BPS) / 1e4) * 0.5
    slip = notional * (_slippage_bps(sym) / 1e4)
    total = fee + half_spread + slip
    return fee, half_spread, slip, total

def _apply_fill(sym: str, fill_px: float, qty: float) -> float:
    """
    Update average cost and realized P&L for a signed quantity at fill_px.
    Returns realized P&L (positive = profit).
    """
    q0 = _positions.get(sym, 0.0)
    ac = _avg_cost.get(sym, 0.0)
    q1 = q0 + qty
    realized = 0.0

    if q0 == 0 or (q0 > 0 and q1 > 0) or (q0 < 0 and q1 < 0):
        # adding to an existing position or opening new: weighted-average price
        new_notional = ac * q0 + fill_px * qty
        _avg_cost[sym] = new_notional / (q1 if q1 != 0 else 1e-12)
    else:
        # reducing/closing/flip side: realized P&L on the closed portion
        close_qty = -qty if abs(q1) < abs(q0) else abs(q0)  # quantity actually closed
        if q0 > 0:  # closing part of a long
            realized = (fill_px - ac) * close_qty
        else:       # closing part of a short
            realized = (ac - fill_px) * close_qty

        # handle potential flip: remaining qty sets new avg cost = fill price
        if q1 == 0:
            _avg_cost[sym] = 0.0
        elif (q0 > 0 and q1 < 0) or (q0 < 0 and q1 > 0):
            _avg_cost[sym] = fill_px

    _positions[sym] = q1
    return realized

async def _tick_updater():
    q = asyncio.Queue()
    BUS.subscribe("tick", q)
    while True:
        t = await q.get()
        _last_price[t.symbol] = float(t.price)

async def run_execution():   # <-- call this from main()
    # ensure we keep last prices current
    asyncio.create_task(_tick_updater())

    q_sig = asyncio.Queue()
    BUS.subscribe("signal", q_sig)

    while True:
        sig: Signal = await q_sig.get()
        sym = sig.symbol
        px  = _last_price.get(sym)
        if px is None:
            # no price yet; ignore the signal
            continue

        # Direction: +1 for buy, -1 for sell
        d = 1.0 if sig.side == "buy" else -1.0

        # Model a realistic fill: cross half-spread + slippage against us
        # fill_px worse than mid/last in the direction of the trade
        worst_bps = (_bps(sym, SPREAD_BPS) * 0.5) + _slippage_bps(sym)
        fill_px = px * (1.0 + d * (worst_bps / 1e4))

        # Compute cost components in USD
        fee, half_spread, slip, total_cost = _cost_components(sym, fill_px, sig.size)

        # Book realized P&L from the trade, then subtract costs
        trade_realized = _apply_fill(sym, fill_px, d * sig.size)
        trade_realized_after_costs = trade_realized - total_cost

        # Update shared state atomically
        st: SharedState = STORE.read()
        st.positions[sym] = _positions.get(sym, 0.0)
        st.realized_pnl += trade_realized_after_costs
        # keep a running total of transaction costs
        current_tc = getattr(st, "transaction_costs", 0.0) or 0.0
        st.transaction_costs = float(current_tc + total_cost)
        st.ts_ms = int(time.time() * 1000)
        STORE.write(st)

        # publish an acknowledgement for the monitor/logs
        note = f"fee={fee:.2f},½spr={half_spread:.2f},slip={slip:.2f},cost={total_cost:.2f}"
        ack = OrderAck(
            ts=datetime.now(timezone.utc),
            symbol=sym,
            side=sig.side,
            size=sig.size,
            client_id=f"sim-{int(time.time())}",
            status="simulated",
            price=float(fill_px),
            venue="sim",
            note=note,
        )
        await BUS.publish("order_ack", ack)
        # NEW: persist for UI "Recent Orders" table
        append_order(ack)
