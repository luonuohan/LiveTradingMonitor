# services/monitor.py
import asyncio, time
from datetime import datetime, timezone

from core.bus import BUS
from core.state import append_intraday, update_last_price

class Health:
    def __init__(self):
        self.last_tick_ts = time.time()
        self.alerts = []
        self.latency_ms = 0.0

HEALTH = Health()

async def run_monitor():
    """
    Listens to 'tick' events, computes latency, updates last price,
    and appends a price point to the 10-minute intraday ring per symbol.
    """
    q = asyncio.Queue()
    BUS.subscribe("tick", q)

    while True:
        try:
            evt = await asyncio.wait_for(q.get(), timeout=5.0)

            # --- latency calculation (robust to missing tz) ---
            if hasattr(evt, "ts") and isinstance(evt.ts, datetime):
                ts = evt.ts
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                HEALTH.latency_ms = max(0.0, (now - ts).total_seconds() * 1000.0)
            else:
                # fallback: use wall clock if evt.ts is missing
                HEALTH.latency_ms = 0.0

            HEALTH.last_tick_ts = time.time()

            # --- persist last price (used by strategy/execution/risk) ---
            update_last_price(evt.symbol, float(evt.price), latency_ms=HEALTH.latency_ms)

            # --- persist intraday price point (10-min window trimmed in append_intraday) ---
            ts_ms = (
                int(evt.ts.replace(tzinfo=timezone.utc).timestamp() * 1000)
                if hasattr(evt, "ts") and isinstance(evt.ts, datetime)
                else int(time.time() * 1000)
            )
            append_intraday(evt.symbol, ts_ms, float(evt.price))

            # --- alert on excessive latency ---
            if HEALTH.latency_ms > 1500:
                HEALTH.alerts.append(f"High latency: {HEALTH.latency_ms:.0f} ms")

        except asyncio.TimeoutError:
            HEALTH.alerts.append("Market data stale >5s")
