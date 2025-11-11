# services/sim_feed.py
import time, math, random
from datetime import datetime, timezone
from core.state import update_last_price, append_intraday

SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD", "XRP-USD", "ADA-USD", "DOGE-USD"]

# starting prices
PX = {
    "BTC-USD": 68000.0,
    "ETH-USD": 3500.0,
    "SOL-USD": 200.0,
    "AVAX-USD": 40.0,
    "XRP-USD": 0.55,
    "ADA-USD": 0.42,
    "DOGE-USD": 0.18,
}

def now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def step_price(px: float, vol_bps: float = 5.0) -> float:
    # random walk in bps with slight mean reversion
    shock = random.gauss(0, vol_bps) / 1e4
    drift = -0.15 * shock
    return max(0.0001, px * (1.0 + shock + drift))

def main():
    print("[sim_feed] starting… (Ctrl-C to stop)")
    while True:
        t = now_ms()
        for s in SYMBOLS:
            PX[s] = step_price(PX[s])
            update_last_price(s, PX[s], latency_ms=5.0)
            append_intraday(s, t, PX[s])
        time.sleep(0.5)  # 2 Hz per symbol

if __name__ == "__main__":
    main()
