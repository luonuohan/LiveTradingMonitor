# services/stream_coinbase.py
import asyncio, json, websockets
from datetime import datetime, timezone
from typing import Sequence
from core.bus import BUS
from core.schemas import Tick
from core.state import update_last_price, append_intraday

URI = "wss://advanced-trade-ws.coinbase.com"

async def run_coinbase_stream(products: Sequence[str] | None = None):
    """
    Stream Coinbase Advanced Trade 'market_trades' and publish ticks to the BUS.
    Also persist last prices & a latency proxy into storage/state.json for the UI.
    """
    if not products:
        products = ["BTC-USD"]
    if isinstance(products, str):          # <— add this guard
        products = [products]
    sub = {
        "type": "subscribe",
        "product_ids": list(products),
        "channel": "market_trades",
    }

    backoff = 1
    trade_count = 0

    while True:
        try:
            async with websockets.connect(URI, ping_interval=15) as ws:
                print("[coinbase] connected; subscribing to", ",".join(products))
                await ws.send(json.dumps(sub))
                first = True
                backoff = 1  # reset backoff on successful connect

                while True:
                    raw = await ws.recv()
                    msg = json.loads(raw)

                    if msg.get("channel") != "market_trades":
                        # ignore other control messages
                        continue

                    for ev in msg.get("events", []):
                        for t in ev.get("trades", []):
                            symbol = t["product_id"]  # e.g., "BTC-USD"
                            px = float(t["price"])
                            sz = float(t["size"])
                            # Coinbase time is ISO8601 with Z
                            ts = datetime.fromisoformat(t["time"].replace("Z", "+00:00"))
                            ts_ms = int(ts.timestamp() * 1000)  

                            if first:
                                print(f"[coinbase] first trade {symbol} {px} @ {ts.isoformat()}")
                                first = False


                            

                            # publish into the in-process bus (for strategy/execution/risk)
                            await BUS.publish(
                                "tick",
                                Tick(ts=ts, symbol=symbol, price=px, size=sz, src="coinbase"),
                            )

                            # persist for the Streamlit UI (last price + latency proxy)
                            # latency proxy = now - exchange timestamp (ms)
                            now = datetime.now(timezone.utc)
                            latency_ms = max(0, int((now - ts).total_seconds() * 1000))
                            update_last_price(symbol, px, latency_ms=latency_ms)
                            append_intraday(symbol, ts_ms, px)   # <-- add this line

                            trade_count += 1
                            if trade_count % 50 == 0:
                                print(f"[coinbase] {trade_count} trades; last {symbol}={px} (lat {latency_ms}ms)")

        except Exception as e:
            print("[coinbase] reconnect after error:", repr(e))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)  # capped exponential backoff
