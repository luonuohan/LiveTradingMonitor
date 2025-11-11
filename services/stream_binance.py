# services/stream_binance.py
import asyncio, json, websockets
from datetime import datetime, timezone
from core.bus import BUS
from core.schemas import Tick
from core.config import CFG

# Try US endpoint first; fall back to .com if someone is outside US
ENDPOINTS = [
    f"wss://stream.binance.us:9443/ws/{CFG.CRYPTO_SYMBOL}@trade",
    f"wss://stream.binance.com:9443/ws/{CFG.CRYPTO_SYMBOL}@trade",
]

async def run_binance_stream():
    while True:
        for uri in ENDPOINTS:
            try:
                async with websockets.connect(uri, ping_interval=15) as ws:
                    print("[binance] connected to", uri)
                    first = True
                    while True:
                        msg = await ws.recv()
                        d = json.loads(msg)
                        if first:
                            print("[binance] first trade price:", d.get("p"), "ts:", d.get("T"))
                            first = False
                        tick = Tick(
                            ts = datetime.fromtimestamp(d["T"]/1000, tz=timezone.utc),
                            symbol = CFG.CRYPTO_SYMBOL.upper(),
                            price = float(d["p"]),
                            size = float(d["q"]),
                            src = "binance.us" if "binance.us" in uri else "binance"
                        )
                        await BUS.publish("tick", tick)
            except Exception as e:
                print(f"[binance] reconnect after error on {uri}:", e)
                await asyncio.sleep(2)
