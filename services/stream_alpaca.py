import asyncio, json, websockets, base64, os
from datetime import datetime, timezone
from core.bus import BUS
from core.schemas import Tick
from core.config import CFG

async def run_alpaca_stream():
    headers = {"Content-Type": "application/json"}
    auth = {"action":"auth","key":CFG.ALPACA_API_KEY,"secret":CFG.ALPACA_API_SECRET}
    subs = {"action":"subscribe","trades":CFG.EQUITY_SYMBOLS}

    while True:
        try:
            async with websockets.connect(CFG.ALPACA_DATA_WS, ping_interval=15, extra_headers=headers) as ws:
                await ws.send(json.dumps(auth))
                await ws.send(json.dumps(subs))
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if isinstance(data, dict) and data.get("msg") in ("connected","authenticated"):
                        continue
                    for evt in data:
                        if evt.get("T") == "t":  # trade event
                            tick = Tick(
                                ts = datetime.fromtimestamp(evt["t"]/1e9, tz=timezone.utc),
                                symbol = evt["S"],
                                price = float(evt["p"]),
                                size = float(evt["s"]),
                                src = "alpaca"
                            )
                            await BUS.publish("tick", tick)
        except Exception:
            await asyncio.sleep(2)
