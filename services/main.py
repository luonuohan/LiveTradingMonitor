# services/main.py
import asyncio, logging
from core.config import CFG
from services.stream_binance import run_binance_stream
from services.stream_alpaca import run_alpaca_stream
from services.stream_coinbase import run_coinbase_stream
#from services.strategy import run_strategy
from services.strategy_stateful import run_strategy     # new
#from services.strategy_rigorous import run_strategy as run_strategy_rigorous
from services.execution_with_costs import run_execution   # <-- use the simulated executor
from services.risk import run_risk
from services.monitor import run_monitor




logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

async def main():
    mode = (CFG.MODE or "coinbase").lower()
    logging.info("Booting services (MODE=%s)", mode)

    tasks: list[asyncio.Task] = []

    if mode == "coinbase":
        symbols = ["BTC-USD", "ETH-USD"]
        tasks += [
            asyncio.create_task(run_coinbase_stream(symbols)),  # ticks
            #asyncio.create_task(run_strategy_rigorous(symbols)),         # signals
            asyncio.create_task(run_strategy(symbols)),
            asyncio.create_task(run_execution()),           # sim fills + PnL
            asyncio.create_task(run_risk()),                    # corr/beta/var
            asyncio.create_task(run_monitor()),                 # optional health
        ]

    elif mode == "alpaca":
        # If your alpaca streamer can take symbols, pass them; else hardcode inside it
        symbols = ["AAPL", "NVDA", "SPY"]
        tasks += [
            asyncio.create_task(run_alpaca_stream()),           # ticks from Alpaca
            asyncio.create_task(run_strategy(symbols)),
            asyncio.create_task(run_execution_sim()),           # or real executor if you have one
            asyncio.create_task(run_risk()),
            asyncio.create_task(run_monitor()),
        ]

    elif mode == "binance":
        symbols = ["BTCUSDT"]
        tasks += [
            asyncio.create_task(run_binance_stream()),          # ticks from Binance
            asyncio.create_task(run_strategy(symbols)),
            asyncio.create_task(run_execution_sim()),
            asyncio.create_task(run_risk()),
            asyncio.create_task(run_monitor()),
        ]

    else:
        raise RuntimeError(f"Unknown MODE={CFG.MODE!r}")

    try:
        await asyncio.gather(*tasks)
    except Exception:
        logging.exception("Fatal exception; cancelling tasks…")
        for t in tasks:
            if not t.cancelled():
                t.cancel()
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[main] interrupted")
