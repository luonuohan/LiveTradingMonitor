import os
from dotenv import load_dotenv
load_dotenv()

class CFG:
    MODE = os.getenv("MODE").lower()  # "alpaca" or "binance"

    # Alpaca
    ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
    ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET")
    ALPACA_BASE = os.getenv("ALPACA_BASE", "https://paper-api.alpaca.markets")
    ALPACA_DATA_WS = os.getenv("ALPACA_DATA_WS", "wss://stream.data.alpaca.markets/v2/iex")

    # Symbols
    EQUITY_SYMBOLS = ["AAPL", "NVDA", "SPY"]
    CRYPTO_SYMBOL = "btcusdt"  # binance symbol (lowercase)
