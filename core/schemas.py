from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class Tick(BaseModel):
    ts: datetime
    symbol: str
    price: float
    size: Optional[float] = None
    src: str  # "binance" or "alpaca"

class Signal(BaseModel):
    ts: datetime
    symbol: str
    side: str   # "buy" or "sell"
    size: float
    reason: str

class OrderAck(BaseModel):
    ts: datetime
    symbol: str
    side: str
    size: float
    client_id: str
    status: str  # "accepted","filled","rejected","canceled","simulated"
    price: Optional[float] = None
    venue: str
    note: Optional[str] = None
