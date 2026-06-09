from dataclasses import dataclass

@dataclass
class BookSnapshot:
    timestamp: float          # exchange timestamp, seconds
    local_timestamp: float    # your collector's wall clock
    market: str
    asset_id: str
    bids: list[tuple[float, float]]   # sorted desc
    asks: list[tuple[float, float]]   # sorted asc
    tick_size: float

@dataclass
class PriceChange:
    timestamp: float
    local_timestamp: float
    market: str
    asset_id: str
    price: float
    size: float               # NEW size at this level, not delta
    side: str                 # "BUY" or "SELL"
    best_bid: float
    best_ask: float

@dataclass
class Trade:
    timestamp: float
    local_timestamp: float
    market: str
    price: float

# Top-level event type
Event = Union[BookSnapshot, PriceChange, Trade]