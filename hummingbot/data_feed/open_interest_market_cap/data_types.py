from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel

IntervalType = Literal["1m", "10m", "15m", "30m", "1h", "1d"]


class OpenInterestData(BaseModel):
    provider: str
    symbol: str
    open_interest: float
    timestamp: int

    @property
    def last_updated(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp / 1000, tz=timezone.utc)


class TokenSupplyData(BaseModel):
    provider: str
    token_id: str
    total_supply: Optional[float] = None
    market_cap: Optional[float] = None
    last_updated: Optional[datetime] = None


class OpenInterestMarketCapConfig(BaseModel):
    trading_pair: str
    interval: IntervalType
