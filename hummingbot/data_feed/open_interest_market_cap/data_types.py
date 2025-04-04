from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel

IntervalType = Literal["1m", "10m", "15m", "30m", "1h", "1d"]


class OpenInterestMarketCapConfig(BaseModel):
    trading_pair: str
    interval: IntervalType


class OpenInterestData(BaseModel):
    provider: str
    symbol: str
    open_interest: float
    timestamp: int
    requested_at: datetime

    @property
    def last_updated(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp / 1000, tz=timezone.utc)


class LiveTokenSupplyData(BaseModel):
    provider: str
    token_id: str
    total_supply: Optional[float] = None
    market_cap: Optional[float] = None
    last_updated: Optional[datetime] = None
    requested_at: datetime


class HistoricalTokenSupplyData(BaseModel):
    provider: str
    token_id: str
    total_supply: float
    timestamp: int
    requested_at: datetime


class OpenInterestMarketCapRecord(BaseModel):
    open_interest_provider: str
    token_supply_provider: str
    symbol: str
    open_interest: float
    token_supply: float
    timestamp: int
    requested_at: datetime
    # whether OI is forward filled because of missing/ problematic value
    is_open_interest_estimated: bool = False
    # there are 2 scenarios:
    # 1. whether token supply is forward filled because of missing/ problematic value
    # 2. whether tokens supply is estimated based on interpolation (e.g. use daily CoinGecko data to interlate hourly data)
    is_token_supply_estimated: bool = False
