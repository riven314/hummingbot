from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


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
    update_interval: float = Field(default=60.0)
