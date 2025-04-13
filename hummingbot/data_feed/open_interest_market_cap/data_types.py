from datetime import datetime
from typing import Any, ClassVar, Literal, Optional, Set

from pydantic import BaseModel, validator

from hummingbot.data_feed.open_interest_market_cap.utils.time_utils import TimeUtility

IntervalType = Literal["1m", "5m", "10m", "15m", "30m", "1h", "1d"]


class TimestampValidatorMixin:
    timestamp_fields: ClassVar[Set[str]]

    @validator("*")
    @classmethod
    def validate_millisecond_timestamp(cls, value: Optional[int], field: Any) -> Optional[int]:
        if field.name not in cls.timestamp_fields or value is None:
            return value
        if not TimeUtility.is_valid_timestamp_ms(value):
            raise ValueError(f"{field.name} must be in milliseconds between 2000-01-01 and 2100-01-01")
        return value


class OpenInterestMarketCapConfig(BaseModel):
    trading_pair: str
    interval: IntervalType
    window: int


class LiveOpenInterestData(BaseModel, TimestampValidatorMixin):
    provider: str
    symbol: str
    open_interest: float
    # last updated timestamp from live OI endpoint
    timestamp: int  # ms
    requested_at: datetime

    timestamp_fields = {"timestamp"}

    @property
    def recorded_at(self) -> datetime:
        return TimeUtility.ms_to_datetime(self.timestamp)


class HistoricalOpenInterestData(LiveOpenInterestData, TimestampValidatorMixin):
    provider: str
    symbol: str
    open_interest: float
    # either opening time if live data is interval based, or last updated timestamp if live data
    timestamp: int
    requested_at: datetime
    # whether OI is forward filled because of missing/ problematic value
    is_estimated: bool = False

    timestamp_fields = {"timestamp"}

    @property
    def recorded_at(self) -> datetime:
        return TimeUtility.ms_to_datetime(self.timestamp)


class LiveTokenSupplyData(BaseModel, TimestampValidatorMixin):
    provider: str
    token_id: str
    total_supply: Optional[float] = None
    market_cap: Optional[float] = None
    # last updated timestamp/ end timestamp of interval
    timestamp: Optional[int] = None  # ms
    requested_at: datetime

    timestamp_fields = {"timestamp"}

    @property
    def recorded_at(self) -> Optional[datetime]:
        if self.timestamp is None:
            return None
        return TimeUtility.ms_to_datetime(self.timestamp)


class HistoricalTokenSupplyData(BaseModel, TimestampValidatorMixin):
    provider: str
    token_id: str
    total_supply: float
    # start timestamp of interval
    timestamp: int
    requested_at: datetime
    is_estimated: bool = False

    timestamp_fields = {"timestamp"}

    @property
    def recorded_at(self) -> datetime:
        return TimeUtility.ms_to_datetime(self.timestamp)


class OpenInterestMarketCapRecord(BaseModel, TimestampValidatorMixin):
    open_interest_provider: str
    token_supply_provider: str
    symbol: str
    open_interest: float
    token_supply: float
    zscore: Optional[float]
    is_open_interest_estimated: bool = False
    is_token_supply_estimated: bool = False
    timestamp: int
    requested_at: datetime

    timestamp_fields = {"timestamp"}

    # no need to use price because it cancels out on denominator and numerator
    @property
    def oi_mcap_ratio(self) -> float:
        return self.open_interest / self.token_supply
