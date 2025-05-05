from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, validator

from hummingbot.data_feed.funding_rate.constants import FUNDING_RATE_INTERVALS, IntervalType
from hummingbot.data_feed.funding_rate.utils.time_utils import TimeUtility


class TimestampValidatorMixin:
    @validator("funding_time", "aligned_funding_time")
    def check_valid_timestamp_ms(cls, v: Any) -> Any:
        if not TimeUtility.is_valid_timestamp_ms(v):
            raise ValueError(f"Invalid millisecond timestamp: {v}")
        return v


class SymbolValidatorMixin:
    @validator("symbol", "trading_pair")
    def validate_symbol(cls, symbol: str) -> str:
        if not symbol:
            raise ValueError("Symbol cannot be empty")
        if "-" in symbol:
            raise ValueError("Symbol cannot contain '-', use format like 'BTCUSDT' instead of 'BTC-USDT'")
        return symbol


class FundingRateConfig(BaseModel, SymbolValidatorMixin):
    trading_pair: str
    interval: IntervalType
    window: int

    @validator("interval", pre=True)
    def check_interval(cls, v: str) -> str:
        if v not in FUNDING_RATE_INTERVALS:
            raise ValueError(f"Interval must be one of {FUNDING_RATE_INTERVALS}")
        return v

    @validator("window", pre=True)
    def check_window(cls, v: int) -> int:
        if v <= 1:
            raise ValueError("Window size must be greater than 1")
        return v


class FundingRateRecord(BaseModel, TimestampValidatorMixin, SymbolValidatorMixin):
    exchange: str
    symbol: str
    # funding time may be milliseconds difference from the aligned funding time
    funding_time: int
    aligned_funding_time: int
    funding_rate: float
    mark_price: float
    zscore: Optional[float] = None
    requested_at: datetime
    is_estimated: bool = False

    @property
    def funding_at(self) -> datetime:
        return TimeUtility.ms_to_datetime(self.funding_time)

    @property
    def aligned_funding_at(self) -> datetime:
        return TimeUtility.ms_to_datetime(self.aligned_funding_time)
