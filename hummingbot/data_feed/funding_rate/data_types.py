import re
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field, validator

from hummingbot.data_feed.funding_rate.constants import FUNDING_RATE_INTERVALS, IntervalType
from hummingbot.data_feed.funding_rate.utils.time_utils import TimeUtility


class TimestampValidatorMixin(BaseModel):
    @validator("*", pre=True, allow_reuse=True)
    def check_valid_timestamp_ms(cls, v: Any, field: Field) -> Any:
        if "time" in field.name.lower() and isinstance(v, (int, float)):
            if not TimeUtility.is_valid_timestamp_ms(v):
                raise ValueError(f"Invalid millisecond timestamp for {field.name}: {v}")
        return v


class SymbolValidatorMixin(BaseModel):
    @validator("symbol", "trading_pair", pre=True, allow_reuse=True)
    def check_symbol_format(cls, v: str) -> str:
        # Expecting format like BTCUSDT (no hyphens or slashes)
        if not re.fullmatch(r"^[A-Z0-9]+$", v):
            raise ValueError(f"Invalid symbol/trading_pair format: {v}. Expected format like 'BTCUSDT'.")
        return v


class FundingRateConfig(SymbolValidatorMixin, BaseModel):
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


class FundingRateRecord(TimestampValidatorMixin, SymbolValidatorMixin, BaseModel):
    provider: str
    symbol: str
    funding_time: int
    funding_rate: float
    mark_price: float
    zscore: Optional[float] = None
    requested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def funding_at(self) -> datetime:
        return TimeUtility.ms_to_datetime(self.funding_time)
