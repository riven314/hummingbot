from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, validator

from hummingbot.data_feed.funding_rate.constants import (
    FUNDING_RATE_INTERVALS,
    FundingRateIntervalType,
    TradingIntervalType,
)
from hummingbot.data_feed.funding_rate.utils.time_utils import TimeUtility


class TimestampValidatorMixin:
    @validator("funding_time", "aligned_funding_time", "start_time", allow_reuse=True)
    def check_valid_timestamp_ms(cls, v: Any) -> Any:
        if not TimeUtility.is_valid_timestamp_ms(v):
            raise ValueError(f"Invalid millisecond timestamp: {v}")
        return v


class SymbolValidatorMixin:
    @validator("symbol", "trading_pair", allow_reuse=True)
    def validate_symbol(cls, symbol: str) -> str:
        if not symbol:
            raise ValueError("Symbol cannot be empty")
        if "-" in symbol:
            raise ValueError("Symbol cannot contain '-', use format like 'BTCUSDT' instead of 'BTC-USDT'")
        return symbol


class FundingRateConfig(BaseModel, SymbolValidatorMixin):
    # not separated by hyphen, e.g. BTCUSDT, ETHUSDT
    trading_pair: str
    update_interval: FundingRateIntervalType
    trading_interval: TradingIntervalType
    zscore_windows: list[int]

    @property
    def max_window(self) -> int:
        if not self.zscore_windows:
            return 0
        return max(self.zscore_windows)

    @validator("update_interval", pre=True)
    def check_interval(cls, v: str) -> str:
        if v not in FUNDING_RATE_INTERVALS:
            raise ValueError(f"Interval must be one of {FUNDING_RATE_INTERVALS}")
        return v

    @validator("trading_interval", pre=True)
    def check_trading_interval(cls, v: str) -> str:
        if v != "1h":
            raise ValueError("Trading interval must be '1h' at the moment")
        return v

    @validator("zscore_windows", pre=True, each_item=True)
    def check_zscore_windows(cls, v: int) -> int:
        if v <= 1:
            raise ValueError("Each zscore window size must be greater than 1")
        return v


class FundingRateRecord(BaseModel, TimestampValidatorMixin, SymbolValidatorMixin):
    exchange: str
    symbol: str
    # funding time may be milliseconds difference from the aligned funding time
    funding_time: int
    aligned_funding_time: int
    funding_rate: float
    mark_price: float
    requested_at: datetime
    is_estimated: bool = False

    @property
    def funding_at(self) -> datetime:
        return TimeUtility.ms_to_datetime(self.funding_time)

    @property
    def aligned_funding_at(self) -> datetime:
        return TimeUtility.ms_to_datetime(self.aligned_funding_time)


class FundingRateInterval(FundingRateRecord):
    start_time: int
    zscores: Optional[dict[str, float]] = None
