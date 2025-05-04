from typing import Literal, Set

from hummingbot.core.api_throttler.data_types import RateLimit

# --- Exchange API ---
BINANCE_PERPETUAL_BASE_URL: str = "https://fapi.binance.com"
BINANCE_FUNDING_RATE_ENDPOINT: str = "/fapi/v1/fundingRate"
BINANCE_FUNDING_RATE_COUNT_LIMIT = 1000

# --- Rate Limits ---
BINANCE_FUNDING_RATE_LIMIT_ID: str = "BinanceFundingRate"
ONE_MINUTE: int = 60
BINANCE_FUNDING_RATE_RATE_LIMITS = [RateLimit(BINANCE_FUNDING_RATE_LIMIT_ID, limit=20, time_interval=ONE_MINUTE)]

# --- Intervals ---
IntervalType = Literal["8h", "4h", "1h"]
FUNDING_RATE_INTERVALS: Set[IntervalType] = {"8h", "4h", "1h"}

# --- Defaults ---
DEFAULT_TIMEOUT: float = 10.0
