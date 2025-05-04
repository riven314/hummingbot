from typing import Literal, Set

# --- Exchange API ---
BINANCE_PERPETUAL_BASE_URL: str = "https://fapi.binance.com"
BINANCE_FUNDING_RATE_ENDPOINT: str = "/fapi/v1/fundingRate"
BINANCE_FUNDING_RATE_COUNT_LIMIT = 1000

# --- Rate Limits ---
BINANCE_FUNDING_RATE_LIMIT_ID: str = "BinanceFundingRate"
BINANCE_REQUESTS_PER_MINUTE_LIMIT: int = 20
ONE_MINUTE: int = 60

# --- Intervals ---
IntervalType = Literal["8h", "4h", "1h"]
FUNDING_RATE_INTERVALS: Set[IntervalType] = {"8h", "4h", "1h"}

# --- Defaults ---
DEFAULT_TIMEOUT: float = 10.0
