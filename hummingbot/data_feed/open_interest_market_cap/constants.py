from hummingbot.core.api_throttler.data_types import RateLimit

TIMEOUT = 5
INTERVAL_TO_DURATION_MS = {
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "2h": 2 * 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "6h": 6 * 60 * 60 * 1000,
    "12h": 12 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}

# Binance Constants
BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com"
BINANCE_OPEN_INTEREST_ENDPOINT = "/fapi/v1/openInterest"
BINANCE_HISTORICAL_OPEN_INTEREST_ENDPOINT = "/futures/data/openInterestHist"
BINANCE_RATE_LIMIT_ID = "binance_open_interest_rate_limit"
BINANCE_HISTORICAL_OI_INTERVALS = ["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"]
BINANCE_HISTORICAL_OI_COUNT_LIMIT = 500

# CoinGecko Constants
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
COINGECKO_MARKETS_ENDPOINT = "/coins/markets"
COINGECKO_RATE_LIMIT_ID = "coingecko_rate_limit"

# CoinCap Constants
COINCAP_BASE_URL = "https://api.coincap.io/v2"
COINCAP_ASSETS_ENDPOINT = "/assets"
COINCAP_RATE_LIMIT_ID = "COINCAP_API"

# Glassnode Constants
GLASSNODE_BASE_URL = "https://api.glassnode.com/v1"
GLASSNODE_SUPPLY_ENDPOINT = "/metrics/supply/current"
GLASSNODE_RATE_LIMIT_ID = "glassnode_rate_limit"
GLASSNODE_TOKEN_ID_MAP = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
}

# Fallback for Token BTC token supply
FALLBACK_TOKEN_SUPPLY: dict[str, float] = {
    "bitcoin": 19844328.0,
}

BINANCE_RATE_LIMITS = [RateLimit(BINANCE_RATE_LIMIT_ID, limit=20, time_interval=60)]
COINGECKO_RATE_LIMITS = [RateLimit(COINGECKO_RATE_LIMIT_ID, limit=10, time_interval=60)]
COINCAP_RATE_LIMITS = [RateLimit(COINCAP_RATE_LIMIT_ID, limit=200, time_interval=60)]
GLASSNODE_RATE_LIMITS = [RateLimit(GLASSNODE_RATE_LIMIT_ID, limit=20, time_interval=60)]
