import logging
import os
from datetime import datetime, timezone
from typing import Optional

from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.data_feed.open_interest_market_cap import constants as CONSTANTS
from hummingbot.data_feed.open_interest_market_cap.constants import GLASSNODE_TOKEN_ID_MAP
from hummingbot.data_feed.open_interest_market_cap.data_types import (
    HistoricalTokenSupplyData,
    IntervalType,
    LiveTokenSupplyData,
)
from hummingbot.data_feed.open_interest_market_cap.token_supply_providers.base import (
    TokenSupplyProviderBase,
    TokenSupplyProviderError,
)
from hummingbot.data_feed.open_interest_market_cap.utils.interval_utils import IntervalUtility
from hummingbot.data_feed.open_interest_market_cap.utils.time_utils import TimeUtility
from hummingbot.logger import HummingbotLogger

GLASSNODE_API_KEY = os.getenv("GLASSNODE_API_KEY")


class GlassnodeTokenSupplyProvider(TokenSupplyProviderBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)  # type: ignore
        return cls._logger  # type: ignore

    def __init__(self, token_id: str):
        self._token_id = self._get_glassnode_token_id(token_id)
        if not GLASSNODE_API_KEY:
            raise TokenSupplyProviderError("GLASSNODE_API_KEY environment variable not set")
        self._api_key = GLASSNODE_API_KEY
        self._throttler = AsyncThrottler(rate_limits=CONSTANTS.GLASSNODE_RATE_LIMITS)
        self._api_factory = WebAssistantsFactory(throttler=self._throttler)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def supply_endpoint(self) -> str:
        return f"{CONSTANTS.GLASSNODE_BASE_URL}{CONSTANTS.GLASSNODE_SUPPLY_ENDPOINT}"

    @property
    def headers(self) -> dict:
        return {"X-Api-Key": self._api_key}

    def _get_glassnode_token_id(self, token_id: str) -> str:
        if token_id not in GLASSNODE_TOKEN_ID_MAP:
            raise ValueError(f"Invalid token ID: {token_id}")
        return GLASSNODE_TOKEN_ID_MAP[token_id]

    def _get_request_params(self, interval: str, since_timestamp: Optional[int] = None) -> dict:
        glassnode_interval = "24h" if interval == "1d" else interval
        params = {
            "a": self._token_id,
            "i": glassnode_interval,
            "f": "json",
        }
        if since_timestamp is not None:
            params["s"] = since_timestamp
        return params

    def _calculate_since_timestamp(self, interval: IntervalType, count: Optional[int] = None) -> int:
        now = int(TimeUtility.now_ms() / 1000)  # Convert to seconds for Glassnode API
        if count is None:
            interval_seconds = int(IntervalUtility.get_duration_ms("1h") / 1000)
            return now - interval_seconds
        interval_seconds = int(IntervalUtility.get_duration_ms(interval) / 1000)
        buffer_multiplier = 1.05
        return now - int(interval_seconds * count * buffer_multiplier)

    async def fetch_live_token_supply(self) -> Optional[LiveTokenSupplyData]:
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            highest_interval: IntervalType = "10m"
            since_timestamp = self._calculate_since_timestamp(interval=highest_interval, count=None)
            params = self._get_request_params(interval=highest_interval, since_timestamp=since_timestamp)
            requested_at = datetime.now(timezone.utc)
            response = await rest_assistant.execute_request(
                url=self.supply_endpoint,
                params=params,
                headers=self.headers,
                throttler_limit_id=CONSTANTS.GLASSNODE_RATE_LIMIT_ID,
                timeout=CONSTANTS.TIMEOUT,
            )

            self._validate_api_response(response)
            latest_entry = response[-1]
            timestamp = int(int(latest_entry["t"]) * 1000 + IntervalUtility.get_duration_ms(highest_interval))
            return LiveTokenSupplyData(
                provider=self.__class__.__name__,
                token_id=self._token_id.lower(),
                total_supply=float(latest_entry["v"]),
                timestamp=timestamp,
                requested_at=requested_at,
            )
        except Exception as e:
            self.logger().error(f"Error fetching live token supply data from Glassnode: {e}", exc_info=True)
            return None

    # glassnode return latest data with ~5 min of delay
    def _validate_data_freshness(self, start_timestamp: int, interval: IntervalType) -> None:
        current_time = TimeUtility.now_ms()
        # timestamp is start time of the interval
        expected_start_timestamp = IntervalUtility.get_last_interval_start_timestamp(current_time, interval)
        if start_timestamp != expected_start_timestamp:
            expected_dt = TimeUtility.ms_to_datetime(expected_start_timestamp).strftime("%Y-%m-%d %H:%M:%S")
            actual_dt = TimeUtility.ms_to_datetime(start_timestamp).strftime("%Y-%m-%d %H:%M:%S")
            raise TokenSupplyProviderError(f"Data not up-to-date. Expected data for {expected_dt}, got {actual_dt}")

    def _validate_api_response(self, response: list[dict]) -> None:
        if len(response) == 0:
            raise TokenSupplyProviderError("No historical token supply data found")

        for data_point in response:
            required_fields = ["t", "v"]
            missing_fields = [field for field in required_fields if field not in data_point]
            if missing_fields:
                raise TokenSupplyProviderError(f"Missing required fields: {missing_fields}")
            if float(data_point.get("v", 0.0)) <= 0:
                raise TokenSupplyProviderError(f"Invalid token supply value: {data_point}")

    async def fetch_historical_token_supply(
        self, interval: IntervalType, count: int
    ) -> Optional[list[HistoricalTokenSupplyData]]:
        if interval not in CONSTANTS.GLASSNODE_HISTORICAL_OI_INTERVALS:
            supported = ", ".join(CONSTANTS.GLASSNODE_HISTORICAL_OI_INTERVALS)
            raise TokenSupplyProviderError(
                f"Glaanode unsupported interval: {interval}. Supported intervals: {supported}"
            )

        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            since_timestamp = self._calculate_since_timestamp(interval=interval, count=count)
            params = self._get_request_params(interval=interval, since_timestamp=since_timestamp)
            requested_at = datetime.now(timezone.utc)
            response = await rest_assistant.execute_request(
                url=self.supply_endpoint,
                params=params,
                headers=self.headers,
                throttler_limit_id=CONSTANTS.GLASSNODE_RATE_LIMIT_ID,
                timeout=CONSTANTS.TIMEOUT,
            )

            self._validate_api_response(response)
            data = sorted(response, key=lambda x: x["t"])
            latest_timestamp = int(data[-1]["t"] * 1000)
            self._validate_data_freshness(latest_timestamp, interval)

            results = []
            for entry in data:
                results.append(
                    HistoricalTokenSupplyData(
                        provider=self.__class__.__name__,
                        token_id=self._token_id.lower(),
                        total_supply=float(entry["v"]),
                        timestamp=entry["t"] * 1000,
                        requested_at=requested_at,
                    )
                )
            return results[-count:]
        except Exception as e:
            self.logger().error(f"Error fetching historical token supply data from Glassnode: {e}", exc_info=True)
            return None
