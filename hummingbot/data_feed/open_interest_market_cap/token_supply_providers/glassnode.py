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
from hummingbot.logger import HummingbotLogger


class GlassnodeTokenSupplyProvider(TokenSupplyProviderBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)  # type: ignore
        return cls._logger  # type: ignore

    def __init__(self, token_id: str):
        self._token_id = self._get_glassnode_token_id(token_id)
        self._api_key = os.getenv("GLASSNODE_API_KEY")
        if not self._api_key:
            raise TokenSupplyProviderError("GLASSNODE_API_KEY environment variable not set")
        self._throttler = AsyncThrottler(rate_limits=CONSTANTS.GLASSNODE_RATE_LIMITS)
        self._api_factory = WebAssistantsFactory(throttler=self._throttler)

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
        now = int(datetime.now(timezone.utc).timestamp())
        if count is None:
            interval_seconds = int(CONSTANTS.INTERVAL_TO_DURATION_MS["1h"] / 1000)
            return now - interval_seconds
        interval_seconds = int(CONSTANTS.INTERVAL_TO_DURATION_MS[interval] / 1000)
        buffer_multiplier = 1.05
        return now - int(interval_seconds * count * buffer_multiplier)

    async def fetch_live_token_supply(self) -> Optional[LiveTokenSupplyData]:
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            highest_interval = "10m"
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
            if len(response) == 0:
                self.logger().warning(f"No live token supply data retrieved for {self._token_id} at {requested_at}")
                return None

            latest_entry = response[-1]
            timestamp = int(latest_entry["t"]) * 1000 + CONSTANTS.INTERVAL_TO_DURATION_MS[highest_interval]
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

    async def fetch_historical_token_supply(
        self, interval: IntervalType, count: int
    ) -> Optional[list[HistoricalTokenSupplyData]]:
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

            results = []
            for entry in response:
                results.append(
                    HistoricalTokenSupplyData(
                        provider=self.__class__.__name__,
                        token_id=self._token_id.lower(),
                        total_supply=float(entry["v"]),
                        timestamp=entry["t"] * 1000,
                        requested_at=requested_at,
                    )
                )
            results = sorted(results, key=lambda x: x.timestamp)
            return results[-count:]
        except Exception as e:
            self.logger().error(f"Error fetching historical token supply data from Glassnode: {e}", exc_info=True)
            return None
