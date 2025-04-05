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

    def _parse_token_supply_from_glassnode_response(
        self, requested_at: datetime, data: list[dict]
    ) -> Optional[LiveTokenSupplyData]:
        if not data:
            return None
        latest_entry = data[-1]
        return LiveTokenSupplyData(
            provider=self.__class__.__name__,
            token_id=self._token_id.lower(),
            total_supply=float(latest_entry["v"]),
            timestamp=latest_entry["t"] * 1000,
            requested_at=requested_at,
        )

    async def fetch_live_token_supply(self) -> Optional[LiveTokenSupplyData]:
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            params = {
                "a": self._token_id,
                "i": "10m",
                "f": "json",
            }
            requested_at = datetime.now(timezone.utc)
            response = await rest_assistant.execute_request(
                url=self.supply_endpoint,
                params=params,
                headers=self.headers,
                throttler_limit_id=CONSTANTS.GLASSNODE_RATE_LIMIT_ID,
                timeout=CONSTANTS.TIMEOUT,
            )
            return self._parse_token_supply_from_glassnode_response(requested_at, response)
        except Exception as e:
            self.logger().error(f"Error fetching live token supply data from Glassnode: {e}", exc_info=True)
            return None

    async def fetch_historical_token_supply(
        self, interval: IntervalType, count: int
    ) -> Optional[list[HistoricalTokenSupplyData]]:
        try:
            # glassnode API treats 1d as 24h in query params
            glassnode_interval = "24h" if interval == "1d" else interval
            rest_assistant = await self._api_factory.get_rest_assistant()
            params = {
                "a": self._token_id,
                "i": glassnode_interval,
                "f": "json",
            }
            requested_at = datetime.now(timezone.utc)
            response = await rest_assistant.execute_request(
                url=self.supply_endpoint,
                params=params,
                headers=self.headers,
                throttler_limit_id=CONSTANTS.GLASSNODE_RATE_LIMIT_ID,
                timeout=CONSTANTS.TIMEOUT,
            )

            result = []
            for entry in response[-count:]:
                result.append(
                    HistoricalTokenSupplyData(
                        provider=self.__class__.__name__,
                        token_id=self._token_id.lower(),
                        total_supply=float(entry["v"]),
                        timestamp=entry["t"] * 1000,
                        requested_at=requested_at,
                    )
                )
            return result
        except Exception as e:
            self.logger().error(f"Error fetching historical token supply data from Glassnode: {e}", exc_info=True)
            return None
