import logging
from datetime import datetime, timezone
from typing import Optional

from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.data_feed.open_interest_market_cap import constants as CONSTANTS
from hummingbot.data_feed.open_interest_market_cap.data_types import (
    HistoricalTokenSupplyData,
    IntervalType,
    LiveTokenSupplyData,
)
from hummingbot.data_feed.open_interest_market_cap.token_supply_providers.base import TokenSupplyProviderBase
from hummingbot.logger import HummingbotLogger


class CoinCapTokenSupplyProvider(TokenSupplyProviderBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)  # type: ignore
        return cls._logger  # type: ignore

    def __init__(self, token_id: str):
        self._token_id = token_id
        self._throttler = AsyncThrottler(rate_limits=CONSTANTS.COINCAP_RATE_LIMITS)
        self._api_factory = WebAssistantsFactory(throttler=self._throttler)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def assets_endpoint(self) -> str:
        return f"{CONSTANTS.COINCAP_BASE_URL}{CONSTANTS.COINCAP_ASSETS_ENDPOINT}/{self._token_id}"

    def _parse_last_updated(self, timestamp_ms: Optional[int]) -> Optional[datetime]:
        if not timestamp_ms:
            self.logger().warning(
                f"Missing timestamp in CoinCap response for token {self._token_id}, defaulting to None"
            )
            return None
        try:
            return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        except Exception:
            self.logger().error(
                f"Failed to parse timestamp ({timestamp_ms}) for token {self._token_id}, defaulting to None",
                exc_info=True,
            )
            return None

    def _parse_token_supply_from_coincap_response(
        self, requested_at: datetime, raw_response: dict
    ) -> Optional[LiveTokenSupplyData]:
        try:
            raw_token_data = raw_response.get("data", {})
            supply = raw_token_data.get("supply")
            if supply is None:
                self.logger().warning(
                    f"Missing field 'supply' in CoinCap response for token {self._token_id}, defaulting to None"
                )

            market_cap = raw_token_data.get("marketCapUsd")
            if market_cap is None:
                self.logger().warning(
                    f"Missing field 'marketCapUsd' in CoinCap response for token {self._token_id}, defaulting to None"
                )

            last_updated = self._parse_last_updated(raw_response.get("timestamp"))

            return LiveTokenSupplyData(
                provider=self.__class__.__name__,
                token_id=self._token_id,
                total_supply=float(supply) if supply is not None else None,
                market_cap=float(market_cap) if market_cap is not None else None,
                recorded_at=last_updated,
                requested_at=requested_at,
            )
        except Exception as e:
            self.logger().error(f"Error parsing token supply data from CoinCap response: {e}", exc_info=True)
            return None

    async def fetch_live_token_supply(self) -> Optional[LiveTokenSupplyData]:
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            requested_at = datetime.now(timezone.utc)
            response: dict = await rest_assistant.execute_request(
                url=self.assets_endpoint,
                throttler_limit_id=CONSTANTS.COINCAP_RATE_LIMIT_ID,
                timeout=CONSTANTS.TIMEOUT,
            )  # type: ignore

            if not response:
                self.logger().error(
                    f"No data returned from CoinCap assets endpoint ({response}) for token {self._token_id}"
                )
                return None

            return self._parse_token_supply_from_coincap_response(requested_at, response)

        except Exception as e:
            self.logger().error(f"Error fetching live token supply data from CoinCap: {e}", exc_info=True)
            return None

    async def fetch_historical_token_supply(
        self, interval: IntervalType, count: int
    ) -> Optional[list[HistoricalTokenSupplyData]]:
        raise NotImplementedError("CoinCap does not support historical token supply data")
