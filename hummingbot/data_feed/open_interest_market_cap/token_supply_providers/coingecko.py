import logging
from datetime import datetime, timezone
from typing import Optional

from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.data_feed.open_interest_market_cap import constants as CONSTANTS
from hummingbot.data_feed.open_interest_market_cap.data_types import TokenSupplyData
from hummingbot.data_feed.open_interest_market_cap.token_supply_providers.base import TokenSupplyProviderBase
from hummingbot.logger import HummingbotLogger


class CoinGeckoTokenSupplyProvider(TokenSupplyProviderBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)  # type: ignore
        return cls._logger  # type: ignore

    def __init__(self, token_id: str):
        self._token_id = token_id
        self._throttler = AsyncThrottler(rate_limits=CONSTANTS.COINGECKO_RATE_LIMITS)
        self._api_factory = WebAssistantsFactory(throttler=self._throttler)

    @property
    def markets_endpoint(self) -> str:
        return f"{CONSTANTS.COINGECKO_BASE_URL}{CONSTANTS.COINGECKO_MARKETS_ENDPOINT}"

    def _parse_last_updated(self, last_updated_str: Optional[str]) -> Optional[datetime]:
        if not last_updated_str:
            self.logger().warning(
                f"Missing field 'last_updated' in CoinGecko response for token {self._token_id}, defaulting to None"
            )
            return None
        try:
            return datetime.fromisoformat(last_updated_str.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
        except Exception:
            self.logger().error(
                f"Failed to parse last_updated field ({last_updated_str}) for token {self._token_id}, defaulting to None",
                exc_info=True,
            )
            return None

    def _parse_token_supply_from_coingecko_response(
        self, requested_at: datetime, raw_token_data: dict
    ) -> Optional[TokenSupplyData]:
        try:
            total_supply = raw_token_data.get("total_supply")
            if total_supply is None:
                self.logger().warning(
                    f"Missing field 'total_supply' in CoinGecko response for token {self._token_id}, defaulting to None"
                )

            market_cap = raw_token_data.get("market_cap")
            if market_cap is None:
                self.logger().warning(
                    f"Missing field 'market_cap' in CoinGecko response for token {self._token_id}, defaulting to None"
                )

            last_updated = self._parse_last_updated(raw_token_data.get("last_updated"))

            return TokenSupplyData(
                provider=self.__class__.__name__,
                token_id=self._token_id,
                total_supply=float(total_supply) if total_supply is not None else None,
                market_cap=float(market_cap) if market_cap is not None else None,
                last_updated=last_updated,
                requested_at=requested_at,
            )
        except Exception as e:
            self.logger().error(f"Error parsing token supply data from CoinGecko response: {e}", exc_info=True)
            return None

    async def fetch_token_supply(self) -> Optional[TokenSupplyData]:
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            params = {
                "vs_currency": "usd",
                "ids": self._token_id,
            }
            requested_at = datetime.now(timezone.utc)
            response: list[dict] = await rest_assistant.execute_request(
                url=self.markets_endpoint, params=params, throttler_limit_id=CONSTANTS.COINGECKO_RATE_LIMIT_ID
            )  # type: ignore

            if not response:
                self.logger().error(
                    f"No data returned from CoinGecko markets endpoint ({response}) for token {self._token_id}"
                )
                return None

            return self._parse_token_supply_from_coingecko_response(requested_at, response[0])

        except Exception as e:
            self.logger().error(f"Error fetching token supply data from CoinGecko: {e}", exc_info=True)
            return None
