import logging
from datetime import datetime, timezone
from typing import Optional

from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.data_feed.open_interest_market_cap import constants as CONSTANTS
from hummingbot.data_feed.open_interest_market_cap.data_types import OpenInterestData
from hummingbot.data_feed.open_interest_market_cap.open_interest_providers.base import OpenInterestProviderBase
from hummingbot.logger import HummingbotLogger


class BinanceOpenInterestProvider(OpenInterestProviderBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)  # type: ignore
        return cls._logger  # type: ignore

    def __init__(self, trading_pair: str):
        self._trading_pair = trading_pair
        self._throttler = AsyncThrottler(rate_limits=CONSTANTS.BINANCE_RATE_LIMITS)
        self._api_factory = WebAssistantsFactory(throttler=self._throttler)

    # async def close(self) -> None:
    #     if self._rest_assistant is not None and hasattr(self._rest_assistant, "_connection"):
    #         client_session = self._rest_assistant._connection._client_session
    #         if not client_session.closed:
    #             await client_session.close()
    #             self.logger().info("Closed Binance REST assistant")
    #         self._rest_assistant = None

    async def fetch_open_interest(self) -> Optional[OpenInterestData]:
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            params = {"symbol": self._trading_pair}

            endpoint = CONSTANTS.BINANCE_OPEN_INTEREST_ENDPOINT
            url = f"{CONSTANTS.BINANCE_FUTURES_BASE_URL}{endpoint}"

            requested_at = datetime.now(timezone.utc)
            response: dict = await rest_assistant.execute_request(
                url=url,
                params=params,
                throttler_limit_id=CONSTANTS.BINANCE_RATE_LIMIT_ID,
                timeout=CONSTANTS.TIMEOUT,
            )  # type: ignore

            return OpenInterestData(
                provider=self.__class__.__name__,
                symbol=response["symbol"],
                open_interest=float(response["openInterest"]),
                timestamp=response["time"],
                requested_at=requested_at,
            )
        except Exception as e:
            self.logger().error(f"Error fetching open interest data from Binance: {e}", exc_info=True)
            return None
