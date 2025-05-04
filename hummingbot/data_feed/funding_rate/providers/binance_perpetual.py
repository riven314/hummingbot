import logging
from typing import Any, Dict, List, Optional

from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.data_feed.funding_rate.constants import (
    BINANCE_FUNDING_RATE_ENDPOINT,
    BINANCE_FUNDING_RATE_LIMIT_ID,
    BINANCE_FUNDING_RATE_RATE_LIMITS,
    BINANCE_PERPETUAL_BASE_URL,
    DEFAULT_TIMEOUT,
    IntervalType,
)
from hummingbot.data_feed.funding_rate.data_types import FundingRateRecord
from hummingbot.data_feed.funding_rate.providers.base import FundingRateProviderBase, FundingRateProviderError
from hummingbot.logger import HummingbotLogger


class BinanceFundingRateProvider(FundingRateProviderBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)  # type: ignore
        return cls._logger  # type: ignore

    def __init__(self, trading_pair: str, interval: IntervalType):
        self._trading_pair = trading_pair
        self._interval = interval
        self._throttler = AsyncThrottler(rate_limits=BINANCE_FUNDING_RATE_RATE_LIMITS)
        self._api_factory = WebAssistantsFactory(throttler=self._throttler)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def funding_rate_endpoint(self) -> str:
        return f"{BINANCE_PERPETUAL_BASE_URL}{BINANCE_FUNDING_RATE_ENDPOINT}"

    async def fetch_funding_rate(self, limit: int) -> List[FundingRateRecord]:
        params: Dict[str, Any] = {"symbol": self._trading_pair, "limit": limit}
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            data: List[Dict[str, Any]] = await rest_assistant.execute_request(
                url=self.funding_rate_endpoint,
                params=params,
                throttler_limit_id=BINANCE_FUNDING_RATE_LIMIT_ID,
                timeout=DEFAULT_TIMEOUT,
            )  # type: ignore

            records: List[FundingRateRecord] = []
            for item in data:
                record = FundingRateRecord(
                    provider=self.name,
                    symbol=item["symbol"],
                    funding_time=int(item["fundingTime"]),
                    funding_rate=float(item["fundingRate"]),
                    mark_price=float(item["markPrice"]),
                )
                records.append(record)

            records.sort(key=lambda r: r.funding_time)
            return records

        except FundingRateProviderError as e:
            raise e
        except Exception as e:
            raise FundingRateProviderError(
                f"Unexpected error fetching funding rate from {self.name} for {self._trading_pair}: {e}"
            ) from e
