from typing import Any, Dict, List

from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.data_feed.funding_rate.constants import (
    BINANCE_FUNDING_RATE_ENDPOINT,
    BINANCE_FUNDING_RATE_LIMIT_ID,
    BINANCE_PERPETUAL_BASE_URL,
    IntervalType,
)
from hummingbot.data_feed.funding_rate.data_types import FundingRateRecord
from hummingbot.data_feed.funding_rate.providers.base import FundingRateProviderBase, FundingRateProviderError


class BinanceFundingRateProvider(FundingRateProviderBase):
    def __init__(self, trading_pair: str, web_assistants_factory: Any, interval: IntervalType):
        super().__init__(
            trading_pair=trading_pair,
            web_assistants_factory=web_assistants_factory,
            throttler_limit_id=BINANCE_FUNDING_RATE_LIMIT_ID,
            interval=interval,
        )

    @property
    def base_url(self) -> str:
        return BINANCE_PERPETUAL_BASE_URL

    @property
    def name(self) -> str:
        return self.__class__.__name__

    async def fetch_funding_rate(self, limit: int) -> List[FundingRateRecord]:
        params: Dict[str, Any] = {"symbol": self._trading_pair, "limit": limit}
        try:
            data: List[Dict[str, Any]] = await self._call_endpoint(
                method=RESTMethod.GET,
                endpoint=BINANCE_FUNDING_RATE_ENDPOINT,
                params=params,
                limit_id=self._throttler_limit_id,
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
