import abc
from typing import Optional

from hummingbot.data_feed.open_interest_market_cap.data_types import (
    HistoricalOpenInterestData,
    IntervalType,
    LiveOpenInterestData,
)


class OpenInterestProviderError(Exception):
    pass


class OpenInterestProviderBase(abc.ABC):
    """Base class for providers of open interest data"""

    @abc.abstractmethod
    async def fetch_live_open_interest(self) -> Optional[LiveOpenInterestData]:
        """
        Fetches open interest for a specific trading pair

        :param trading_pair: The trading pair to fetch open interest for
        :return: OpenInterestData or None if fetch failed
        """
        pass

    @abc.abstractmethod
    async def fetch_historical_open_interest(
        self, interval: IntervalType, count: int
    ) -> Optional[list[HistoricalOpenInterestData]]:
        """
        Fetches historical open interest for a specific trading pair
        """
        pass
