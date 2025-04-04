import abc
from typing import Optional

from hummingbot.data_feed.open_interest_market_cap.data_types import (
    HistoricalTokenSupplyData,
    IntervalType,
    LiveTokenSupplyData,
)


class TokenSupplyProviderError(Exception):
    pass


class TokenSupplyProviderBase(abc.ABC):
    """Base class for providers of token supply data"""

    @abc.abstractmethod
    async def fetch_live_token_supply(self) -> Optional[LiveTokenSupplyData]:
        """
        Fetches total token supply for a specific token

        :param token_id: The ID of the token (e.g. 'bitcoin')
        :return: TokenSupplyData or None if fetch failed
        """
        pass

    @abc.abstractmethod
    async def fetch_historical_token_supply(
        self, interval: IntervalType, count: int
    ) -> Optional[list[HistoricalTokenSupplyData]]:
        """
        Fetches historical token supply for a specific token

        :param interval: The interval of the historical token supply
        :param count: The number of historical records to fetch
        :return: HistoricalTokenSupplyData or None if fetch failed
        """
        pass
