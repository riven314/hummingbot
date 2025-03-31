import abc
from typing import Optional

from hummingbot.data_feed.open_interest_market_cap.data_types import TokenSupplyData


class TokenSupplyProviderBase(abc.ABC):
    """Base class for providers of token supply data"""

    @abc.abstractmethod
    async def fetch_token_supply(self) -> Optional[TokenSupplyData]:
        """
        Fetches total token supply for a specific token

        :param token_id: The ID of the token (e.g. 'bitcoin')
        :return: TokenSupplyData or None if fetch failed
        """
        pass
