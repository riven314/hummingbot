from abc import ABC, abstractmethod
from typing import List

from hummingbot.data_feed.funding_rate.data_types import FundingRateRecord


class FundingRateProviderError(Exception):
    pass


class FundingRateProviderBase(ABC):

    @property
    @abstractmethod
    def funding_rate_endpoint(self) -> str:
        raise NotImplementedError

    @abstractmethod
    async def fetch_funding_rate(self, limit: int) -> List[FundingRateRecord]:
        raise NotImplementedError
