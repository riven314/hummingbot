import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import aiohttp

from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.data_feed.funding_rate.constants import IntervalType
from hummingbot.data_feed.funding_rate.data_types import FundingRateRecord


class FundingRateProviderError(Exception):
    pass


class FundingRateProviderBase(ABC):
    def __init__(
        self,
        trading_pair: str,
        web_assistants_factory: WebAssistantsFactory,
        throttler_limit_id: str,
        interval: IntervalType,
    ):
        self._trading_pair: str = trading_pair
        self._interval: IntervalType = interval
        self._web_assistants_factory: WebAssistantsFactory = web_assistants_factory
        self._throttler_limit_id: str = throttler_limit_id
        self._client_session: Optional[aiohttp.ClientSession] = None
        self._rest_assistant = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._client_session is None:
            self._client_session = await self._web_assistants_factory.get_client_session()
        return self._client_session

    async def _get_rest_assistant(self) -> Any:
        if self._rest_assistant is None:
            self._rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        return self._rest_assistant

    async def _call_endpoint(
        self,
        method: RESTMethod,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        is_auth_required: bool = False,
        limit_id: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        rest_assistant = await self._get_rest_assistant()
        session = await self._get_session()
        url = f"{self.base_url}{endpoint}"
        limit_id = limit_id or self._throttler_limit_id

        try:
            response = await rest_assistant.call(
                method=method,
                url=url,
                params=params,
                is_auth_required=is_auth_required,
                throttler_limit_id=limit_id,
                session=session,
                **kwargs,
            )
            return await response.json()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            raise FundingRateProviderError(f"Error calling {method.name} {url} with params {params}: {e}") from e

    @property
    @abstractmethod
    def base_url(self) -> str:
        raise NotImplementedError

    @abstractmethod
    async def fetch_funding_rate(self, limit: int) -> List[FundingRateRecord]:
        raise NotImplementedError
