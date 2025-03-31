import asyncio
import logging
from abc import ABC
from typing import Optional

from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.data_feed_base import DataFeedBase
from hummingbot.data_feed.open_interest_market_cap.data_types import (
    OpenInterestData,
    OpenInterestMarketCapConfig,
    TokenSupplyData,
)
from hummingbot.data_feed.open_interest_market_cap.open_interest_providers import BinanceOpenInterestProvider
from hummingbot.data_feed.open_interest_market_cap.token_supply_providers import (
    CoinCapTokenSupplyProvider,
    CoinGeckoTokenSupplyProvider,
)
from hummingbot.logger import HummingbotLogger


class OpenInterestMarketCapFeed(DataFeedBase, ABC):
    oi_mcap_logger: Optional[HummingbotLogger] = None
    _oi_mcap_shared_instance: Optional["OpenInterestMarketCapFeed"] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls.oi_mcap_logger is None:
            cls.oi_mcap_logger = logging.getLogger(__name__)  # type: ignore
        return cls.oi_mcap_logger  # type: ignore

    def __init__(self, config: OpenInterestMarketCapConfig):
        super().__init__()
        self._config = config
        self._fetch_loop_task: Optional[asyncio.Task] = None
        self._open_interest_provider = BinanceOpenInterestProvider(self._config.trading_pair)
        self._token_supply_provider = CoinGeckoTokenSupplyProvider(self.token_id)
        self._fallback_token_supply_provider = CoinCapTokenSupplyProvider(self.token_id)
        self._latest_open_interest: Optional[OpenInterestData] = None
        self._latest_token_supply: Optional[TokenSupplyData] = None

    @property
    def token_id(self) -> str:
        if self._config.trading_pair == "BTCUSDT":
            return "bitcoin"
        raise ValueError(f"Unsupported token_id for trading_pair {self._config.trading_pair}")

    @property
    def update_interval(self) -> float:
        return self._config.update_interval

    @property
    def name(self) -> str:
        return "open_interest_market_cap"

    async def start_network(self):
        await self.stop_network()
        self._fetch_loop_task = safe_ensure_future(self._fetch_loop())

    async def stop_network(self):
        if self._fetch_loop_task is not None:
            self._fetch_loop_task.cancel()
            self._fetch_loop_task = None

    async def _fetch_loop(self):
        while True:
            try:
                success = await self._fetch_data()
                if success:
                    self._ready_event.set()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error in {self.name} data feed: {e}", exc_info=True)
            await asyncio.sleep(self._update_interval)

    # TODO: validate the token supply and open interest data looks odd
    async def _fetch_data(self) -> bool:
        try:
            open_interest_task = self._open_interest_provider.fetch_open_interest()
            token_supply_task = self._token_supply_provider.fetch_token_supply()
            fallback_token_supply_task = self._fallback_token_supply_provider.fetch_token_supply()

            oi_result, ts_result, fallback_ts_result = await asyncio.gather(
                open_interest_task, token_supply_task, fallback_token_supply_task
            )

            if oi_result is not None:
                self._latest_open_interest = oi_result

            if ts_result is not None:
                self._latest_token_supply = ts_result

            return oi_result is not None and ts_result is not None
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(f"Error fetching data: {e}", exc_info=True)
            return False

    def get_open_interest_to_market_cap_ratio(self, price: float) -> Optional[float]:
        assert self._latest_open_interest and self._latest_token_supply and self._latest_token_supply.total_supply

        market_cap = self._latest_token_supply.total_supply * price
        if market_cap == 0:
            return None

        return self._latest_open_interest.open_interest / market_cap
