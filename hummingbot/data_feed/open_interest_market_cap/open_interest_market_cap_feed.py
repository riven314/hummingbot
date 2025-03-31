import asyncio
import logging
import time
from abc import ABC
from datetime import datetime, timezone
from typing import Optional

from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.data_feed_base import DataFeedBase
from hummingbot.data_feed.open_interest_market_cap.data_types import (
    IntervalType,
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
        if self._config.trading_pair == "BTC-USDT":
            return "bitcoin"
        raise ValueError(f"Unsupported token_id for trading_pair {self._config.trading_pair}")

    @property
    def update_interval(self) -> float:
        return self._parse_interval_to_seconds(self._config.interval)

    @property
    def name(self) -> str:
        return "open_interest_market_cap"

    def _parse_interval_to_seconds(self, interval: IntervalType) -> float:
        if interval == "1m":
            return 60
        elif interval == "10m":
            return 600
        elif interval == "15m":
            return 900
        elif interval == "30m":
            return 1800
        elif interval == "1h":
            return 3600
        elif interval == "1d":
            return 86400

    def get_next_update_timestamp(self) -> float:
        """Calculate the next update timestamp based on the interval."""
        now = datetime.fromtimestamp(time.time(), tz=timezone.utc)
        now_timestamp = int(now.timestamp())
        interval_seconds = self.update_interval

        if self._config.interval == "1d":
            next_day = datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp() + interval_seconds
            return next_day

        intervals_passed = now_timestamp // interval_seconds
        next_interval_timestamp = (intervals_passed + 1) * interval_seconds
        return next_interval_timestamp

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
                # Calculate time until next update
                next_update_time = self._get_next_update_timestamp()
                current_time = time.time()
                sleep_time = max(0, next_update_time - current_time)

                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

                success = await self._fetch_data()
                if success:
                    self._ready_event.set()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error in {self.name} data feed: {e}", exc_info=True)
                await asyncio.sleep(1)

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
            elif fallback_ts_result is not None:
                self._latest_token_supply = fallback_ts_result

            return oi_result is not None and (ts_result is not None or fallback_ts_result is not None)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(f"Error fetching data: {e}", exc_info=True)
            return False

    def get_open_interest_to_market_cap_ratio(self, price: float) -> Optional[float]:
        if (
            not self._latest_open_interest
            or not self._latest_token_supply
            or not self._latest_token_supply.total_supply
        ):
            return None

        market_cap = self._latest_token_supply.total_supply * price
        if market_cap == 0:
            return None

        return self._latest_open_interest.open_interest / market_cap
