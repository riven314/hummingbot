import asyncio
import logging
from abc import ABC
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import hummingbot.data_feed.open_interest_market_cap.constants as CONSTANTS
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.data_feed_base import DataFeedBase
from hummingbot.data_feed.open_interest_market_cap.data_types import (
    IntervalType,
    OpenInterestMarketCapConfig,
    OpenInterestMarketCapRecord,
)
from hummingbot.data_feed.open_interest_market_cap.open_interest_providers import BinanceOpenInterestProvider
from hummingbot.data_feed.open_interest_market_cap.token_supply_providers import (
    CoinGeckoTokenSupplyProvider,
    GlassnodeTokenSupplyProvider,
)
from hummingbot.logger import HummingbotLogger


# TODO: add retry on each provider
# TODO: handle OpenInterestData or TokenSupplyData has None value on critical fields
# TODO: check and handle obsolete data at get_open_interest_to_market_cap_ratio
# TODO: tg notification on API error at each providers (e.g. API upgrade for coincap) ==> random error
# OSError: Error executing request GET https://api.coincap.io/v2/assets/bitcoin. HTTP status is 429. Error: {"data":{"message":"We are deprecating this version of the CoinCap API on March 31, 2025. Sign up for our new V3 API at https://pro.coincap.io/dashboard"},"timestamp":1743435300643}
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
        self._coingecko_token_supply_provider = CoinGeckoTokenSupplyProvider(self.token_id)
        self._glassnode_token_supply_provider = GlassnodeTokenSupplyProvider(self.token_id)
        self._queue: deque[OpenInterestMarketCapRecord] = deque(maxlen=self._config.window)

    @property
    def token_id(self) -> str:
        if self._config.trading_pair == "BTCUSDT":
            return "bitcoin"
        raise ValueError(f"Unsupported token_id for trading_pair {self._config.trading_pair}")

    @property
    def update_interval(self) -> float:
        return self._parse_interval_to_seconds(self._config.interval)

    @property
    def name(self) -> str:
        return f"{self.__class__.__name__}:{self._config.trading_pair}:{self._config.interval}"

    def _parse_interval_to_seconds(self, interval: IntervalType) -> float:
        return CONSTANTS.INTERVAL_TO_DURATION_MS[interval] / 1000

    def get_next_update_timestamp(self) -> float:
        """
        Calculate the next update timestamp based on the configured interval.

        Example:
            If current time is 2024-01-01 14:35:00 and interval is "1h":
            - The last interval started at 14:00:00
            - The next interval will start at 15:00:00

            If current time is 2024-01-01 14:35:00 and interval is "15m":
            - The last interval started at 14:30:00
            - The next interval will start at 14:45:00
        """
        now = datetime.now(timezone.utc)
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
        self.logger().info(f"Starting {self.name} fetch lopp task...")
        self._fetch_loop_task = safe_ensure_future(self._fetch_loop())

    async def stop_network(self):
        if self._fetch_loop_task is not None:
            self._fetch_loop_task.cancel()
            self._fetch_loop_task = None
        self._queue.clear()

    async def check_network(self) -> NetworkStatus:
        return NetworkStatus.CONNECTED

    async def _fetch_loop(self):
        if not self.ready:
            await self._fetch_historical_data()
            self._ready_event.set()

        while True:
            try:
                next_update_timestamp = self.get_next_update_timestamp()
                current_timestamp = datetime.now(timezone.utc).timestamp()
                sleep_time = max(0, next_update_timestamp - current_timestamp)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

                is_fetch_live_success = await self._fetch_live_data()
                if not is_fetch_live_success:
                    self.logger().warning(
                        f"Failed to fetch live OI and Token Supplydata for {self._config.trading_pair}."
                    )

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error at fetch loop from {self.name}: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _fetch_historical_data(self) -> None:
        requested_at = datetime.now(timezone.utc)
        historical_oi_task = self._open_interest_provider.fetch_historical_open_interest(
            interval=self._config.interval, count=self._config.window
        )
        historical_fallback_ts_task = self._glassnode_token_supply_provider.fetch_historical_token_supply(
            interval=self._config.interval, count=self._config.window
        )
        oi_results, ts_results = await asyncio.gather(historical_oi_task, historical_fallback_ts_task)

        # check invalid historical data
        if oi_results is None or ts_results is None or len(oi_results) != len(ts_results):
            raise Exception(f"Failed to fetch historical OI and Token Supplydata for {self._config.trading_pair}.")

        for oi, ts in zip(oi_results, ts_results):
            # check timestamp mismatch
            if oi.timestamp != ts.timestamp:
                raise ValueError(f"Open interest and token supply timestamp mismatch: {oi.timestamp} != {ts.timestamp}")

            self._queue.append(
                OpenInterestMarketCapRecord(
                    open_interest_provider=self._open_interest_provider.name,
                    token_supply_provider=self._glassnode_token_supply_provider.name,
                    symbol=self._config.trading_pair,
                    open_interest=oi.open_interest,
                    token_supply=ts.total_supply,
                    timestamp=oi.timestamp,
                    requested_at=requested_at,
                )
            )
        self.logger().info(
            f"Successfully fetched {len(oi_results)} historical OI and Token Supplydata for {self._config.trading_pair}."
        )

    async def _fetch_live_data(self, timestamp: int) -> bool:
        requested_at = datetime.now(timezone.utc)
        open_interest_task = self._open_interest_provider.fetch_live_open_interest()
        token_supply_task = self._coingecko_token_supply_provider.fetch_live_token_supply()
        oi_result, ts_result = await asyncio.gather(open_interest_task, token_supply_task)

        is_oi_estimated, is_ts_estimated = False, False

        # handle API request failure, and fallback to Glassnode
        if ts_result is None or ts_result.total_supply == 0.0:
            self.logger().warning(
                f"Live token supply fetched from {self._coingecko_token_supply_provider.name} "
                f"for {self._config.trading_pair} is None or 0 ({ts_result}), fallback to Glassnode."
            )
            ts_result = await self._glassnode_token_supply_provider.fetch_live_token_supply()

        # handle None or problematic returning data
        if ts_result is None or ts_result.total_supply is None or ts_result.total_supply == 0.0:
            last_token_supply = self._queue[-1].token_supply
            token_supply = last_token_supply
            is_ts_estimated = True
            self.logger().warning(
                f"Live token supply fetched from {self._coingecko_token_supply_provider.name} "
                f"for {self._config.trading_pair} is None or 0 ({ts_result}), fallback to previous record ({last_token_supply})"
            )
        else:
            token_supply = ts_result.total_supply

        if oi_result is None or oi_result.open_interest is None or oi_result.open_interest == 0.0:
            last_open_interest = self._queue[-1].open_interest
            open_interest = last_open_interest
            is_oi_estimated = True
            self.logger().warning(
                f"Live open interest fetched from {self._open_interest_provider.name} "
                f"for {self._config.trading_pair} is None or 0 ({oi_result}), fallback to previous record ({last_open_interest})"
            )
        else:
            open_interest = oi_result.open_interest

        open_timestamp = int(timestamp - self.update_interval)
        if oi_result and ts_result:
            self._queue.append(
                OpenInterestMarketCapRecord(
                    open_interest_provider=self._open_interest_provider.name,
                    token_supply_provider=self._coingecko_token_supply_provider.name,
                    symbol=self._config.trading_pair,
                    open_interest=open_interest,
                    token_supply=token_supply,
                    timestamp=open_timestamp,
                    requested_at=requested_at,
                    is_open_interest_estimated=is_oi_estimated,
                    is_token_supply_estimated=is_ts_estimated,
                )
            )
        return True
