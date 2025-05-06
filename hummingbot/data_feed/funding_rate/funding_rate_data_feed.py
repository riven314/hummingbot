import asyncio
import logging
import statistics
from collections import deque
from typing import Any, Deque, List, Optional, Tuple

from hummingbot.core.network_iterator import NetworkStatus  # type: ignore
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.data_feed_base import DataFeedBase
from hummingbot.data_feed.funding_rate.constants import (
    BINANCE_FUNDING_RATE_COUNT_LIMIT,
    BINANCE_TRADING_PAIR_TO_FUNDING_INTERVAL,
)
from hummingbot.data_feed.funding_rate.data_types import FundingRateConfig, FundingRateRecord
from hummingbot.data_feed.funding_rate.providers.base import FundingRateProviderBase
from hummingbot.data_feed.funding_rate.providers.binance_perpetual import BinanceFundingRateProvider
from hummingbot.data_feed.funding_rate.utils.interval_utils import IntervalUtility
from hummingbot.data_feed.funding_rate.utils.time_utils import TimeUtility
from hummingbot.logger import HummingbotLogger


class FundingRateDataFeed(DataFeedBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))  # type: ignore
        return cls._logger  # type: ignore

    def __init__(
        self,
        config: FundingRateConfig,
    ):
        super().__init__()
        self._config: FundingRateConfig = config
        expected_interval = BINANCE_TRADING_PAIR_TO_FUNDING_INTERVAL[self._config.trading_pair]
        if self._config.interval != expected_interval:
            raise ValueError(
                f"Binance {self._config.trading_pair} expects funding interval to be {expected_interval}, but {self._config.interval} was provided"
            )
        self._provider: FundingRateProviderBase = BinanceFundingRateProvider(
            trading_pair=self._config.trading_pair,
        )
        self._funding_rate_deque: Deque[FundingRateRecord] = deque(maxlen=self.deque_size)
        self._fetch_task: Optional[asyncio.Task] = None

    @property
    def name(self) -> str:
        return f"{self.__class__.__name__}:{self._config.trading_pair}:{self._config.interval}"

    @property
    def funding_rate_records(self) -> List[FundingRateRecord]:
        return list(self._funding_rate_deque)

    @property
    def window(self) -> int:
        return self._config.window

    @property
    def deque_size(self) -> int:
        return self.window + 5

    @property
    def interval_ms(self) -> int:
        return IntervalUtility.get_duration_ms(self._config.interval)

    @property
    def last_funding_rate_record(self) -> Optional[FundingRateRecord]:
        return self._funding_rate_deque[-1] if self._funding_rate_deque else None

    async def start_network(self):
        await self.stop_network()
        self.logger().info(f"Started fetch loop for {self.name}")
        self._fetch_task = safe_ensure_future(self._fetch_loop())

    async def stop_network(self):
        if self._fetch_task:
            self._fetch_task.cancel()
            await asyncio.sleep(1.0)
            self._fetch_task = None
        self._funding_rate_deque.clear()

    async def check_network(self) -> NetworkStatus:
        return NetworkStatus.CONNECTED

    async def _fetch_loop(self):
        if not self.ready:
            await self._fetch_historical_data()
            self._ready_event.set()

        while True:
            try:
                await self._wait_for_next_fetch()
                await self._fetch_live_data_loop()

            except asyncio.CancelledError:
                self.logger().info(f"Cancelling fetch loop for {self.name}...")
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error in fetch loop for {self.name}: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _wait_for_next_fetch(self):
        now_ms = TimeUtility.now_ms()
        next_fetch_time_ms = IntervalUtility.get_next_interval_timestamp(now_ms, self._config.interval)
        sleep_s = (next_fetch_time_ms - now_ms) / 1000.0
        sleep_duration = max(
            0.0,
            sleep_s,
        )
        self.logger().info(f"Sleeping for {sleep_duration}s before next fetch for {self.name}...")
        await asyncio.sleep(sleep_duration)

    async def _try_fetch_api(self, limit: int) -> list[FundingRateRecord]:
        try:
            return await self._provider.fetch_funding_rate(limit=limit)
        except Exception as e:
            self.logger().error(f"Error fetching data from API for {self.name}: {e}", exc_info=True)
            return []

    async def _fetch_historical_data(self):
        fetch_count = min(self.deque_size, BINANCE_FUNDING_RATE_COUNT_LIMIT)
        self.logger().info(f"Fetching {fetch_count} historical records from API for {self.name}...")
        api_records = await self._try_fetch_api(limit=fetch_count)

        if api_records:
            self._funding_rate_deque.extend(api_records)
            self._update_zscores_for_range(0, len(self._funding_rate_deque))
        else:
            self.logger().warning(f"API returned no historical records for {self.name}.")

        if len(self._funding_rate_deque) >= self.window:  # Check readiness based on window size
            self.logger().info(f"{self.name} is ready with {len(self._funding_rate_deque)} records.")
        else:
            self.logger().warning(
                f"{self.name} could not gather enough initial data ({len(self._funding_rate_deque)}/{self.window}). Will retry on next interval."
            )

    # TODO: more robust way to insert live data (e.g. miss N previous records, or missing live record)
    # add a fallback if new entry still not available after M retries
    async def _fetch_live_data_loop(self):
        while True:
            self.logger().info(f"Fetching live funding rate data for {self.name}...")
            latest_api_records = await self._try_fetch_api(limit=3)
            if not latest_api_records:
                self.logger().warning(f"New funding rate data not yet available for {self.name}. Retrying in 1s.")
                await asyncio.sleep(1.0)
                continue

            newest_api_record = latest_api_records[-1]
            last_deque_record = self.last_funding_rate_record
            if last_deque_record is None or newest_api_record.funding_time > last_deque_record.funding_time:
                self._funding_rate_deque.append(newest_api_record)
                update_start_index = len(self._funding_rate_deque) - 1
                self._update_zscores_for_range(update_start_index, len(self._funding_rate_deque))
                self.logger().info(
                    f"New funding rate data added for {self.name}, "
                    f"funding time: {TimeUtility.ms_to_datetime(newest_api_record.funding_time)}"
                )
                break

            else:
                self.logger().info(f"New funding rate data not yet available for {self.name}. Retrying in 1s.")
                await asyncio.sleep(1.0)

    def get_last_zscore(self) -> Optional[float]:
        if not self._funding_rate_deque:
            return None
        return self._funding_rate_deque[-1].zscore

    def get_zscore(self, records: List[FundingRateRecord]) -> float:
        if len(records) < 2:
            raise ValueError("Not enough records to calculate z-score")

        rates = [record.funding_rate for record in records]
        mean = statistics.mean(rates)
        try:
            stdev = statistics.stdev(rates)
        except statistics.StatisticsError:
            self.logger().warning(f"Stdev is 0 when calculating z-score from {len(records)} records, return 0.")
            return 0.0

        last_rate = records[-1].funding_rate
        return (last_rate - mean) / stdev

    def _update_zscores_for_range(self, start_index: int, end_index: int):
        for i in range(start_index, end_index):
            # update if record i has enough of past records and its zscore is not set
            if i >= self.window - 1 and self._funding_rate_deque[i].zscore is None:
                _start_idx = i - self.window + 1
                _end_idx = i + 1
                window_slice = list(self._funding_rate_deque)[_start_idx:_end_idx]
                zscore = self.get_zscore(window_slice)
                self._funding_rate_deque[i].zscore = zscore

    def is_updated(self) -> bool:
        if not self.ready:
            return False

        last_aligned_timestamp_ms = IntervalUtility.align_timestamp(TimeUtility.now_ms(), self._config.interval)
        last_record = self.last_funding_rate_record
        if last_record is None:
            self.logger().warning(
                f"No funding rate data available for {self.name} when checking if the data feed is updated"
            )
            return False
        return last_record.aligned_funding_time == last_aligned_timestamp_ms

    def format_status_records(self, num_records: int) -> List[Tuple[str, Any]]:
        recent_records = self.funding_rate_records[-num_records:]
        formatted = []
        for record in recent_records:
            ts = TimeUtility.ms_to_datetime(record.funding_time).strftime("%Y-%m-%d %H:%M:%S")
            rate_str = f"{record.funding_rate:.8f}"
            zscore_str = f"{record.zscore:.4f}" if record.zscore is not None else "N/A"
            formatted.append(f"  {ts} | Rate: {rate_str} | Z-Score: {zscore_str}")
        return [("Recent Funding Rates:", "\n".join(formatted))] if formatted else []

    def format_status(self) -> str:
        lines = []
        lines.append(f"  Trading Pair: {self._config.trading_pair}")
        lines.append(f"  Interval: {self._config.interval}")
        lines.append(f"  Window: {self.window}\n\n")

        record_lines = self.format_status_records(num_records=6)
        if record_lines:
            lines.append(record_lines[0][0])  # Header
            lines.append(record_lines[0][1])  # Records
        return "\n".join(lines)
