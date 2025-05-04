import asyncio
import logging
import statistics
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

from hummingbot.core.network_iterator import NetworkStatus  # type: ignore
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.data_feed_base import DataFeedBase
from hummingbot.data_feed.funding_rate.constants import BINANCE_FUNDING_RATE_COUNT_LIMIT, DEFAULT_TIMEOUT
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
        self._provider: FundingRateProviderBase = BinanceFundingRateProvider(
            trading_pair=self._config.trading_pair,
            interval=self._config.interval,
        )
        self._funding_rate_deque: Deque[FundingRateRecord] = deque(maxlen=self._config.window)
        self._fetch_task: Optional[asyncio.Task] = None
        self._data_ready_event: asyncio.Event = asyncio.Event()
        self._last_update_ms: int = 0
        self._interval_ms: int = IntervalUtility.get_duration_ms(self._config.interval)

    @property
    def name(self) -> str:
        return f"{self.__class__.__name__}:{self._config.trading_pair}:{self._config.interval}"

    @property
    def funding_rate_records(self) -> List[FundingRateRecord]:
        return list(self._funding_rate_deque)

    def get_last_funding_rate_record(self) -> Optional[FundingRateRecord]:
        return self._funding_rate_deque[-1] if self._funding_rate_deque else None

    def get_last_zscore(self) -> Optional[float]:
        last_record = self.get_last_funding_rate_record()
        return last_record.zscore if last_record else None

    async def start_network(self):
        await self.stop_network()
        self.logger().info(f"Started fetch loop for {self.name}")
        self._fetch_task = safe_ensure_future(self._fetch_loop())

    async def stop_network(self):
        if self._fetch_task:
            self._fetch_task.cancel()
            try:
                await self._fetch_task
            except asyncio.CancelledError:
                pass  # Expected
            except Exception:
                self.logger().exception(f"Error cancelling fetch loop for {self.name}", exc_info=True)
            self._fetch_task = None
            self.logger().info(f"Stopped fetch loop for {self.name}")
        self._data_ready_event.clear()

    async def check_network(self) -> NetworkStatus:
        return NetworkStatus.CONNECTED

    async def _fetch_loop(self):
        try:
            # Initial population
            await self._initial_fetch()

            while True:
                now_ms = TimeUtility.now_ms()
                next_fetch_time_ms = IntervalUtility.get_next_interval_timestamp(now_ms, self._config.interval)
                sleep_duration = max(0.0, (next_fetch_time_ms - now_ms) / 1000.0)

                self.logger().debug(
                    f"{self.name}: Sleeping for {sleep_duration:.2f}s until next fetch at {TimeUtility.ms_to_datetime(next_fetch_time_ms)}"
                )
                await self._sleep(sleep_duration)

                try:
                    await self._fetch_live_data()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger().error(f"Error fetching live funding rate data for {self.name}: {e}", exc_info=True)
                    # Wait before retrying to avoid spamming logs/API
                    await self._sleep(DEFAULT_TIMEOUT)

        except asyncio.CancelledError:
            self.logger().info(f"Fetch loop cancelled for {self.name}.")
        except Exception as e:
            self.logger().critical(f"Unexpected error in fetch loop for {self.name}: {e}", exc_info=True)
        finally:
            self.logger().info(f"Fetch loop finished for {self.name}.")
            self._data_ready_event.clear()  # Mark data as not ready if loop stops

    async def _initial_fetch(self):
        self.logger().info(f"Performing initial data fetch for {self.name}...")
        # 1. Try loading from DB first
        # db_records = self._db.get_historical_records(
        #     provider=self._provider.PROVIDER_NAME, symbol=self._config.trading_pair, limit=self._config.window
        # )
        # if db_records:
        #     self._funding_rate_deque.extend(db_records)
        #     self.logger().info(f"Loaded {len(db_records)} records from DB for {self.name}.")

        # 2. Fetch from API if DB is empty or doesn't have enough data
        if len(self._funding_rate_deque) < self._config.window:
            needed = self._config.window - len(self._funding_rate_deque)
            self.logger().info(f"Fetching {needed} historical records from API for {self.name}...")
            try:
                fetch_limit = min(needed + 5, BINANCE_FUNDING_RATE_COUNT_LIMIT)
                api_records = await self._provider.fetch_funding_rate(limit=fetch_limit)

                if api_records:
                    merged_records = self._merge_and_sort_records(list(self._funding_rate_deque), api_records)
                    self._funding_rate_deque.clear()
                    self._funding_rate_deque.extend(merged_records)
                    self._db.insert_records(merged_records)
                    self.logger().info(f"Saved {len(merged_records)} new records to DB for {self.name}.")
                else:
                    self.logger().warning(f"API returned no historical records for {self.name}.")

            except Exception as e:
                self.logger().error(f"Error fetching historical data for {self.name}: {e}", exc_info=True)
                # Continue even if historical fetch fails, live data might work

        if (
            len(self._funding_rate_deque) >= self._config.window // 2
        ):  # Consider ready if we have at least half the window
            self._calculate_zscore()  # Calculate initial z-score
            self._data_ready_event.set()
            self._last_update_ms = TimeUtility.now_ms()
            self.logger().info(f"{self.name} is ready with {len(self._funding_rate_deque)} records.")
        else:
            self.logger().warning(
                f"{self.name} could not gather enough initial data ({len(self._funding_rate_deque)}/{self._config.window}). Will retry on next interval."
            )

    def _merge_and_sort_records(
        self, existing: List[FundingRateRecord], new: List[FundingRateRecord]
    ) -> List[FundingRateRecord]:
        """Merges two lists of records, ensuring uniqueness by funding_time and sorting."""
        record_map: Dict[int, FundingRateRecord] = {r.funding_time: r for r in existing}
        for r in new:
            record_map[r.funding_time] = r  # Overwrite existing if new data for same timestamp exists
        # Sort by funding_time ascending and return
        return sorted(record_map.values(), key=lambda r: r.funding_time)

    async def _fetch_live_data(self):
        self.logger().debug(f"Fetching live funding rate for {self.name}...")
        try:
            # Fetch the latest record (limit=1 might return the *last* known, not necessarily *new*)
            # Fetch a few records to be safe and find the actual latest one not already in deque
            fetch_limit = 3
            latest_records = await self._provider.fetch_funding_rate(limit=fetch_limit)

            if not latest_records:
                self.logger().warning(f"API returned no live records for {self.name}.")
                return

            last_known_time = self._funding_rate_deque[-1].funding_time if self._funding_rate_deque else 0
            new_records = [r for r in latest_records if r.funding_time > last_known_time]

            if new_records:
                # Sort new records just in case API doesn't guarantee order
                new_records.sort(key=lambda r: r.funding_time)
                self._funding_rate_deque.extend(new_records)
                self._calculate_zscore()  # Recalculate z-score with new data
                self._db.insert_records(new_records)  # Save new records
                self._last_update_ms = TimeUtility.now_ms()
                self.logger().info(
                    f"Fetched {len(new_records)} new funding rate records for {self.name}. Last z-score: {self.get_last_zscore():.4f}"
                )
                if not self.is_ready and len(self._funding_rate_deque) >= self._config.window // 2:
                    self._data_ready_event.set()
                    self.logger().info(f"{self.name} is now ready.")
            else:
                self.logger().debug(f"No new funding rate data found for {self.name}.")

        except Exception as e:
            self.logger().error(f"Error fetching live funding rate data for {self.name}: {e}", exc_info=True)

    def _calculate_zscore(self):
        if len(self._funding_rate_deque) < 2:
            # Need at least 2 points to calculate mean/stddev
            if self._funding_rate_deque:
                self._funding_rate_deque[-1].zscore = None  # Ensure zscore is None if calculation not possible
            return

        rates = [record.funding_rate for record in self._funding_rate_deque]
        mean = statistics.mean(rates)
        try:
            stdev = statistics.stdev(rates)
        except statistics.StatisticsError:  # Happens if all values are the same
            stdev = 0.0

        last_record = self._funding_rate_deque[-1]
        if stdev == 0.0:
            # Avoid division by zero; z-score is 0 if std dev is 0
            last_record.zscore = 0.0
        else:
            last_record.zscore = (last_record.funding_rate - mean) / stdev

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
        lines.append(f"Data Feed: {self.name}")
        lines.append(f"  Trading Pair: {self._config.trading_pair}")
        lines.append(f"  Interval: {self._config.interval}")
        lines.append(f"  Window: {self._config.window}")
        lines.append(f"  Status: {'Ready' if self.ready else 'Initializing'}")
        lines.append(f"  Records in Deque: {len(self._funding_rate_deque)}")

        last_record = self.get_last_funding_rate_record()
        if last_record:
            last_update_dt = TimeUtility.ms_to_datetime(self._last_update_ms)
            last_funding_dt = TimeUtility.ms_to_datetime(last_record.funding_time)
            lines.append(f"  Last Update: {last_update_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            lines.append(f"  Last Funding Time: {last_funding_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            lines.append(f"  Last Funding Rate: {last_record.funding_rate:.8f}")
            lines.append(
                f"  Last Z-Score: {last_record.zscore:.4f}" if last_record.zscore is not None else "  Last Z-Score: N/A"
            )
        else:
            lines.append("  No funding rate data available yet.")

        record_lines = self.format_status_records(num_records=5)
        if record_lines:
            lines.append(record_lines[0][0])  # Header
            lines.append(record_lines[0][1])  # Records

        return "\n".join(lines)
