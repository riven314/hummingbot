import asyncio
import logging
import math
from collections import deque
from typing import Any, Deque, List, Optional, Tuple

import pandas as pd  # type: ignore

from hummingbot.core.network_iterator import NetworkStatus  # type: ignore
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.data_feed_base import DataFeedBase
from hummingbot.data_feed.funding_rate.constants import (
    BINANCE_FUNDING_RATE_COUNT_LIMIT,
    BINANCE_TRADING_PAIR_TO_FUNDING_INTERVAL,
)
from hummingbot.data_feed.funding_rate.data_types import FundingRateConfig, FundingRateInterval, FundingRateRecord
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
        if self._config.update_interval != expected_interval:
            raise ValueError(
                f"Binance {self._config.trading_pair} expects funding interval to be {expected_interval}, but {self._config.update_interval} was provided"
            )
        self._provider: FundingRateProviderBase = BinanceFundingRateProvider(
            trading_pair=self._config.trading_pair,
        )
        self._funding_rate_deque: Deque[FundingRateRecord] = deque(maxlen=self.deque_size)
        self._fetch_task: Optional[asyncio.Task] = None

    @property
    def name(self) -> str:
        return f"{self.__class__.__name__}:{self._config.trading_pair}:{self._config.update_interval}:{self._config.trading_interval}"

    @property
    def funding_rate_records(self) -> list[FundingRateRecord]:
        return list(self._funding_rate_deque)

    @property
    def window(self) -> int:
        return self._config.window

    @property
    def deque_size(self) -> int:
        trading_interval_ms = IntervalUtility.get_duration_ms(self._config.trading_interval)
        fetch_interval_ms = IntervalUtility.get_duration_ms(self._config.update_interval)
        window_duration_ms = self._config.window * trading_interval_ms
        records_needed = math.ceil(window_duration_ms / fetch_interval_ms)
        return records_needed + 10

    @property
    def interval_ms(self) -> int:
        return IntervalUtility.get_duration_ms(self._config.update_interval)

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
        next_fetch_time_ms = IntervalUtility.get_next_interval_timestamp(now_ms, self._config.update_interval)
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
                self.logger().info(
                    f"New funding rate data added for {self.name}, "
                    f"funding time: {TimeUtility.ms_to_datetime(newest_api_record.funding_time)}"
                )
                break

            else:
                self.logger().info(f"New funding rate data not yet available for {self.name}. Retrying in 1s.")
                await asyncio.sleep(1.0)

    def is_updated(self) -> bool:
        if not self.ready:
            return False

        last_aligned_timestamp_ms = IntervalUtility.get_previous_interval_timestamp(
            TimeUtility.now_ms(), self._config.trading_interval
        )
        last_record = self.last_funding_rate_interval
        if last_record is None:
            self.logger().warning(
                f"No funding rate data available for {self.name} when checking if the data feed is updated"
            )
            return False
        return last_record.start_time == last_aligned_timestamp_ms

    def get_trading_interval_dataframe(self) -> pd.DataFrame:
        empty_df_columns = [
            "exchange",
            "symbol",
            "funding_time",
            "aligned_funding_time",
            "funding_rate",
            "mark_price",
            "requested_at",
            "is_estimated",
            "zscore",
        ]
        empty_df_index = pd.DatetimeIndex([])

        if not self._funding_rate_deque:
            self.logger().warning(
                "No funding rate data available when getting trading interval DataFrame, returning empty DataFrame"
            )
            return pd.DataFrame(columns=empty_df_columns).set_index(empty_df_index)

        records_data = [
            {
                "exchange": r.exchange,
                "symbol": r.symbol,
                "funding_time": r.funding_time,
                "aligned_funding_time": r.aligned_funding_time,
                "aligned_funding_at": r.aligned_funding_at,
                "funding_rate": r.funding_rate,
                "mark_price": r.mark_price,
                "requested_at": r.requested_at,
                "is_estimated": r.is_estimated,
            }
            for r in self._funding_rate_deque
        ]
        df = pd.DataFrame(records_data)
        df = df.set_index("aligned_funding_at")

        start_time = df.index.min()
        now_ms = TimeUtility.now_ms()
        nearest_past_hour_start_ms = IntervalUtility.get_previous_interval_timestamp(
            now_ms, self._config.trading_interval
        )
        end_time = TimeUtility.ms_to_datetime(nearest_past_hour_start_ms)

        hourly_index = pd.date_range(start=start_time, end=end_time, freq=self._config.trading_interval)
        if hourly_index.empty:
            self.logger().warning(f"hourly_index is empty from {start_time} to {end_time}, returning empty DataFrame")
            return pd.DataFrame(columns=empty_df_columns).set_index(empty_df_index)

        # reindex and forward fill
        # ensure all original indices plus the new hourly_index are considered, then select only hourly_index
        combined_index = df.index.union(hourly_index).sort_values()
        df_resampled = df.reindex(combined_index)
        df_resampled = df_resampled.ffill()
        df_resampled = df_resampled.loc[hourly_index]
        # should not be necessary if hourly_index is unique
        df_resampled = df_resampled[~df_resampled.index.duplicated(keep="first")]
        # set index to be the start timestamp of the trading interval
        df_resampled.index = df_resampled.index - pd.Timedelta(self._config.trading_interval)

        # calculate Z-score
        if len(df_resampled) >= self.window:
            rolling_mean = df_resampled["funding_rate"].rolling(window=self.window).mean()
            rolling_std = df_resampled["funding_rate"].rolling(window=self.window).std()
            df_resampled["zscore"] = (df_resampled["funding_rate"] - rolling_mean) / rolling_std
        else:
            self.logger().warning(
                f"Not enough data ({len(df_resampled)}) to calculate Z-score of window size {self.window}, returning empty DataFrame"
            )
            df_resampled["zscore"] = pd.NA

        return df_resampled[empty_df_columns]

    @property
    def funding_rate_intervals(self) -> List[FundingRateInterval]:
        df: pd.DataFrame = self.get_trading_interval_dataframe()
        if df.empty:
            return []

        intervals: List[FundingRateInterval] = []
        for timestamp_index, row_data in df.iterrows():
            start_time_ms: int = TimeUtility.datetime_to_ms(timestamp_index.to_pydatetime())
            z_score_value: Optional[float] = row_data["zscore"] if pd.notna(row_data["zscore"]) else None
            intervals.append(
                FundingRateInterval(
                    start_time=start_time_ms,
                    zscore=z_score_value,
                    funding_rate=row_data["funding_rate"],
                    mark_price=row_data["mark_price"],
                    exchange=row_data["exchange"],
                    symbol=row_data["symbol"],
                    funding_time=row_data["funding_time"],
                    aligned_funding_time=row_data["aligned_funding_time"],
                    requested_at=row_data["requested_at"],
                    is_estimated=row_data["is_estimated"],
                )
            )
        return intervals

    @property
    def last_funding_rate_interval(self) -> Optional[FundingRateInterval]:
        intervals = self.funding_rate_intervals
        return intervals[-1] if intervals else None

    def format_status_records(self, num_records: int) -> List[Tuple[str, Any]]:
        recent_records = self.funding_rate_records[-num_records:]
        formatted = []
        for record in recent_records:
            ts = TimeUtility.ms_to_datetime(record.funding_time).strftime("%Y-%m-%d %H:%M:%S")
            rate_str = f"{record.funding_rate:.8f}"
            formatted.append(f"  {ts} | Rate: {rate_str}")
        return [("Recent Funding Rates:", "\n".join(formatted))] if formatted else []

    def format_status_dataframe(self, num_records: int) -> list[tuple[str, Any]]:
        df = self.get_trading_interval_dataframe()
        if df.empty:
            return []

        formatted_lines = []
        df = df.tail(num_records)
        for timestamp, row in df.iterrows():
            ts_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            rate_str = f"{row['funding_rate']:.8f}"
            zscore_str = f"{row['zscore']:.4f}" if pd.notna(row["zscore"]) else "N/A"
            formatted_lines.append(f"  {ts_str} | Rate: {rate_str} | ZScore: {zscore_str}")
        return [("Recent Hours Statistics:", "\n".join(formatted_lines))]

    def format_status(self) -> str:
        if not self.ready:
            return "  FundingRateDataFeed is not ready yet..."

        lines = []
        lines.append(f"  Trading Pair: {self._config.trading_pair}")
        lines.append(f"  Interval: {self._config.update_interval}")
        lines.append(f"  Trading Interval: {self._config.trading_interval}")
        lines.append(f"  Window: {self.window}\n\n")

        record_lines = self.format_status_records(num_records=4)
        if record_lines:
            lines.append(record_lines[0][0])  # Header
            lines.append(record_lines[0][1])  # Records

        lines.append("\n\n")
        dataframe_lines = self.format_status_dataframe(num_records=9)
        if dataframe_lines:
            lines.append(dataframe_lines[0][0])  # Header
            lines.append(dataframe_lines[0][1])  # DataFrame

        return "\n".join(lines)
