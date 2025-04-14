import asyncio
import logging
import statistics
from abc import ABC
from collections import deque
from datetime import datetime, timezone
from typing import Optional

from hummingbot.core.network_iterator import NetworkStatus  # type: ignore
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.data_feed_base import DataFeedBase
from hummingbot.data_feed.open_interest_market_cap.data_types import (
    OpenInterestMarketCapConfig,
    OpenInterestMarketCapRecord,
)
from hummingbot.data_feed.open_interest_market_cap.open_interest_providers import BinanceOpenInterestProvider
from hummingbot.data_feed.open_interest_market_cap.token_supply_providers import (
    CoinGeckoTokenSupplyProvider,
    GlassnodeTokenSupplyProvider,
)
from hummingbot.data_feed.open_interest_market_cap.utils.interval_utils import IntervalUtility
from hummingbot.data_feed.open_interest_market_cap.utils.time_utils import TimeUtility
from hummingbot.logger import HummingbotLogger


# TODO: handle the case when update interval (e.g. 10m) is higher resolution than trading interval (e.g. 1h)
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
        return TimeUtility.to_seconds(IntervalUtility.get_duration_ms(self._config.interval))

    @property
    def name(self) -> str:
        return f"{self.__class__.__name__}:{self._config.trading_pair}:{self._config.interval}"

    def get_next_update_timestamp(self) -> float:
        """
        Calculate the next update timestamp based on the configured interval, in milliseconds.

        Example:
            If current time is 2024-01-01 14:35:00 and interval is "1h":
            - The last interval started at 14:00:00
            - The next interval will start at 15:00:00

            If current time is 2024-01-01 14:35:00 and interval is "15m":
            - The last interval started at 14:30:00
            - The next interval will start at 14:45:00
        """
        now_ms = TimeUtility.now_ms()
        next_interval_ms = IntervalUtility.get_next_interval_timestamp(now_ms, self._config.interval)
        return next_interval_ms

    async def start_network(self):
        await self.stop_network()
        self.logger().info(f"Starting {self.name} fetch loop task...")
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
                next_update_timestamp_ms = self.get_next_update_timestamp()
                current_timestamp_ms = TimeUtility.now_ms()
                sleep_time = max(0, TimeUtility.to_seconds(next_update_timestamp_ms - current_timestamp_ms))
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

                await self._fetch_live_data(timestamp_ms=next_update_timestamp_ms)

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
                    zscore=None,  # no zscore in warm up period
                    is_open_interest_estimated=oi.is_estimated,
                    is_token_supply_estimated=ts.is_estimated,
                    timestamp=oi.timestamp,
                    requested_at=requested_at,
                )
            )
        self._queue[-1].zscore = self.get_last_zscore()
        self.logger().info(
            f"Successfully fetched {len(oi_results)} historical OI and Token Supplydata for {self._config.trading_pair}."
        )

    async def _fetch_live_data(self, timestamp_ms: int):
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

        # handle None or problematic returning data and fallback to previous record
        last_record = self._queue[-1]
        if ts_result is None or ts_result.total_supply is None or ts_result.total_supply == 0.0:
            token_supply = last_record.token_supply
            is_ts_estimated = True
            self.logger().warning(
                f"Live token supply fetched from {self._coingecko_token_supply_provider.name} "
                f"for {self._config.trading_pair} is None or 0 ({ts_result}), fallback to previous record ({last_record.token_supply})"
            )
        else:
            token_supply = ts_result.total_supply

        if oi_result is None or oi_result.open_interest is None or oi_result.open_interest == 0.0:
            open_interest = last_record.open_interest
            is_oi_estimated = True
            self.logger().warning(
                f"Live open interest fetched from {self._open_interest_provider.name} "
                f"for {self._config.trading_pair} is None or 0 ({oi_result}), fallback to previous record ({last_record.open_interest})"
            )
        else:
            open_interest = oi_result.open_interest

        interval_ms = IntervalUtility.get_duration_ms(self._config.interval)
        open_timestamp_ms = timestamp_ms - interval_ms
        self._queue.append(
            OpenInterestMarketCapRecord(
                open_interest_provider=self._open_interest_provider.name,
                token_supply_provider=self._coingecko_token_supply_provider.name,
                symbol=self._config.trading_pair,
                open_interest=open_interest,
                token_supply=token_supply,
                zscore=None,
                is_open_interest_estimated=is_oi_estimated,
                is_token_supply_estimated=is_ts_estimated,
                timestamp=open_timestamp_ms,
                requested_at=requested_at,
            )
        )
        self._queue[-1].zscore = self.get_last_zscore()

    def is_updated(self) -> bool:
        if not self.ready:
            return False

        now_ms = TimeUtility.now_ms()
        expected_timestamp_ms = IntervalUtility.get_last_interval_start_timestamp(now_ms, self._config.interval)
        last_record = self._queue[-1]
        _is_updated = last_record.timestamp == expected_timestamp_ms
        if not _is_updated:
            self.logger().warning(
                f"Open interest market cap data is not updated, last_record: {last_record.timestamp} vs expected: {expected_timestamp_ms}"
            )
        return _is_updated

    def get_last_zscore(self) -> float:
        ratios = [record.oi_mcap_ratio for record in self._queue]
        mean = statistics.mean(ratios)
        std_dev = statistics.stdev(ratios)
        last_ratio = ratios[-1]
        return (last_ratio - mean) / std_dev

    def format_status_records(self, record_count: int) -> str:
        lines = []
        lines.append("\nOpen Interest Market Cap Records:")

        if len(self._queue) > 0:
            # create headers
            headers = [
                "Open Interest",
                "Token Supply",
                "Timestamp",
                "Requested At",
                "OI/MCap Ratio",
                "Z-Score",
            ]

            widths = {header: len(header) for header in headers}
            sample_record = self._queue[-1]
            timestamp = datetime.fromtimestamp(sample_record.timestamp / 1000, tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            widths[headers[0]] = max(widths[headers[0]], len(f"{sample_record.open_interest:,.2f}"))
            widths[headers[1]] = max(widths[headers[1]], len(f"{sample_record.token_supply:,.2f}"))
            widths[headers[2]] = max(widths[headers[2]], len(timestamp))
            widths[headers[3]] = max(widths[headers[3]], len(sample_record.requested_at.strftime("%Y-%m-%d %H:%M:%S")))
            widths[headers[4]] = max(widths[headers[4]], len(f"{sample_record.oi_mcap_ratio:.5f}"))
            widths[headers[5]] = max(widths[headers[5]], 6)

            format_str = "  ".join(f"{{:{widths[header]}}}" for header in headers)
            lines.append(format_str.format(*headers))
            lines.append("-" * (sum(widths.values()) + 2 * (len(headers) - 1)))

            # create rows
            for record in list(self._queue)[-record_count:]:
                timestamp = datetime.fromtimestamp(record.timestamp / 1000, tz=timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                requested_at = record.requested_at.strftime("%Y-%m-%d %H:%M:%S")

                row = format_str.format(
                    f"{record.open_interest:,.2f}",
                    f"{record.token_supply:,.2f}",
                    timestamp,
                    requested_at,
                    f"{record.oi_mcap_ratio:.5f}",
                    f"{record.zscore:.4f}" if record.zscore is not None else "N/A",
                )
                lines.append(row)

        return "\n".join(lines)
