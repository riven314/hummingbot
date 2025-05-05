import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.data_feed.funding_rate.constants import (
    BINANCE_FUNDING_RATE_ENDPOINT,
    BINANCE_FUNDING_RATE_LIMIT_ID,
    BINANCE_FUNDING_RATE_RATE_LIMITS,
    BINANCE_PERPETUAL_BASE_URL,
    BINANCE_TRADING_PAIR_TO_FUNDING_INTERVAL,
    DEFAULT_TIMEOUT,
    IntervalType,
)
from hummingbot.data_feed.funding_rate.data_types import FundingRateRecord
from hummingbot.data_feed.funding_rate.providers.base import FundingRateProviderBase, FundingRateProviderError
from hummingbot.data_feed.funding_rate.utils.interval_utils import IntervalUtility
from hummingbot.data_feed.funding_rate.utils.time_utils import TimeUtility
from hummingbot.logger import HummingbotLogger


class BinanceFundingRateProvider(FundingRateProviderBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)  # type: ignore
        return cls._logger  # type: ignore

    def __init__(self, trading_pair: str):
        if trading_pair not in BINANCE_TRADING_PAIR_TO_FUNDING_INTERVAL:
            raise FundingRateProviderError(f"Funding Interval not available for trading pair: {trading_pair}")

        self._trading_pair = trading_pair
        self._interval = BINANCE_TRADING_PAIR_TO_FUNDING_INTERVAL[trading_pair]
        self._throttler = AsyncThrottler(rate_limits=BINANCE_FUNDING_RATE_RATE_LIMITS)
        self._api_factory = WebAssistantsFactory(throttler=self._throttler)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def funding_rate_endpoint(self) -> str:
        return f"{BINANCE_PERPETUAL_BASE_URL}{BINANCE_FUNDING_RATE_ENDPOINT}"

    # TODO: drop invalid entry without raising error to reject all data
    def _validate_api_response(self, response: List[Dict[str, Any]]) -> None:
        if len(response) == 0:
            raise FundingRateProviderError("No funding rate data found")

        for data_point in response:
            required_fields = ["symbol", "fundingTime", "fundingRate", "markPrice"]
            missing_fields = [field for field in required_fields if field not in data_point]
            if missing_fields:
                raise FundingRateProviderError(f"Missing required fields: {missing_fields}")

            # validate funding rate field
            funding_rate_value = data_point.get("fundingRate")
            if isinstance(funding_rate_value, (int, float)):
                pass
            elif isinstance(funding_rate_value, str):
                try:
                    float(funding_rate_value)
                except ValueError:
                    raise FundingRateProviderError(f"Invalid fundingRate string value from Binance API: {data_point}")
            else:
                raise FundingRateProviderError(f"Invalid fundingRate type from Binance API: {data_point}")

            # validate funding time field
            funding_time_value = data_point.get("fundingTime")
            if isinstance(funding_time_value, (int, float)):
                pass
            elif isinstance(funding_time_value, str):
                try:
                    funding_time_ms = int(funding_time_value)
                    if not TimeUtility.is_valid_timestamp_ms(funding_time_ms):
                        raise FundingRateProviderError(f"Invalid millisecond timestamp for fundingTime: {data_point}")
                except ValueError:
                    raise FundingRateProviderError(f"Invalid fundingTime string value from Binance API: {data_point}")

    def _validate_data_freshness(self, latest_timestamp: int, interval: IntervalType) -> None:
        current_time_ms: int = TimeUtility.now_ms()
        expected_latest_ms: int = IntervalUtility.align_timestamp(current_time_ms, interval)
        latest_aligned_ms: int = IntervalUtility.align_timestamp(latest_timestamp, interval)

        if latest_aligned_ms != expected_latest_ms:
            expected_dt: datetime = TimeUtility.ms_to_datetime(expected_latest_ms)
            actual_dt: datetime = TimeUtility.ms_to_datetime(latest_aligned_ms)
            expected_dt_str: str = expected_dt.strftime("%Y-%m-%d %H:%M:%S")
            actual_dt_str: str = actual_dt.strftime("%Y-%m-%d %H:%M:%S")
            raise FundingRateProviderError(
                f"Data not up-to-date. Expected data for {expected_dt_str}, got {actual_dt_str}"
            )

    def _check_and_fill_missing_data(
        self, data: List[FundingRateRecord], interval: IntervalType
    ) -> List[FundingRateRecord]:
        if len(data) <= 1:
            raise FundingRateProviderError("At least 2 data points are required for data validation and filling")

        interval_ms: int = IntervalUtility.get_duration_ms(interval)
        start_ts = data[0].aligned_funding_time
        end_ts = data[-1].aligned_funding_time

        existing_data = {entry.aligned_funding_time: entry for entry in data}
        result = []
        last_valid_entry = data[0]

        current_ts = start_ts
        while current_ts <= end_ts:
            if current_ts in existing_data:
                result.append(existing_data[current_ts])
                last_valid_entry = existing_data[current_ts]
            else:
                self.logger().warning(
                    f"Missing funding rate data at timestamp: {current_ts}, forward fill by last valid entry"
                )
                filled_entry = FundingRateRecord(
                    provider=last_valid_entry.provider,
                    symbol=last_valid_entry.symbol,
                    funding_time=current_ts,
                    aligned_funding_time=current_ts,
                    funding_rate=last_valid_entry.funding_rate,
                    mark_price=last_valid_entry.mark_price,
                    requested_at=last_valid_entry.requested_at,
                    is_estimated=True,
                )
                result.append(filled_entry)
            current_ts += interval_ms

        return result

    async def fetch_funding_rate(self, limit: int) -> List[FundingRateRecord]:
        params: Dict[str, Any] = {"symbol": self._trading_pair, "limit": limit}
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            data: List[Dict[str, Any]] = await rest_assistant.execute_request(
                url=self.funding_rate_endpoint,
                params=params,
                throttler_limit_id=BINANCE_FUNDING_RATE_LIMIT_ID,
                timeout=DEFAULT_TIMEOUT,
            )  # type: ignore

            self._validate_api_response(data)

            requested_at = datetime.now(timezone.utc)
            records: List[FundingRateRecord] = []
            for item in data:
                funding_time_ms = int(item["fundingTime"])
                record = FundingRateRecord(
                    provider=self.name,
                    symbol=item["symbol"],
                    funding_time=funding_time_ms,
                    aligned_funding_time=IntervalUtility.align_timestamp(funding_time_ms, self._interval),
                    funding_rate=float(item["fundingRate"]),
                    mark_price=float(item["markPrice"]),
                    requested_at=requested_at,
                )
                records.append(record)

            records.sort(key=lambda r: r.funding_time)
            self._validate_data_freshness(records[-1].aligned_funding_time, self._interval)
            records = self._check_and_fill_missing_data(records, self._interval)
            return records[-limit:]

        except FundingRateProviderError as e:
            raise e
        except Exception as e:
            raise FundingRateProviderError(
                f"Unexpected error fetching funding rate from {self.name} for {self._trading_pair}: {e}"
            ) from e
