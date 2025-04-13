import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.data_feed.open_interest_market_cap import constants as CONSTANTS
from hummingbot.data_feed.open_interest_market_cap.data_types import (
    HistoricalOpenInterestData,
    IntervalType,
    LiveOpenInterestData,
)
from hummingbot.data_feed.open_interest_market_cap.open_interest_providers.base import (
    OpenInterestProviderBase,
    OpenInterestProviderError,
)
from hummingbot.data_feed.open_interest_market_cap.utils.interval_utils import IntervalUtility
from hummingbot.data_feed.open_interest_market_cap.utils.time_utils import TimeUtility
from hummingbot.logger import HummingbotLogger


class BinanceOpenInterestProvider(OpenInterestProviderBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)  # type: ignore
        return cls._logger  # type: ignore

    def __init__(self, trading_pair: str):
        self._trading_pair = trading_pair
        self._throttler = AsyncThrottler(rate_limits=CONSTANTS.BINANCE_RATE_LIMITS)
        self._api_factory = WebAssistantsFactory(throttler=self._throttler)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def live_oi_endpoint(self) -> str:
        endpoint = CONSTANTS.BINANCE_OPEN_INTEREST_ENDPOINT
        url = f"{CONSTANTS.BINANCE_FUTURES_BASE_URL}{endpoint}"
        return url

    @property
    def historical_oi_endpoint(self) -> str:
        endpoint = CONSTANTS.BINANCE_HISTORICAL_OPEN_INTEREST_ENDPOINT
        url = f"{CONSTANTS.BINANCE_FUTURES_BASE_URL}{endpoint}"
        return url

    # async def close(self) -> None:
    #     if self._rest_assistant is not None and hasattr(self._rest_assistant, "_connection"):
    #         client_session = self._rest_assistant._connection._client_session
    #         if not client_session.closed:
    #             await client_session.close()
    #             self.logger().info("Closed Binance REST assistant")
    #         self._rest_assistant = None

    async def fetch_live_open_interest(self) -> Optional[LiveOpenInterestData]:
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            params = {"symbol": self._trading_pair}

            requested_at = datetime.now(timezone.utc)
            response: dict = await rest_assistant.execute_request(
                url=self.live_oi_endpoint,
                params=params,
                throttler_limit_id=CONSTANTS.BINANCE_RATE_LIMIT_ID,
                timeout=CONSTANTS.TIMEOUT,
            )  # type: ignore

            return LiveOpenInterestData(
                provider=self.__class__.__name__,
                symbol=response["symbol"],
                open_interest=float(response["openInterest"]),
                timestamp=response["time"],
                requested_at=requested_at,
            )
        except Exception as e:
            self.logger().error(f"Error fetching open interest data from Binance: {e}", exc_info=True)
            return None

    # OI raw data use closing time as timestamp
    def _validate_data_freshness(self, latest_timestamp: int, interval: IntervalType) -> None:
        current_time = TimeUtility.now_ms()
        expected_latest = IntervalUtility.align_timestamp(current_time, interval)
        if latest_timestamp != expected_latest:
            expected_dt = TimeUtility.ms_to_datetime(expected_latest).strftime("%Y-%m-%d %H:%M:%S")
            actual_dt = TimeUtility.ms_to_datetime(latest_timestamp).strftime("%Y-%m-%d %H:%M:%S")
            raise OpenInterestProviderError(f"Data not up-to-date. Expected data for {expected_dt}, got {actual_dt}")

    def _validate_api_response(self, response: List[Dict[str, Any]]) -> None:
        if len(response) == 0:
            raise OpenInterestProviderError("No historical open interest data found")

        for data_point in response:
            required_fields = ["timestamp", "sumOpenInterest", "symbol"]
            missing_fields = [field for field in required_fields if field not in data_point]
            if missing_fields:
                raise OpenInterestProviderError(f"Missing required fields: {missing_fields}")
            if float(data_point.get("sumOpenInterest", 0)) <= 0.1:
                raise OpenInterestProviderError(f"Invalid open interest value: {data_point}")

    def _get_interval_duration_ms(self, interval: IntervalType) -> int:
        return IntervalUtility.get_duration_ms(interval)

    def _is_belong_to_interval(self, timestamp: int, interval: IntervalType) -> bool:
        return IntervalUtility.is_aligned(timestamp, interval)

    # I saw missing data from binance historical OI endpoint before
    def _check_and_fill_missing_data(
        self, data: List[HistoricalOpenInterestData], interval: IntervalType
    ) -> List[HistoricalOpenInterestData]:
        if len(data) <= 1:
            raise OpenInterestProviderError("At least 2 data points are required for data validation and filling")

        interval_ms = self._get_interval_duration_ms(interval)
        start_ts = data[0].timestamp
        end_ts = data[-1].timestamp

        # Create lookup dictionary for existing data
        existing_data = {entry.timestamp: entry for entry in data}

        # Initialize result list and tracking variables
        result = []
        last_valid_entry = data[0]

        # Generate all expected timestamps and fill missing data
        current_ts = start_ts
        while current_ts <= end_ts:
            if current_ts in existing_data:
                result.append(existing_data[current_ts])
                last_valid_entry = existing_data[current_ts]
            else:
                self.logger().warning(
                    f"Missing historical open interest data at timestamp: {current_ts}, forward fill by last valid entry"
                )
                filled_entry = HistoricalOpenInterestData(
                    provider=last_valid_entry.provider,
                    symbol=last_valid_entry.symbol,
                    open_interest=last_valid_entry.open_interest,
                    timestamp=current_ts,
                    requested_at=last_valid_entry.requested_at,
                    is_estimated=True,
                )
                result.append(filled_entry)
            current_ts += interval_ms

        return result

    async def fetch_historical_open_interest(
        self, interval: IntervalType, count: int
    ) -> Optional[List[HistoricalOpenInterestData]]:
        # special case for 10m interval, we can use 5m to construct
        if interval == "10m":
            request_interval = "5m"
            request_count = count * 2 + 2
            self.logger().warning(
                f"Using 5m interval (count: {request_count}) to fetch 10m (count: {count}) historical OI data for {self._trading_pair}"
            )
        elif interval not in CONSTANTS.BINANCE_HISTORICAL_OI_INTERVALS:
            supported = ", ".join(CONSTANTS.BINANCE_HISTORICAL_OI_INTERVALS)
            raise ValueError(f"Unsupported interval: {interval}. Supported intervals: {supported}")
        else:
            request_interval = interval
            request_count = count

        if request_count > CONSTANTS.BINANCE_HISTORICAL_OI_COUNT_LIMIT:
            raise ValueError(
                f"Binance only supports a max of 500 historical open interest data points (request count: {request_count})"
            )

        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            params = {
                "symbol": self._trading_pair,
                "period": request_interval,
                "limit": request_count,
            }

            requested_at = datetime.now(timezone.utc)
            response: List[dict] = await rest_assistant.execute_request(
                url=self.historical_oi_endpoint,
                params=params,
                throttler_limit_id=CONSTANTS.BINANCE_RATE_LIMIT_ID,
                timeout=CONSTANTS.TIMEOUT,
            )  # type: ignore

            # validation pre-transformation
            self._validate_api_response(response)
            data = sorted(response, key=lambda x: x["timestamp"])

            # data transform
            interval_ms = self._get_interval_duration_ms(interval)
            result = []
            for item in data:
                # API returns closing time as timestamp, convert it to opening time
                if not self._is_belong_to_interval(item["timestamp"], interval):
                    continue
                closing_timestamp = item["timestamp"]
                opening_timestamp = closing_timestamp - interval_ms
                result.append(
                    HistoricalOpenInterestData(
                        provider=self.__class__.__name__,
                        symbol=item["symbol"],
                        open_interest=float(item["sumOpenInterest"]),
                        timestamp=opening_timestamp,
                        requested_at=requested_at,
                    )
                )

            # validation post transformation
            latest_close_timestamp = result[-1].timestamp + interval_ms
            self._validate_data_freshness(latest_close_timestamp, interval)
            result = self._check_and_fill_missing_data(result, interval)
            result = result[-count:]
            return result
        except Exception as e:
            self.logger().error(f"Error fetching historical open interest data from Binance: {e}", exc_info=True)
            return None
