import logging
import time
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
        current_time = int(time.time() * 1000)
        interval_ms = self._get_interval_duration_ms(interval)
        expected_latest = (current_time // interval_ms) * interval_ms
        if latest_timestamp != expected_latest:
            expected_dt = datetime.fromtimestamp(expected_latest / 1000).strftime("%Y-%m-%d %H:%M:%S")
            actual_dt = datetime.fromtimestamp(latest_timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S")
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
        return CONSTANTS.INTERVAL_TO_DURATION_MS[interval]

    async def fetch_historical_open_interest(
        self, interval: IntervalType, count: int
    ) -> Optional[List[HistoricalOpenInterestData]]:
        if interval not in CONSTANTS.BINANCE_HISTORICAL_OI_INTERVALS:
            supported = ", ".join(CONSTANTS.BINANCE_HISTORICAL_OI_INTERVALS)
            raise ValueError(f"Unsupported interval: {interval}. Supported intervals: {supported}")
        if count > CONSTANTS.BINANCE_HISTORICAL_OI_COUNT_LIMIT:
            raise ValueError("Binance only supports a max of 500 historical open interest data points")

        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            params = {
                "symbol": self._trading_pair,
                "period": interval,
                "limit": count,
            }

            requested_at = datetime.now(timezone.utc)
            response: List[dict] = await rest_assistant.execute_request(
                url=self.historical_oi_endpoint,
                params=params,
                throttler_limit_id=CONSTANTS.BINANCE_RATE_LIMIT_ID,
                timeout=CONSTANTS.TIMEOUT,
            )  # type: ignore

            # data validation
            self._validate_api_response(response)
            data = sorted(response, key=lambda x: x["timestamp"])
            self._validate_data_freshness(data[-1]["timestamp"], interval)

            # data transform
            interval_ms = self._get_interval_duration_ms(interval)
            result = []
            for item in data:
                # API returns closing time as timestamp, convert it to opening time
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
            return result
        except Exception as e:
            self.logger().error(f"Error fetching historical open interest data from Binance: {e}", exc_info=True)
            return None
