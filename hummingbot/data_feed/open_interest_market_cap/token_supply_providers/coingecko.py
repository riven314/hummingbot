"""
related documentation:
https://docs.coingecko.com/v3.0.1/reference/introduction

CONSIDERATIONS:
- antipation various scenarios of data issue from the API (e.g. missing data, wrong data, etc.)
- data resolution that the endpoint can fetch may be lower than the requested resolution at live deployment
    (e.g. 1d historical data can be fetched but live deployment is in 1h interval)
- responsibility of provider class, e.g.
    - should it be stateless (i.e. no need to store data) or stateful (i.e. need to store data)
- stateful functions are harder to maintain (i.e. those with now() or datetime.now())
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.data_feed.open_interest_market_cap import constants as CONSTANTS
from hummingbot.data_feed.open_interest_market_cap.data_types import (
    HistoricalTokenSupplyData,
    IntervalType,
    LiveTokenSupplyData,
)
from hummingbot.data_feed.open_interest_market_cap.token_supply_providers.base import (
    TokenSupplyProviderBase,
    TokenSupplyProviderError,
)
from hummingbot.logger import HummingbotLogger


# TODO: may require to add API key at request header
class CoinGeckoTokenSupplyProvider(TokenSupplyProviderBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)  # type: ignore
        return cls._logger  # type: ignore

    def __init__(self, token_id: str):
        self._token_id = token_id
        self._throttler = AsyncThrottler(rate_limits=CONSTANTS.COINGECKO_RATE_LIMITS)
        self._api_factory = WebAssistantsFactory(throttler=self._throttler)

    @property
    def markets_endpoint(self) -> str:
        return f"{CONSTANTS.COINGECKO_BASE_URL}{CONSTANTS.COINGECKO_MARKETS_ENDPOINT}"

    @property
    def market_chart_endpoint(self) -> str:
        return f"{CONSTANTS.COINGECKO_BASE_URL}/coins/{self._token_id}/market_chart"

    def _parse_last_updated(self, last_updated_str: Optional[str]) -> Optional[datetime]:
        if not last_updated_str:
            self.logger().warning(
                f"Missing field 'last_updated' in CoinGecko response for token {self._token_id}, defaulting to None"
            )
            return None
        try:
            return datetime.fromisoformat(last_updated_str.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
        except Exception:
            self.logger().error(
                f"Failed to parse last_updated field ({last_updated_str}) for token {self._token_id}, defaulting to None",
                exc_info=True,
            )
            return None

    def _parse_token_supply_from_coingecko_response(
        self, requested_at: datetime, raw_token_data: dict
    ) -> Optional[LiveTokenSupplyData]:
        try:
            total_supply = raw_token_data.get("total_supply")
            if total_supply is None:
                self.logger().warning(
                    f"Missing field 'total_supply' in CoinGecko response for token {self._token_id}, defaulting to None"
                )

            market_cap = raw_token_data.get("market_cap")
            if market_cap is None:
                self.logger().warning(
                    f"Missing field 'market_cap' in CoinGecko response for token {self._token_id}, defaulting to None"
                )

            last_updated = self._parse_last_updated(raw_token_data.get("last_updated"))

            return LiveTokenSupplyData(
                provider=self.__class__.__name__,
                token_id=self._token_id,
                total_supply=float(total_supply) if total_supply is not None else None,
                market_cap=float(market_cap) if market_cap is not None else None,
                recorded_at=last_updated,
                requested_at=requested_at,
            )
        except Exception as e:
            self.logger().error(f"Error parsing token supply data from CoinGecko response: {e}", exc_info=True)
            return None

    async def fetch_live_token_supply(self) -> Optional[LiveTokenSupplyData]:
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            params = {
                "vs_currency": "usd",
                "ids": self._token_id,
            }
            requested_at = datetime.now(timezone.utc)
            response: list[dict] = await rest_assistant.execute_request(
                url=self.markets_endpoint,
                params=params,
                throttler_limit_id=CONSTANTS.COINGECKO_RATE_LIMIT_ID,
                timeout=CONSTANTS.TIMEOUT,
            )  # type: ignore

            if not response:
                self.logger().error(
                    f"No data returned from CoinGecko markets endpoint ({response}) for token {self._token_id}"
                )
                return None

            return self._parse_token_supply_from_coingecko_response(requested_at, response[0])

        except Exception as e:
            self.logger().error(f"Error fetching live token supply data from CoinGecko: {e}", exc_info=True)
            return None

    def _validate_historical_data_entry(self, price_entry: list, market_cap_entry: list) -> bool:
        price_timestamp_ms, price = price_entry
        market_cap_timestamp_ms, market_cap = market_cap_entry
        if price_timestamp_ms is None or price is None:
            self.logger().warning(f"Missing timestamp or price in CoinGecko response for token {self._token_id}")
            return False
        if market_cap_timestamp_ms is None or market_cap is None:
            self.logger().warning(f"Missing timestamp or market_cap in CoinGecko response for token {self._token_id}")
            return False
        if price_timestamp_ms != market_cap_timestamp_ms:
            self.logger().warning(
                f"Price and market_cap timestamps ({price_timestamp_ms} vs {market_cap_timestamp_ms}) don't match for token {self._token_id}"
            )
            return False
        return True

    def _is_daily_timestamp(self, timestamp_ms: int) -> bool:
        """Check if timestamp is on a daily boundary (midnight UTC)"""
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        return dt.hour == 0 and dt.minute == 0 and dt.second == 0

    def _is_today_daily_timestamp(self, timestamp_ms: int) -> bool:
        """
        check if the last entry of token supply datais up-to-date
        """
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        now = datetime.now(timezone.utc)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return dt.date() == today.date()

    def _process_daily_data(
        self, market_caps_data: list, prices_data: list, requested_at: datetime
    ) -> list[HistoricalTokenSupplyData]:
        result: list[HistoricalTokenSupplyData] = []

        for price_entry, market_cap_entry in zip(prices_data, market_caps_data):
            if not self._validate_historical_data_entry(price_entry, market_cap_entry):
                continue

            timestamp_ms, price = price_entry
            _, market_cap = market_cap_entry

            if not self._is_daily_timestamp(timestamp_ms):
                continue

            total_supply = float(market_cap) / float(price)
            result.append(
                HistoricalTokenSupplyData(
                    provider=self.__class__.__name__,
                    token_id=self._token_id,
                    total_supply=total_supply,
                    timestamp=int(timestamp_ms),
                    requested_at=requested_at,
                )
            )

        return result

    def _validate_daily_data(self, data: list[HistoricalTokenSupplyData]) -> None:
        if not data:
            raise TokenSupplyProviderError(f"No historical data available for token {self._token_id}")

        last_result = data[-1]
        if not self._is_today_daily_timestamp(last_result.timestamp):
            raise TokenSupplyProviderError(
                f"Last historical data for token {self._token_id} is not fresh enough for interval 1d"
            )

    async def fetch_historical_daily_token_supply(self, count: int) -> Optional[list[HistoricalTokenSupplyData]]:
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            params = {"vs_currency": "usd", "days": count, "interval": "daily"}

            requested_at = datetime.now(timezone.utc)
            response = await rest_assistant.execute_request(
                url=self.market_chart_endpoint,
                params=params,
                throttler_limit_id=CONSTANTS.COINGECKO_RATE_LIMIT_ID,
                timeout=CONSTANTS.TIMEOUT,
            )

            market_caps_data: list = response.get("market_caps", [])
            prices_data: list = response.get("prices", [])

            if not market_caps_data or not prices_data:
                raise TokenSupplyProviderError(
                    f"Missing market_caps or prices fields in CoinGecko response for token {self._token_id}"
                )

            result = self._process_daily_data(market_caps_data, prices_data, requested_at)

            if len(result) < count:
                self.logger().warning(
                    f"Expected {count} historical data entries, got {len(result)} for token {self._token_id}"
                )

            result.sort(key=lambda x: x.timestamp)

            self._validate_daily_data(result)
            return result

        except Exception as e:
            self.logger().error(f"Error fetching historical token supply data from CoinGecko: {e}", exc_info=True)
            raise TokenSupplyProviderError(f"Failed to fetch historical data: {str(e)}")

    def _simulate_interval_timestamps(
        self, current_datetime: datetime, interval: IntervalType, count: int
    ) -> list[int]:
        interval_map = {
            "1m": timedelta(minutes=1),
            "10m": timedelta(minutes=10),
            "15m": timedelta(minutes=15),
            "30m": timedelta(minutes=30),
            "1h": timedelta(hours=1),
            "1d": timedelta(days=1),
        }
        delta = interval_map[interval]

        aligned_datetime = self._get_interval_aligned_datetime(current_datetime, interval)

        timestamps = []
        current_datetime = aligned_datetime
        for _ in range(count):
            timestamps.append(int(current_datetime.timestamp() * 1000))
            current_datetime -= delta

        return sorted(timestamps)

    def _get_interval_aligned_datetime(self, time: datetime, interval: IntervalType) -> datetime:
        if interval == "1d":
            return time.replace(hour=0, minute=0, second=0, microsecond=0)
        elif interval == "1h":
            return time.replace(minute=0, second=0, microsecond=0)
        else:
            minutes = time.minute
            interval_minutes = int(interval.replace("m", ""))
            aligned_minutes = (minutes // interval_minutes) * interval_minutes
            return time.replace(minute=aligned_minutes, second=0, microsecond=0)

    def _forward_fill_data(
        self, interval_timestamps: list[int], daily_data: list[HistoricalTokenSupplyData]
    ) -> list[HistoricalTokenSupplyData]:
        result = []
        daily_data_dict = {entry.timestamp: entry for entry in daily_data}
        daily_timestamps = sorted(daily_data_dict.keys())
        for timestamp_ms in interval_timestamps:
            # Find the most recent daily data point that's not after this timestamp
            idx = 0
            while idx < len(daily_timestamps) and daily_timestamps[idx] <= timestamp_ms:
                idx += 1

            if idx > 0:  # We found a valid daily data point
                daily_entry = daily_data_dict[daily_timestamps[idx - 1]]
                result.append(
                    HistoricalTokenSupplyData(
                        provider=daily_entry.provider,
                        token_id=daily_entry.token_id,
                        total_supply=daily_entry.total_supply,
                        timestamp=timestamp_ms,
                        requested_at=daily_entry.requested_at,
                    )
                )
        return result

    def _calculate_days_needed_for_forward_fill(
        self, current_datetime: datetime, interval_timestamps: list[int]
    ) -> int:
        oldest_timestamp_ms = min(interval_timestamps)
        oldest_date = datetime.fromtimestamp(oldest_timestamp_ms / 1000, tz=timezone.utc).date()
        today = current_datetime.date()
        return (today - oldest_date).days + 1

    # using CoinGecko API to get historical token supply is not accurate!!
    async def fetch_historical_token_supply(
        self, interval: IntervalType, count: int
    ) -> Optional[list[HistoricalTokenSupplyData]]:
        # if interval == "1d":
        #     return await self.fetch_historical_daily_token_supply(count)

        # now_datetime = datetime.now(timezone.utc)
        # interval_timestamps = self._simulate_interval_timestamps(now_datetime, interval, count)
        # days_needed = self._calculate_days_needed_for_forward_fill(now_datetime, interval_timestamps)
        # daily_data = await self.fetch_historical_daily_token_supply(days_needed)
        # if not daily_data:
        #     raise TokenSupplyProviderError(
        #         f"{interval} days of historical data are not available for token {self._token_id}"
        #     )

        # self.logger().info(
        #     f"Simulate {count} {interval} historical token supply data by forward filling "
        #     f"based on {days_needed} days of historical data for token {self._token_id}"
        # )
        # return self._forward_fill_data(interval_timestamps, daily_data)
        raise NotImplementedError
