from typing import Dict

from hummingbot.data_feed.funding_rate.constants import FundingRateIntervalType, TradingIntervalType


class IntervalUtility:
    _INTERVAL_TO_MILLISECONDS: Dict[FundingRateIntervalType | TradingIntervalType, int] = {
        "1h": 60 * 60 * 1000,
        "4h": 4 * 60 * 60 * 1000,
        "8h": 8 * 60 * 60 * 1000,
    }

    @staticmethod
    def get_duration_ms(interval: FundingRateIntervalType | TradingIntervalType) -> int:
        if interval not in IntervalUtility._INTERVAL_TO_MILLISECONDS:
            raise ValueError(f"Unsupported interval: {interval}")
        return IntervalUtility._INTERVAL_TO_MILLISECONDS[interval]

    @staticmethod
    def align_timestamp(timestamp_ms: int, interval: FundingRateIntervalType | TradingIntervalType) -> int:
        interval_ms: int = IntervalUtility.get_duration_ms(interval)
        return (timestamp_ms // interval_ms) * interval_ms

    @staticmethod
    def get_next_interval_timestamp(
        current_timestamp_ms: int, interval: FundingRateIntervalType | TradingIntervalType
    ) -> int:
        interval_ms: int = IntervalUtility.get_duration_ms(interval)
        aligned_timestamp: int = IntervalUtility.align_timestamp(current_timestamp_ms, interval)
        return aligned_timestamp + interval_ms

    @staticmethod
    def get_last_interval_start_timestamp(
        timestamp_ms: int, interval: FundingRateIntervalType | TradingIntervalType
    ) -> int:
        """
        Get last interval start timestamp
        Example:
            If current time is 2024-01-01 14:35:00 and interval is "1h":
            - last interval start timestamp is 13:00:00

            If current time is 2024-01-01 14:35:00 and interval is "15m":
            - last interval start timestamp is 14:15:00
        """
        interval_ms = IntervalUtility.get_duration_ms(interval)
        aligned = IntervalUtility.align_timestamp(timestamp_ms, interval)
        return aligned - interval_ms
