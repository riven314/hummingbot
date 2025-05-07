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
    def get_previous_interval_timestamp(
        current_timestamp_ms: int, interval: FundingRateIntervalType | TradingIntervalType
    ) -> int:
        interval_ms: int = IntervalUtility.get_duration_ms(interval)
        aligned_timestamp: int = IntervalUtility.align_timestamp(current_timestamp_ms, interval)
        # If current time is exactly on the interval boundary, return the previous one
        if current_timestamp_ms == aligned_timestamp:
            return aligned_timestamp - interval_ms
        return aligned_timestamp
