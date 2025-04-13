from typing import List


class IntervalUtility:
    # Map interval strings to milliseconds
    INTERVAL_TO_DURATION_MS = {
        "1m": 60 * 1000,
        "5m": 5 * 60 * 1000,
        "10m": 10 * 60 * 1000,
        "15m": 15 * 60 * 1000,
        "30m": 30 * 60 * 1000,
        "1h": 60 * 60 * 1000,
        "2h": 2 * 60 * 60 * 1000,
        "4h": 4 * 60 * 60 * 1000,
        "6h": 6 * 60 * 60 * 1000,
        "12h": 12 * 60 * 60 * 1000,
        "1d": 24 * 60 * 60 * 1000,
    }

    @staticmethod
    def get_duration_ms(interval: str) -> int:
        """Get interval duration in milliseconds"""
        if interval not in IntervalUtility.INTERVAL_TO_DURATION_MS:
            raise ValueError(f"Unsupported interval: {interval}")
        return IntervalUtility.INTERVAL_TO_DURATION_MS[interval]

    @staticmethod
    def is_supported(interval: str, supported_intervals: List[str]) -> bool:
        """Check if interval is supported"""
        return interval in supported_intervals

    @staticmethod
    def align_timestamp(timestamp_ms: int, interval: str) -> int:
        """Align timestamp to interval start"""
        interval_ms = IntervalUtility.get_duration_ms(interval)
        return (timestamp_ms // interval_ms) * interval_ms

    @staticmethod
    def get_next_interval_timestamp(timestamp_ms: int, interval: str) -> int:
        """Get next interval timestamp"""
        interval_ms = IntervalUtility.get_duration_ms(interval)
        aligned = IntervalUtility.align_timestamp(timestamp_ms, interval)
        return aligned + interval_ms

    @staticmethod
    def get_last_interval_start_timestamp(timestamp_ms: int, interval: str) -> int:
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

    @staticmethod
    def get_interval_timestamps(end_ms: int, interval: str, count: int) -> List[int]:
        """Generate sequence of interval timestamps"""
        if count <= 0:
            return []

        interval_ms = IntervalUtility.get_duration_ms(interval)
        aligned_end = IntervalUtility.align_timestamp(end_ms, interval)

        timestamps = []
        current = aligned_end
        for _ in range(count):
            timestamps.append(current)
            current -= interval_ms

        return sorted(timestamps)

    @staticmethod
    def is_aligned(timestamp_ms: int, interval: str) -> bool:
        """Check if timestamp is aligned to interval"""
        return IntervalUtility.align_timestamp(timestamp_ms, interval) == timestamp_ms
