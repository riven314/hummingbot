from datetime import datetime, timezone
from typing import Optional


class TimeUtility:
    # Constants
    MIN_TIMESTAMP_MS = 946684800000  # 2000-01-01 00:00:00
    MAX_TIMESTAMP_MS = 4102444800000  # 2100-01-01 00:00:00

    @staticmethod
    def is_valid_timestamp_ms(timestamp_ms: int) -> bool:
        """Check if timestamp is within valid range"""
        return TimeUtility.MIN_TIMESTAMP_MS <= timestamp_ms <= TimeUtility.MAX_TIMESTAMP_MS

    @staticmethod
    def to_milliseconds(seconds: float) -> int:
        """Convert seconds to milliseconds"""
        return int(seconds * 1000)

    @staticmethod
    def to_seconds(milliseconds: int) -> float:
        """Convert milliseconds to seconds"""
        return milliseconds / 1000

    @staticmethod
    def datetime_to_ms(dt: datetime) -> int:
        """Convert datetime to milliseconds timestamp"""
        return int(dt.timestamp() * 1000)

    @staticmethod
    def ms_to_datetime(timestamp_ms: int) -> datetime:
        """Convert milliseconds timestamp to datetime"""
        return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

    @staticmethod
    def now_ms() -> int:
        """Get current time in milliseconds"""
        return TimeUtility.datetime_to_ms(datetime.now(timezone.utc))

    @staticmethod
    def parse_iso_string(iso_string: str) -> Optional[int]:
        """Parse ISO datetime string to milliseconds"""
        try:
            dt = datetime.fromisoformat(iso_string.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
            return TimeUtility.datetime_to_ms(dt)
        except (ValueError, TypeError):
            return None
