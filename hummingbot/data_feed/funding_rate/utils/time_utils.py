import time
from datetime import datetime, timezone
from typing import Union


class TimeUtility:
    @staticmethod
    def now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def ms_to_datetime(timestamp_ms: Union[int, float]) -> datetime:
        return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

    @staticmethod
    def datetime_to_ms(dt: datetime) -> int:
        return int(dt.timestamp() * 1000)

    @staticmethod
    def is_valid_timestamp_ms(timestamp: Union[int, float]) -> bool:
        try:
            # Check if it's a number and within a reasonable range
            if not isinstance(timestamp, (int, float)):
                return False
            # Assuming timestamps are roughly between year 2000 and 2100
            min_ts = 946684800000  # 2000-01-01 UTC
            max_ts = 4102444800000  # 2100-01-01 UTC
            return min_ts <= timestamp <= max_ts
        except Exception:
            return False
