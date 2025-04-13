"""
python -m pytest hummingbot/data_feed/open_interest_market_cap/test/test_interval_utils.py -v -s
"""

from datetime import datetime

import pytest

from hummingbot.data_feed.open_interest_market_cap.utils.interval_utils import IntervalUtility


def timestamp_ms_from_str(date_str: str) -> int:
    return int(datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)


@pytest.mark.parametrize(
    "interval,expected_ms",
    [
        ("1m", 60 * 1000),
        ("15m", 15 * 60 * 1000),
        ("1h", 60 * 60 * 1000),
        ("1d", 24 * 60 * 60 * 1000),
    ],
)
def test_get_duration_ms(interval: str, expected_ms: int):
    assert IntervalUtility.get_duration_ms(interval) == expected_ms


def test_get_duration_ms_invalid_interval():
    with pytest.raises(ValueError, match="Unsupported interval: 2s"):
        IntervalUtility.get_duration_ms("2s")


@pytest.mark.parametrize(
    "interval,supported_intervals,expected",
    [
        ("1m", ["1m", "5m"], True),
        ("15m", ["1m", "5m"], False),
        ("1h", ["1h", "4h"], True),
    ],
)
def test_is_supported(interval: str, supported_intervals: list[str], expected: bool):
    assert IntervalUtility.is_supported(interval, supported_intervals) == expected


@pytest.mark.parametrize(
    "timestamp_str,interval,expected_str",
    [
        ("2024-01-01 14:35:45", "1h", "2024-01-01 14:00:00"),
        ("2024-01-01 14:35:45", "15m", "2024-01-01 14:30:00"),
        ("2024-01-01 14:35:45", "1m", "2024-01-01 14:35:00"),
    ],
)
def test_align_timestamp(timestamp_str: str, interval: str, expected_str: str):
    timestamp_ms = timestamp_ms_from_str(timestamp_str)
    expected_ms = timestamp_ms_from_str(expected_str)
    assert IntervalUtility.align_timestamp(timestamp_ms, interval) == expected_ms


@pytest.mark.parametrize(
    "timestamp_str,interval,expected_str",
    [
        ("2024-01-01 14:35:45", "1h", "2024-01-01 15:00:00"),
        ("2024-01-01 14:35:45", "15m", "2024-01-01 14:45:00"),
        ("2024-01-01 14:35:45", "1m", "2024-01-01 14:36:00"),
        ("2024-01-01 14:00:00", "1h", "2024-01-01 15:00:00"),
    ],
)
def test_get_next_interval_timestamp(timestamp_str: str, interval: str, expected_str: str):
    timestamp_ms = timestamp_ms_from_str(timestamp_str)
    expected_ms = timestamp_ms_from_str(expected_str)
    assert IntervalUtility.get_next_interval_timestamp(timestamp_ms, interval) == expected_ms


@pytest.mark.parametrize(
    "timestamp_str,interval,expected_str",
    [
        ("2024-01-01 14:35:45", "1h", "2024-01-01 13:00:00"),
        ("2024-01-01 14:35:45", "15m", "2024-01-01 14:15:00"),
        ("2024-01-01 14:00:00", "1h", "2024-01-01 13:00:00"),
    ],
)
def test_get_previous_interval_timestamp(timestamp_str: str, interval: str, expected_str: str):
    timestamp_ms = timestamp_ms_from_str(timestamp_str)
    expected_ms = timestamp_ms_from_str(expected_str)
    assert IntervalUtility.get_last_interval_start_timestamp(timestamp_ms, interval) == expected_ms


@pytest.mark.parametrize(
    "end_str,interval,count,expected_strs",
    [
        (
            "2024-01-01 14:00:00",
            "1h",
            3,
            [
                "2024-01-01 12:00:00",
                "2024-01-01 13:00:00",
                "2024-01-01 14:00:00",
            ],
        ),
        (
            "2024-01-01 14:35:45",
            "15m",
            2,
            [
                "2024-01-01 14:15:00",
                "2024-01-01 14:30:00",
            ],
        ),
    ],
)
def test_get_interval_timestamps(end_str: str, interval: str, count: int, expected_strs: list[str]):
    end_ms = timestamp_ms_from_str(end_str)
    expected_ms = [timestamp_ms_from_str(s) for s in expected_strs]
    assert IntervalUtility.get_interval_timestamps(end_ms, interval, count) == expected_ms


def test_get_interval_timestamps_zero_count():
    timestamp_ms = timestamp_ms_from_str("2024-01-01 14:00:00")
    assert IntervalUtility.get_interval_timestamps(timestamp_ms, "1h", 0) == []


@pytest.mark.parametrize(
    "timestamp_str,interval,expected",
    [
        ("2024-01-01 14:00:00", "1h", True),
        ("2024-01-01 14:35:45", "1h", False),
        ("2024-01-01 14:30:00", "30m", True),
        ("2024-01-01 14:35:00", "5m", True),
        ("2024-01-01 14:35:01", "5m", False),
    ],
)
def test_is_aligned(timestamp_str: str, interval: str, expected: bool):
    timestamp_ms = timestamp_ms_from_str(timestamp_str)
    assert IntervalUtility.is_aligned(timestamp_ms, interval) == expected
