import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from hummingbot.data_feed.funding_rate.data_types import FundingRateRecord
from hummingbot.logger import HummingbotLogger


class FundingRateDatabase:
    _logger: Optional[HummingbotLogger] = None
    _shared_instance: Optional["FundingRateDatabase"] = None

    @classmethod
    def get_instance(cls, db_path: Optional[Path] = None) -> "FundingRateDatabase":
        if cls._shared_instance is None:
            if db_path is None:
                raise ValueError("Database path must be provided for the first instance.")
            cls._shared_instance = FundingRateDatabase(db_path)
        return cls._shared_instance

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))  # type: ignore
        return cls._logger  # type: ignore

    def __init__(self, db_path: Path):
        self._db_path: Path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._db_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure directory exists
        self._connect()
        self._create_table()

    def _connect(self):
        try:
            self._conn = sqlite3.connect(self._db_path, isolation_level=None)  # Autocommit mode
            self._conn.row_factory = sqlite3.Row  # Return rows as dict-like objects
            self.logger().info(f"Connected to FundingRate database: {self._db_path}")
        except sqlite3.Error as e:
            self.logger().error(f"Error connecting to database {self._db_path}: {e}", exc_info=True)
            raise

    def _create_table(self):
        if not self._conn:
            raise ConnectionError("Database connection is not available.")
        try:
            cursor = self._conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS FundingRate (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    funding_time INTEGER NOT NULL,
                    aligned_funding_time INTEGER NOT NULL,
                    funding_rate REAL NOT NULL,
                    mark_price REAL NOT NULL,
                    zscore REAL NULL,
                    requested_at TEXT NOT NULL,
                    UNIQUE (provider, symbol, aligned_funding_time)
                )
                """
            )
            # Add index for faster lookups
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_funding_rate_lookup
                ON FundingRate (provider, symbol, aligned_funding_time DESC)
                """
            )
            self._conn.commit()
            self.logger().info("FundingRate table created or already exists.")
        except sqlite3.Error as e:
            self.logger().error(f"Error creating FundingRate table: {e}", exc_info=True)
            raise

    def insert_records(self, records: List[FundingRateRecord]):
        if not self._conn:
            raise ConnectionError("Database connection is not available.")
        if not records:
            return

        try:
            cursor = self._conn.cursor()
            data_to_insert = [
                (
                    r.provider,
                    r.symbol,
                    r.funding_time,
                    r.aligned_funding_time,
                    r.funding_rate,
                    r.mark_price,
                    r.zscore,
                    r.requested_at.isoformat(),
                )
                for r in records
            ]
            cursor.executemany(
                """
                INSERT OR IGNORE INTO FundingRate
                (provider, symbol, funding_time, aligned_funding_time, funding_rate, mark_price, zscore, requested_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                data_to_insert,
            )
            self._conn.commit()
            self.logger().debug(f"Inserted or ignored {len(data_to_insert)} records.")
        except sqlite3.Error as e:
            self.logger().error(f"Error inserting funding rate records: {e}", exc_info=True)
            # Don't raise here, allow the feed to continue

    def get_last_timestamp(self, provider: str, symbol: str) -> Optional[int]:
        if not self._conn:
            raise ConnectionError("Database connection is not available.")
        try:
            cursor = self._conn.cursor()
            cursor.execute(
                """
                SELECT MAX(funding_time) as last_ts
                FROM FundingRate
                WHERE provider = ? AND symbol = ?
                """,
                (provider, symbol),
            )
            result = cursor.fetchone()
            return result["last_ts"] if result and result["last_ts"] is not None else None
        except sqlite3.Error as e:
            self.logger().error(f"Error fetching last timestamp for {provider}/{symbol}: {e}", exc_info=True)
            return None  # Allow feed to proceed, might fetch more history

    def get_historical_records(self, provider: str, symbol: str, limit: int) -> List[FundingRateRecord]:
        if not self._conn:
            raise ConnectionError("Database connection is not available.")
        records = []
        try:
            cursor = self._conn.cursor()
            cursor.execute(
                """
                SELECT provider, symbol, funding_time, aligned_funding_time, funding_rate, mark_price, zscore, requested_at
                FROM FundingRate
                WHERE provider = ? AND symbol = ?
                ORDER BY aligned_funding_time DESC
                LIMIT ?
                """,
                (provider, symbol, limit),
            )
            rows = cursor.fetchall()
            for row in rows:
                row_dict = dict(row)
                # Convert requested_at back to datetime
                row_dict["requested_at"] = datetime.fromisoformat(row_dict["requested_at"])
                records.append(FundingRateRecord.parse_obj(row_dict))
            # Return in ascending time order for deque population
            records.reverse()
            return records
        except sqlite3.Error as e:
            self.logger().error(f"Error fetching historical records for {provider}/{symbol}: {e}", exc_info=True)
            return []  # Return empty list on error

    def close(self):
        if self._conn:
            try:
                self._conn.close()
                self.logger().info("FundingRate database connection closed.")
                self._conn = None
                # Reset shared instance if this instance is closed
                if FundingRateDatabase._shared_instance is self:
                    FundingRateDatabase._shared_instance = None
            except sqlite3.Error as e:
                self.logger().error(f"Error closing database connection: {e}", exc_info=True)
