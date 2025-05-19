import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from hummingbot.data_feed.funding_rate.data_types import FundingRateInterval
from hummingbot.logger import HummingbotLogger


# TODO: add field sma_prices and close_price in schema
class FundingRateControllersDatabase:
    _logger: Optional[HummingbotLogger] = None

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
            self._conn = sqlite3.connect(self._db_path)
            self._conn.row_factory = sqlite3.Row

            # set WAL journal mode to improve write performance
            # cursor = self._conn.cursor()
            # cursor.execute("PRAGMA journal_mode=WAL")

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
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    start_time INTEGER NOT NULL,
                    funding_time INTEGER NOT NULL,
                    aligned_funding_time INTEGER NOT NULL,
                    funding_rate REAL NOT NULL,
                    mark_price REAL NOT NULL,
                    zscores TEXT NULL,
                    is_estimated BOOLEAN NOT NULL,
                    requested_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (exchange, symbol, start_time)
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_funding_rate_lookup
                ON FundingRate (exchange, symbol, start_time DESC)
                """
            )
            self._conn.commit()
            self.logger().info("FundingRate table created or already exists.")
        except sqlite3.Error as e:
            self.logger().error(f"Error creating FundingRate table: {e}", exc_info=True)
            raise

    def _round_zscores(self, zscores: dict[str, float]) -> dict[str, float]:
        return {k: round(v, 5) for k, v in zscores.items()}

    def insert_records(self, records: List[FundingRateInterval]) -> None:
        if not self._conn:
            raise ConnectionError("Database connection is not available.")
        if not records:
            return

        try:
            cursor = self._conn.cursor()
            batch_size = 200
            current_time = datetime.now().isoformat()

            for i in range(0, len(records), batch_size):
                end_idx = i + batch_size
                batch = records[i:end_idx]
                data_to_insert = [
                    (
                        r.exchange,
                        r.symbol,
                        r.funding_time,
                        r.aligned_funding_time,
                        r.funding_rate,
                        r.mark_price,
                        json.dumps(self._round_zscores(r.zscores)) if r.zscores is not None else None,
                        r.requested_at.isoformat(),
                        r.start_time,
                        r.is_estimated,
                        current_time,
                    )
                    for r in batch
                ]

                cursor.execute("BEGIN TRANSACTION")
                cursor.executemany(
                    """
                    INSERT OR IGNORE INTO FundingRate
                    (exchange, symbol, funding_time, aligned_funding_time, funding_rate, mark_price, zscores, requested_at, start_time, is_estimated, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    data_to_insert,
                )
                cursor.execute("COMMIT")
                self.logger().info(f"Inserted batch of {len(batch)} records ({i+len(batch)}/{len(records)} total)")

        except sqlite3.Error as e:
            self.logger().error(f"Error inserting funding rate records: {e}", exc_info=True)

    def get_last_start_time(self, exchange: str, symbol: str) -> int:
        if not self._conn:
            raise ConnectionError("Database connection is not available.")
        try:
            cursor = self._conn.cursor()
            cursor.execute(
                """
                SELECT start_time as last_ts
                FROM FundingRate
                WHERE exchange = ? AND symbol = ?
                ORDER BY start_time DESC
                LIMIT 1
                """,
                (exchange, symbol),
            )
            result = cursor.fetchone()
            return result["last_ts"] if result and result["last_ts"] is not None else 0
        except sqlite3.Error as e:
            self.logger().error(f"Error fetching last start_time for {exchange} {symbol}: {e}", exc_info=True)
            return 0

    def get_historical_records(self, exchange: str, symbol: str, limit: int) -> List[FundingRateInterval]:
        if not self._conn:
            raise ConnectionError("Database connection is not available.")
        records = []
        try:
            cursor = self._conn.cursor()
            cursor.execute(
                """
                SELECT exchange, symbol, funding_time, aligned_funding_time, funding_rate, mark_price, zscores, requested_at, start_time, is_estimated
                FROM FundingRate
                WHERE exchange = ? AND symbol = ?
                ORDER BY start_time DESC
                LIMIT ?
                """,
                (exchange, symbol, limit),
            )
            rows = cursor.fetchall()
            for row in rows:
                row_dict = dict(row)
                row_dict["requested_at"] = datetime.fromisoformat(row_dict["requested_at"])
                zscores_str = row_dict.get("zscores")
                if zscores_str:
                    try:
                        row_dict["zscores"] = json.loads(zscores_str)
                    except json.JSONDecodeError:
                        self.logger().error(
                            f"Error decoding zscores string: {zscores_str} for {exchange}, {symbol}, {row_dict['start_time']}"
                        )
                        row_dict["zscores"] = None
                else:
                    row_dict["zscores"] = None
                records.append(FundingRateInterval.parse_obj(row_dict))
            records.sort(key=lambda x: x.start_time)
            return records
        except sqlite3.Error as e:
            self.logger().error(f"Error fetching historical records for {exchange} {symbol}: {e}", exc_info=True)
            return []

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
