import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from hummingbot.data_feed.open_interest_market_cap.data_types import OpenInterestMarketCapRecord
from hummingbot.logger import HummingbotLogger


class OpenInterestMarketCapControllersDatabase:
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))  # type: ignore
        return cls._logger  # type: ignore

    def __init__(self, db_path: Path):
        self._db_path: Path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connect()
        self._create_table()

    def _connect(self):
        try:
            self._conn = sqlite3.connect(self._db_path)
            self._conn.row_factory = sqlite3.Row

            # set WAL journal mode to improve write performance
            # cursor = self._conn.cursor()
            # cursor.execute("PRAGMA journal_mode=WAL;")

            self.logger().info(f"Connected to OpenInterestMarketCapControllersDatabase: {self._db_path}")
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
                CREATE TABLE IF NOT EXISTS OIMCapData (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    open_interest_provider TEXT NOT NULL,
                    token_supply_provider TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    open_interest REAL,
                    token_supply REAL,
                    oi_mcap_ratio REAL,
                    zscores TEXT,
                    is_open_interest_estimated BOOLEAN,
                    is_token_supply_estimated BOOLEAN,
                    requested_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (exchange, symbol, timestamp)
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_oimcap_lookup
                ON OIMCapData (exchange, symbol, timestamp DESC)
                """
            )
            self._conn.commit()
            self.logger().info("OIMCapData table created or already exists.")
        except sqlite3.Error as e:
            self.logger().error(f"Error creating OIMCapData table: {e}", exc_info=True)
            raise

    def _round_zscores(self, zscores: Dict[int, float]) -> Dict[str, float]:
        return {str(k): round(v, 5) for k, v in zscores.items()}

    def insert_records(self, records: List[OpenInterestMarketCapRecord]) -> None:  # Return type changed to None
        if not self._conn:
            raise ConnectionError("Database connection is not available.")
        if not records:
            return

        try:
            cursor = self._conn.cursor()
            batch_size = 200
            current_time_iso = datetime.now(timezone.utc).isoformat()

            for i in range(0, len(records), batch_size):
                end_idx = i + batch_size
                batch = records[i:end_idx]
                data_to_insert = []
                for r in batch:
                    zscores_json = json.dumps(self._round_zscores(r.zscores)) if r.zscores is not None else None
                    data_to_insert.append(
                        (
                            r.exchange,
                            r.symbol,
                            r.timestamp,
                            r.open_interest,
                            r.token_supply,
                            r.oi_mcap_ratio,
                            zscores_json,
                            r.is_open_interest_estimated,
                            r.is_token_supply_estimated,
                            r.requested_at.isoformat(),
                            current_time_iso,
                            r.open_interest_provider,
                            r.token_supply_provider,
                        )
                    )

                cursor.execute("BEGIN TRANSACTION")
                cursor.executemany(
                    """
                    INSERT OR IGNORE INTO OIMCapData
                    (exchange, symbol, timestamp, open_interest, token_supply, oi_mcap_ratio,
                    zscores, is_open_interest_estimated, is_token_supply_estimated, requested_at, created_at,
                    open_interest_provider, token_supply_provider)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    data_to_insert,
                )
                cursor.execute("COMMIT")  # Commit each batch
                self.logger().info(
                    f"Inserted batch of {len(batch)} OI MCAP records ({i + len(batch)}/{len(records)} total)"
                )

        except sqlite3.Error as e:
            self.logger().error(f"Error inserting OI Market Cap records: {e}", exc_info=True)
            # Rollback if a transaction was started and failed, though individual commits per batch limit scope.
            # self._conn.rollback() # Might be needed if BEGIN TRANSACTION is outside loop.

    def get_last_timestamp(self, exchange: str, symbol: str) -> int:
        if not self._conn:
            raise ConnectionError("Database connection is not available.")
        try:
            cursor = self._conn.cursor()
            cursor.execute(
                """
                SELECT timestamp as last_ts
                FROM OIMCapData
                WHERE exchange = ? AND symbol = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (exchange, symbol),
            )
            result = cursor.fetchone()
            return result["last_ts"] if result and result["last_ts"] is not None else 0
        except sqlite3.Error as e:
            self.logger().error(f"Error fetching last timestamp for {exchange} {symbol}: {e}", exc_info=True)
            return 0

    def get_historical_records(self, exchange: str, symbol: str, limit: int) -> List[OpenInterestMarketCapRecord]:
        if not self._conn:
            raise ConnectionError("Database connection is not available.")

        db_records = []
        try:
            cursor = self._conn.cursor()
            cursor.execute(
                """
                SELECT exchange, symbol, timestamp, open_interest, token_supply, oi_mcap_ratio,
                       zscores, is_open_interest_estimated, is_token_supply_estimated, requested_at,
                       open_interest_provider, token_supply_provider
                FROM OIMCapData
                WHERE exchange = ? AND symbol = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (exchange, symbol, limit),
            )
            rows = cursor.fetchall()
            for row_data in rows:
                row_dict = dict(row_data)

                zscores_deserialized: Optional[Dict[int, float]] = None
                if row_dict["zscores"]:
                    try:
                        loaded_zscores_str_keys = json.loads(row_dict["zscores"])
                        zscores_deserialized = {int(k): float(v) for k, v in loaded_zscores_str_keys.items()}
                    except json.JSONDecodeError:
                        self.logger().error(
                            f"Error decoding zscores JSON for {exchange}-{symbol} ts {row_dict['timestamp']}: {row_dict['zscores']}"
                        )
                    except ValueError:
                        self.logger().error(
                            f"Error converting zscore keys/values for {exchange}-{symbol} ts {row_dict['timestamp']}: {row_dict['zscores']}"
                        )

                record = OpenInterestMarketCapRecord(
                    open_interest_provider=row_dict["open_interest_provider"],
                    token_supply_provider=row_dict["token_supply_provider"],
                    exchange=row_dict["exchange"],
                    symbol=row_dict["symbol"],
                    timestamp=row_dict["timestamp"],
                    open_interest=row_dict["open_interest"],
                    token_supply=row_dict["token_supply"],
                    zscores=zscores_deserialized,
                    is_open_interest_estimated=bool(row_dict["is_open_interest_estimated"]),
                    is_token_supply_estimated=bool(row_dict["is_token_supply_estimated"]),
                    requested_at=datetime.fromisoformat(row_dict["requested_at"]),
                )
                db_records.append(record)

            db_records.sort(key=lambda x: x.timestamp)
            return db_records
        except sqlite3.Error as e:
            self.logger().error(
                f"Error fetching historical OI Market Cap records for {exchange} {symbol}: {e}", exc_info=True
            )
            return []
        except Exception as e:
            self.logger().error(
                f"Unexpected error during historical record processing for {exchange} {symbol}: {e}", exc_info=True
            )
            return []

    def close(self):
        if self._conn:
            try:
                self._conn.close()
                self.logger().info("OpenInterestMarketCapControllersDatabase connection closed.")
            except sqlite3.Error as e:
                self.logger().error(f"Error closing database connection: {e}", exc_info=True)
            finally:
                self._conn = None
