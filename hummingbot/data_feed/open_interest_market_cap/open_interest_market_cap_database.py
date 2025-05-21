import os
import sqlite3
from typing import List, Optional

from hummingbot.data_feed.open_interest_market_cap.data_types import OpenInterestMarketCapRecord
from hummingbot.logger import HummingbotLogger


class OpenInterestMarketCapDatabase:
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = HummingbotLogger(__name__)
        return cls._logger

    def __init__(self, db_path: str):
        self.db_path = db_path
        db_dir = "/".join(db_path.split("/")[:-1])
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self.conn: Optional[sqlite3.Connection] = None
        self._initialize_database()

    def _initialize_database(self):
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS oi_mcap_records (
                timestamp INTEGER,
                symbol TEXT,
                open_interest_provider TEXT,
                token_supply_provider TEXT,
                open_interest REAL,
                token_supply REAL,
                zscore REAL,
                is_open_interest_estimated INTEGER,
                is_token_supply_estimated INTEGER,
                requested_at TEXT,
                PRIMARY KEY (symbol, timestamp)
            )
            """
        )
        self.conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_last_timestamp(self, trading_pair: str) -> int:
        if not self.conn:
            self.logger().warning("Database connection not initialized")
            return 0
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT MAX(timestamp) FROM oi_mcap_records WHERE symbol = ?",
            (trading_pair,),
        )
        result = cursor.fetchone()
        return result[0] if (result and result[0] is not None) else 0

    def insert_records(self, records: List[OpenInterestMarketCapRecord]) -> int:
        if not records:
            return 0
        if not self.conn:
            self.logger().error("Database connection not initialized")
            return 0

        cursor = self.conn.cursor()
        self.conn.execute("BEGIN TRANSACTION")
        try:
            insert_query = """
                INSERT INTO oi_mcap_records (
                    timestamp, symbol, open_interest_provider, token_supply_provider,
                    open_interest, token_supply, zscore,
                    is_open_interest_estimated, is_token_supply_estimated, requested_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            data = []
            for record in records:
                zscore_to_store = None
                if record.zscores:  # Check if it's not None and not empty
                    if len(record.zscores) == 1:
                        value = list(record.zscores.values())[0]
                        zscore_to_store = float(value)
                    elif len(record.zscores) > 1:
                        OpenInterestMarketCapDatabase.logger().warning(
                            f"Record for {record.symbol} at {record.timestamp} has {len(record.zscores)} z-scores. "
                            f"Storing NULL in DB. Z-scores: {record.zscores}"
                        )

                record_tuple = (
                    record.timestamp,
                    record.symbol,
                    record.open_interest_provider,
                    record.token_supply_provider,
                    float(record.open_interest),
                    float(record.token_supply),
                    zscore_to_store,  # Use the calculated value
                    int(record.is_open_interest_estimated),
                    int(record.is_token_supply_estimated),
                    record.requested_at.isoformat(),
                )
                data.append(record_tuple)
            cursor.executemany(insert_query, data)
            self.conn.commit()
            return len(data)
        except Exception as e:
            self.conn.rollback()
            self.logger().error(f"Error saving records (sample record: {records[:1]}) to database: {e}")
            return 0
