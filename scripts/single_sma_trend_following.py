"""
TODO:
1. check balance before entering position
"""

import os
from decimal import Decimal
from typing import Dict, List, Optional

import pandas as pd
from pydantic import Field

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.data_feed.candles_feed.candles_factory import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class SingleSMATrendFollowingConfig(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    markets: Dict[str, List[str]] = {}
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []

    exchange: str = Field(
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the exchange name"),
    )
    trading_pair: str = Field(
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the trading pair")
    )
    position_size: Decimal = Field(
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the position size"),
    )
    candle_interval: str = Field(
        client_data=ClientFieldData(
            prompt_on_new=True, prompt=lambda mi: "Enter the candle interval (e.g. 1d, 1h, 1m)"
        ),
    )
    sma_window: int = Field(
        gt=0,
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the rolling window length"),
    )
    sma_threshold: Decimal = Field(
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the SMA threshold (e.g. 0.05 for 5%)"),
    )
    exit_after_seconds: int = Field(
        gt=0,
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the time limit in seconds"),
    )
    leverage: int = Field(
        default=1,
        gt=0,
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Leverage (e.g. 10 for 10x)"),
    )

    @property
    def triple_barrier_config(self) -> TripleBarrierConfig:
        return TripleBarrierConfig(
            stop_loss=None,
            take_profit=None,
            time_limit=self.exit_after_seconds,
            open_order_type=OrderType.MARKET,
            time_limit_order_type=OrderType.MARKET,
        )


class SingleSMATrendFollowing(StrategyV2Base):
    account_config_set = False

    @classmethod
    def init_markets(cls, config: SingleSMATrendFollowingConfig):
        cls.markets = {config.exchange: {config.trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: SingleSMATrendFollowingConfig):
        if len(config.candles_config) == 0:
            config.candles_config.append(
                CandlesConfig(
                    connector=config.exchange,
                    trading_pair=config.trading_pair,
                    interval=config.candle_interval,
                    max_records=config.sma_window * 3,
                )
            )
        super().__init__(connectors, config)
        self.config = config
        self._is_stop_triggered = False

    def start(self, clock: Clock, timestamp: float) -> None:
        self._last_timestamp = timestamp
        self.apply_initial_settings()

    async def on_stop(self):
        # avoid corner case when the stop command is triggered and then the strategy is creating another entry
        self._is_stop_triggered = True
        self.logger().info("Stopping strategy")
        await super().on_stop()

    def get_candle_data(self, max_records: int) -> pd.DataFrame:
        candles_df = self.market_data_provider.get_candles_df(
            connector_name=self.config.exchange,
            trading_pair=self.config.trading_pair,
            interval=self.config.candle_interval,
            max_records=max_records,
        )
        return candles_df

    def is_ready_for_new_position(self) -> bool:
        active_position_executors = self.filter_executors(
            executors=self.get_all_executors(),
            filter_func=lambda e: e.connector_name == self.config.exchange
            and e.trading_pair == self.config.trading_pair
            and e.is_active,
        )
        return self.ready_to_trade and len(active_position_executors) == 0

    def is_near_candle_close(self, last_seconds_from_close: int) -> bool:
        candles_df = self.get_candle_data(max_records=2)
        if len(candles_df) == 0:
            return False

        current_candle = candles_df.iloc[-1]
        current_timestamp = self.current_timestamp
        candle_close_timestamp = current_candle["timestamp"] + pd.Timedelta(self.config.candle_interval).total_seconds()
        remaining_seconds_from_close = candle_close_timestamp - current_timestamp
        is_near_close = remaining_seconds_from_close <= last_seconds_from_close

        if is_near_close:
            candle_start_time = pd.Timestamp(current_candle["timestamp"], unit="s")
            current_time = pd.Timestamp(current_timestamp, unit="s")
            self.logger().info(
                f"Near candle close - Candle start: {candle_start_time}, Current time: {current_time}, "
                f"Current seconds until close: {remaining_seconds_from_close:.2f}"
            )
        return is_near_close

    def get_last_sma_change(self) -> Optional[Decimal]:
        candles_df = self.get_candle_data(max_records=self.config.sma_window + 10)
        if len(candles_df) < 2:
            self.logger().warning(f"Not enough candles to get SMA change for {self.config.trading_pair}")
            return None

        candles_df["sma"] = candles_df["close"].rolling(window=self.config.sma_window).mean()
        last_sma = candles_df["sma"].iloc[-1]
        prev_sma = candles_df["sma"].iloc[-2]
        if last_sma and prev_sma:
            return Decimal(str((last_sma - prev_sma) / prev_sma))
        self.logger().warning(f"Invalid SMA values for {self.config.trading_pair}: {last_sma=} and {prev_sma=}")
        return None

    def should_create_entry_now(self) -> bool:
        if not self.is_ready_for_new_position():
            return False

        if not self.is_near_candle_close(last_seconds_from_close=10):
            return False

        sma_percentage_change = self.get_last_sma_change()
        if sma_percentage_change and sma_percentage_change >= self.config.sma_threshold:
            self.logger().info(
                f"Current SMA change ({sma_percentage_change:.4%}) greater than threshold ({self.config.sma_threshold:.4%}), "
                f"entering position that will exit after {self.config.exit_after_seconds} seconds"
            )
            return True
        return False

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        if self._is_stop_triggered:
            self.logger().info("Stop triggered, skip creating entries")
            return []

        if not self.should_create_entry_now():
            return []
        return [
            CreateExecutorAction(
                executor_config=PositionExecutorConfig(
                    timestamp=self.current_timestamp,
                    connector_name=self.config.exchange,
                    trading_pair=self.config.trading_pair,
                    side=TradeType.BUY,
                    amount=self.config.position_size,
                    triple_barrier_config=self.config.triple_barrier_config,
                    leverage=self.config.leverage,
                )
            )
        ]

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        return []

    def apply_initial_settings(self):
        if not self.account_config_set:
            for connector_name, connector in self.connectors.items():
                if self.is_perpetual(connector_name):
                    connector.set_position_mode(self.config.position_mode)
                    for trading_pair in self.market_data_provider.get_trading_pairs(connector_name):
                        connector.set_leverage(trading_pair, self.config.leverage)
            self.account_config_set = True

    def format_status(self) -> str:
        if not self.ready_to_trade:
            return "Market connectors are not ready."

        lines = []
        candles_df = self.get_candle_data(max_records=self.config.sma_window + 10)
        if len(candles_df) > 0:
            candles_df["sma"] = candles_df["close"].rolling(window=self.config.sma_window).mean()
            candles_df["sma_pct_change"] = candles_df["sma"].pct_change()

            last_4_rows = candles_df.tail(4)
            lines.append("\nSMA Analysis:")
            lines.append("Timestamp               SMA Value         % Change")
            lines.append("-" * 50)

            for _, row in last_4_rows.iterrows():
                timestamp = pd.Timestamp(row["timestamp"], unit="s").strftime("%Y-%m-%d %H:%M:%S")
                sma_value = f"{row['sma']:.8f}" if pd.notna(row["sma"]) else "N/A"
                pct_change = f"{row['sma_pct_change']:.4%}" if pd.notna(row["sma_pct_change"]) else "N/A"
                lines.append(f"{timestamp}    {sma_value}    {pct_change}")

        return "\n".join(lines)
