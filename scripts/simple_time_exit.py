import os
from decimal import Decimal
from typing import Dict, List, Set

from pydantic import Field

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class SimpleTimeExitConfig(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []
    markets: Dict[str, Set[str]] = {}

    connector_name: str = Field(
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the connector name (e.g. binance_perpetual): ", prompt_on_new=True
        ),
    )

    trading_pair: str = Field(
        client_data=ClientFieldData(prompt=lambda mi: "Enter the trading pair (e.g. BTC-USDT): ", prompt_on_new=True),
    )

    position_size: Decimal = Field(
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the position size in base asset (e.g. 0.01 BTC): ", prompt_on_new=True
        ),
    )


class SimpleTimeExit(StrategyV2Base):
    @classmethod
    def init_markets(cls, config: SimpleTimeExitConfig):
        cls.markets = {config.connector_name: {config.trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: SimpleTimeExitConfig):
        super().__init__(connectors, config)
        self.config = config
        self._active_position_executor = None

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        return []

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        if self._active_position_executor is not None:
            active_executors = self.filter_executors(
                executors=self.get_all_executors(), filter_func=lambda x: x.id == self._active_position_executor
            )
            if len(active_executors) > 0 and not active_executors[0].is_done:
                self.logger().info(
                    f"Position executor {active_executors[0].id} is not done, skip creating new position executor"
                )
                return []

            self.logger().info(f"Position executor {active_executors[0].id} is done, creating new position executor")
            self._active_position_executor = None

        triple_barrier_config = TripleBarrierConfig(
            stop_loss=None,
            take_profit=None,
            time_limit=60,
            open_order_type=OrderType.MARKET,
            time_limit_order_type=OrderType.MARKET,
        )

        position_executor_config = PositionExecutorConfig(
            timestamp=self.current_timestamp,
            trading_pair=self.config.trading_pair,
            connector_name=self.config.connector_name,
            side=TradeType.BUY,
            amount=self.config.position_size,
            triple_barrier_config=triple_barrier_config,
        )

        self._active_position_executor = position_executor_config.id
        self.logger().info(f"Creating position executor: {position_executor_config.id}")
        return [CreateExecutorAction(executor_config=position_executor_config)]

    def format_status(self) -> str:
        if not self.ready_to_trade:
            return "Strategy not ready to trade."

        lines = []
        lines.append(f"Trading pair: {self.config.trading_pair}")
        lines.append(f"Position size: {self.config.position_size}")

        if self._active_position_executor is not None:
            active_executors = self.filter_executors(
                executors=self.get_all_executors(), filter_func=lambda x: x.id == self._active_position_executor
            )
            if len(active_executors) > 0:
                executor = active_executors[0]
                lines.append(f"Active position: {executor.trading_pair}")
                lines.append(f"Entry price: {executor.entry_price}")
                lines.append(f"Time elapsed: {int(self.current_timestamp - executor.timestamp)} seconds")
        else:
            lines.append("No active position")

        return "\n".join(lines)
