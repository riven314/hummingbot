import os
from decimal import Decimal
from typing import Dict, List, Set

from pydantic import Field

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class TestStrategyConfig(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))

    spot_connector: str
    perp_connector: str
    token: str
    position_size: Decimal

    @property
    def trading_pair(self) -> str:
        return f"{self.token}-USDT"


class TestStrategy(StrategyV2Base):
    def __init__(self, connectors: Dict[str, ConnectorBase], config: TestStrategyConfig):
        super().__init__(connectors, config)
        self.config = config
        self._orders_created = False

    @classmethod
    def init_markets(cls, config: TestStrategyConfig):
        markets: Dict[str, Set[str]] = {
            config.spot_connector: {config.trading_pair},
            config.perp_connector: {config.trading_pair},
        }
        cls.markets = markets

    def on_tick(self):
        super().on_tick()
        funding_rate = self.get_funding_rate()
        self.logger().info(f"Current funding rate: {funding_rate}")

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        if self._orders_created:
            return []

        trading_pair = self.config.trading_pair
        position_size = self.config.position_size

        spot_config = PositionExecutorConfig(
            timestamp=self.current_timestamp,
            connector_name=self.config.spot_connector,
            trading_pair=trading_pair,
            side=TradeType.BUY,
            amount=position_size,
            # entry_price=Decimal(2.0),
            triple_barrier_config=TripleBarrierConfig(
                open_order_type=OrderType.MARKET,
                take_profit=None,
                stop_loss=None,
            ),
        )

        perp_config = PositionExecutorConfig(
            timestamp=self.current_timestamp,
            connector_name=self.config.perp_connector,
            trading_pair=trading_pair,
            side=TradeType.SELL,
            amount=position_size,
            triple_barrier_config=TripleBarrierConfig(
                open_order_type=OrderType.MARKET,
                take_profit=None,
                stop_loss=None,
            ),
        )

        self._orders_created = True
        return [CreateExecutorAction(executor_config=spot_config), CreateExecutorAction(executor_config=perp_config)]

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        return []

    def get_funding_rate(self) -> Decimal:
        perp_connector = self.connectors[self.config.perp_connector]
        trading_pair = self.config.trading_pair
        funding_info = perp_connector.get_funding_info(trading_pair)
        return Decimal(str(funding_info.rate))

    def format_status(self) -> str:
        lines = []
        executors = self.get_all_executors()
        for executor in executors:
            open_filled_amount = executor.custom_info.get("open_filled_amount", 0)
            entry_price = executor.custom_info.get("current_position_average_price", 0)
            lines.append(
                f"Status: {executor.status}, Open Filled Amount: {open_filled_amount:.4f}, Entry Price: {entry_price:.4f}, Fee: {executor.cum_fees_quote:.6f}, Custom Info: {executor.custom_info}"
            )

        return "\n".join(lines)
