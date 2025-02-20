import os
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.event.events import FundingPaymentCompletedEvent
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class FundingRateArbitrageConfig(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []
    markets: Dict[str, Set[str]] = {}

    # Market Configuration
    spot_connector: str
    perp_connector: str
    token: str
    position_size: Decimal

    # Entry Thresholds
    entry_spread_threshold: Decimal

    # Exit Thresholds
    exit_divergence_threshold: Decimal
    take_profit_threshold: Decimal
    max_drawdown_threshold: Decimal

    @property
    def trading_pair(self) -> str:
        return f"{self.token}-USDT"


class PositionState(BaseModel):
    spot_connector: str
    perp_connector: str
    trading_pair: str
    spot_executor_id: str
    perp_executor_id: str
    spot_avg_entry_price: Decimal = Field(default=Decimal("0"))
    perp_avg_entry_price: Decimal = Field(default=Decimal("0"))
    spot_filled_amount: Decimal = Field(default=Decimal("0"))
    perp_filled_amount: Decimal = Field(default=Decimal("0"))
    spot_fee_in_quote: Decimal = Field(default=Decimal("0"))
    perp_fee_in_quote: Decimal = Field(default=Decimal("0"))
    funding_payments: List[FundingPaymentCompletedEvent] = Field(default_factory=list)

    class Config:
        allow_mutation = True

    def get_total_fees_in_quote(self) -> Decimal:
        return self.spot_fee_in_quote + self.perp_fee_in_quote

    def get_position_pnl_quote(self, current_spot_price: Decimal, current_perp_price: Decimal) -> Decimal:
        spot_pnl = (current_spot_price - self.spot_avg_entry_price) * self.spot_filled_amount
        perp_pnl = (self.perp_avg_entry_price - current_perp_price) * self.perp_filled_amount
        return spot_pnl + perp_pnl

    def is_fully_opened(self, expected_amount: Decimal, tolerance: Decimal = Decimal("0.0015")) -> bool:
        spot_deviation = abs(self.spot_filled_amount - expected_amount) / expected_amount
        perp_deviation = abs(self.perp_filled_amount - expected_amount) / expected_amount
        return spot_deviation <= tolerance and perp_deviation <= tolerance

    def get_total_funding_payments(self) -> Decimal:
        return sum([payment.amount for payment in self.funding_payments])


class FundingRateArbitrage(StrategyV2Base):
    def __init__(self, connectors: Dict[str, ConnectorBase], config: FundingRateArbitrageConfig):
        super().__init__(connectors, config)
        self.config = config
        self._position_state: Optional[PositionState] = None

    @classmethod
    def init_markets(cls, config: FundingRateArbitrageConfig):
        markets: Dict[str, Set[str]] = {
            config.spot_connector: {config.trading_pair},
            config.perp_connector: {config.trading_pair},
        }
        cls.markets = markets

    def on_tick(self):
        """Main tick handler to process strategy logic"""
        super().on_tick()
        self.track_position_status()

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        """Main entry point for creating new positions"""
        if not self.check_entry_conditions():
            return []
        return self._create_position_actions()

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        """Main exit point for closing positions"""
        if not self.check_exit_conditions():
            return []

        return self._create_stop_actions()

    def did_complete_funding_payment(self, event: FundingPaymentCompletedEvent):
        """Handle funding payment events from the perpetual market"""
        if self._position_state and self.config.trading_pair == event.trading_pair:
            self._position_state.funding_payments.append(event.dict())

    def check_entry_conditions(self) -> bool:
        """Check all preconditions before entering a new position"""
        if self._position_state is None:
            return False
        if not self.is_balance_sufficient():
            return False
        if self.get_position_spread() > self.config.entry_spread_threshold:
            return False
        return True

    def check_exit_conditions(self) -> bool:
        """Check conditions for exiting positions"""
        if self._position_state is None:
            return False

        return (
            self.get_total_pnl() >= self.config.take_profit_threshold
            or self.get_position_drawdown() >= self.config.max_drawdown_threshold
            or self.get_position_spread() >= self.config.exit_divergence_threshold
        )

    def _get_current_prices(self) -> Tuple[Decimal, Decimal, Decimal, Decimal]:
        spot_connector = self.connectors[self.config.spot_connector]
        perp_connector = self.connectors[self.config.perp_connector]
        trading_pair = self.config.trading_pair

        spot_ask = spot_connector.get_price_by_type(trading_pair, PriceType.BestAsk)
        spot_bid = spot_connector.get_price_by_type(trading_pair, PriceType.BestBid)
        perp_ask = perp_connector.get_price_by_type(trading_pair, PriceType.BestAsk)
        perp_bid = perp_connector.get_price_by_type(trading_pair, PriceType.BestBid)

        return spot_ask, spot_bid, perp_ask, perp_bid

    def _get_mid_prices(self) -> Tuple[Decimal, Decimal]:
        spot_connector = self.connectors[self.config.spot_connector]
        perp_connector = self.connectors[self.config.perp_connector]
        trading_pair = self.config.trading_pair

        spot_price = spot_connector.get_price_by_type(trading_pair, PriceType.MidPrice)
        perp_price = perp_connector.get_price_by_type(trading_pair, PriceType.MidPrice)

        return spot_price, perp_price

    def get_total_pnl(self) -> Decimal:
        if not self._position_state:
            return Decimal("0")

        spot_price, perp_price = self._get_mid_prices()
        position_pnl = self._position_state.get_position_pnl_quote(spot_price, perp_price)
        funding_pnl = self._position_state.get_total_funding_payments()

        return position_pnl + funding_pnl

    def get_position_drawdown(self) -> Decimal:
        if self._position_state is None:
            return Decimal("0")

        spot_price, perp_price = self._get_mid_prices()
        start_spot_price = self._position_state.spot_avg_entry_price
        start_perp_price = self._position_state.perp_avg_entry_price

        spot_pnl = (spot_price - start_spot_price) / start_spot_price
        perp_pnl = (start_perp_price - perp_price) / start_perp_price
        return abs(min(Decimal("0"), spot_pnl, perp_pnl))

    def get_position_spread(self) -> Decimal:
        spot_ask, spot_bid, perp_ask, perp_bid = self._get_current_prices()

        if not self._position_state:
            return (spot_ask - perp_bid) / perp_bid
        return (spot_bid - perp_ask) / perp_ask

    def is_balance_sufficient(self) -> bool:
        spot_connector = self.connectors[self.config.spot_connector]
        perp_connector = self.connectors[self.config.perp_connector]
        position_size = self.config.position_size

        spot_ask, _, _, perp_bid = self._get_current_prices()

        reserved_spot_quote_balance = spot_connector.get_available_balance("USDT") * Decimal("0.95")
        reserved_perp_quote_balance = perp_connector.get_available_balance("USDT") * Decimal("0.95")

        required_spot_balance = position_size * spot_ask
        required_perp_balance = position_size * perp_bid

        return (
            reserved_spot_quote_balance >= required_spot_balance
            and reserved_perp_quote_balance >= required_perp_balance
        )

    def track_position_status(self):
        """Track and update position status"""
        if not self._position_state:
            return

        executors = self.get_all_executors()
        spot_executor = next((e for e in executors if e.id == self._position_state.spot_executor_id), None)
        perp_executor = next((e for e in executors if e.id == self._position_state.perp_executor_id), None)

        if not spot_executor or not perp_executor:
            self._position_state = None
            return

        self._position_state.spot_filled_amount = Decimal(str(spot_executor.custom_info.get("open_filled_amount", 0)))
        self._position_state.perp_filled_amount = Decimal(str(perp_executor.custom_info.get("open_filled_amount", 0)))
        self._position_state.spot_avg_entry_price = Decimal(
            str(spot_executor.custom_info.get("current_position_average_price", 0))
        )
        self._position_state.perp_avg_entry_price = Decimal(
            str(perp_executor.custom_info.get("current_position_average_price", 0))
        )
        self._position_state.spot_fee_in_quote = Decimal(str(spot_executor.cum_fees_quote))
        self._position_state.perp_fee_in_quote = Decimal(str(perp_executor.cum_fees_quote))

    def is_position_fully_opened(self, expected_amount: Decimal) -> bool:
        if not self._position_state:
            return False
        return self._position_state.is_fully_opened(expected_amount)

    def get_funding_rate(self) -> Decimal:
        """Get current funding rate from perpetual market"""
        perp_connector = self.connectors[self.config.perp_connector]
        trading_pair = self.config.trading_pair
        funding_info = perp_connector.get_funding_info(trading_pair)
        return Decimal(str(funding_info.rate))

    # TODO: position entry price on trade event
    def _create_position_actions(self) -> List[CreateExecutorAction]:
        """Create the spot and perp position actions"""
        trading_pair = self.config.trading_pair
        position_size = self.config.position_size
        spot_config = PositionExecutorConfig(
            timestamp=self.current_timestamp,
            connector_name=self.config.spot_connector,
            trading_pair=trading_pair,
            side=TradeType.BUY,
            amount=position_size,
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
        spot_action = CreateExecutorAction(executor_config=spot_config)
        perp_action = CreateExecutorAction(executor_config=perp_config)

        # Initialize position state
        self._position_state = PositionState(
            spot_connector=self.config.spot_connector,
            perp_connector=self.config.perp_connector,
            trading_pair=trading_pair,
            spot_executor_id=spot_config.id,
            perp_executor_id=perp_config.id,
        )
        return [spot_action, perp_action]

    def _create_stop_actions(self) -> List[StopExecutorAction]:
        """Create actions to stop/close positions"""
        if not self._position_state:
            return []

        stop_actions = [
            StopExecutorAction(executor_id=self._position_state.spot_executor_id),
            StopExecutorAction(executor_id=self._position_state.perp_executor_id),
        ]
        self._position_state = None
        return stop_actions

    def format_status(self) -> str:
        """Format status for display"""
        raise NotImplementedError
