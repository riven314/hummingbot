from collections import deque
from decimal import Decimal
from enum import Enum
from typing import List, Set, Dict, Tuple
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)

from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.data_type.limit_order import LimitOrder  # type: ignore

FLOAT_NAN = float("nan")
DECIMAL_NAN = Decimal("nan")
DECIMAL_ZERO = Decimal(0)


class CrossExchangeMarketMakingConfig:
    trading_pair = "ETH-USDT"
    trading_pairs = {trading_pair}
    base_order_amount: Decimal = Decimal("0.1")
    taker_exchange: str = "kucoin"
    maker_exchange: str = "bybit"
    anti_order_adjust_duration: float = 60.0


class CrossExchangeMarketMaking(ScriptStrategyBase):
    # read from config
    trading_pair: str = CrossExchangeMarketMakingConfig.trading_pair
    trading_pairs: Set[str] = CrossExchangeMarketMakingConfig.trading_pairs
    maker_exchange: str = CrossExchangeMarketMakingConfig.maker_exchange
    taker_exchange: str = CrossExchangeMarketMakingConfig.taker_exchange
    markets: Dict[str, Set[str]] = {
        maker_exchange: trading_pairs,
        taker_exchange: trading_pairs,
    }
    base_order_amount: Decimal = CrossExchangeMarketMakingConfig.base_order_amount
    anti_order_adjust_duration: float = (
        CrossExchangeMarketMakingConfig.anti_order_adjust_duration
    )

    # internal states to maintain
    # TODO: need to maker to taker orders state? (same as strategy based)
    _anti_order_adjust_timer: Dict[str, float] = {}

    # HIGH LEVEL INTERFACES
    def on_tick(self) -> None:
        if self.is_pending_taker_orders():
            return

        self.logger().info(
            f"Processing {self.trading_pair} tick. Maker exchange: {self.maker_exchange}"
        )
        safe_ensure_future(self.main())  # type: ignore
        return

    async def main(self) -> None:
        # TODO: change to async when scale up to multi pairs
        for trading_pair in self.trading_pairs:
            await self.process_trading_pair(trading_pair)

    async def process_trading_pair(self, trading_pair: str) -> None:
        timestamp = self.current_timestamp
        has_active_bid, has_active_ask = False, False
        anti_adjust_order_time = self._anti_order_adjust_timer.get(trading_pair, 0.0)

        self.update_suggested_price_samples(trading_pair)

        for active_order in self.active_maker_orders(trading_pair):
            is_buy = active_order.is_buy
            if is_buy:
                has_active_bid = True
            else:
                has_active_ask = True

            current_hedge_price = self.get_effective_hedge_price(
                trading_pair, is_buy, active_order.quantity
            )

            if not await self.is_maker_order_still_profitable(
                trading_pair, active_order, current_hedge_price
            ):
                self.cancel_maker_order(trading_pair, active_order.client_order_id)
                continue

            if not await self.is_sufficient_balance(trading_pair, active_order):
                self.cancel_maker_order(trading_pair, active_order.client_order_id)
                continue

            if (timestamp > anti_adjust_order_time) and (
                await self.is_price_drifted(trading_pair, active_order)
            ):
                self.cancel_maker_order(trading_pair, active_order.client_order_id)
                self._anti_order_adjust_timer[trading_pair] = (
                    timestamp + self.anti_order_adjust_duration
                )
                continue

        if has_active_bid and has_active_ask:
            return

        await self.check_and_create_maker_orders(
            trading_pair, has_active_bid, has_active_ask
        )
        return

    # CURATING ORDER PRICES
    def update_suggested_price_samples(self, trading_pair: str) -> None:
        pass

    def get_suggested_price_samples(self, trading_pair: str) -> Tuple[deque, deque]:
        return deque(), deque()

    def get_effective_hedge_price(
        self, trading_pair: str, is_buy: bool, quantity: Decimal
    ) -> Decimal:
        return DECIMAL_ZERO

    def get_maker_price(
        self, trading_pair: str, is_bid: bool, order_size: Decimal
    ) -> Decimal:
        return DECIMAL_ZERO

    # CHECKING CRITERIA FOR ORDER CANCELLING
    async def is_maker_order_still_profitable(
        self, trading_pair: str, order: LimitOrder, hedge_price: Decimal
    ) -> bool:
        return False

    async def is_sufficient_balance(self, trading_pair: str, order: LimitOrder) -> bool:
        return False

    async def is_price_drifted(self, trading_pair: str, order: LimitOrder) -> bool:
        return False

    def is_pending_taker_orders(self) -> bool:
        return False

    # ORDER MANAGEMENT, E.G. QUERY, CANCEL, CREATE ORDERS
    def active_maker_orders(self, trading_pair: str) -> List[LimitOrder]:
        active_orders: List[LimitOrder] = []
        return active_orders

    def cancel_maker_order(self, trading_pair: str, order_id: str) -> None:
        self.cancel(self.taker_exchange, trading_pair, order_id)

    async def check_and_create_maker_orders(
        self, trading_pair: str, has_active_bid: bool, has_active_ask: bool
    ) -> None:
        pass

    # EVENT HANDLING
    def did_create_buy_order(self, event: BuyOrderCreatedEvent) -> None:
        pass

    def did_create_sell_order(self, event: SellOrderCreatedEvent) -> None:
        pass

    def did_fill_order(self, event: OrderFilledEvent) -> None:
        pass

    def did_cancel_order(self, event: OrderCancelledEvent) -> None:
        pass

    def did_fail_order(self, event: MarketOrderFailureEvent) -> None:
        pass

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent) -> None:
        pass

    def did_complete_sell_order(self, event: SellOrderCompletedEvent) -> None:
        pass

    # HELPER METHODS OF THE ABOVE INTERFACES

    # FORMAT STATUS
    def format_status(self) -> str:
        return ""

    # HELPER METHODS FOR FORMAT STATUS
