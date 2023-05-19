from collections import deque
from decimal import Decimal
from typing import List, Set, Dict, Tuple, Deque
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.connector.connector_base import ConnectorBase  # type: ignore
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.connector.utils import split_hb_trading_pair
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
    collect_price_samples_interval: int = 5
    price_samples_size: int = 12
    # what percentage of asset balance would you like to use for hedging trades on the taker market?
    order_size_taker_balance_factor = 99.5
    # allowed slippage in percentage to fill ensure taker orders are filled.
    slippage_buffer = 5.0


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
    collect_price_samples_interval: int = (
        CrossExchangeMarketMakingConfig.collect_price_samples_interval
    )
    price_samples_size: int = CrossExchangeMarketMakingConfig.price_samples_size
    order_size_taker_balance_factor: Decimal = Decimal(
        CrossExchangeMarketMakingConfig.order_size_taker_balance_factor
    )
    slippage_buffer: Decimal = Decimal(CrossExchangeMarketMakingConfig.slippage_buffer)

    # internal states to maintain
    # TODO: need to introduce maker to taker orders state? (e.g. _maker_to_taker_order_ids, _taker_to_maker_order_ids)
    _anti_order_adjust_timer: Dict[str, float] = {}
    _suggested_price_samples: Dict[str, Tuple[Deque[Decimal], Deque[Decimal]]] = {}
    _last_timestamp: float = 0.0

    # HIGH LEVEL INTERFACES
    def on_tick(self) -> None:
        if self.is_pending_taker_orders():
            return

        self.logger().info(
            f"Processing {self.trading_pair} tick. Maker exchange: {self.maker_exchange}"
        )
        safe_ensure_future(self.main(self.current_timestamp))  # type: ignore
        return

    async def main(self, timestamp: float) -> None:
        # TODO: change to async when scale up to multi pairs
        for trading_pair in self.trading_pairs:
            await self.process_trading_pair(trading_pair, timestamp)

        # its an async call so tiemstamp may not be self.current_timestamp
        # particularly when main runs longer than 1 sec
        self._last_timestamp = timestamp

    async def process_trading_pair(self, trading_pair: str, timestamp: float) -> None:
        timestamp = self.current_timestamp
        has_active_bid, has_active_ask = False, False
        anti_adjust_order_time = self._anti_order_adjust_timer.get(trading_pair, 0.0)

        self.update_suggested_price_samples(trading_pair, timestamp)

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
    def get_effective_hedge_price(
        self, trading_pair: str, is_buy: bool, quantity: Decimal
    ) -> Decimal:
        return DECIMAL_ZERO

    def get_maker_price(
        self, trading_pair: str, is_bid: bool, base_order_size: Decimal
    ) -> Decimal:
        return DECIMAL_ZERO

    def get_maker_order_size(self, trading_pair: str, is_bid: bool) -> Decimal:
        adjusted_size = self._get_adjusted_base_order_amount(trading_pair)
        maker_connector, taker_connector = (
            self.connectors[self.maker_exchange],
            self.connectors[self.taker_exchange],
        )
        base_asset, quote_asset = split_hb_trading_pair(trading_pair)
        fair_base_in_quote = RateOracle.get_instance().get_pair_rate(
            trading_pair
        )  # X quote / base

        if is_bid:
            # maker buy, taker sell
            maker_balance_in_base = (
                self._get_available_balance(maker_connector, quote_asset)
                / fair_base_in_quote
            )
            taker_balance_in_base = (
                self._get_available_balance(taker_connector, base_asset)
                * self.order_size_taker_balance_factor
            )
            taker_action = "selling"
            is_taker_buy = False
        else:
            # maker sell, taker buy
            maker_balance_in_base = self._get_available_balance(
                maker_connector, base_asset
            )
            taker_balance_in_base = (
                self._get_available_balance(taker_connector, quote_asset)
                * self.order_size_taker_balance_factor
            ) / fair_base_in_quote
            taker_action = "buying"
            is_taker_buy = True

        try:
            # TODO: find alternative to get_vwap_for_volume
            # TODO: double check True refers to buying order as market order
            taker_price = taker_connector.get_vwap_for_volume(
                trading_pair, is_taker_buy, taker_balance_in_base
            ).result_price
        except ZeroDivisionError:
            self.logger().warning(
                f"ZeroDivisionError on get_vwap_for_volume for taker {taker_action} {trading_pair} at "
                f"order size of {taker_balance_in_base}. No order will be submitted for maker order."
            )
            assert (
                adjusted_size == DECIMAL_ZERO
            ), f"adjusted_order_amount ({adjusted_size}) is not {DECIMAL_ZERO}"
            return DECIMAL_ZERO

        if taker_price is None:
            self.logger().warning(
                f"taker_price is None at taker action: {taker_action}. "
                "No order will be submitted for maker order."
            )
            order_amount = DECIMAL_ZERO
        else:
            order_amount = min(
                maker_balance_in_base, taker_balance_in_base, adjusted_size
            )

        return maker_connector.quantize_order_price(trading_pair, Decimal(order_amount))

    def update_suggested_price_samples(
        self, trading_pair: str, timestamp: float
    ) -> None:
        if not self._should_collect_new_price_sample(timestamp):
            return

        if trading_pair not in self._suggested_price_samples:
            self._suggested_price_samples[trading_pair] = deque(), deque()
        (
            bid_price_samples_deque,
            ask_price_samples_deque,
        ) = self._suggested_price_samples[trading_pair]

        (
            top_bid_price,
            top_ask_price,
        ) = self.get_maker_top_bid_ask_price_from_dual_source(trading_pair)
        bid_price_samples_deque.append(top_bid_price)
        ask_price_samples_deque.append(top_ask_price)
        return

    def get_maker_top_bid_ask_price_from_dual_source(
        self, trading_pair: str
    ) -> Tuple[Decimal, Decimal]:
        """
        consider 2 price sources: current snapshot & suggested price samples
        combine them to derive top bid ask price
        """
        (
            current_top_bid_price,
            current_top_ask_price,
        ) = self._get_maker_top_bid_ask_price(trading_pair)
        (
            bid_price_samples,
            ask_price_samples,
        ) = self.get_suggested_bid_ask_price_samples(trading_pair)

        # TODO: we can still use the deque even if it contains nan
        if not any(Decimal.is_nan(p) for p in bid_price_samples) and not Decimal.is_nan(
            current_top_bid_price
        ):
            top_bid_price = max(list(bid_price_samples) + [current_top_bid_price])
        else:
            top_bid_price = current_top_bid_price

        if not any(Decimal.is_nan(p) for p in ask_price_samples) and not Decimal.is_nan(
            current_top_ask_price
        ):
            top_ask_price = min(list(ask_price_samples) + [current_top_ask_price])
        else:
            top_ask_price = current_top_ask_price

        return top_bid_price, top_ask_price

    def get_suggested_bid_ask_price_samples(
        self, trading_pair: str
    ) -> Tuple[Deque[Decimal], Deque[Decimal]]:
        default_return: Tuple[deque, deque] = deque(), deque()
        return self._suggested_price_samples.get(trading_pair, default_return)

    def _get_adjusted_taker_price(
        self, trading_pair: str, is_buy: bool, base_order_amount: Decimal
    ) -> Decimal:
        return DECIMAL_ZERO

    def _get_maker_top_bid_ask_price(
        self, trading_pair: str
    ) -> Tuple[Decimal, Decimal]:
        top_ask_price = self.connectors[self.maker_exchange].get_price(
            self.trading_pair, True
        )
        top_bid_price = self.connectors[self.maker_exchange].get_price(
            self.trading_pair, False
        )
        return top_bid_price, top_ask_price

    def _should_collect_new_price_sample(self, timestamp: float) -> bool:
        last_quotient = self._last_timestamp // self._price_sample_interval
        this_quotient = timestamp // self._price_sample_interval
        return this_quotient > last_quotient

    def _get_adjusted_base_order_amount(self, trading_pair: str) -> Decimal:
        config_order_amount = self.base_order_amount
        order_amount = self.connectors[self.maker_exchange].quantize_order_amount(
            trading_pair, config_order_amount
        )
        return order_amount

    def _get_available_balance(self, connector: ConnectorBase, asset: str) -> Decimal:
        return connector.get_available_balance(asset)

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
        return True

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

    # FORMAT STATUS
    def format_status(self) -> str:
        base_asset, quote_asset = split_hb_trading_pair(self.trading_pair)
        usdt_conversion_rate = RateOracle.get_instance().get_pair_rate(
            self.trading_pair
        )  # USD/BASE
        usd_conversion_rate = RateOracle.get_instance().get_pair_rate(
            f"{base_asset}-USD"
        )
        lines = [
            "",
            f"usdt_conversion_rate: {usdt_conversion_rate}",
            f"usd_conversion_rate: {usd_conversion_rate}",
        ]
        return "\n".join(lines)

    # HELPER METHODS FOR FORMAT STATUS
