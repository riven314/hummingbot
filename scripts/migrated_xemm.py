from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from math import ceil, floor
from typing import List, Optional, Set, Dict, Tuple, Deque, Union
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderExpiredEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
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
    # expressed in %, what percentage of asset balance would you like to use for hedging trades on the taker market?
    order_size_taker_balance_factor = 99.5
    # expressed in %, max. allowed slippage on limit taker order to ensure it is likely filled.
    hedge_slippage_buffer = 5.0
    # expressed in %
    min_profitability = 0.40


# TODO: add expiry time for hedge order
# TODO: raise log if base asset sum doesn't match and no pending hedge orders
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
    ) / Decimal("100")
    hedge_slippage_buffer: Decimal = Decimal(
        CrossExchangeMarketMakingConfig.hedge_slippage_buffer
    ) / Decimal("100")
    min_profitability: Decimal = Decimal(
        CrossExchangeMarketMakingConfig.min_profitability
    ) / Decimal("100")

    # internal states to maintain
    # TODO: need to introduce maker to taker orders state? (e.g. _maker_to_taker_order_ids, _taker_to_maker_order_ids)
    _anti_order_adjust_timer: Dict[str, float] = {}
    _suggested_price_samples: Dict[str, Tuple[Deque[Decimal], Deque[Decimal]]] = {}
    _last_timestamp: float = 0.0

    # HIGH LEVEL INTERFACES
    def on_tick(self) -> None:
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
        if self.is_pending_hedge_orders(trading_pair):
            self.logger().info(
                f"[{trading_pair}] There are pending hedge orders, skip processing the trading pair..."
            )
            return

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

            current_hedge_price = self.get_taker_hedge_price(
                trading_pair, is_buy, active_order.quantity
            )

            if current_hedge_price is None or Decimal.is_nan(current_hedge_price):
                self.logger().info(
                    f"[{trading_pair}] Cancelling active maker order (client_order_id: {active_order.client_order_id}) "
                    f"because hedge price is None or nan ({current_hedge_price}). "
                    "No buffer time before next maker order creation."
                )
                self.cancel_maker_order(trading_pair, active_order.client_order_id)
                continue

            if not await self.is_maker_order_still_profitable(
                trading_pair, active_order, current_hedge_price
            ):
                self.logger().info(
                    f"[{trading_pair}] Cancelling active maker order (client_order_id: {active_order.client_order_id}) "
                    f"because it is no longer profitable based on min_profitability."
                    "No buffer time before next maker order creation."
                )
                self.cancel_maker_order(trading_pair, active_order.client_order_id)
                continue

            if not await self.is_sufficient_balance(trading_pair, active_order):
                self.logger().info(
                    f"[{trading_pair}] Cancelling active maker order (client_order_id: {active_order.client_order_id}) "
                    f"because there is no sufficient balances in either taker exchange or maker exchange. "
                    "No buffer time before next maker order creation."
                )
                self.cancel_maker_order(trading_pair, active_order.client_order_id)
                continue

            if (timestamp > anti_adjust_order_time) and (
                self.is_price_drifted(trading_pair, active_order)
            ):
                updated_anti_order_adjust_timestamp = (
                    timestamp + self.anti_order_adjust_duration
                )
                self.logger().info(
                    f"[{trading_pair}] Cancelling active maker order (client_order_id: {active_order.client_order_id}) "
                    f"because order book price from maker exchange is detected to be drifted. "
                    f"Set buffer time so that no new maker order is created before: {updated_anti_order_adjust_timestamp}."
                )
                self.cancel_maker_order(trading_pair, active_order.client_order_id)
                self._anti_order_adjust_timer[
                    trading_pair
                ] = updated_anti_order_adjust_timestamp
                continue

        if has_active_bid and has_active_ask:
            return

        # TODO: place orders in batch on both side
        if not has_active_bid:
            await self.check_and_create_maker_orders(trading_pair, is_bid=True)
        if not has_active_ask:
            await self.check_and_create_maker_orders(trading_pair, is_bid=False)
        return

    # CALCULATING PRICES
    def get_fair_price_in_quote(self, trading_pair: str) -> Decimal:
        # TODO: trading_pair may not be available in binance's rate oracle
        return RateOracle.get_instance().get_pair_rate(trading_pair)

    def get_taker_hedge_price(
        self, trading_pair: str, is_taker_buy: bool, base_order_size: Decimal
    ) -> Optional[Decimal]:
        taker_connector = self.connectors[self.taker_exchange]
        # prepare detailed logging for warning/ error
        verbose_footprint = (
            f"trading_pair: {trading_pair}, taker exchange: {self.taker_exchange}, "
            f"base order size: {base_order_size}, is_taker_buy: {is_taker_buy}."
        )
        try:
            taker_price = taker_connector.get_vwap_for_volume(
                trading_pair, is_taker_buy, base_order_size
            ).result_price

        except ZeroDivisionError:
            self.logger().warning(
                "ZeroDivisionError on get_vwap_for_volume for:\n" + verbose_footprint
            )
            return DECIMAL_NAN

        if taker_price is None:
            self.logger().error(
                "taker_price is None on get_maker_price:\n" + verbose_footprint
            )
            return None
        return taker_price

    def get_price_with_slippage(self, price: Decimal, is_buy: bool) -> Decimal:
        # if slippage occur, you may buy at a higher price or sell at a lower price
        slippage_factor = (
            Decimal("1") + self.hedge_slippage_buffer
            if is_buy
            else Decimal("1") + self.hedge_slippage_buffer
        )
        return price * slippage_factor

    def get_maker_price(
        self, trading_pair: str, is_bid: bool, base_order_size: Decimal
    ) -> Optional[Decimal]:
        # taker action is opposite of maker action
        is_taker_buy = not is_bid
        taker_price = self.get_taker_hedge_price(
            trading_pair, is_taker_buy, base_order_size
        )
        if taker_price is None or Decimal.is_nan(taker_price):
            self.logger().error(
                f"taker_price from get_maker_price is None or nan ({taker_price}). "
            )
            return taker_price

        maker_price = (
            taker_price / (1 + self.min_profitability)
            if is_bid
            else taker_price * (1 + self.min_profitability)
        )

        # TODO: add feature to conditionally put price slightly better than top bid/ top ask
        maker_connector = self.connectors[self.maker_exchange]
        price_quantum = maker_connector.get_order_price_quantum(
            trading_pair, maker_price
        )
        # round down for maker bid and round up for maker ask to ensure profitability
        rounding_func = floor if is_bid else ceil
        return Decimal(rounding_func(maker_price / price_quantum)) * price_quantum

    def get_maker_order_size(self, trading_pair: str, is_bid: bool) -> Decimal:
        adjusted_size = self._get_adjusted_base_order_amount(trading_pair)
        maker_connector, taker_connector = (
            self.connectors[self.maker_exchange],
            self.connectors[self.taker_exchange],
        )
        # X quote / base
        base_asset, quote_asset = split_hb_trading_pair(trading_pair)
        fair_base_in_quote = self.get_fair_price_in_quote(trading_pair)

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
            is_taker_buy = True

        # TODO: find alternative to get_vwap_for_volume
        # TODO: double check True refers to buying order as market order
        taker_price = self.get_taker_hedge_price(
            trading_pair, is_taker_buy, adjusted_size
        )

        if taker_price is None:
            self.logger().warning(
                "taker_price is None at get_maker_order_size. No order will be submitted for maker order."
            )
            return DECIMAL_ZERO
        if Decimal.is_nan(taker_price):
            self.logger().warning(
                "taker_price is Decimal nan at get_maker_order_size. No order will be submitted for maker order."
            )
            assert (
                adjusted_size == DECIMAL_ZERO
            ), f"adjusted_order_amount ({adjusted_size}) is not {DECIMAL_ZERO}"
            return DECIMAL_ZERO

        order_amount = min(maker_balance_in_base, taker_balance_in_base, adjusted_size)
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

    def _get_maker_top_bid_ask_price(
        self, trading_pair: str
    ) -> Tuple[Decimal, Decimal]:
        top_ask_price = self.connectors[self.maker_exchange].get_price(
            trading_pair, True
        )
        top_bid_price = self.connectors[self.maker_exchange].get_price(
            trading_pair, False
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
        if hedge_price is None:
            self.logger().warning(
                "hedge_price from is_maker_order_still_profitable is None, returning False for order profitability."
            )
            return False

        # TODO: set expected vs ideal profitability, cancel when its worse than expected
        is_maker_buy = order.is_buy
        order_price = order.price
        if is_maker_buy:
            is_profitable = order_price > hedge_price / (1 + self.min_profitability)
        else:
            is_profitable = order_price < hedge_price * (1 + self.min_profitability)

        if is_profitable:
            limit_order_type_str = "bid" if is_maker_buy else "ask"
            base_asset, quote_asset = split_hb_trading_pair(trading_pair)
            price, amount = order.price, order.quantity
            self.logger().info(
                f"[{trading_pair}] Maker {limit_order_type_str} order at "
                f"{amount:.8f} {base_asset} @ {price:.8f} {quote_asset} is "
                f"no longer profitable in maker exchange {self.maker_exchange}."
            )
        return is_profitable

    async def is_sufficient_balance(self, trading_pair: str, order: LimitOrder) -> bool:
        return False

    def is_price_drifted(self, trading_pair: str, order: LimitOrder) -> bool:
        is_buy = order.is_buy
        order_price = order.price
        order_quantity = order.quantity
        suggested_price = self.get_maker_price(trading_pair, is_buy, order_quantity)
        is_drifted = suggested_price != order_price
        if is_drifted:
            base_asset, quote_asset = split_hb_trading_pair(trading_pair)
            order_action = "bid" if is_buy else "ask"
            self.logger().info(
                f"({trading_pair}) The current limit {order_action} order for "
                f"{order.quantity} {base_asset} at "
                f"{order_price:.8g} {quote_asset} is now inferrior than the suggested order "
                f"price at {suggested_price}.",
            )
        return is_drifted

    def is_pending_hedge_orders(self, trading_pair: str) -> bool:
        return True

    # ORDER MANAGEMENT, E.G. QUERY, CANCEL, CREATE ORDERS
    def active_maker_orders(self, trading_pair: str) -> List[LimitOrder]:
        active_orders: List[LimitOrder] = [
            order
            for order in self.get_active_orders(connector_name=self.maker_exchange)
            if order.trading_pair == trading_pair
            and order.order_type == OrderType.LIMIT
        ]
        return active_orders

    def cancel_maker_order(self, trading_pair: str, order_id: str) -> None:
        self.cancel(self.taker_exchange, trading_pair, order_id)

    async def check_and_create_maker_orders(
        self, trading_pair: str, is_bid: bool
    ) -> None:
        # TODO: handle the case when order_size doesn't meet the minimum order size from taker exchange
        order_size = self.get_maker_order_size(trading_pair, is_bid)
        order_action = "bid" if is_bid else "ask"
        if order_size <= DECIMAL_ZERO:
            self.logger().warning(
                f"[{trading_pair}] "
                f"Order book on taker is too thin to place order. "
                f"Skip creating maker {order_action} order with size {order_size:.8f}."
            )
            return

        order_price = self.get_maker_price(trading_pair, True, order_size)
        if order_price is None:
            self.logger().warning(
                f"[{trading_pair}] "
                f"Maker bid price is None. "
                f"Skip creating maker {order_action} order with size {order_size:.8f}."
            )
        elif Decimal.is_nan(order_price):
            self.logger().warning(
                f"[{trading_pair}] "
                f"Maker bid price is NaN because taker order book is too. "
                f"Skip creating maker {order_action} order with size {order_size:.8f}."
            )
            return

        taker_hedge_price = self.get_taker_hedge_price(trading_pair, is_bid, order_size)
        base_asset, quote_asset = split_hb_trading_pair(trading_pair)
        self.logger().info(
            f"[{trading_pair}] Creating limit {order_action} order for "
            f"{order_size:.8f} {base_asset} at {order_price:.8f} {quote_asset}. "
            f"Current hedging price: {taker_hedge_price:.8f} {quote_asset}."
        )
        order_func = self.buy if is_bid else self.sell
        order_func(
            self.maker_exchange,
            trading_pair=trading_pair,
            amount=order_size,
            order_type=OrderType.LIMIT,
            price=order_price,
        )
        return

    def check_and_hedge_maker_orders(
        self, trading_pair: str, base_order_size: Decimal, is_taker_buy: bool
    ) -> None:
        taker_exchange = self.taker_exchange
        order_action = "BUY" if is_taker_buy else "SELL"

        # determine limit order size for the hedge
        base_asset, quote_asset = split_hb_trading_pair(trading_pair)
        if is_taker_buy:
            fair_base_in_quote = self.get_fair_price_in_quote(trading_pair)
            taker_balance_in_quote = (
                self._get_available_balance(self.taker_exchange, quote_asset)
                * self.order_size_taker_balance_factor
            )
            taker_balance_in_base = taker_balance_in_quote / fair_base_in_quote
        else:
            taker_balance_in_base = (
                self._get_available_balance(self.taker_exchange, base_asset)
                * self.order_size_taker_balance_factor
            )

        if taker_balance_in_base < base_order_size:
            self.logger().warning(
                f"[{trading_pair}] Balance from taker exchange ({taker_exchange}) is: "
                f"{taker_balance_in_base:.8f} converted in {base_asset}. "
                f"Its not enough to send a {order_action} order of target order size of {base_order_size:.8f} in {base_asset}. "
                f"Adjust the order size to be {taker_balance_in_base:.8f} in {base_asset}."
            )
            adjusted_order_size = taker_balance_in_base
        else:
            adjusted_order_size = base_order_size

        # determine limit order price for the hedge
        taker_hedge_price = self.get_taker_hedge_price(
            trading_pair, is_taker_buy, adjusted_order_size
        )
        if taker_hedge_price is None or Decimal.is_nan(taker_hedge_price):
            self.logger().info(
                f"taker_hedge_price from check_and_hedge_maker_orders is None or nan ({taker_hedge_price})."
                f"Skip placing hedge order on taker exchange: {taker_exchange}."
            )
            return

        hedge_price_with_slippage = self.get_price_with_slippage(
            taker_hedge_price, is_buy=is_taker_buy
        )

        # submit hedge order in taker exchange
        quantized_adjusted_order_size = self.connectors[
            taker_exchange
        ].quantize_order_amount(trading_pair, Decimal(adjusted_order_size))
        order_func = self.buy if is_taker_buy else self.sell
        self.logger().info(
            f"[{trading_pair}] Submitting {order_action} order as a hedge in taker exchange ({taker_exchange}). "
            f"Order size: {quantized_adjusted_order_size:.8f} in {base_asset}. "
            f"Order price: {hedge_price_with_slippage:.8f} in {quote_asset}."
        )
        order_func(
            taker_exchange,
            trading_pair,
            amount=quantized_adjusted_order_size,
            order_type=OrderType.LIMIT,
            price=hedge_price_with_slippage,
        )
        return

    # EVENT HANDLING ON ORDER STATUS CHANGE
    def did_fill_order(self, event: OrderFilledEvent) -> None:
        order_id = event.order_id
        if self._is_exchange_active_order(self.maker_exchange, order_id):
            order_action = "BUY" if event.trade_type == TradeType.BUY else "SELL"
            base_asset, quote_asset = split_hb_trading_pair(event.trading_pair)
            self.logger().info(
                f"[{event.trading_pair}] Maker {order_action} order is filled: "
                f"{event.amount:.8f} {base_asset} @ {event.price:.8f} {quote_asset}. "
                f"order_id: {event.order_id}, exchange_order_id: {event.exchange_order_id}. "
                f"Proceed to do hedging on taker exchange: {self.taker_exchange}."
            )

            # if maker sell order is filled, then taker should be buy as a hedge
            is_taker_buy = event.trade_type == TradeType.SELL
            self.check_and_hedge_maker_orders(
                event.trading_pair, event.amount, is_taker_buy
            )
        return

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent) -> None:
        return self._log_complete_order_event(event, order_side="BUY")

    def did_complete_sell_order(self, event: SellOrderCompletedEvent) -> None:
        return self._log_complete_order_event(event, order_side="SELL")

    def did_cancel_order(self, event: OrderCancelledEvent) -> None:
        self._log_abnormal_order_event(event, order_status="CANCELLED")

    def did_fail_order(self, event: MarketOrderFailureEvent) -> None:
        self._log_abnormal_order_event(event, order_status="FAILED")

    def did_expire_order(self, event: OrderExpiredEvent) -> None:
        self._log_abnormal_order_event(event, order_status="EXPIRED")

    def _log_abnormal_order_event(
        self,
        event: Union[OrderCancelledEvent, MarketOrderFailureEvent, OrderExpiredEvent],
        order_status: str,
    ) -> None:
        # TODO: event doesn't contian order price, order amount, trading pair, find a way to recover them
        # TODO: add retry logic for taker hedge orders
        order_id = event.order_id
        maker_connector, taker_connector = self.maker_exchange, self.taker_exchange
        if self._is_exchange_active_order(taker_connector, order_id):
            self.logger().error(
                f"[Taker exchange: {taker_connector}] Hedge order is {order_status} (Event: {event}). "
                f"Raise error logs here for suspicious behavior but SKIP retrying hedge."
            )
        elif self._is_exchange_active_order(maker_connector, order_id):
            # normal to have maker orders cancelled
            if not isinstance(event, OrderCancelledEvent):
                self.logger().error(
                    f"[Maker exchange: {maker_connector}] Maker order is {order_status} (Event: {event}). "
                    "Raise error logs here for suspicious behavior but SKIP recreating maker order in this event."
                )
        else:
            self.logger().error(
                f"Order doesnt belong to taker exchange ({taker_connector}) or maker exchange ({maker_connector}) "
                f"and the order is {order_status} (Event: {event}). "
                "Raise error logs here for suspicious behavior because this is suspicious!"
            )
        return

    def _log_complete_order_event(
        self,
        event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent],
        order_side: str,
    ) -> None:
        order_id = event.order_id
        exchange_order_id = event.exchange_order_id
        base_asset, quote_asset = event.base_asset, event.quote_asset
        trading_pair = f"{base_asset}-{quote_asset}"
        order_base_amount = event.base_asset_amount

        maker_connector, taker_connector = self.maker_exchange, self.taker_exchange
        if self._is_exchange_active_order(taker_connector, order_id):
            self.logger().info(
                f"[Taker exchange: {taker_connector}, {trading_pair}] Taker hedge {order_side} order has been completely filled: "
                f"{order_base_amount:.8f} {base_asset} (order_id: {order_id}, exchange_order_id: {exchange_order_id})."
            )
        elif self._is_exchange_active_orders(maker_connector, order_id):
            self.logger().info(
                f"[Maker exchnge: {taker_connector}, {trading_pair}] Maker {order_side} order has been completely filled: "
                f"{order_base_amount:.8f} {base_asset} (order_id: {order_id}, exchange_order_id: {exchange_order_id})."
            )
        else:
            self.logger().error(
                "Unknown order is completely filled but it doesnt belong to "
                f"taker exchange ({taker_connector}) or maker exchanges ({maker_connector}). "
                f"(order_id: {order_id}, exchange_order_id: {exchange_order_id})"
            )
        return

    # TODO: avoid looping active orders when scaled up to multi tokens
    def _is_exchange_active_order(self, connector_name: str, order_id: str) -> bool:
        for order in self.get_active_orders(connector_name=connector_name):
            if order.client_order_id == order_id:
                return True
        return False

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
