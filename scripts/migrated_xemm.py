from collections import deque
from collections import defaultdict
from decimal import Decimal
from math import ceil, floor
from typing import List, Optional, Set, Dict, Tuple, Deque, Union, DefaultDict
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderExpiredEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.connector.connector_base import ConnectorBase  # type: ignore
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.limit_order import LimitOrder  # type: ignore

FLOAT_NAN = float("nan")
DECIMAL_NAN = Decimal("nan")
DECIMAL_ZERO = Decimal(0)


class CrossExchangeMarketMakingConfig:
    trading_pairs: Set[str] = {"ARB-USDT"}
    base_order_amount: Decimal = Decimal("1")
    taker_exchange: str = "kucoin"
    maker_exchange: str = "bybit"
    anti_order_adjust_duration: float = 60.0
    # if the hedge orders hanging in self._pending_hedge_orders for too long (in seconds), raise a log
    hedge_order_tolerance_duration: float = 60 * 5
    collect_price_samples_interval: int = 5
    price_samples_size: int = 12
    # expressed in %, what percentage of asset balance would you like to use for hedging trades on the taker market?
    order_size_taker_balance_factor: float = 99.5
    # expressed in %, max. allowed slippage on limit taker order to ensure it is likely filled.
    hedge_slippage_buffer: float = 5.0
    # expressed in %
    min_profitability: float = 0.03


# TODO: add expiry time for hedge order
# TODO: raise log if base asset sum doesn't match and no pending hedge orders
class CrossExchangeMarketMaking(ScriptStrategyBase):
    # read from config
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
    hedge_order_tolerance_duration: float = (
        CrossExchangeMarketMakingConfig.hedge_order_tolerance_duration
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
    _pending_hedge_orders: DefaultDict[
        str, List[Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]]
    ] = defaultdict(list)
    _active_maker_order_ids = set()
    _active_taker_order_ids = set()

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
        maker_exchange, taker_exchange = self.maker_exchange, self.taker_exchange
        if self.is_pending_hedge_orders(trading_pair):
            self.logger().info(
                f"[T: {taker_exchange}, {trading_pair}] There are pending hedge orders, skip processing the trading pair."
            )
            return

        if stale_hedge_orders := self.get_stale_pending_hedge_orders(
            trading_pair, timestamp
        ):
            self.logger().warning(
                f"[T: {taker_exchange}, {trading_pair}] There are stale hedge orders pending for more than {self.hedge_order_tolerance_duration} sec, "
                f"skip processing the trading pair. {stale_hedge_orders=}"
            )
            return

        has_active_bid, has_active_ask = False, False
        anti_adjust_order_time = self._anti_order_adjust_timer.get(trading_pair, 0.0)

        self.update_suggested_price_samples(trading_pair, timestamp)

        for active_order in self.active_maker_orders(trading_pair):
            client_order_id = active_order.client_order_id
            is_buy = active_order.is_buy
            if is_buy:
                has_active_bid = True
                order_action = "BUY"
            else:
                has_active_ask = True
                order_action = "SELL"

            current_hedge_price = self.get_taker_hedge_price(
                trading_pair, is_buy, active_order.quantity
            )

            if current_hedge_price is None or Decimal.is_nan(current_hedge_price):
                self.logger().info(
                    f"[M: {maker_exchange}, {trading_pair}] Maker {order_action} order ({client_order_id=}) "
                    f"is CANCELLING because hedge price ({current_hedge_price=}) is None or nan. "
                    "No buffer time before next maker order creation."
                )
                self.cancel_maker_order(trading_pair, active_order.client_order_id)
                continue

            if not await self.is_maker_order_still_profitable(
                trading_pair, active_order, current_hedge_price
            ):
                self.logger().info(
                    f"[M: {maker_exchange}, {trading_pair}] Maker {order_action} order ({client_order_id=}) "
                    f"is CANCELLING because it is no longer profitable. "
                    "No buffer time before next maker order creation."
                )
                self.cancel_maker_order(trading_pair, active_order.client_order_id)
                continue

            if not await self.is_sufficient_balance(trading_pair, active_order):
                self.logger().info(
                    f"[M: {maker_exchange}, {trading_pair}] Maker {order_action} order ({client_order_id=}) "
                    f"is CANCELLING because there is no sufficient balances in either {taker_exchange=} or {maker_exchange=}. "
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
                    f"[M: {maker_exchange}, {trading_pair}] Maker {order_action} order ({client_order_id=}) "
                    f"is CANCELLNG because order book price from {maker_exchange=} is detected to be drifted. "
                    f"Set buffer time so that no new maker order is created before: {updated_anti_order_adjust_timestamp:.1f}."
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
    def get_price_shift(self, trading_pair: str) -> Decimal:
        maker_top_bid, maker_top_ask = self._get_top_bid_ask_price(
            self.maker_exchange, trading_pair
        )
        taker_top_bid, taker_top_ask = self._get_top_bid_ask_price(
            self.taker_exchange, trading_pair
        )
        maker_mid_price = (maker_top_bid + maker_top_ask) / Decimal(2)
        taker_mid_price = (taker_top_bid + taker_top_ask) / Decimal(2)
        return maker_mid_price - taker_mid_price

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
            else Decimal("1") - self.hedge_slippage_buffer
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

        price_shift = self.get_price_shift(trading_pair)
        maker_price = (
            taker_price / (1 + self.min_profitability)
            if is_bid
            else taker_price * (1 + self.min_profitability)
        ) + price_shift

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
        base_asset, _ = split_hb_trading_pair(trading_pair)

        if is_bid:
            # maker buy, taker sell
            maker_balance_in_base = (
                self._estimate_base_balance_from_quote_in_worst_case(
                    maker_connector, trading_pair
                )
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
                self._estimate_base_balance_from_quote_in_worst_case(
                    taker_connector, trading_pair
                )
            )
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
        if order_amount < adjusted_size:
            maker_exchange = self.maker_exchange
            taker_exchange = self.taker_exchange
            self.logger().warning(
                f"[M:: {maker_exchange}, {trading_pair}] {order_amount=:.8f} is smaller than {adjusted_size=:.8f}. "
                f"Based on {maker_exchange=} balance in {base_asset}: {maker_balance_in_base=:.8f}, "
                f"and {taker_exchange=} balance in {base_asset}: {taker_balance_in_base=:.8f}."
            )
        quantized_order_amount = maker_connector.quantize_order_price(
            trading_pair, Decimal(order_amount)
        )
        return quantized_order_amount

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
        ) = self._get_top_bid_ask_price(self.maker_exchange, trading_pair)
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

    def _get_top_bid_ask_price(
        self, connector_name: str, trading_pair: str
    ) -> Tuple[Decimal, Decimal]:
        top_ask_price = self.connectors[connector_name].get_price(trading_pair, True)
        top_bid_price = self.connectors[connector_name].get_price(trading_pair, False)
        return top_bid_price, top_ask_price

    def _should_collect_new_price_sample(self, timestamp: float) -> bool:
        last_quotient = self._last_timestamp // self.collect_price_samples_interval
        this_quotient = timestamp // self.collect_price_samples_interval
        return this_quotient > last_quotient

    def _get_adjusted_base_order_amount(self, trading_pair: str) -> Decimal:
        config_order_amount = self.base_order_amount
        order_amount = self.connectors[self.maker_exchange].quantize_order_amount(
            trading_pair, config_order_amount
        )
        return order_amount

    def _get_available_balance(self, connector: ConnectorBase, asset: str) -> Decimal:
        return connector.get_available_balance(asset)

    def _estimate_base_balance_from_quote_in_worst_case(
        self, connector: ConnectorBase, trading_pair: str
    ) -> Decimal:
        """
        its the estimate in worst scenario because get_price_for_quote_volume returns the worst price at a given quote volume
        """
        _, quote_asset = split_hb_trading_pair(trading_pair)
        quote_balance = self._get_available_balance(connector, quote_asset)
        connector_name = connector.name
        if connector_name == self.taker_exchange:
            adjusted_quote_balance = (
                quote_balance * self.order_size_taker_balance_factor
            )
        else:
            adjusted_quote_balance = quote_balance
        worst_order_price_in_quote = (
            self.connectors[connector_name]
            .get_price_for_quote_volume(trading_pair, True, adjusted_quote_balance)
            .result_price
        )
        return adjusted_quote_balance / worst_order_price_in_quote

    # CHECKING CRITERIA FOR ORDER CANCELLING
    async def is_maker_order_still_profitable(
        self, trading_pair: str, order: LimitOrder, hedge_price: Decimal
    ) -> bool:
        maker_exchange, taker_exchange = self.maker_exchange, self.taker_exchange
        if hedge_price is None:
            self.logger().warning(
                f"[T: {taker_exchange}, {trading_pair}] {hedge_price=} from is_maker_order_still_profitable is None, "
                "returning False for order profitability."
            )
            return False

        # TODO: set expected vs ideal profitability, cancel when its worse than expected
        is_maker_buy = order.is_buy
        order_price = order.price
        price_shift = self.get_price_shift(trading_pair)
        if is_maker_buy:
            reference_maker_price = (
                hedge_price / (1 + self.min_profitability) + price_shift
            )
            is_profitable = order_price <= reference_maker_price
        else:
            reference_maker_price = (
                hedge_price * (1 + self.min_profitability) + price_shift
            )
            is_profitable = order_price >= reference_maker_price

        if not is_profitable:
            client_order_id = order.client_order_id
            order_action = "BUY" if is_maker_buy else "ASK"
            base_asset, quote_asset = split_hb_trading_pair(trading_pair)
            price, amount = order.price, order.quantity
            min_profitability_percent = self.min_profitability * 100
            self.logger().info(
                f"[M: {maker_exchange}, {trading_pair}] Maker {order_action} order ({client_order_id=}) "
                f"{amount:.8f} {base_asset} @ {price:.8f} {quote_asset}."
                f"is no longer profitable."
            )
            self.logger().info(
                f"{min_profitability_percent=:.3f}%, {hedge_price=:.8f} {quote_asset} and {price_shift=:.8f} {quote_asset}."
            )
        return is_profitable

    async def is_sufficient_balance(self, trading_pair: str, order: LimitOrder) -> bool:
        is_buy, order_price = order.is_buy, order.price
        maker_exchange, taker_exchange = self.maker_exchange, self.taker_exchange
        base_asset, quote_asset = split_hb_trading_pair(trading_pair)

        if is_buy:
            # maker buy, taker sell
            taker_balance_in_base = self._get_available_balance(
                self.connectors[taker_exchange], base_asset
            )
            maker_balance_in_quote = self._get_available_balance(
                self.connectors[maker_exchange], quote_asset
            )
            maker_balance_in_base = maker_balance_in_quote / order_price

        else:
            # maker sell, taker buy
            maker_balance_in_base = self._get_available_balance(
                self.connectors[maker_exchange], base_asset
            )
            taker_balance_in_quote = self._get_available_balance(
                self.connectors[taker_exchange], quote_asset
            )
            # TODO: get_price_for_quote_volume is not used in any script-based example, is it working??
            taker_price = (
                self.connectors[taker_exchange]
                .get_price_for_quote_volume(
                    trading_pair, is_buy=True, volume=taker_balance_in_quote
                )
                .result_price
            )
            taker_price_with_slippage = self.get_price_with_slippage(
                taker_price, is_buy=True
            )
            taker_balance_in_base = taker_balance_in_quote / taker_price_with_slippage

        discounted_taker_balance_in_base = (
            taker_balance_in_base * self.order_size_taker_balance_factor
        )
        order_size_limit = min(maker_balance_in_base, discounted_taker_balance_in_base)
        quantized_order_size_limit = self.connectors[
            maker_exchange
        ].quantize_order_amount(trading_pair, order_size_limit)

        if order.quantity > quantized_order_size_limit:
            client_order_id = order.client_order_id
            order_action = "BUY" if is_buy else "SELL"
            self.logger().warning(
                f"[M: {maker_exchange}, {trading_pair}] Maker {order_action} order ({client_order_id=}) "
                f"doesnt have enough of balance to support: {order.quantity=:.8f} > {quantized_order_size_limit=:.8f}. "
                f"quantized_order_size_limit is the min of {maker_balance_in_base=} and {discounted_taker_balance_in_base=} ({taker_balance_in_base=})."
            )
            return False
        return True

    def is_price_drifted(self, trading_pair: str, order: LimitOrder) -> bool:
        is_buy = order.is_buy
        order_price = order.price
        order_quantity = order.quantity
        suggested_price = self.get_maker_price(trading_pair, is_buy, order_quantity)
        is_drifted = suggested_price != order_price
        if is_drifted:
            base_asset, quote_asset = split_hb_trading_pair(trading_pair)
            order_action = "BUY" if is_buy else "SELL"
            client_order_id = order.client_order_id
            maker_exchange = self.maker_exchange
            self.logger().info(
                f"[M: {maker_exchange}, {trading_pair}] Maker {order_action} order ({client_order_id=}) "
                f"has {order_price=:.8f} inferrior than {suggested_price=:.8f}: "
                f"{order.quantity} {base_asset} @ {order_price:.8f} {quote_asset}."
            )
        return is_drifted

    def is_pending_hedge_orders(self, trading_pair: str) -> bool:
        return len(self._pending_hedge_orders[trading_pair]) > 0

    def get_stale_pending_hedge_orders(
        self, trading_pair: str, timestamp: float
    ) -> List[Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]]:
        return [
            o
            for o in self._pending_hedge_orders[trading_pair]
            if timestamp - o.creation_timestamp >= self.hedge_order_tolerance_duration
        ]

    # ORDER MANAGEMENT, E.G. QUERY, CANCEL, CREATE ORDERS
    def active_maker_orders(self, trading_pair: str) -> List[LimitOrder]:
        return [
            o
            for o in self.get_active_orders(self.maker_exchange)
            if o.trading_pair == trading_pair
        ]

    def cancel_maker_order(self, trading_pair: str, order_id: str) -> None:
        self.cancel(self.maker_exchange, trading_pair, order_id)

    async def check_and_create_maker_orders(
        self, trading_pair: str, is_bid: bool
    ) -> None:
        # TODO: handle the case when order_size doesn't meet the minimum order size from taker exchange
        order_size = self.get_maker_order_size(trading_pair, is_bid)
        order_action = "BUY" if is_bid else "SELL"
        maker_exchange, taker_exchange = self.maker_exchange, self.taker_exchange
        if order_size <= DECIMAL_ZERO:
            self.logger().warning(
                f"[M: {maker_exchange}, {trading_pair}] SKIP create maker {order_action} order with {order_size=:.8f} "
                "because order book on taker is too thin to place hedge order."
            )
            return

        order_price = self.get_maker_price(trading_pair, is_bid, order_size)
        if order_price is None or Decimal.is_nan(order_price):
            self.logger().warning(
                f"[M: {maker_exchange}, {trading_pair}] SKIP create maker {order_action} order with {order_size=:.8f} "
                f"because maker bid price is None or NaN ({order_price=})."
            )
            return

        taker_hedge_price = self.get_taker_hedge_price(trading_pair, is_bid, order_size)
        base_asset, quote_asset = split_hb_trading_pair(trading_pair)
        price_shift = self.get_price_shift(trading_pair)
        min_profitability_percent = self.min_profitability * 100
        self.logger().info(
            f"[M: {maker_exchange}, {trading_pair}] CREATING maker {order_action} order: "
            f"{order_size:.8f} {base_asset} @ {order_price:.8f} {quote_asset}."
        )
        self.logger().info(
            f"Maker {order_action} order price is based on "
            f"current hedging price: {taker_hedge_price:.8f} {quote_asset} from {taker_exchange=}, "
            f"price shift (maker mid price - taker mid price): {price_shift:.8f} {quote_asset}, "
            f"and min profitability: {min_profitability_percent:.2f}%."
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
            taker_connector = self.connectors[taker_exchange]
            taker_balance_in_base = (
                self._estimate_base_balance_from_quote_in_worst_case(
                    taker_connector, trading_pair
                )
            )
        else:
            taker_connector = self.connectors[self.taker_exchange]
            taker_balance_in_base = (
                self._get_available_balance(taker_connector, base_asset)
                * self.order_size_taker_balance_factor
            )

        if taker_balance_in_base < base_order_size:
            self.logger().warning(
                f"[T: {taker_exchange}, {trading_pair}] ADJUST order size of {order_action} order as hedge to be {taker_balance_in_base:.8f} {base_asset} "
                f"because taker balance is {taker_balance_in_base:.8f} converted in {base_asset}, which is less than target order size: {base_order_size:.8f} {base_asset}."
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
                f"[T: {taker_exchange}, {trading_pair}] SKIP create {order_action} order as hedge "
                f"because {taker_hedge_price=} from check_and_hedge_maker_orders is None or NaN."
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
            f"[T: {taker_exchange}, {trading_pair}] CREATING {order_action} order as hedge: "
            f"{quantized_adjusted_order_size:.8f} {base_asset} @ {hedge_price_with_slippage:.8f} {quote_asset}."
        )
        order_func(
            taker_exchange,
            trading_pair,
            amount=quantized_adjusted_order_size,
            order_type=OrderType.LIMIT,
            price=hedge_price_with_slippage,
        )
        return

    def _is_order_belong_to_this_bot(self, trading_pair: str) -> bool:
        return trading_pair in self.trading_pairs

    # EVENT HANDLING ON ORDER STATUS CHANGE
    def did_fill_order(self, event: OrderFilledEvent) -> None:
        if not self._is_order_belong_to_this_bot(event.trading_pair):
            return

        order_id = event.order_id
        exchange_order_id = event.exchange_order_id

        if order_id in self._active_maker_order_ids:
            order_action = "BUY" if event.trade_type == TradeType.BUY else "SELL"
            base_asset, quote_asset = split_hb_trading_pair(event.trading_pair)
            self.logger().info(
                f"[M: {self.maker_exchange}, {event.trading_pair}] Maker {order_action} order ({order_id=}, {exchange_order_id=}) is FILLED: "
                f"{event.amount:.8f} {base_asset} @ {event.price:.8f} {quote_asset}. "
                f"Proceeding to do hedging on taker exchange: {self.taker_exchange}."
            )
            self.logger().info(f"{event=}")

            # if maker sell order is filled, then taker should be buy as a hedge
            is_taker_buy = event.trade_type == TradeType.SELL
            self.check_and_hedge_maker_orders(
                event.trading_pair, event.amount, is_taker_buy
            )
        return

    def did_create_buy_order(self, event: BuyOrderCreatedEvent) -> None:
        self._log_and_handle_created_order(event, is_buy=True)
        return

    def did_create_sell_order(self, event: SellOrderCreatedEvent) -> None:
        self._log_and_handle_created_order(event, is_buy=False)
        return

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent) -> None:
        self._log_and_handle_completed_order(event, order_action="BUY")
        return

    def did_complete_sell_order(self, event: SellOrderCompletedEvent) -> None:
        self._log_and_handle_completed_order(event, order_action="SELL")
        return

    def did_cancel_order(self, event: OrderCancelledEvent) -> None:
        self._log_and_handle_noncompleted_order(event, order_status="CANCELLED")

    def did_fail_order(self, event: MarketOrderFailureEvent) -> None:
        self._log_and_handle_noncompleted_order(event, order_status="FAILED")

    def did_expire_order(self, event: OrderExpiredEvent) -> None:
        self._log_and_handle_noncompleted_order(event, order_status="EXPIRED")

    def _add_pending_hedge_order(
        self, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent], is_buy: bool
    ) -> None:
        order_action = "BUY" if is_buy else "SELL"
        taker_exchange = self.taker_exchange
        order_id = event.order_id
        trading_pair, price, amount = event.trading_pair, event.price, event.amount
        base_asset, quote_asset = split_hb_trading_pair(trading_pair)
        pending_hedge_orders = self._pending_hedge_orders[trading_pair]
        self.logger().info(
            f"[T: {taker_exchange}, {trading_pair}] {order_action} order as hedge ({order_id=}) is CREATED: "
            f"{amount:.8f} {base_asset} @ {price:.8f} {quote_asset}."
        )
        self.logger().info(
            f"Appending the hedge order ({event=}) to {pending_hedge_orders=}."
        )
        pending_hedge_orders.append(event)
        return

    def _remove_pending_hedge_order(
        self, event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]
    ) -> None:
        order_id, exchange_order_id = event.order_id, event.exchange_order_id
        taker_exchange = self.taker_exchange
        base_asset, quote_asset = event.base_asset, event.quote_asset
        trading_pair = f"{base_asset}-{quote_asset}"
        order_action = "BUY" if isinstance(event, BuyOrderCompletedEvent) else "SELL"
        self.logger().info(
            f"[T: {taker_exchange}, {trading_pair}] {order_action} order as hedge is FILLED ({event=})."
        )
        pending_hedge_orders = self._pending_hedge_orders[trading_pair]
        if exchange_order_id is not None:
            self.logger().info(
                f"[T: {taker_exchange}, {trading_pair}] Removing {order_action} order ({exchange_order_id=}, {order_id=}) from pending_hedge_orders."
            )
            self.logger().info(
                f"pending_hedge_orders before the order removal: {pending_hedge_orders}"
            )
            self._pending_hedge_orders[trading_pair] = [
                o
                for o in pending_hedge_orders
                if o.exchange_order_id != exchange_order_id
            ]
        else:
            self.logger().info(
                f"[T: {taker_exchange}, {trading_pair}] Removing {order_action} order ({order_id=}) from pending_hedge_orders."
            )
            self.logger().info(
                f"pending_hedge_orders before the order removal: {pending_hedge_orders}"
            )
            self._pending_hedge_orders[trading_pair] = [
                o for o in pending_hedge_orders if o.order_id != order_id
            ]
        return

    def _log_and_handle_created_order(
        self, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent], is_buy: bool
    ) -> None:
        trading_pair = event.trading_pair
        if not self._is_order_belong_to_this_bot(trading_pair):
            return

        maker_exchange, taker_exchange = self.maker_exchange, self.taker_exchange
        if self._is_exchange_active_order(self.taker_exchange, event.order_id):
            self._add_pending_hedge_order(event, is_buy=is_buy)
            self._active_taker_order_ids.add(event.order_id)
            self.logger().info(
                f"[T: {taker_exchange}, {trading_pair}] Active taker orders after addition: {self._active_taker_order_ids}."
            )
        elif self._is_exchange_active_order(self.maker_exchange, event.order_id):
            self._active_maker_order_ids.add(event.order_id)
            self.logger().info(
                f"[M: {maker_exchange}, {trading_pair}] Active maker orders after addition: {self._active_maker_order_ids}."
            )
        else:
            order_id, exchange_order_id = event.order_id, event.exchange_order_id
            self.logger().warning(
                f"[{trading_pair}] Unknow order is CREATED but it doesnt belong to "
                f"{taker_exchange=} or {maker_exchange=} ({order_id=}, {exchange_order_id=})."
            )
        return

    def _log_and_handle_noncompleted_order(
        self,
        event: Union[MarketOrderFailureEvent, OrderExpiredEvent, OrderCancelledEvent],
        order_status: str,
    ) -> None:
        order_id = event.order_id
        maker_exchange, taker_exchange = self.maker_exchange, self.taker_exchange
        if order_id in self._active_maker_order_ids:
            self._active_maker_order_ids.remove(order_id)
            self.logger().info(
                f"[M: {maker_exchange}] Order ({order_id=}) is {order_status}. "
                f"Active maker orders after its removal: {self._active_maker_order_ids}"
            )
        elif order_id in self._active_taker_order_ids:
            self._active_taker_order_ids.remove(order_id)
            self.logger().info(
                f"[T: {taker_exchange}] Order ({order_id=}) is {order_status}. "
                f"Active taker orders after its removal: {self._active_taker_order_ids}"
            )
        else:
            self.logger().warning(f"Unknown order ({order_id=}) is {order_status}.")
            self.logger().info(
                f"{self._active_maker_order_ids=}, {self._active_taker_order_ids=}."
            )
        return

    def _log_and_handle_completed_order(
        self,
        event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent],
        order_action: str,
    ) -> None:
        base_asset, quote_asset = event.base_asset, event.quote_asset
        trading_pair = f"{base_asset}-{quote_asset}"
        if not self._is_order_belong_to_this_bot(trading_pair):
            return

        order_id, exchange_order_id = event.order_id, event.exchange_order_id
        maker_exchange, taker_exchange = self.maker_exchange, self.taker_exchange
        order_base_amount = event.base_asset_amount
        if order_id in self._active_maker_order_ids:
            self.logger().info(
                f"[M: {maker_exchange}, {trading_pair}] Maker {order_action} order is COMPLETELY FILLED: "
                f"{order_base_amount:.8f} {base_asset} ({order_id=}, {exchange_order_id=})."
            )
            self._active_maker_order_ids.remove(order_id)
            self.logger().info(
                f"[M: {maker_exchange}, {trading_pair}] Active maker orders after its removal: {self._active_maker_order_ids}."
            )
        elif order_id in self._active_taker_order_ids:
            self.logger().info(
                f"[T: {taker_exchange}, {trading_pair}] Taker {order_action} order as hedge is COMPLETELY FILLED: "
                f"{order_base_amount:.8f} {base_asset} ({order_id=}, {exchange_order_id=})."
            )
            self._remove_pending_hedge_order(event)
            self._active_taker_order_ids.remove(order_id)
            self.logger().info(
                f"[T: {taker_exchange}, {trading_pair}] Active taker orders after its removal: {self._active_taker_order_ids}."
            )
        else:
            self.logger().warning(
                f"[{trading_pair}] Unknow order is COMPLETELY FILLED but it doesnt belong to "
                f"{taker_exchange=} or {maker_exchange=} ({order_id=}, {exchange_order_id=})."
            )
            self.logger().info(
                f"{self._active_maker_order_ids=}, {self._active_taker_order_ids=}."
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
        lines = ["", "HELLO WORLD!!", ""]
        active_maker_orders = self.active_maker_orders("ARB-USDT")
        lines.append(f"Active Maker Orders: {len(active_maker_orders)}")
        return "\n".join(lines)
