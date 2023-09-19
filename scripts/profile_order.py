import csv
import os
from decimal import Decimal
from enum import Enum
from time import time
from typing import Dict, List, Optional, Set, Tuple

from pydantic import BaseModel

from hummingbot.client.settings import ConnectorSetting
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.event.events import (
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
)
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class State(Enum):
    ORDER_CREATING = 1
    ORDER_CREATED = 2
    ORDER_CANCELING = 3
    ORDER_CANCELED = 4


class OrderEvent(Enum):
    ORDER_CREATION = "ORDER_CREATION"
    ORDER_CANCELATION = "ORDER_CANCELATION"


class OrderEventData(BaseModel):
    exchange: str
    trading_pair: str
    order_id: str
    event: OrderEvent
    time_spent: float


class CSVWriter:
    def __init__(self, filename: str):
        self.filename = filename

    # model.model_dump v.s model.dict: model_dump is introduced in pydantic v2.0
    def write_data(self, data: List[OrderEventData]) -> None:
        is_file_exist = os.path.exists(self.filename)
        with open(self.filename, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].dict().keys())
            if not is_file_exist:
                writer.writeheader()
            for item in data:
                writer.writerow(item.dict())
        return


class ProfileOrder(ScriptStrategyBase):
    """
    This example places shows how to add a logic to only place three buy orders in the market,
    use an event to increase the counter and stop the strategy once the task is done.
    """

    order_quantity = Decimal(0.005)
    trading_pair = "ETH-USDT"
    markets = {"bybit": {trading_pair}, "kucoin": {trading_pair}}
    order_canceled: Dict[str, int] = {exchange: 0 for exchange in markets}
    state: Dict[str, State] = {exchange: State.ORDER_CANCELED for exchange in markets}
    # map from exchange to timestamp
    cache_creation_timestamp: Dict[str, Optional[float]] = {
        exchange: None for exchange in markets
    }
    cache_cancel_timestamp: Dict[str, Optional[float]] = {
        exchange: None for exchange in markets
    }
    exchange_to_order_ids: Dict[str, Set[str]] = {
        exchange: set() for exchange in markets
    }
    order_to_cancel = 10

    def __init__(self, connectors: Dict[str, ConnectorSetting]):
        super().__init__(connectors)
        self.csv_writer = CSVWriter(filename="data/order_time.csv")

    def on_tick(self) -> None:
        for exchange in self.markets:
            self.process_exchange(exchange, self.trading_pair)

    def process_exchange(self, exchange: str, trading_pair: str) -> None:
        log_prefix = self.get_log_prefix(exchange, trading_pair)
        order_canceled = self.order_canceled[exchange]

        if order_canceled >= self.order_to_cancel:
            self.logger().info(
                f"{log_prefix} All orders are created and canceled! "
                "The script is completed! Canceling residual orders."
            )
            for order in self.get_active_orders(connector_name=exchange):
                self.cancel(
                    connector_name=exchange,
                    trading_pair=order.trading_pair,
                    order_id=order.client_order_id,
                )
            return

        state = self.state[exchange]
        if state == State.ORDER_CANCELED:
            best_bid_price, _ = self.get_best_bid_ask_price(exchange, trading_pair)
            price = best_bid_price * Decimal(0.5)
            self.logger().info(f"{log_prefix} Creating order")
            self.cache_creation_timestamp[exchange] = time()
            self.state[exchange] = State.ORDER_CREATING
            self.buy(
                connector_name=exchange,
                trading_pair=trading_pair,
                amount=self.order_quantity,
                order_type=OrderType.LIMIT,
                price=price,
            )

        elif state == State.ORDER_CREATING:
            self.logger().info(f"{log_prefix} Waiting for order to be created")

        elif state == State.ORDER_CREATED:
            active_orders = [
                order for order in self.get_active_orders(connector_name=exchange)
            ]
            if len(active_orders) == 0:
                self.logger().info(
                    f"{log_prefix} No active orders are detected yet. Wait to next on_tick"
                )
                return

            if len(active_orders) > 1:
                self.logger().warning(
                    f"{log_prefix} There are more than one active order. Something is going wrong."
                )
            order = active_orders[0]
            self.logger().info(
                f"{log_prefix} Canceling order ({order.client_order_id})"
            )
            self.cache_cancel_timestamp[order.client_order_id] = time()
            self.state[exchange] = State.ORDER_CANCELING
            self.cancel(
                connector_name=exchange,
                trading_pair=order.trading_pair,
                order_id=order.client_order_id,
            )

        else:
            self.logger().info(f"{log_prefix} Waiting for order to be canceled")

    def get_log_prefix(self, exchange: str, trading_pair: str) -> str:
        order_canceled = self.order_canceled[exchange] + 1
        return f"[{exchange}, {trading_pair}, {order_canceled}/{self.order_to_cancel}]"

    def get_best_bid_ask_price(
        self, connector_name: str, trading_pair: str
    ) -> Tuple[Decimal, Decimal]:
        best_ask_price = self.connectors[connector_name].get_price(trading_pair, True)
        best_bid_price = self.connectors[connector_name].get_price(trading_pair, False)
        return best_bid_price, best_ask_price

    def get_exchange_from_order_id(self, order_id: str) -> Optional[str]:
        for exchange in self.markets:
            if any(
                [
                    order
                    for order in self.get_active_orders(connector_name=exchange)
                    if order.client_order_id == order_id
                ]
            ):
                return exchange
        return None

    def did_create_buy_order(self, event: BuyOrderCreatedEvent) -> None:
        trading_pair = event.trading_pair
        order_id = event.order_id
        exchange = self.get_exchange_from_order_id(order_id)
        if exchange is None:
            self.logger().warning(
                f"Failed to associate order id {order_id} to any exchange. Something is going wrong."
            )
            return

        log_prefix = self.get_log_prefix(exchange, trading_pair)
        time_diff = time() - self.cache_creation_timestamp[exchange]
        data = OrderEventData(
            exchange=exchange,
            trading_pair=trading_pair,
            order_id=order_id,
            event=OrderEvent.ORDER_CREATION,
            time_spent=time_diff,
        )
        self.csv_writer.write_data([data])

        self.logger().info(
            f"{log_prefix} Created order. Log the time spent to CSV file."
        )
        self.cache_creation_timestamp[exchange] = None
        self.state[exchange] = State.ORDER_CREATED
        self.exchange_to_order_ids[exchange].add(order_id)

    def did_cancel_order(self, event: OrderCancelledEvent) -> None:
        order_id = event.order_id
        trading_pair = self.trading_pair
        exchange = [
            exchange
            for exchange, order_ids in self.exchange_to_order_ids.items()
            if order_id in order_ids
        ][0]
        log_prefix = self.get_log_prefix(exchange, self.trading_pair)
        time_diff = time() - self.cache_cancel_timestamp.get(order_id, 0)
        data = OrderEventData(
            exchange=exchange,
            trading_pair=trading_pair,
            order_id=order_id,
            event=OrderEvent.ORDER_CANCELATION,
            time_spent=time_diff,
        )
        self.csv_writer.write_data([data])

        self.logger().info(
            f"{log_prefix} Canceled order. Log the time spent to CSV file."
        )
        self.cache_creation_timestamp[exchange] = None
        self.state[exchange] = State.ORDER_CANCELED
        self.exchange_to_order_ids[exchange].remove(order_id)
        self.order_canceled[exchange] += 1

    def did_fail_order(self, event: MarketOrderFailureEvent) -> None:
        self.logger().warning(f"Failed to create order: {event.order_id}")
