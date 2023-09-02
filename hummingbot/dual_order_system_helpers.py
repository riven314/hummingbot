import csv
import os
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional
from uuid import uuid4

from pydantic import BaseModel


class OrderStatus(Enum):
    CREATED = "CREATED"
    CANCELED = "CANCELED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"
    PARTIAL_FILLED = "PARTIAL_FILLED"
    COMPLETE_FILLED = "COMPLETE_FILLED"


class OrderSide(Enum):
    BID = "BID"
    ASK = "ASK"


class MakerOrder(BaseModel):
    order_id: str
    exchange_order_id: Optional[str]
    exchange: str
    price: Decimal
    base_amount: Decimal
    base: str
    quote: str
    status: OrderStatus
    side: OrderSide
    created_timestamp: float
    completed_timestamp: Optional[float] = None

    # class Config:
    #     validate_assignment = True


maker_order = MakerOrder(
    order_id="1",
    exchange_order_id="1",
    exchange="1",
    price=Decimal("1"),
    base_amount=Decimal("1"),
    base="1",
    quote="1",
    status=OrderStatus.CREATED,
    side=OrderSide.BID,
    created_timestamp=datetime.timestamp(datetime.utcnow()),
)


class HedgeOrder(MakerOrder):
    maker_order_id: str
    maker_exchange_order_id: Optional[str]


class CycleMetadata(BaseModel):
    maker_exchange: str
    taker_exchange: str
    maker_order_id: str
    # exchange_order_id can be None
    maker_exchange_order_id: Optional[str]
    completed_hedge_order_ids: List[str]
    total_profit_percent: Decimal
    min_profitability_percent: Decimal

    @property
    def price_shift_profit_percent(self) -> Decimal:
        return self.total_profit_percent - self.min_profitability_percent


class GroupedTradeData(BaseModel):
    exchange: str
    is_offset_shift: bool
    group_id: str  # uuid4
    order_id: str
    exchange_order_id: Optional[str]
    price: Decimal
    base_amount: Decimal
    base: str
    quote: str
    side: OrderSide
    created_timestamp: float
    completed_timestamp: Optional[float] = None


class CSVWriter:
    def __init__(self, filename: str):
        self.filename = filename

    # model.model_dump v.s model.dict: model_dump is introduced in pydantic v2.0
    def write_data(self, data: List[GroupedTradeData]) -> None:
        is_file_exist = os.path.exists(self.filename)
        with open(self.filename, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].dict().keys())
            if not is_file_exist:
                writer.writeheader()
            for item in data:
                writer.writerow(item.dict())
        return


class ShiftOrderManager:
    # the total amount of all hedge orders may not exactly equal to maker order amount
    hedge_complete_rate_threshold: float = 0.99
    creating_order_tolerance_in_seconds: int = 60
    csv_path = "data/group_traded_data.csv"

    def __init__(
        self,
        maker_exchange: str,
        taker_exchange: str,
        base: str,
        quote: str,
        min_profitability_percent: Decimal,
    ) -> None:
        self.maker_exchange: str = maker_exchange
        self.taker_exchange: str = taker_exchange
        self.base: str = base
        self.quote: str = quote
        self.min_profitability_percent: Decimal = min_profitability_percent
        self.maker_order: Optional[MakerOrder] = None
        self.hedge_orders: List[HedgeOrder] = []
        self.is_offset_shift = False
        # state to manage
        self._creating_maker_order_timestamp: Optional[float]
        self._creating_hedge_order_timestamp: Optional[float]
        # helper class
        self.csv_writer = CSVWriter(self.csv_path)

    @property
    def csv_filename(self) -> str:
        return self.csv_writer.filename

    def is_idle(self) -> bool:
        return self.maker_order is None and self._creating_maker_order_timestamp is None

    def set_creating_maker_order_timestamp(self, timestamp: float) -> None:
        self._creating_maker_order_timestamp = timestamp

    def set_creating_hedge_orders(self, timestamp: float) -> None:
        self._creating_hedge_order_timestamp = timestamp

    def is_creating_maker_order(self) -> bool:
        return self._creating_maker_order_timestamp is not None

    def is_hedge_in_progress(self) -> bool:
        return (
            self._creating_hedge_order_timestamp is not None
            or len(self.hedge_orders) > 0
        )

    def is_maker_order_partially_filled(self) -> bool:
        return (
            self.maker_order is not None
            and self.maker_order.status == OrderStatus.PARTIAL_FILLED
        )

    def is_creating_maker_order_too_long(self, ref_timestamp: float) -> bool:
        assert (
            self._creating_maker_order_timestamp is not None
        ), "Creating maker order timestamp is not set yet"
        return (
            ref_timestamp - self._creating_maker_order_timestamp
        ) >= self.creating_order_tolerance_in_seconds

    def is_creating_hedge_order_too_long(self, ref_timestamp: float) -> bool:
        assert (
            self._creating_hedge_order_timestamp is not None
        ), "Creating hedge order timestamp is not set yet"
        return (
            ref_timestamp - self._creating_hedge_order_timestamp
        ) >= self.creating_order_tolerance_in_seconds

    def is_maker_order(self, order_id: str) -> bool:
        return self.maker_order is not None and self.maker_order.order_id == order_id

    def is_hedge_order(self, order_id: str) -> bool:
        return any([order.order_id == order_id for order in self.hedge_orders])

    def propose_maker_price(self, taker_ref_price: Decimal, is_bid: bool) -> Decimal:
        if is_bid:
            maker_price = taker_ref_price / (1 + self.min_profitability_percent / 100)
        else:
            maker_price = taker_ref_price * (1 + self.min_profitability_percent / 100)
        return maker_price

    def get_maker_order(self) -> MakerOrder:
        assert self.maker_order is not None, "Maker order is not created yet"
        return self.maker_order

    def maker_order_created(
        self,
        order_id: str,
        exchange_order_id: Optional[str],
        price: Decimal,
        amount: Decimal,
        order_side: OrderSide,
    ) -> None:
        self.maker_order = MakerOrder(
            order_id=order_id,
            exchange_order_id=exchange_order_id,
            exchange=self.maker_exchange,
            price=price,
            base_amount=amount,
            base=self.base,
            quote=self.quote,
            status=OrderStatus.CREATED,
            side=order_side,
            created_timestamp=datetime.timestamp(datetime.utcnow()),
        )
        self._creating_maker_order_timestamp = None
        return

    def maker_order_partially_filled(self) -> None:
        assert self.maker_order is not None, "Maker order is not created yet"
        self.maker_order.status = OrderStatus.PARTIAL_FILLED
        return

    def maker_order_completed(self) -> None:
        assert self.maker_order is not None, "Maker order is not created yet"
        self.maker_order.status = OrderStatus.COMPLETE_FILLED
        return

    def hedge_order_created(
        self,
        order_id: str,
        exchange_order_id: Optional[str],
        price: Decimal,
        amount: Decimal,
        order_side: OrderSide,
    ) -> None:
        assert self.maker_order is not None, "Maker order is not created yet"
        hedge_order = HedgeOrder(
            order_id=order_id,
            exchange_order_id=exchange_order_id,
            exchange=self.taker_exchange,
            price=price,
            base_amount=amount,
            base=self.base,
            quote=self.quote,
            status=OrderStatus.CREATED,
            side=order_side,
            maker_order_id=self.maker_order.order_id,
            maker_exchange_order_id=self.maker_order.exchange_order_id,
            created_timestamp=datetime.timestamp(datetime.utcnow()),
        )
        self.hedge_orders.append(hedge_order)
        self._creating_hedge_order_timestamp = None

    def hedge_order_completed(self, order_id: str) -> None:
        hedge_order = self._get_hedge_order(order_id)
        hedge_order.status = OrderStatus.COMPLETE_FILLED
        return

    def hedge_order_incompleted(self, order_id: str, order_status: OrderStatus) -> None:
        hedge_order = self._get_hedge_order(order_id)
        hedge_order.status = order_status
        return

    def is_cycle_complete(self) -> bool:
        if self.maker_order is None:
            return False

        completed_hedge_orders = self._get_completed_hedge_orders()
        hedge_ratio = (
            sum([order.base_amount for order in completed_hedge_orders])
            / self.maker_order.base_amount
        )
        is_hedge_orders_completed = hedge_ratio >= self.hedge_complete_rate_threshold
        is_maker_order_completed = (
            self.maker_order.status == OrderStatus.COMPLETE_FILLED
        )
        return is_hedge_orders_completed and is_maker_order_completed

    def get_completed_cycle_metadata(self) -> CycleMetadata:
        assert self.maker_order is not None, "Maker order is not created yet"
        self._validate_nonempty_hedge_orders()
        completed_hedge_orders = self._get_completed_hedge_orders()
        completed_hedge_order_ids = [order.order_id for order in completed_hedge_orders]

        maker_volume = self.maker_order.base_amount * self.maker_order.price
        taker_volume = sum(
            [order.base_amount * order.price for order in completed_hedge_orders]
        )
        if MakerOrder.side == OrderSide.BID:
            profit = (taker_volume - maker_volume) / maker_volume
        else:
            profit = (maker_volume - taker_volume) / taker_volume
        total_profit_percent = profit * Decimal(100.0)
        return CycleMetadata(
            maker_exchange=self.maker_exchange,
            taker_exchange=self.taker_exchange,
            maker_order_id=self.maker_order.order_id,
            maker_exchange_order_id=self.maker_order.exchange_order_id,
            completed_hedge_order_ids=completed_hedge_order_ids,
            total_profit_percent=total_profit_percent,
            min_profitability_percent=self.min_profitability_percent,
        )

    def write_trades_to_csv_at_completion(self) -> None:
        assert self.maker_order is not None, "Maker order is not created yet"
        group_id = str(uuid4())
        trade_data = []
        maker_trade_data = GroupedTradeData(
            exchange=self.maker_order.exchange,
            is_offset_shift=self.is_offset_shift,
            group_id=group_id,
            order_id=self.maker_order.order_id,
            exchange_order_id=self.maker_order.exchange_order_id,
            price=self.maker_order.price,
            base_amount=self.maker_order.base_amount,
            base=self.maker_order.base,
            quote=self.maker_order.quote,
            side=self.maker_order.side,
            created_timestamp=self.maker_order.created_timestamp,
            completed_timestamp=self.maker_order.completed_timestamp,
        )
        trade_data.append(maker_trade_data)
        for trade in self._get_completed_hedge_orders():
            trade_data.append(
                GroupedTradeData(
                    exchange=trade.exchange,
                    is_offset_shift=self.is_offset_shift,
                    group_id=group_id,
                    order_id=trade.order_id,
                    exchange_order_id=trade.exchange_order_id,
                    price=trade.price,
                    base_amount=trade.base_amount,
                    base=trade.base,
                    quote=trade.quote,
                    side=trade.side,
                    created_timestamp=trade.created_timestamp,
                    completed_timestamp=trade.completed_timestamp,
                )
            )
        self.csv_writer.write_data(trade_data)
        return

    def reset(self) -> None:
        assert self.maker_order is not None, "Maker order is not created yet"
        self.maker_order = None
        self.hedge_orders = []
        self._creating_hedge_order_timestamp = None
        self._creating_hedge_order_timestamp = None
        return

    def _get_hedge_order(self, order_id: str) -> HedgeOrder:
        hedge_order = [
            order for order in self.hedge_orders if order.order_id == order_id
        ][0]
        return hedge_order

    def _get_completed_hedge_orders(self) -> List[HedgeOrder]:
        return [
            order
            for order in self.hedge_orders
            if order.status == OrderStatus.COMPLETE_FILLED
        ]

    def _validate_nonempty_hedge_orders(self) -> None:
        assert len(self.hedge_orders) > 0, "Hedge orders are not created yet"


# TODO: handle the case when the job has price_shift_profit_percent < 0
# ans: only consider the case when maker ask is higher than taker ask by min_profit
class OffsetShiftOrderManager(ShiftOrderManager):
    def __init__(
        self,
        maker_exchange: str,
        taker_exchange: str,
        base: str,
        quote: str,
        min_profitability_percent: Decimal,
        price_shift_discount_percent: Decimal,
    ) -> None:
        super().__init__(
            maker_exchange, taker_exchange, base, quote, min_profitability_percent
        )
        self.is_offset_shift = True
        self.price_shift_discount_percent = price_shift_discount_percent
        # below params are not None until a job is assigned
        self.price_shift_profit_percent: Optional[Decimal] = None
        self.offset_job_maker_order_id: Optional[str] = None

    def assign_offset_job(
        self, maker_order_id: str, price_shift_profit_percent: Decimal
    ) -> None:
        self.offset_job_maker_order_id = maker_order_id
        self.price_shift_profit_percent = price_shift_profit_percent
        return

    def propose_maker_price(self, taker_ref_price: Decimal, is_bid: bool) -> Decimal:
        assert (
            self.price_shift_profit_percent is not None
            and self.offset_job_maker_order_id is not None
        ), "Offset job is not assigned yet"

        if self.price_shift_profit_percent > 0:
            allowable_loss_percent = Decimal("-1") * self.discounted_price_shift_percent
            if is_bid:
                maker_price = (
                    taker_ref_price
                    / (1 + self.min_profitability_percent / 100)
                    * (1 + allowable_loss_percent / 100)
                )
            else:
                maker_price = (
                    taker_ref_price
                    * (1 + self.min_profitability_percent / 100)
                    / (1 + allowable_loss_percent / 100)
                )

        # don't make any discount if previous incurred loss on price shift, it will make the order even harder to be filled
        else:
            loss_to_recover_in_percent = Decimal("-1") * self.price_shift_profit_percent
            if is_bid:
                maker_price = (
                    taker_ref_price
                    / (1 + self.min_profitability_percent / 100)
                    / (1 + loss_to_recover_in_percent / 100)
                )
            else:
                maker_price = (
                    taker_ref_price
                    * (1 + self.min_profitability_percent / 100)
                    * (1 + loss_to_recover_in_percent / 100)
                )
        return maker_price

    # e.g. we earn 0.5% price shift previously, and price shift discount = 50%, then this time price shift can't loss more than 0.25%
    @property
    def discounted_price_shift_percent(self) -> Decimal:
        assert self.price_shift_profit_percent is not None, "Price shift profit is None"
        return self.price_shift_profit_percent * (
            self.price_shift_discount_percent / 100
        )

    def reset(self) -> None:
        assert self.maker_order is not None, "Maker order is not created yet"
        self.offset_job_maker_order_id = None
        self.price_shift_profit_percent = None
        self.maker_order = None
        self.hedge_orders.clear()
        self._creating_hedge_order_timestamp = None
        self._creating_hedge_order_timestamp = None
        return


class PendingOffsetJobs:
    def __init__(self) -> None:
        self.jobs: List[CycleMetadata] = []

    def is_empty(self) -> bool:
        return len(self.jobs) == 0

    def peek(self) -> CycleMetadata:
        return self.jobs[0]

    def add(self, job: CycleMetadata) -> None:
        self.jobs.append(job)
        # sort by easiest fill job (largest price shift profit percent)
        sorted(self.jobs, key=lambda x: x.price_shift_profit_percent, reverse=True)
        return

    def remove(self, maker_order_id: str) -> None:
        before_n = len(self.jobs)
        jobs = [job for job in self.jobs if job.maker_order_id != maker_order_id]
        after_n = len(self.jobs)
        self.jobs = jobs
        if before_n == after_n:
            raise ValueError(
                f"Maker order id {maker_order_id} not found, failed to remove it from PendingOffsetJobs"
            )

    def __len__(self) -> int:
        return len(self.jobs)
