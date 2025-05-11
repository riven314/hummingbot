import asyncio
from decimal import Decimal
from enum import Enum
from typing import Any, List, Optional

import pandas as pd
from pydantic import Field

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.data_feed.funding_rate.data_types import FundingRateInterval
from hummingbot.data_feed.funding_rate.funding_rate_data_feed import FundingRateDataFeed
from hummingbot.data_feed.funding_rate.utils.interval_utils import IntervalUtility
from hummingbot.data_feed.funding_rate.utils.time_utils import TimeUtility
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerBase,
    DirectionalTradingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction


class PositionDirection(str, Enum):
    LONG = "long"
    SHORT = "short"


class FundingRateControllerConfig(DirectionalTradingControllerConfigBase):
    controller_type = "funding_rate_mean_reversion"
    exchange: str = Field(
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the exchange name"),
    )
    trading_pair: str = Field(
        default="BTC-USDT", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the trading pair")
    )
    position_direction: PositionDirection = Field(
        client_data=ClientFieldData(
            prompt_on_new=True, prompt=lambda mi: "Enter the position direction ('long' or 'short')"
        ),
    )
    position_size: Decimal = Field(
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the position size (token amount)"),
    )
    entry_window_in_sec: int = Field(
        gt=0,
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Enter the entry window in seconds (how many seconds after candle open to check for entry/exit)",
        ),
    )
    upper_threshold: Decimal = Field(
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the upper threshold for z-score"),
    )
    lower_threshold: Decimal = Field(
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the lower threshold for z-score"),
    )
    zscore_window: int = Field(
        gt=2,
        client_data=ClientFieldData(
            prompt_on_new=True, prompt=lambda mi: "Enter the window size for z-score calculation"
        ),
    )
    entry_sma_window: Optional[int] = Field(
        default=None,
        client_data=ClientFieldData(
            prompt_on_new=True, prompt=lambda mi: "Enter the SMA window size for entry (leave empty for no SMA check)"
        ),
    )
    exit_sma_window: Optional[int] = Field(
        default=None,
        client_data=ClientFieldData(
            prompt_on_new=True, prompt=lambda mi: "Enter the SMA window size for exit (leave empty for no SMA check)"
        ),
    )

    @property
    def triple_barrier_config(self) -> TripleBarrierConfig:
        return TripleBarrierConfig(
            stop_loss=None,
            take_profit=None,
            open_order_type=OrderType.MARKET,
        )


class FundingRateController(DirectionalTradingControllerBase):
    def __init__(
        self,
        config: FundingRateControllerConfig,
        market_data_provider: MarketDataProvider,
        actions_queue: asyncio.Queue,
    ):
        super().__init__(config, market_data_provider, actions_queue)
        self.config: FundingRateControllerConfig = config
        self._funding_rate_feed: Optional[FundingRateDataFeed] = None

    def set_funding_rate_feed(self, feed: FundingRateDataFeed):
        self._funding_rate_feed = feed

    @property
    def active_position(self) -> Optional[dict[str, Any]]:
        active_executors = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda e: e.is_active
            and e.trading_pair == self.config.trading_pair
            and e.connector_name == self.config.exchange,
        )
        if not active_executors:
            return None

        if len(active_executors) > 1:
            self.logger().warning(f"Found {len(active_executors)} active executors. Using the first one.")
        selected_executor = active_executors[0]
        entry_price = selected_executor.custom_info.get("current_position_average_price")
        return {
            "create_timestamp": selected_executor.timestamp,
            "executor_id": selected_executor.id,
            "trading_pair": selected_executor.trading_pair,
            "current_position_average_price": entry_price,
            "is_trading": selected_executor.is_trading,
        }

    async def update_processed_data(self):
        pass

    def get_candle_df(self) -> pd.DataFrame:
        max_records = 10
        if self.config.entry_sma_window and self.config.exit_sma_window:
            max_records = max((self.config.entry_sma_window or 0) + 10, (self.config.exit_sma_window or 0) + 10)
        elif self.config.entry_sma_window:
            max_records = (self.config.entry_sma_window or 0) + 10
        elif self.config.exit_sma_window:
            max_records = (self.config.exit_sma_window or 0) + 10

        candles_df = self.market_data_provider.get_candles_df(
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            interval=self.config.candle_interval,
            max_records=max_records,
        )
        return candles_df

    def get_last_close_price_value(self) -> Optional[Decimal]:
        candles_df = self.get_candle_df()
        if len(candles_df) < 2:
            return None
        return Decimal(str(candles_df.iloc[-2]["close"]))

    def get_last_sma_value(self, window: int) -> Optional[Decimal]:
        candles_df = self.get_candle_df()
        if len(candles_df) < window + 1:
            return None
        sma = candles_df["close"].rolling(window=window).mean()
        return Decimal(str(sma.iloc[-2]))

    def get_last_zscore_value(self) -> Optional[Decimal]:
        if not self._funding_rate_feed:
            return None

        # Option 1: Use z-score from feed if window matches
        if self._funding_rate_feed.window == self.config.zscore_window:
            last_interval_data: Optional[FundingRateInterval] = self._funding_rate_feed.last_funding_rate_interval
            if last_interval_data and last_interval_data.zscore is not None:
                return Decimal(str(last_interval_data.zscore))
        else:
            # Option 2: Calculate z-score manually if windows differ
            # This requires getting raw funding rate records and applying z-score calculation
            # For simplicity, this part is stubbed. A robust implementation would be needed here.
            # self.logger().warning(f"Controller zscore_window {self.config.zscore_window} differs from feed window {self._funding_rate_feed.window}. Manual calculation needed.")
            # For now, let's assume we can get it from the feed if it's available, otherwise log warning.
            last_interval_data: Optional[FundingRateInterval] = self._funding_rate_feed.last_funding_rate_interval
            if last_interval_data and last_interval_data.zscore is not None:
                # This z-score is based on the FEED's window, not necessarily the controller's.
                return Decimal(str(last_interval_data.zscore))
        return None

    def get_entry_size(self) -> Decimal:
        # This logic is simplified from the original script.
        # A more robust solution would involve the MarketDataProvider or connector to get balance.
        # For controller level, it might rely on total_amount_quote from base config.
        # Here, we directly use self.config.position_size.
        # TODO: Re-evaluate how size is determined, possibly using self.config.total_amount_quote
        # and current price to calculate base asset amount, or ensure connector balances are accessible.
        # For now, directly using configured position_size.
        return self.config.position_size

    def is_within_entry_window(self) -> bool:
        candles_df = self.get_candle_df()
        if len(candles_df) < 1:  # Need current candle
            return False
        current_timestamp_s = self.market_data_provider.time()
        current_candle_open_timestamp_s = candles_df.iloc[-1]["timestamp"]
        if pd.isna(current_candle_open_timestamp_s):
            return False
        elapsed_seconds_since_open = current_timestamp_s - float(current_candle_open_timestamp_s)
        return 0 <= elapsed_seconds_since_open <= self.config.entry_window_in_sec

    def is_candle_data_updated(self) -> bool:
        candles_df = self.get_candle_df()
        if len(candles_df) < 2:
            return False
        last_completed_candle_timestamp_s = candles_df.iloc[-2]["timestamp"]
        if pd.isna(last_completed_candle_timestamp_s):
            return False
        now_ms = TimeUtility.now_ms()
        expected_last_completed_interval_start_ms = IntervalUtility.get_last_interval_start_timestamp(
            now_ms, self.config.candle_interval
        )
        last_completed_candle_start_timestamp_ms = int(float(last_completed_candle_timestamp_s) * 1000)
        return last_completed_candle_start_timestamp_ms == expected_last_completed_interval_start_ms

    def is_funding_rate_data_updated(self) -> bool:
        if not self._funding_rate_feed or not self._funding_rate_feed.ready:
            return False
        return self._funding_rate_feed.is_updated()

    def should_entry_on_zscore_and_sma(self) -> bool:
        if self._last_zscore is None or self._last_close_price is None:
            return False

        is_price_entry_condition_met: bool = True
        if self.config.position_direction == PositionDirection.LONG:
            is_zscore_entry_condition_met = self._last_zscore <= self.config.lower_threshold
            if self.config.entry_sma_window is not None:
                if self._last_entry_sma is None:
                    return False
                is_price_entry_condition_met = self._last_close_price > self._last_entry_sma
        else:  # SHORT
            is_zscore_entry_condition_met = self._last_zscore >= self.config.upper_threshold
            if self.config.entry_sma_window is not None:
                if self._last_entry_sma is None:
                    return False
                is_price_entry_condition_met = self._last_close_price < self._last_entry_sma

        return is_zscore_entry_condition_met and is_price_entry_condition_met

    def should_exit_on_zscore_and_sma(self) -> bool:
        if self._last_zscore is None or self._last_close_price is None:
            return False

        is_price_exit_condition_met: bool = False  # Default to False if no SMA check
        if self.config.position_direction == PositionDirection.LONG:
            is_zscore_exit_condition_met = self._last_zscore >= self.config.upper_threshold
            if self.config.exit_sma_window is not None:
                if self._last_exit_sma is None:
                    return False
                is_price_exit_condition_met = self._last_close_price < self._last_exit_sma
        else:  # SHORT
            is_zscore_exit_condition_met = self._last_zscore <= self.config.lower_threshold
            if self.config.exit_sma_window is not None:
                if self._last_exit_sma is None:
                    return False
                is_price_exit_condition_met = self._last_close_price > self._last_exit_sma

        return is_zscore_exit_condition_met or is_price_exit_condition_met

    def should_create_entry(self) -> bool:
        if not self.market_data_provider.ready:
            return False  # from ControllerBase
        if len(self.get_active_executors()) > 0:
            return False  # Simplified check for active position for this controller
        if not self.is_within_entry_window():
            return False
        if not self.is_candle_data_updated():
            return False
        if not self.is_funding_rate_data_updated():
            return False

        is_potential_entry = self.should_entry_on_zscore_and_sma()
        is_potential_exit = self.should_exit_on_zscore_and_sma()  # Check to avoid conflicting signals
        if is_potential_entry and is_potential_exit:
            return False
        return is_potential_entry

    def should_create_exit(self) -> bool:
        if not self.market_data_provider.ready:
            return False
        if not self.get_active_executors():
            return False  # No active position to exit
        if not self.is_within_entry_window():
            return False
        if not self.is_candle_data_updated():
            return False
        if not self.is_funding_rate_data_updated():
            return False

        is_potential_entry = self.should_entry_on_zscore_and_sma()  # Check to avoid conflicting signals
        is_potential_exit = self.should_exit_on_zscore_and_sma()
        if is_potential_entry and is_potential_exit:
            return False
        return is_potential_exit

    def determine_executor_actions(self) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        if self.should_create_entry():
            entry_size = self.get_entry_size()
            if entry_size > Decimal("0"):
                price = self.market_data_provider.get_price_by_type(
                    self.config.connector_name,
                    self.config.trading_pair,
                    (
                        PriceType.BestAsk
                        if self.config.position_direction == PositionDirection.LONG
                        else PriceType.BestBid
                    ),
                )  # Market order, price might be for reference or if executor needs it
                if price is None or price <= Decimal("0"):
                    price = self._last_close_price  # Fallback

                trade_type = (
                    TradeType.BUY if self.config.position_direction == PositionDirection.LONG else TradeType.SELL
                )
                executor_config = PositionExecutorConfig(
                    timestamp=self.market_data_provider.time(),
                    controller_id=self.config.id,
                    connector_name=self.config.connector_name,
                    trading_pair=self.config.trading_pair,
                    side=trade_type,
                    entry_price=price,  # For market orders, executor might ignore this or use for slippage control
                    amount=entry_size,
                    triple_barrier_config=self.config.triple_barrier_config,
                    leverage=self.config.leverage,
                    position_mode=self.config.position_mode,
                )
                actions.append(CreateExecutorAction(executor_config=executor_config, controller_id=self.config.id))

        if self.should_create_exit():
            active_execs = self.get_active_executors()
            for exec_info in active_execs:
                actions.append(StopExecutorAction(executor_id=exec_info.id, controller_id=self.config.id))
        return actions

    def to_format_status(self) -> List[str]:
        lines = super().to_format_status()
        lines.append(
            f"  Funding Rate Feed: {self._funding_rate_feed.name if self._funding_rate_feed else 'N/A'} "
            f"(Ready: {self._funding_rate_feed.ready if self._funding_rate_feed else 'N/A'})"
        )
        lines.append(f"  Last Z-Score: {self._last_zscore if self._last_zscore is not None else 'N/A'}")
        lines.append(f"  Last Close Price: {self._last_close_price if self._last_close_price is not None else 'N/A'}")
        lines.append(f"  Position Direction: {self.config.position_direction.value}")
        lines.append(
            f"  Upper Threshold: {self.config.upper_threshold}, Lower Threshold: {self.config.lower_threshold}"
        )
        return lines
