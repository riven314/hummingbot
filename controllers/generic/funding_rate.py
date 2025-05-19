import asyncio
from decimal import Decimal
from enum import Enum
from typing import Any, Optional

import pandas as pd
from pydantic import Field, validator

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.data_feed.funding_rate.constants import TradingIntervalType
from hummingbot.data_feed.funding_rate.data_types import FundingRateInterval
from hummingbot.data_feed.funding_rate.funding_rate_data_feed import FundingRateDataFeed
from hummingbot.data_feed.funding_rate.utils.interval_utils import IntervalUtility
from hummingbot.data_feed.funding_rate.utils.time_utils import TimeUtility
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction


class PositionDirection(str, Enum):
    LONG = "long"
    SHORT = "short"


class FundingRateControllerConfig(ControllerConfigBase):
    # controller type follows its parent class, controller name follows its file name
    controller_type = "generic"
    controller_name = "funding_rate"
    exchange: str = Field(
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the exchange name"),
    )
    trading_pair: str = Field(
        default="BTC-USDT", client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the trading pair")
    )
    candle_interval: TradingIntervalType = Field(
        default="1h",
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the candle interval (e.g. 1h)"),
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
    leverage: int = Field(
        default=1,
        gt=0,
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Leverage (e.g. 10 for 10x)"),
    )

    @validator("exchange")
    def validate_exchange(cls, v: str) -> str:
        if v != "binance_perpetual":
            raise ValueError("Exchange must be binance_perpetual")
        return v

    @property
    def triple_barrier_config(self) -> TripleBarrierConfig:
        return TripleBarrierConfig(
            stop_loss=None,
            take_profit=None,
            open_order_type=OrderType.MARKET,
        )


class FundingRateController(ControllerBase):
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

    def determine_executor_actions(self) -> list[ExecutorAction]:
        actions: list[ExecutorAction] = []
        actions.extend(self.create_actions_proposal())
        actions.extend(self.stop_actions_proposal())
        # actions.extend(self.store_actions_proposal())
        return actions

    def get_available_balance(self, asset: str) -> Decimal:
        connector = self.market_data_provider.get_connector(self.config.exchange)
        return connector.get_available_balance(asset)

    def notify_hb_app(self, msg: str):
        from hummingbot.client.hummingbot_application import HummingbotApplication

        HummingbotApplication.main_application().notify(msg)

    def notify_hb_app_with_timestamp(self, msg: str):
        timestamp = pd.Timestamp.fromtimestamp(self.current_timestamp)
        self.notify_hb_app(f"({timestamp}) {msg}")

    def start(self):
        super().start()
        msg = f"Starting FundingRateController for {self.config.trading_pair}: "
        msg += f"position_direction: {self.config.position_direction}, "
        msg += f"zscore_window: {self.config.zscore_window}, "
        msg += f"entry_sma_window: {self.config.entry_sma_window}, "
        msg += f"exit_sma_window: {self.config.exit_sma_window}"
        self.notify_hb_app_with_timestamp(msg)

    @property
    def current_timestamp(self) -> float:
        return self.market_data_provider.time()

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
            connector_name=self.config.exchange,
            trading_pair=self.config.trading_pair,
            interval=self.config.candle_interval,
            max_records=max_records,
        )
        return candles_df

    def get_candle_data(self, max_records: int) -> pd.DataFrame:
        candles_df = self.market_data_provider.get_candles_df(
            connector_name=self.config.exchange,
            trading_pair=self.config.trading_pair,
            interval=self.config.candle_interval,
            max_records=max_records,
        )
        return candles_df

    def get_last_close_price(self) -> Optional[Decimal]:
        candles_df = self.get_candle_df()
        if len(candles_df) < 2:
            self.logger().warning("Not enough candle data to get last close price.")
            return None
        last_completed_candle = candles_df.iloc[-2]
        return Decimal(str(last_completed_candle["close"]))

    def get_last_sma(self, window: int) -> Optional[Decimal]:
        candles_df = self.get_candle_df()
        if len(candles_df) < window + 1:
            self.logger().warning(
                f"Not enough candle data ({len(candles_df)}) for SMA window {window} to calculate for previous candle."
            )
            return None
        sma = candles_df["close"].rolling(window=window).mean()
        return Decimal(str(sma.iloc[-2]))

    def get_last_zscore(self) -> Optional[Decimal]:
        if not self._funding_rate_feed or not self._funding_rate_feed.is_updated():
            self.logger().warning("FundingRateDataFeed is not updated, return None for last zscore value")
            return None

        last_interval_data: Optional[FundingRateInterval] = self._funding_rate_feed.last_funding_rate_interval
        if not last_interval_data:
            self.logger().warning(
                f"last entry of FundingRateInterval is not available from {self._funding_rate_feed.name}"
            )
            return None

        if last_interval_data.zscores is None:
            self.logger().warning(
                f"Last entry of FundingRateInterval has zscores = None for {self._funding_rate_feed.name}"
            )
            return None

        zscore_key = f"zscore_{self.config.zscore_window}"
        specific_zscore_value = last_interval_data.zscores.get(zscore_key)
        if specific_zscore_value is None:
            self.logger().warning(
                f"Z-score for window {self.config.zscore_window} (key: {zscore_key}) not found in "
                f"available zscores: {last_interval_data.zscores} from {self._funding_rate_feed.name}."
            )
            return None
        return Decimal(str(specific_zscore_value))

    def get_entry_size(self) -> Decimal:
        trading_pair = self.config.trading_pair
        base_asset, quote_asset = trading_pair.split("-")
        available_balance_quote = self.get_available_balance(quote_asset)

        # Estimate price, as we use market order.
        price_type = (
            PriceType.BestAsk if self.config.position_direction == PositionDirection.LONG else PriceType.BestBid
        )
        price = self.market_data_provider.get_price_by_type(self.config.exchange, trading_pair, price_type)
        if not isinstance(price, Decimal) or price <= Decimal("0"):
            self.logger().warning(
                f"Invalid price ({price}) for {trading_pair} using {price_type.name}, cannot calculate max possible size."
            )
            return Decimal("0")

        # Calculate max size possible with available quote balance and leverage
        # Amount_base = (Balance_quote * Leverage) / Price_base_quote
        max_possible_size_quote_adjusted = (available_balance_quote * self.config.leverage) / price

        configured_size_base = self.config.position_size
        entry_size_base = min(max_possible_size_quote_adjusted, configured_size_base)

        if entry_size_base <= Decimal("0"):
            self.logger().warning(
                f"Not enough balance to open position for {trading_pair}. "
                f"Available quote: {available_balance_quote:.6f} {quote_asset}, "
                f"Max possible size (leveraged): {max_possible_size_quote_adjusted:.6f} {base_asset}. "
                f"Configured size: {configured_size_base:.6f} {base_asset}."
            )
            return Decimal("0")

        if entry_size_base < configured_size_base and entry_size_base > Decimal("0"):
            self.logger().warning(
                f"Available balance for {quote_asset} with leverage {self.config.leverage}x "
                f"allows max size of {max_possible_size_quote_adjusted:.6f} {base_asset}. "
                f"Configured position size is {configured_size_base:.6f} {base_asset}. "
                f"Using smaller size: {entry_size_base:.6f} {base_asset}."
            )
        return entry_size_base

    def is_market_data_ready(self) -> bool:
        is_ready = self.market_data_provider.ready
        if not is_ready:
            self.logger().warning("MarketDataProvider is not ready, skipping entry/exit decision.")
        return is_ready

    def is_ready_for_new_position(self) -> bool:
        return self.active_position is None

    def is_within_entry_window(self) -> bool:
        candles_df = self.get_candle_df()
        if len(candles_df) < 2:
            self.logger().warning("Not enough candle data to determine entry window.")
            return False

        current_timestamp_s = self.current_timestamp  # in seconds
        current_candle_open_timestamp_s = candles_df.iloc[-1]["timestamp"]  # This is float from pandas
        if pd.isna(current_candle_open_timestamp_s):
            self.logger().warning("Current candle open timestamp is NaN.")
            return False

        elapsed_seconds_since_open = current_timestamp_s - float(current_candle_open_timestamp_s)
        is_within_window = 0 <= elapsed_seconds_since_open <= self.config.entry_window_in_sec

        if is_within_window:
            current_candle_open_time = pd.Timestamp(current_candle_open_timestamp_s, unit="s", tz="UTC")
            current_time = pd.Timestamp(current_timestamp_s, unit="s", tz="UTC")
            self.logger().info(
                f"Current time: {current_time} IS within entry window of candle opened at {current_candle_open_time} "
                f"({elapsed_seconds_since_open:.2f}s elapsed of {self.config.entry_window_in_sec}s window)"
            )
        return is_within_window

    def is_candle_data_updated(self) -> bool:
        candles_df = self.get_candle_df()
        if len(candles_df) < 2:  # Need at least two candles: one completed, one current
            self.logger().warning("Not enough candle data to check if updated.")
            return False

        # Last completed candle is at index -2
        last_completed_candle_timestamp_s = candles_df.iloc[-2]["timestamp"]
        if pd.isna(last_completed_candle_timestamp_s):
            self.logger().warning("Last completed candle timestamp is NaN.")
            return False

        now_ms = TimeUtility.now_ms()
        expected_last_completed_interval_start_ms = IntervalUtility.get_last_interval_start_timestamp(
            now_ms, self.config.candle_interval
        )

        last_completed_candle_start_timestamp_ms = int(float(last_completed_candle_timestamp_s) * 1000)
        is_updated = last_completed_candle_start_timestamp_ms == expected_last_completed_interval_start_ms
        if not is_updated:
            self.logger().warning(
                f"Candle data not updated. Last completed candle start time: {pd.Timestamp(last_completed_candle_start_timestamp_ms, unit='ms', tz='UTC')} "
                f"(expected start time: {pd.Timestamp(expected_last_completed_interval_start_ms, unit='ms', tz='UTC')})."
            )
        return is_updated

    def is_funding_rate_data_updated(self) -> bool:
        if not self._funding_rate_feed or not self._funding_rate_feed.is_updated():
            self.logger().warning("FundingRateDataFeed is not updated, return None for last zscore value")
            return False
        is_updated = self._funding_rate_feed.is_updated()
        if not is_updated:
            self.logger().warning(f"{self._funding_rate_feed.name} is not updated.")
        return is_updated

    def should_entry_on_zscore_and_sma(self) -> bool:
        zscore = self.get_last_zscore()
        if zscore is None:
            self.logger().warning("Last zscore is None at should_entry_on_zscore_and_sma")
            return False

        last_close_price = self.get_last_close_price()
        if last_close_price is None:
            self.logger().warning("Last close price is None at should_entry_on_zscore_and_sma")
            return False

        # Default to true if no SMA check
        is_price_entry_condition_met: bool = True
        if self.config.position_direction == PositionDirection.LONG:
            is_zscore_entry_condition_met = zscore <= self.config.lower_threshold
            if self.config.entry_sma_window is not None:
                entry_sma = self.get_last_sma(self.config.entry_sma_window)
                if entry_sma is None:
                    self.logger().warning("Last entry SMA is None for LONG entry check")
                    return False
                is_price_entry_condition_met = last_close_price > entry_sma
        else:
            is_zscore_entry_condition_met = zscore >= self.config.upper_threshold
            if self.config.entry_sma_window is not None:
                entry_sma = self.get_last_sma(self.config.entry_sma_window)
                if entry_sma is None:
                    self.logger().warning("Last entry SMA is None for SHORT entry check")
                    return False
                is_price_entry_condition_met = last_close_price < entry_sma

        return is_zscore_entry_condition_met and is_price_entry_condition_met

    def should_exit_on_zscore_and_sma(self) -> bool:
        zscore = self.get_last_zscore()
        if zscore is None:
            self.logger().warning("Last zscore is None at should_exit_on_zscore_and_sma")
            return False

        last_close_price = self.get_last_close_price()
        if last_close_price is None:
            self.logger().warning("Last close price is None at should_exit_on_zscore_and_sma")
            return False

        # Default to false if no SMA check, so that exit signal won't always trigger
        is_price_exit_condition_met: bool = False
        if self.config.position_direction == PositionDirection.LONG:
            is_zscore_exit_condition_met = zscore >= self.config.upper_threshold
            if self.config.exit_sma_window is not None:
                exit_sma = self.get_last_sma(self.config.exit_sma_window)
                if exit_sma is None:
                    self.logger().warning("Last exit SMA is None for LONG exit check")
                    return False
                is_price_exit_condition_met = last_close_price < exit_sma
        else:
            is_zscore_exit_condition_met = zscore <= self.config.lower_threshold
            if self.config.exit_sma_window is not None:
                exit_sma = self.get_last_sma(self.config.exit_sma_window)
                if exit_sma is None:
                    self.logger().warning("Last exit SMA is None for SHORT exit check")
                    return False
                is_price_exit_condition_met = last_close_price > exit_sma

        return is_zscore_exit_condition_met or is_price_exit_condition_met

    def should_create_entry(self) -> bool:
        if not self.is_market_data_ready():
            return False
        if not self.is_ready_for_new_position():
            return False
        if not self.is_within_entry_window():
            return False
        if not self.is_candle_data_updated():
            return False
        if not self.is_funding_rate_data_updated():
            return False

        is_potential_entry = self.should_entry_on_zscore_and_sma()
        is_potential_exit = self.should_exit_on_zscore_and_sma()
        if is_potential_entry and is_potential_exit:
            self.logger().info("Entry and Exit signals triggered simultaneously. Ignoring both for entry decision.")
            return False
        return is_potential_entry

    def should_create_exit(self) -> bool:
        if not self.is_market_data_ready():
            return False
        if self.active_position is None:
            return False
        if not self.is_within_entry_window():
            return False
        if not self.is_candle_data_updated():
            return False
        if not self.is_funding_rate_data_updated():
            return False

        is_potential_entry = self.should_entry_on_zscore_and_sma()
        is_potential_exit = self.should_exit_on_zscore_and_sma()
        if is_potential_entry and is_potential_exit:
            self.logger().info("Entry and Exit signals triggered simultaneously. Ignoring both for exit decision.")
            return False
        return is_potential_exit

    def create_actions_proposal(self) -> list[CreateExecutorAction]:
        if not self.should_create_entry():
            return []

        entry_size = self.get_entry_size()
        if entry_size <= Decimal("0"):
            return []

        trade_type = TradeType.BUY if self.config.position_direction == PositionDirection.LONG else TradeType.SELL
        executor_config = PositionExecutorConfig(
            timestamp=self.current_timestamp,
            connector_name=self.config.exchange,
            trading_pair=self.config.trading_pair,
            side=trade_type,
            amount=entry_size,
            triple_barrier_config=self.config.triple_barrier_config,
            leverage=self.config.leverage,
        )

        last_close_price = self.get_last_close_price()
        zscore = self.get_last_zscore()
        assert isinstance(last_close_price, Decimal) and isinstance(zscore, Decimal)
        self.log_and_notify_open_position(entry_size, last_close_price, zscore)
        return [CreateExecutorAction(executor_config=executor_config)]

    def stop_actions_proposal(self) -> list[StopExecutorAction]:
        if not self.should_create_exit():
            return []

        assert self.active_position
        executor_id = self.active_position["executor_id"]
        last_close_price = self.get_last_close_price()
        zscore = self.get_last_zscore()
        if last_close_price is None or zscore is None:
            self.logger().warning(
                f"Cant create exit action due to missing price ({last_close_price}) or z-score ({zscore})."
            )
            return []

        self.log_and_notify_close_position(last_close_price, zscore)
        return [StopExecutorAction(executor_id=executor_id)]

    def log_and_notify_open_position(self, entry_size: Decimal, last_close_price: Decimal, zscore: Decimal):
        entry_sma_str = "N/A"
        if self.config.entry_sma_window is not None:
            entry_sma = self.get_last_sma(self.config.entry_sma_window)
            if entry_sma is not None:
                entry_sma_str = f"{entry_sma:.4f}"

        direction_str = self.config.position_direction.value.upper()
        zscore_condition_str = (
            f"(ZSCORE: {zscore:.4f} <= LOWER_THRESHOLD: {self.config.lower_threshold:.4f})"
            if self.config.position_direction == PositionDirection.LONG
            else f"(ZSCORE: {zscore:.4f} >= UPPER_THRESHOLD: {self.config.upper_threshold:.4f})"
        )
        price_condition_str = (
            f"(CLOSE PRICE: {last_close_price:.4f} > ENTRY_SMA: {entry_sma_str})"
            if self.config.position_direction == PositionDirection.LONG
            else f"(CLOSE PRICE: {last_close_price:.4f} < ENTRY_SMA: {entry_sma_str})"
        )

        msg = (
            f"Opening {direction_str} for {self.config.trading_pair} | Size: {entry_size:.6f} | "
            f"{zscore_condition_str} AND {price_condition_str}"
        )
        self.logger().info(msg)
        self.notify_hb_app_with_timestamp(msg)

    def log_and_notify_close_position(self, last_close_price: Decimal, zscore: Decimal):
        exit_sma_str = "N/A"
        if self.config.exit_sma_window is not None:
            exit_sma = self.get_last_sma(self.config.exit_sma_window)
            if exit_sma is not None:
                exit_sma_str = f"{exit_sma:.4f}"

        direction_str = self.config.position_direction.value.upper()
        zscore_condition_str = (
            f"(ZSCORE: {zscore:.4f} >= UPPER_THRESHOLD: {self.config.upper_threshold:.4f})"
            if self.config.position_direction == PositionDirection.LONG
            else f"(ZSCORE: {zscore:.4f} <= LOWER_THRESHOLD: {self.config.lower_threshold:.4f})"
        )
        price_condition_str = (
            f"(CLOSE PRICE: {last_close_price:.4f} < EXIT_SMA: {exit_sma_str})"
            if self.config.position_direction == PositionDirection.LONG
            else f"(CLOSE PRICE: {last_close_price:.4f} > EXIT_SMA: {exit_sma_str})"
        )
        msg = (
            f"Closing {direction_str} for {self.config.trading_pair} | "
            f"{zscore_condition_str} OR {price_condition_str}"
        )
        self.logger().info(msg)
        self.notify_hb_app_with_timestamp(msg)

    def to_format_status(self) -> list[str]:
        if not self.is_market_data_ready():
            return ["MarketDataProvider is not ready, skipping entry/exit decision."]

        lines = []
        entry_sma_str = self.config.entry_sma_window if self.config.entry_sma_window is not None else "N/A"
        exit_sma_str = self.config.exit_sma_window if self.config.exit_sma_window is not None else "N/A"

        lines.append("Controller Parameters:")
        lines.append(f"  Position Direction: {self.config.position_direction.value.capitalize()}")
        lines.append(f"  Upper Threshold: {self.config.upper_threshold}")
        lines.append(f"  Lower Threshold: {self.config.lower_threshold}")
        lines.append(f"  Z-score Window: {self.config.zscore_window}")
        lines.append(f"  Entry SMA Window: {entry_sma_str}")
        lines.append(f"  Exit SMA Window: {exit_sma_str}")

        return lines
