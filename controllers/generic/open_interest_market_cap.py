import asyncio
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional

import pandas as pd  # type: ignore
from pydantic import Field

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.data_feed.market_data_provider import MarketDataProvider
from hummingbot.data_feed.open_interest_market_cap.data_types import IntervalType, OpenInterestMarketCapRecord
from hummingbot.data_feed.open_interest_market_cap.open_interest_market_cap_feed import OpenInterestMarketCapFeed
from hummingbot.data_feed.open_interest_market_cap.utils import IntervalUtility, TimeUtility
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction


class PositionDirection(str, Enum):
    LONG = "long"
    SHORT = "short"


class OpenInterestMarketCapControllerConfig(ControllerConfigBase):
    controller_type: str = "generic"
    controller_name: str = "open_interest_market_cap"

    exchange: str = Field(
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the exchange name"),
    )
    trading_pair: str = Field(
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the trading pair (e.g., BTC-USDT)")
    )
    position_direction: PositionDirection = Field(
        client_data=ClientFieldData(
            prompt_on_new=True, prompt=lambda mi: "Enter the position direction ('long' or 'short')"
        ),
    )
    position_size: Decimal = Field(
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the position size (token amount)"),
    )
    candle_interval: IntervalType = Field(
        client_data=ClientFieldData(
            prompt_on_new=True, prompt=lambda mi: "Enter the candle interval (e.g. 1d, 1h, 1m)"
        ),
    )
    entry_window_in_sec: int = Field(
        gt=0,
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Enter the entry window in seconds",
        ),
    )
    upper_threshold: Decimal = Field(
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the upper threshold for z-score"),
    )
    lower_threshold: Decimal = Field(
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Enter the lower threshold for z-score"),
    )
    zscore_window: int = Field(
        gt=0,
        client_data=ClientFieldData(
            prompt_on_new=True, prompt=lambda mi: "Enter the window size for z-score calculation"
        ),
    )
    entry_sma_window: Optional[int] = Field(
        default=None,
        client_data=ClientFieldData(
            prompt_on_new=True, prompt=lambda mi: "Enter the SMA window size for entry (optional)"
        ),
    )
    exit_sma_window: Optional[int] = Field(
        default=None,
        client_data=ClientFieldData(
            prompt_on_new=True, prompt=lambda mi: "Enter the SMA window size for exit (optional)"
        ),
    )
    leverage: int = Field(
        default=1,
        gt=0,
        client_data=ClientFieldData(prompt_on_new=True, prompt=lambda mi: "Leverage (e.g. 10 for 10x)"),
    )

    @property
    def triple_barrier_config(self) -> TripleBarrierConfig:
        return TripleBarrierConfig(
            stop_loss=None,
            take_profit=None,
            open_order_type=OrderType.MARKET,
        )


class OpenInterestMarketCapController(ControllerBase):
    def __init__(
        self,
        config: OpenInterestMarketCapControllerConfig,
        market_data_provider: MarketDataProvider,
        actions_queue: asyncio.Queue,
    ):
        super().__init__(config, market_data_provider, actions_queue)
        self.config: OpenInterestMarketCapControllerConfig = config
        self._oi_mcap_feed: Optional[OpenInterestMarketCapFeed] = None

    def set_oi_mcap_feed(self, feed: OpenInterestMarketCapFeed):
        self._oi_mcap_feed = feed

    @property
    def current_timestamp(self) -> float:
        return self.market_data_provider.time()

    @property
    def tag(self) -> str:
        return (
            f"{self.config.controller_name}:{self.config.trading_pair}:{self.config.position_direction.value}:"
            f"z{self.config.zscore_window}:u{self.config.upper_threshold:.2f}:l{self.config.lower_threshold:.2f}"
        )

    def _notify_hb_app(self, msg: str):
        from hummingbot.client.hummingbot_application import HummingbotApplication

        if HummingbotApplication.main_application():
            HummingbotApplication.main_application().notify(msg)

    def _notify_hb_app_with_timestamp(self, msg: str):
        timestamp = pd.Timestamp.fromtimestamp(self.current_timestamp)
        self._notify_hb_app(f"({timestamp}) [{self.tag}] {msg}")

    def start(self):
        super().start()
        msg = (
            f"Starting {self.config.controller_name} for {self.config.trading_pair} | "
            f"Direction: {self.config.position_direction.value} | Z-Window: {self.config.zscore_window} | "
            f"Upper: {self.config.upper_threshold:.2f} | Lower: {self.config.lower_threshold:.2f} | "
            f"Entry SMA: {self.config.entry_sma_window or 'N/A'} | Exit SMA: {self.config.exit_sma_window or 'N/A'}"
        )
        self.logger().info(msg)
        self._notify_hb_app_with_timestamp(
            f"Controller started. Config: {self.config.dict(exclude={'controller_name', 'controller_type', 'id'})}"
        )

    def determine_executor_actions(self) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        actions.extend(self.create_actions_proposal())
        actions.extend(self.stop_actions_proposal())
        return actions

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
            self.logger().warning(f"Not enough candle data ({len(candles_df)}) for SMA window {window}.")
            return None
        sma = candles_df["close"].rolling(window=window).mean()
        return Decimal(str(sma.iloc[-2]))

    def get_last_zscore(self) -> Optional[Decimal]:
        if not self._oi_mcap_feed or not self._oi_mcap_feed.ready:
            self.logger().warning("OI MCAP Feed is not available or not ready.")
            return None
        if not self._oi_mcap_feed._queue:
            self.logger().warning("OI MCAP Feed queue is empty.")
            return None

        last_record: OpenInterestMarketCapRecord = self._oi_mcap_feed._queue[-1]
        if last_record.zscores and self.config.zscore_window in last_record.zscores:
            zscore_value = last_record.zscores[self.config.zscore_window]
            return Decimal(str(zscore_value)) if zscore_value is not None else None

        self.logger().warning(
            f"Z-score for window {self.config.zscore_window} not found in last OI MCAP record. Available: {last_record.zscores}"
        )
        return None

    def is_market_data_ready(self) -> bool:
        is_ready = self.market_data_provider.ready
        if not is_ready:
            self.logger().debug("MarketDataProvider is not ready.")
        return is_ready

    def is_oi_mcap_data_updated(self) -> bool:
        if not self._oi_mcap_feed:
            self.logger().debug("OI MCAP Feed not set.")
            return False
        if not self._oi_mcap_feed.ready:
            self.logger().debug(f"{self._oi_mcap_feed.name} (OI MCAP Feed) is not ready.")
            return False
        is_updated = self._oi_mcap_feed.is_updated()
        if not is_updated:
            self.logger().info(f"{self._oi_mcap_feed.name} (OI MCAP Feed) is not updated.")
        return is_updated

    def is_candle_data_updated(self) -> bool:
        candles_df = self.get_candle_df()
        if len(candles_df) < 2:
            self.logger().debug("Not enough candle data to check if updated.")
            return False

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
            self.logger().info(
                f"Candle data not updated. Last completed: {pd.Timestamp(last_completed_candle_start_timestamp_ms, unit='ms', tz='UTC')} "
                f"(Expected: {pd.Timestamp(expected_last_completed_interval_start_ms, unit='ms', tz='UTC')})."
            )
        return is_updated

    def is_ready_for_new_position(self) -> bool:
        return self.active_position is None

    def is_within_entry_window(self) -> bool:
        candles_df = self.get_candle_df()
        if len(candles_df) < 1:
            self.logger().debug("Not enough candle data for entry window check.")
            return False

        current_timestamp_s = self.current_timestamp
        current_candle_open_timestamp_s = candles_df.iloc[-1]["timestamp"]
        if pd.isna(current_candle_open_timestamp_s):
            self.logger().warning("Current candle open timestamp is NaN for entry window check.")
            return False

        elapsed_seconds_since_open = current_timestamp_s - float(current_candle_open_timestamp_s)
        is_within_window = 0 <= elapsed_seconds_since_open <= self.config.entry_window_in_sec

        if is_within_window:
            self.logger().debug(
                f"Current time is within entry window: {elapsed_seconds_since_open:.2f}s / {self.config.entry_window_in_sec}s."
            )
        return is_within_window

    def should_entry_on_zscore_and_sma(self) -> bool:
        zscore = self.get_last_zscore()
        if zscore is None:
            return False

        last_close_price = self.get_last_close_price()
        if last_close_price is None:
            return False

        is_price_entry_condition_met: bool = True
        if self.config.position_direction == PositionDirection.LONG:
            is_zscore_entry_condition_met = zscore <= self.config.lower_threshold
            if self.config.entry_sma_window is not None:
                entry_sma = self.get_last_sma(self.config.entry_sma_window)
                if entry_sma is None:
                    return False
                is_price_entry_condition_met = last_close_price > entry_sma
        else:
            is_zscore_entry_condition_met = zscore >= self.config.upper_threshold
            if self.config.entry_sma_window is not None:
                entry_sma = self.get_last_sma(self.config.entry_sma_window)
                if entry_sma is None:
                    return False
                is_price_entry_condition_met = last_close_price < entry_sma

        return is_zscore_entry_condition_met and is_price_entry_condition_met

    def should_exit_on_zscore_and_sma(self) -> bool:
        zscore = self.get_last_zscore()
        if zscore is None:
            return False

        last_close_price = self.get_last_close_price()
        if last_close_price is None:
            return False

        is_price_exit_condition_met: bool = False
        if self.config.position_direction == PositionDirection.LONG:
            is_zscore_exit_condition_met = zscore >= self.config.upper_threshold
            if self.config.exit_sma_window is not None:
                exit_sma = self.get_last_sma(self.config.exit_sma_window)
                if exit_sma is None:
                    return False
                is_price_exit_condition_met = last_close_price < exit_sma
        else:
            is_zscore_exit_condition_met = zscore <= self.config.lower_threshold
            if self.config.exit_sma_window is not None:
                exit_sma = self.get_last_sma(self.config.exit_sma_window)
                if exit_sma is None:
                    return False
                is_price_exit_condition_met = last_close_price > exit_sma

        if self.config.exit_sma_window is not None:
            return is_zscore_exit_condition_met or is_price_exit_condition_met
        else:
            return is_zscore_exit_condition_met

    def should_create_entry(self) -> bool:
        if not all(
            [
                self.is_market_data_ready(),
                self.is_ready_for_new_position(),
                self.is_within_entry_window(),
                self.is_candle_data_updated(),
                self.is_oi_mcap_data_updated(),
            ]
        ):
            return False

        is_entry_signal = self.should_entry_on_zscore_and_sma()
        is_exit_signal = self.should_exit_on_zscore_and_sma()

        if is_entry_signal and is_exit_signal:
            self.logger().info("Entry and Exit signals triggered simultaneously. Skipping entry.")
            return False
        return is_entry_signal

    def should_create_exit(self) -> bool:
        if not all(
            [
                self.is_market_data_ready(),
                self.active_position is not None,
                self.is_within_entry_window(),
                self.is_candle_data_updated(),
                self.is_oi_mcap_data_updated(),
            ]
        ):
            return False

        is_entry_signal = self.should_entry_on_zscore_and_sma()
        is_exit_signal = self.should_exit_on_zscore_and_sma()

        if is_entry_signal and is_exit_signal:
            self.logger().info("Entry and Exit signals triggered simultaneously. Prioritizing exit if conditions met.")
        return is_exit_signal

    def get_entry_size(self) -> Decimal:
        trading_pair = self.config.trading_pair
        base_asset, quote_asset = trading_pair.split("-")

        connector = self.market_data_provider.get_connector(self.config.exchange)
        available_balance_quote = connector.get_available_balance(quote_asset)

        price_type = (
            PriceType.BestAsk if self.config.position_direction == PositionDirection.LONG else PriceType.BestBid
        )
        price = self.market_data_provider.get_price_by_type(self.config.exchange, trading_pair, price_type)

        if not isinstance(price, Decimal) or price <= Decimal("0"):
            self.logger().warning(
                f"Invalid price ({price}) for {trading_pair} using {price_type.name}. Cannot calculate max size."
            )
            return Decimal("0")

        max_possible_size = (available_balance_quote * self.config.leverage) / price
        configured_size = self.config.position_size
        entry_size = min(max_possible_size, configured_size)

        if entry_size <= Decimal("0"):
            self.logger().warning(
                f"Not enough balance for {trading_pair}. Avail: {available_balance_quote:.4f} {quote_asset}, "
                f"Price: {price:.4f}, Max Size (Lev): {max_possible_size:.4f} {base_asset}. "
                f"Config Size: {configured_size:.4f} {base_asset}."
            )
            return Decimal("0")

        if entry_size < configured_size:
            self.logger().info(
                f"Balance for {quote_asset} (lev {self.config.leverage}x) allows max size {max_possible_size:.4f} {base_asset}. "
                f"Using smaller size: {entry_size:.4f} {base_asset} (Config: {configured_size:.4f} {base_asset})."
            )
        return entry_size

    def _log_and_notify_open_position(self, entry_size: Decimal, last_close_price: Decimal, zscore: Decimal):
        entry_sma_str = "N/A"
        if self.config.entry_sma_window is not None:
            entry_sma = self.get_last_sma(self.config.entry_sma_window)
            if entry_sma is not None:
                entry_sma_str = f"{entry_sma:.4f}"

        direction_str = self.config.position_direction.value.upper()
        zscore_cond_str = (
            f"(ZSCORE: {zscore:.4f} <= LOWER_THRESHOLD: {self.config.lower_threshold:.4f})"
            if self.config.position_direction == PositionDirection.LONG
            else f"(ZSCORE: {zscore:.4f} >= UPPER_THRESHOLD: {self.config.upper_threshold:.4f})"
        )
        price_cond_str = (
            f"(CLOSE PRICE: {last_close_price:.4f} > ENTRY_SMA: {entry_sma_str})"
            if self.config.position_direction == PositionDirection.LONG
            else f"(CLOSE PRICE: {last_close_price:.4f} < ENTRY_SMA: {entry_sma_str})"
        )
        if self.config.entry_sma_window is None:
            price_cond_str = "(No SMA Check)"

        msg = (
            f"Opening {direction_str} for {self.config.trading_pair} | Size: {entry_size:.6f} | "
            f"{zscore_cond_str} AND {price_cond_str}"
        )
        self.logger().info(msg)
        self._notify_hb_app_with_timestamp(msg)

    def _log_and_notify_close_position(self, last_close_price: Decimal, zscore: Decimal):
        exit_sma_str = "N/A"
        if self.config.exit_sma_window is not None:
            exit_sma = self.get_last_sma(self.config.exit_sma_window)
            if exit_sma is not None:
                exit_sma_str = f"{exit_sma:.4f}"

        direction_str = self.config.position_direction.value.upper()
        zscore_cond_str = (
            f"(ZSCORE: {zscore:.4f} >= UPPER_THRESHOLD: {self.config.upper_threshold:.4f})"
            if self.config.position_direction == PositionDirection.LONG
            else f"(ZSCORE: {zscore:.4f} <= LOWER_THRESHOLD: {self.config.lower_threshold:.4f})"
        )
        price_cond_str = (
            f"(CLOSE PRICE: {last_close_price:.4f} < EXIT_SMA: {exit_sma_str})"
            if self.config.position_direction == PositionDirection.LONG
            else f"(CLOSE PRICE: {last_close_price:.4f} > EXIT_SMA: {exit_sma_str})"
        )
        if self.config.exit_sma_window is None:
            price_cond_str = "(No SMA Check for Price)"

        msg = f"Closing {direction_str} for {self.config.trading_pair} | " f"{zscore_cond_str} OR {price_cond_str}"
        self.logger().info(msg)
        self._notify_hb_app_with_timestamp(msg)

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
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
        if not (isinstance(last_close_price, Decimal) and isinstance(zscore, Decimal)):
            self.logger().warning(
                f"Missing price or zscore for logging open action. Price: {last_close_price}, Zscore: {zscore}"
            )
        else:
            self._log_and_notify_open_position(entry_size, last_close_price, zscore)

        return [CreateExecutorAction(controller_id=self.config.id, executor_config=executor_config)]

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        if not self.should_create_exit():
            return []

        active_pos = self.active_position
        if not active_pos or "executor_id" not in active_pos:
            self.logger().warning("Exit condition met, but no active position or executor_id found.")
            return []

        executor_id = active_pos["executor_id"]

        last_close_price = self.get_last_close_price()
        zscore = self.get_last_zscore()
        if not (isinstance(last_close_price, Decimal) and isinstance(zscore, Decimal)):
            self.logger().warning(
                f"Missing price or zscore for logging close action. Price: {last_close_price}, Zscore: {zscore}"
            )
        else:
            self._log_and_notify_close_position(last_close_price, zscore)

        return [StopExecutorAction(controller_id=self.config.id, executor_id=executor_id)]

    @property
    def active_position(self) -> Optional[Dict[str, Any]]:
        active_executors = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda e: e.is_active
            and e.trading_pair == self.config.trading_pair
            and e.connector_name == self.config.exchange,
        )
        if not active_executors:
            return None

        if len(active_executors) > 1:
            self.logger().warning(
                f"Found {len(active_executors)} active executors for {self.config.trading_pair}. Using the first one."
            )

        selected_executor = active_executors[0]
        entry_price = selected_executor.custom_info.get("current_position_average_price")
        if entry_price is None and selected_executor.filled_amount_base > 0:  # type: ignore
            entry_price = selected_executor.filled_amount_quote / selected_executor.filled_amount_base  # type: ignore

        return {
            "executor_id": selected_executor.id,
            "trading_pair": selected_executor.trading_pair,
            "connector_name": selected_executor.connector_name,
            "side": selected_executor.side,
            "amount": selected_executor.amount,  # type: ignore
            "entry_price": entry_price,
            "timestamp": selected_executor.timestamp,
            "is_trading": selected_executor.is_trading,
        }

    def to_format_status(self) -> List[str]:
        lines = []
        lines.append(f"\nController: {self.config.id} ({self.tag})")
        lines.append(f"  Exchange: {self.config.exchange}, Pair: {self.config.trading_pair}")
        lines.append(f"  Direction: {self.config.position_direction.value}, Leverage: {self.config.leverage}")
        lines.append(
            f"  Z-Score Window: {self.config.zscore_window}, Upper: {self.config.upper_threshold:.2f}, Lower: {self.config.lower_threshold:.2f}"
        )
        lines.append(
            f"  Entry SMA: {self.config.entry_sma_window or 'N/A'}, Exit SMA: {self.config.exit_sma_window or 'N/A'}"
        )
        lines.append(f"  Entry Window (s): {self.config.entry_window_in_sec}")

        active_pos = self.active_position
        if active_pos:
            lines.append("  Active Position:")
            lines.append(f"    Executor ID: {active_pos['executor_id']}")
            lines.append(f"    Side: {active_pos['side'].name}, Amount: {active_pos['amount']:.6f}")  # type: ignore
            entry_price_str = f"{active_pos['entry_price']:.4f}" if active_pos["entry_price"] else "N/A"
            lines.append(f"    Avg Entry Price: {entry_price_str}")
            created_ts = pd.Timestamp(active_pos["timestamp"], unit="s", tz="UTC")
            lines.append(f"    Created: {created_ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            lines.append(f"    Is Trading: {active_pos['is_trading']}")
        else:
            lines.append("  No active position.")

        lines.append(f"  Market Data Ready: {self.is_market_data_ready()}")
        lines.append(f"  Candle Data Updated: {self.is_candle_data_updated()}")
        lines.append(f"  OI MCAP Feed Set: {self._oi_mcap_feed is not None}")
        if self._oi_mcap_feed:
            lines.append(f"  OI MCAP Feed Ready: {self._oi_mcap_feed.ready}")
            lines.append(f"  OI MCAP Data Updated: {self.is_oi_mcap_data_updated()}")
            last_z = self.get_last_zscore()
            lines.append(f"  Last Z-Score ({self.config.zscore_window}): {last_z if last_z is not None else 'N/A'}")

        return lines
