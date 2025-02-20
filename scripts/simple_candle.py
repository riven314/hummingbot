import datetime
import os
from typing import Dict, List, Set

from pydantic import Field

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.data_feed.candles_feed.candles_factory import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class CandleMonitorConfig(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    markets: Dict[str, Set[str]] = {}
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []
    exchange: str = Field(
        default="binance",
        client_data=ClientFieldData(prompt=lambda mi: "Enter the exchange name (e.g. binance): ", prompt_on_new=True),
    )
    trading_pair: str = Field(
        default="BTC-USDT",
        client_data=ClientFieldData(prompt=lambda mi: "Enter the trading pair (e.g. BTC-USDT): ", prompt_on_new=True),
    )


class CandleMonitor(StrategyV2Base):
    @classmethod
    def init_markets(cls, config: CandleMonitorConfig):
        cls.markets = {config.exchange: {config.trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: CandleMonitorConfig):
        if len(config.candles_config) == 0:
            # Initialize candle configs for different timeframes
            config.candles_config.extend(
                [
                    CandlesConfig(
                        connector=config.exchange, trading_pair=config.trading_pair, interval="1h", max_records=100
                    ),
                    CandlesConfig(
                        connector=config.exchange, trading_pair=config.trading_pair, interval="1m", max_records=100
                    ),
                    CandlesConfig(
                        connector=config.exchange, trading_pair=config.trading_pair, interval="3m", max_records=100
                    ),
                ]
            )
        super().__init__(connectors, config)
        self.config = config

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        return []

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        return []

    def format_status(self) -> str:
        if not self.ready_to_trade:
            return "Market connectors are not ready."

        lines = []
        lines.append("\nCandle Data Monitor:")

        for interval in ["1h", "1m", "3m"]:
            candles_df = self.market_data_provider.get_candles_df(
                connector_name=self.config.exchange,
                trading_pair=self.config.trading_pair,
                interval=interval,
                max_records=1,
            )

            if not candles_df.empty:
                last_candle = candles_df.iloc[-1]
                timestamp = datetime.datetime.fromtimestamp(last_candle["timestamp"], tz=datetime.timezone.utc)
                lines.append(f"\n{interval} Candle:")
                lines.append(f"  Timestamp: {timestamp}")
                lines.append(f"  Open: {last_candle['open']:.8f}")
                lines.append(f"  High: {last_candle['high']:.8f}")
                lines.append(f"  Low: {last_candle['low']:.8f}")
                lines.append(f"  Close: {last_candle['close']:.8f}")
                lines.append(f"  Volume: {last_candle['volume']:.8f}")
                # lines.append(f"  Raw Row: {last_candle.to_dict()}")
            else:
                lines.append(f"\n{interval} Candle: No data available")

        return "\n".join(lines)
