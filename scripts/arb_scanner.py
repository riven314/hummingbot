from dataclasses import dataclass
from typing import List

import numpy as np
import pandas as pd  # type: ignore

from hummingbot.strategy.script_strategy_base import ScriptStrategyBase

LONG_LIST_BASE_ASSETS = [
    "BTC",
    "ETH",
    "SOL",
    "BNB",
    "ZIL",
    "ZEC",
    "MATIC",
    "DOGE",
    "YGG",
    "XEM",
    "XMR",
    "WOO",
    "WAVES",
    "VET",
    "TRX",
    "TRU",
    "SUPER",
]
SHORT_LIST_BASE_ASSETS = LONG_LIST_BASE_ASSETS[:3]


@dataclass
class ArbCandidate:
    trading_pair: str
    buy_exchange: str
    buy_price: float
    sell_exchange: str
    sell_price: float

    @property
    def return_percent(self) -> float:
        return ((self.sell_price - self.buy_price) / self.buy_price) * 100


@dataclass
class ArbLeg:
    exchange: str
    trading_pair: str
    price: float


# TODO: address API rate limit (currently 1200 calls per minute)
# TODO: now only support 2 exchanges, needa extend to more than 2 exchanges
class ArbListener(ScriptStrategyBase):
    exchange_a = "kucoin_paper_trade"
    exchange_b = "binance_paper_trade"
    trading_pairs = [f"{base_asset}-USDT" for base_asset in LONG_LIST_BASE_ASSETS]
    markets = {
        exchange_a: set(trading_pairs),
        exchange_b: set(trading_pairs),
    }
    min_profit_threshold = 0.0010

    @property
    def ask_legs(self) -> List[ArbLeg]:
        exchange_a_buy_legs = [
            ArbLeg(
                exchange=self.exchange_a,
                trading_pair=trading_pair,
                price=self.connectors[self.exchange_a].get_price(trading_pair, True),
            )
            for trading_pair in self.trading_pairs
        ]
        exchange_b_buy_legs = [
            ArbLeg(
                exchange=self.exchange_b,
                trading_pair=trading_pair,
                price=self.connectors[self.exchange_b].get_price(trading_pair, True),
            )
            for trading_pair in self.trading_pairs
        ]
        return exchange_a_buy_legs + exchange_b_buy_legs

    @property
    def bid_legs(self) -> List[ArbLeg]:
        exchange_a_sell_legs = [
            ArbLeg(
                exchange=self.exchange_b,
                trading_pair=trading_pair,
                price=self.connectors[self.exchange_a].get_price(trading_pair, False),
            )
            for trading_pair in self.trading_pairs
        ]
        exchange_b_sell_legs = [
            ArbLeg(
                exchange=self.exchange_a,
                trading_pair=trading_pair,
                price=self.connectors[self.exchange_b].get_price(trading_pair, False),
            )
            for trading_pair in self.trading_pairs
        ]
        return exchange_b_sell_legs + exchange_a_sell_legs

    def on_tick(self) -> None:
        arb_candidates = self.get_arb_candidates()
        if len(arb_candidates) <= 0:
            return
        for arb_candidate in arb_candidates:
            self.log_arb_candidate(arb_candidate)
        return

    def format_status(self) -> str:
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []
        warning_lines = []
        warning_lines.extend(
            self.network_warning(self.get_market_trading_pair_tuples())
        )

        balance_df = self.get_balance_df()
        lines.extend(
            ["", "  Balances:"]
            + ["    " + line for line in balance_df.to_string(index=False).split("\n")]
        )
        self.logger().info("About to enter debug mode")
        arb_status_df = self.get_arb_status_df()
        lines.extend(
            ["", " Best Ask Status Data Frame:"]
            + [
                "    " + line
                for line in arb_status_df.to_string(index=False).split("\n")
            ]
        )
        warning_lines.extend(
            self.balance_warning(self.get_market_trading_pair_tuples())
        )
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)
        return "\n".join(lines)

    def get_arb_status_df(self) -> pd.DataFrame:
        market_status_df = self._get_corrected_market_status_df()
        best_ask_df = _parse_best_ask_df(market_status_df)
        best_bid_df = _parse_best_bid_df(market_status_df)
        merge_df = pd.concat([best_ask_df, best_bid_df], axis=1)
        return merge_df

    def _get_corrected_market_status_df(self) -> pd.DataFrame:
        market_status_df = self.market_status_data_frame(
            self.get_market_trading_pair_tuples()
        )
        market_status_df["Exchange"] = market_status_df.apply(
            lambda x: x["Exchange"].strip("PaperTrade") + "paper_trade", axis=1
        )
        return market_status_df

    def log_arb_candidate(self, arb_candidate: ArbCandidate) -> None:
        arb_return_percent = arb_candidate.return_percent
        trading_pair = arb_candidate.trading_pair
        buy_exchange, buy_price = arb_candidate.buy_exchange, arb_candidate.buy_price
        sell_exchange, sell_price = (
            arb_candidate.sell_exchange,
            arb_candidate.sell_price,
        )
        self.logger().info(
            f"[ARB DETECT::{trading_pair}] RETURN: {arb_return_percent:.5f}%\n",
            f"BUY EXCHANGE::{buy_exchange} PRICE@{buy_price:.4f})\n",
            f"SELL EXCHANGE::{sell_exchange} PRICE@{sell_price:.4f})\n",
        )

    def get_arb_candidates(self) -> List[ArbCandidate]:
        ask_legs = self.ask_legs
        bid_legs = self.bid_legs
        arb_candidate_idxs = self._locate_profitable_arb_candidates(ask_legs, bid_legs)
        arb_candidates = _parse_arb_candidates(ask_legs, bid_legs, arb_candidate_idxs)
        return arb_candidates

    def _locate_profitable_arb_candidates(
        self, ask_legs: List[ArbLeg], bid_legs: List[ArbLeg]
    ) -> List[int]:
        best_asks = np.array([leg.price for leg in ask_legs])
        best_bids = np.array([leg.price for leg in bid_legs])
        arb_returns = _buy_ask_sell_bid_returns(best_asks, best_bids)
        arb_candidate_idxs = np.where(arb_returns >= self.min_profit_threshold)[0]
        return list(arb_candidate_idxs)


def _parse_best_ask_df(market_status_df: pd.DataFrame) -> pd.DataFrame:
    best_ask_df = market_status_df[["Market", "Exchange", "Best Ask Price"]].copy()
    best_ask_df.rename(columns={"Exchange": "Buy Exchange"}, inplace=True)
    return best_ask_df


def _parse_best_bid_df(market_status_df: pd.DataFrame) -> pd.DataFrame:
    best_bid_df = market_status_df[["Exchange", "Best Bid Price"]].copy()
    idxs = list(range(len(best_bid_df)))
    mid_idx = len(best_bid_df) // 2
    reorder_idxs = idxs[mid_idx:] + idxs[:mid_idx]
    best_bid_df = best_bid_df.iloc[reorder_idxs].reset_index(drop=True)
    best_bid_df.rename(columns={"Exchange": "Sell Exchange"}, inplace=True)
    return best_bid_df


def _parse_arb_candidates(
    ask_legs: List[ArbLeg], bid_legs: List[ArbLeg], idxs: List[int]
) -> List[ArbCandidate]:
    arb_candidates: List[ArbCandidate] = []
    for idx in idxs:
        ask_leg, bid_leg = ask_legs[idx], bid_legs[idx]
        arb_candidate = ArbCandidate(
            trading_pair=ask_leg.trading_pair,
            buy_exchange=ask_leg.exchange,
            buy_price=ask_leg.price,
            sell_exchange=bid_leg.exchange,
            sell_price=bid_leg.price,
        )
        arb_candidates.append(arb_candidate)
    return arb_candidates


def _buy_ask_sell_bid_returns(
    ask_prices: np.ndarray, bid_prices: np.ndarray
) -> np.ndarray:
    return (bid_prices - ask_prices) / (ask_prices)
