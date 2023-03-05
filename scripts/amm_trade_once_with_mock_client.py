import asyncio
from typing import Any, Dict, Optional, Tuple, TypedDict

from pydantic import BaseModel

from hummingbot.client.settings import GatewayConnectionSetting
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.event.events import TradeType
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import Decimal, ScriptStrategyBase

IS_MOCK = True


class SlippagePriceError(Exception):
    pass


class Transaction(TypedDict):
    network: str
    timestamp: int
    latency: float
    base: str
    quote: str
    amount: str
    rawAmount: str
    expectedIn: str
    price: str
    gasPrice: int
    gasPriceToken: str
    gasLimit: int
    gasCost: str
    nonce: int
    txHash: str


class TransactionStatus(TypedDict):
    network: str
    currentBlock: int
    timestamp: int
    txHash: str
    txBlock: int
    txStatus: int
    txData: Dict[str, Any]
    txReceipt: Optional[Any]


class MockGatewayHttpClient:
    DUMMY_TRANSACTION: Transaction = {
        "network": "mainnet",
        "timestamp": 1678002318019,
        "latency": 2.27,
        "base": "0x55d398326f99059fF775485246999027B3197955",
        "quote": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "amount": "1.000000000000000000",
        "rawAmount": "1000000000000000000",
        "expectedIn": "0.0034832998",
        "price": "0.0034488117",
        "gasPrice": 5,
        "gasPriceToken": "BNB",
        "gasLimit": 3000000,
        "gasCost": "0.000753440000000000",
        "nonce": 18,
        "txHash": "0x3d6452d1e938bc4420a18dcaf9972bbf4289f3b113c41a27a925d3df412071c0",
    }

    DUMMY_TRANSACTION_STATUS: TransactionStatus = {
        "network": "mainnet",
        "currentBlock": 26195606,
        "timestamp": 1678002321004,
        "txHash": "0x3d6452d1e938bc4420a18dcaf9972bbf4289f3b113c41a27a925d3df412071c0",
        "txBlock": -1,
        "txStatus": 2,
        "txData": {
            "hash": "0x3d6452d1e938bc4420a18dcaf9972bbf4289f3b113c41a27a925d3df412071c0",
            "type": 0,
            "accessList": None,
            "blockHash": None,
            "blockNumber": None,
            "transactionIndex": None,
            "confirmations": 0,
            "from": "0xe871bc4D06E9337fD5611c28812e7E29478E9145",
            "gasPrice": "5000000000",
            "gasLimit": "3000000",
            "to": "0x10ED43C718714eb63d5aA57B78B54704E256024E",
            "value": "0",
            "nonce": 18,
            "data": "0x8803dbee0000000000000000000000000000000000000000000000000de0b6b3a7640000000000000000000000000000000000000000000000000000000c40ad0d8d8fed00000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000e871bc4d06e9337fd5611c28812e7e29478e914500000000000000000000000000000000000000000000000000000000640449bb0000000000000000000000000000000000000000000000000000000000000002000000000000000000000000bb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c00000000000000000000000055d398326f99059ff775485246999027b3197955",
            "r": "0x38945c0d61e07052232a5bd2e1dc0202d05bd333945f818fbffaefaedaeb4cdb",
            "s": "0x1c38beb548010b9c6a18ae43fd08ef0f92da6de09ca3a5319608ad382217c149",
            "v": 147,
            "creates": None,
            "chainId": 56,
        },
        "txReceipt": None,
    }

    def __init__(self):
        self.counter = 0

    @classmethod
    def get_instance(cls) -> "MockGatewayHttpClient":
        return MockGatewayHttpClient()

    async def amm_trade(self, *args, **kwargs) -> Transaction:
        await asyncio.sleep(3)
        return self.DUMMY_TRANSACTION

    async def get_transaction_status(self, *args, **kwargs) -> TransactionStatus:
        await asyncio.sleep(1.5)
        self.counter += 1
        if self.counter > 2:
            self.DUMMY_TRANSACTION_STATUS["txStatus"] = 1
        return self.DUMMY_TRANSACTION_STATUS


class AmmTradePoll(ScriptStrategyBase):
    # define markets
    connector = "pancakeswap"
    chain = "binance-smart-chain"
    network = "mainnet"
    connector_chain_network = f"{connector}_{chain}_{network}"
    trading_pair = "USDT-WBNB"
    trading_pairs = {trading_pair}
    markets = {
        connector_chain_network: trading_pairs,
    }
    gateway_client = (
        GatewayHttpClient.get_instance()
        if not IS_MOCK
        else MockGatewayHttpClient.get_instance()
    )

    # related trade logic
    slippage_buffer = Decimal("0.004")
    wallet_address: Optional[str] = None
    transaction = None
    trade_once = False
    is_submit_tx_incomplete = False
    is_poll_tx_incomplete = False

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        self.wallet_address = self.get_wallet_address()

    def on_tick(self) -> None:
        if self.trade_once:
            self.logger().info("Already traded once. Skip the rest of on_tick")
            return

        if self.is_submit_tx_incomplete:
            self.logger().info("Submit swap request in progress .Skip this on_tick")
            return

        if self.is_poll_tx_incomplete:
            tx_hash = self.transaction["txHash"]
            self.logger().info(
                f"Poll transaction (txHash: {tx_hash}) is incomplete. Skip this on_tick"
            )
            return

        if not self.wallet_address:
            self.logger().warning("Wallet address is not set. Skip on_tick")
            return

        if self.transaction:
            safe_ensure_future(self.poll_transaction())
            return

        self.logger().info(
            "self.transaction is None, Execute trade by self.execute_trade"
        )
        safe_ensure_future(self.execute_trade())

    async def poll_transaction(self):
        self.is_poll_tx_incomplete = True
        is_success = await self.get_transaction_status()
        if is_success:
            self.logger().info(
                f"Transaction is successful! No more transaction afterwards!"
            )
            self.transaction = None
            self.trade_once = True
        self.is_poll_tx_incomplete = False
        return

    async def get_transaction_status(self) -> bool:
        assert self.transaction is not None, "Transaction is not set."
        transaction_hash = self.transaction["txHash"]
        transaction_status = await self.gateway_client.get_transaction_status(
            self.chain, self.network, transaction_hash
        )
        self.logger().info(f"Transaction Status: {transaction_status}")
        return transaction_status["txStatus"] == 1

    async def execute_trade(self):
        self.is_submit_tx_incomplete = True
        try:
            transaction = await self._amm_trade()
            self.logger().info(f"Transaction: {transaction}")
            self.transaction = transaction
        except SlippagePriceError as e:
            self.logger().info(f"Trade failed due to slippage limit")
        except Exception as e:
            self.logger().info(f"Trade failed due to unknown cause: {str(e)}")
        finally:
            self.is_submit_tx_incomplete = False

    async def _amm_trade(self):
        assert self.wallet_address is not None, "Wallet address is not set."
        try:
            base, quote = split_base_quote(self.trading_pair)
            transaction = await self.gateway_client.amm_trade(
                self.chain,
                self.network,
                self.connector,
                self.wallet_address,
                base,
                quote,
                TradeType.BUY,
                Decimal("1"),
                Decimal("0.0035"),
            )
            return transaction
        except ValueError as e:
            if "than limitPrice" in str(e):
                raise SlippagePriceError(
                    "Trade Price is too low or too high for slippage limit"
                )
            raise e
        except Exception as e:
            raise e

    def get_wallet_address(self) -> Optional[str]:
        gateway_conf = GatewayConnectionSetting.load()
        wallet = [
            w
            for w in gateway_conf
            if w["chain"] == self.chain and w["network"] == self.network
        ]
        if len(wallet) == 0:
            self.logger().info(
                f"No wallet found for chain {self.chain} and network {self.network}"
            )
            return None
        return wallet[0]["wallet_address"]


def split_base_quote(trading_pair: str) -> Tuple[str, str]:
    base, quote = trading_pair.split("-")
    return base, quote
