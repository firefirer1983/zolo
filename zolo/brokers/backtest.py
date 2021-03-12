import logging

from . import TradingContext
from .base import BrokerBase
from ..dtypes import Credential, Fill, Trade, Order, Tick, OrderStatus, \
    CREDENTIAL_EMPTY
from ..consts import BACKTEST, AIAO, LONG, INVALID, \
    POSITION_SIDE_EMPTY

log = logging.getLogger(__name__)


class BacktestBroker(BrokerBase):
    def __init__(self, exchange: str, market: str):
        super().__init__(exchange, market, BACKTEST)
        super().register_context(
            "default", INVALID, POSITION_SIDE_EMPTY, AIAO,
            CREDENTIAL_EMPTY
        )

    def on_order(self, order: Order):
        if order.state == OrderStatus.FULFILLED:
            self.context.on_order(order)
        else:
            log.warning(f"Ignore order: {order}")

    def on_fill(self, fill: Fill):
        log.info(f"{fill}")

    def deposit(self, amount: float):
        return self.context.adapter.deposit(self.context.instrument_id, amount)
