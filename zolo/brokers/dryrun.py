from typing import List
import logging
from ..consts import AIAO, DRYRUN, LONG, INVALID, \
    POSITION_SIDE_EMPTY
from ..posts import OrderPostType
from .broker import BrokerBase
from ..dtypes import Order, Fill, CREDENTIAL_EMPTY, OrderStatus


log = logging.getLogger(__name__)


class DryrunBroker(BrokerBase):

    def __init__(self, exchange: str, market: str):
        super().__init__(exchange, market, DRYRUN)
        self._context = self.register_context(
            "default", INVALID, POSITION_SIDE_EMPTY,
            AIAO, CREDENTIAL_EMPTY
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

