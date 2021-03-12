import logging
from typing import List, Union, Type

from . import TradingContext
from ..dtypes import Bar, Tick, Order, Credential, ChannelConfig, Message, \
    GatewayConfig, OrderStatus, Fill, CREDENTIAL_EMPTY
from ..consts import RESTFUL, AIAO, GATEWAY_SUBSCRIBE, GATEWAY_UNSUBSCRIBE, \
    ON_BAR, ON_TICK, POSITION_SIDE_EMPTY, INVALID, DEFAULT_LEVERAGE
from ..indicators.base import Indicator
from .base import BrokerBase
from ..hub import evt_hub

log = logging.getLogger(__name__)


class CryptoBroker(BrokerBase):
    
    def __init__(self, exchange: str, market: str, gateway: str = RESTFUL):
        super().__init__(exchange, market, gateway)
        self._context = TradingContext(
            "default", exchange, market, INVALID,
            POSITION_SIDE_EMPTY, gateway, AIAO, CREDENTIAL_EMPTY)
    
    def on_order(self, order: Order):
        if order.state == OrderStatus.FULFILLED:
            self.context.on_order(order)
        else:
            log.warning(f"Ignore order: {order}")
    
    def on_fill(self, fill: Fill):
        log.info(f"{fill}")
    
    def indicator(self, ind: str, **kwargs) -> Indicator:
        ind = super().indicator(ind, **kwargs)
        # TODO 暂时默认restful
        if callable(getattr(ind, ON_BAR, None)):
            evt_hub.post_event(
                Message(
                    GATEWAY_SUBSCRIBE,
                    ChannelConfig(
                        GatewayConfig(RESTFUL, self.exchange), self.market,
                        self.instrument_id, self.context.credential, Bar,
                        parameters=kwargs
                    )))
        if callable(getattr(ind, ON_TICK, None)):
            evt_hub.post_event(
                Message(
                    GATEWAY_SUBSCRIBE,
                    ChannelConfig(
                        GatewayConfig(RESTFUL, self.exchange), self.market,
                        self.instrument_id, self.context.credential, Tick,
                        parameters=dict()
                    )))
        return ind
