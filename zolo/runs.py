import logging
from typing import List, Union, Tuple, Iterable

from .consts import (
    ON_TICK,
    ON_BAR,
    ON_FILL,
    ON_ORDER,
    ON_TRADE,
    ON_TIMER,
    ON_START,
    ON_STOP,
    ON_MESSAGE,
    GATEWAY_SUBSCRIBE,
    GATEWAY_UNSUBSCRIBE,
    GATEWAY_START,
    GATEWAY_STOP,
    GATEWAY_REBOOT,
    GATEWAY_HEARTBEAT,
)

from .feeds.integrator import HybridDataFeed
from .brokers import BacktestBroker, CryptoBroker, DryrunBroker
from .dtypes import Fill, Trade, Order, Bar, Tick, Timer, Message
from .hub import evt_hub
from .base import Strategy
from .utils import create_filter, create_timer, BYPASS_FILTER, \
    create_in_filter, \
    iterable
from .engine import vtx

log = logging.getLogger(__name__)


class CryptoRunner:
    def __init__(self, pipe: str = "tcp://*:5555"):
        self._loop = True
        self._pipe = pipe
    
    def start(self, stg):
        on_start_cb = getattr(stg, ON_START, lambda: print("strategy on start"))
        on_start_cb()
        
        if callable(getattr(stg, ON_TICK, None)):
            evt_hub.attach_sink(Tick, BYPASS_FILTER, getattr(stg, ON_TICK))
        
        if callable(getattr(stg, ON_BAR, None)):
            evt_hub.attach_sink(Bar, BYPASS_FILTER, getattr(stg, ON_BAR))
        
        if callable(getattr(stg, ON_FILL, None)):
            evt_hub.attach_sink(Fill, BYPASS_FILTER, getattr(stg, ON_FILL))
        
        if callable(getattr(stg, ON_TRADE, None)):
            evt_hub.attach_sink(Trade, BYPASS_FILTER, getattr(stg, ON_TRADE))
        
        if callable(getattr(stg, ON_TIMER, None)):
            evt_hub.attach_sink(Timer, BYPASS_FILTER, getattr(stg, ON_TIMER))
        
        if callable(getattr(stg, ON_ORDER, None)):
            evt_hub.attach_sink(Order, BYPASS_FILTER, getattr(stg, ON_ORDER))
        
        flt = create_in_filter(
            "cmd",
            (
                GATEWAY_START, GATEWAY_STOP, GATEWAY_REBOOT, GATEWAY_SUBSCRIBE,
                GATEWAY_UNSUBSCRIBE, GATEWAY_HEARTBEAT
            )
        )
        evt_hub.attach_sink(Message, flt, evt_hub.gateways.on_message)
        evt_hub.attach_sink(Timer, BYPASS_FILTER, evt_hub.gateways.on_timer)
        evt_hub.start_timer()
        evt_hub.start_zmq(self._pipe)
        
        while self._loop:
            try:
                evt = evt_hub.get_event()
                evt_hub.dispatch(evt)
            except KeyboardInterrupt:
                self._loop = False
        
        evt_hub.stop()
        
        on_stop_cb = getattr(stg, ON_STOP, lambda: print("strategy on stop"))
        on_stop_cb()


class BacktestRunner:
    def __init__(self, datafeed: HybridDataFeed):
        self._loop = True
        self._datafeed = iter(datafeed)
    
    def start(self, stg: Strategy):
        on_start_cb = getattr(stg, ON_START, lambda: print("strategy on start"))
        on_start_cb()
        
        if callable(getattr(stg, ON_TICK, None)):
            evt_hub.attach_sink(Tick, BYPASS_FILTER, getattr(stg, ON_TICK))
        
        if callable(getattr(stg, ON_BAR)):
            evt_hub.attach_sink(Bar, BYPASS_FILTER, getattr(stg, ON_BAR))
        
        if callable(getattr(stg, ON_FILL, None)):
            evt_hub.attach_sink(Fill, BYPASS_FILTER, getattr(stg, ON_FILL))
        
        if callable(getattr(stg, ON_TRADE, None)):
            evt_hub.attach_sink(Trade, BYPASS_FILTER, getattr(stg, ON_TRADE))
        
        if callable(getattr(stg, ON_TIMER, None)):
            evt_hub.attach_sink(Timer, BYPASS_FILTER, getattr(stg, ON_TIMER))
        
        if callable(getattr(stg, ON_ORDER, None)):
            evt_hub.attach_sink(Order, BYPASS_FILTER, getattr(stg, ON_ORDER))
        
        if callable(getattr(stg, ON_TRADE, None)):
            evt_hub.attach_sink(Trade, BYPASS_FILTER, getattr(stg, ON_TRADE))
        
        evt_hub.attach_sink(Tick, BYPASS_FILTER, vtx.on_tick)
        
        while self._loop:
            try:
                while True:
                    evt = next(self._datafeed)
                    evt_hub.dispatch(evt)
                    try:
                        vtx.poll()
                    except InterruptedError:
                        break
                
                for evt in vtx.get_order():
                    evt_hub.dispatch(evt)
                
                for evt in vtx.get_fill():
                    evt_hub.dispatch(evt)
                
                for brk in stg.brokers:
                    for evt in brk.get_trade():
                        evt_hub.dispatch(evt)
            
            except (KeyboardInterrupt, EOFError):
                self._loop = False
        
        on_stop_cb = getattr(stg, ON_STOP, lambda: print("strategy on stop"))
        on_stop_cb()


class DryRunner:
    def __init__(self):
        self._loop = True
    
    def start(self, stg: Strategy):
        on_start_cb = getattr(stg, ON_START, lambda x: print("on start stg"))
        on_start_cb()
        
        evt_hub.start()
        
        while self._loop:
            try:
                evt = evt_hub.get_event()
                evt_hub.dispatch(evt)
            except KeyboardInterrupt:
                self._loop = False
        
        evt_hub.stop()
        
        on_stop_cb = getattr(stg, ON_STOP, lambda: print("on start stg"))
        on_stop_cb()
