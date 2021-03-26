import logging
from typing import Dict, Type, List, Callable, Optional

from .gateways import GatewayManager
from .dtypes import Evt
from .gateways.mq import ZmqGateway
from queue import Queue

from .pipelines import PipelineRegistry

log = logging.getLogger(__name__)


class EventHub:
    
    def __init__(self):
        self._use_gateway: bool = False
        self._evt_q = Queue()
        self._pipelines: PipelineRegistry = PipelineRegistry()
        self._gateways: GatewayManager = GatewayManager(self._evt_q)
        self.zmq: ZmqGateway = None
    
    def attach_sink(self, evt_type: Type[Evt], flt: Callable, on_evt: Callable):
        return self._pipelines.attach_sink(evt_type, flt, on_evt)
    
    def get_event(self, timeout: float = 0) -> Evt:
        if timeout > 0:
            return self._evt_q.get(timeout=timeout)
        else:
            return self._evt_q.get()
    
    def post_event(self, evt: Evt):
        return self._evt_q.put(evt)
    
    def dispatch(self, evt: Evt):
        return self._pipelines.dispatch(evt)
    
    def stop(self):
        self.zmq.stop()
        self.gateways.stop()
    
    @property
    def gateways(self) -> GatewayManager:
        return self._gateways
    
    def start_timer(self):
        return self.gateways.start_timer()
    
    def start_zmq(self, host: str):
        if not self.zmq:
            self.zmq = ZmqGateway(host)
            self.zmq.start(self._evt_q)


evt_hub: EventHub = EventHub()
