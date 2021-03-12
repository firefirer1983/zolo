import abc
import logging
from collections import defaultdict
from datetime import datetime
from queue import Queue
from typing import Dict, Type
from ..consts import GATEWAY_HEARTBEAT, \
    GATEWAY_STOP, GATEWAY_UNSUBSCRIBE, \
    GATEWAY_SUBSCRIBE, GATEWAY_REBOOT, UNIX_EPOCH
from ..dtypes import Message, GatewayConfig, ChannelConfig
from .mq import UserMessageGateway
from .timer import TimerGen


log = logging.getLogger(__name__)


class GatewayManager:
    registry: Dict[str, Type["Gateway"]] = dict()
    heartbeats: Dict[str, datetime] = defaultdict(lambda: UNIX_EPOCH)
    gateways: Dict[str, "Gateway"] = dict()
    
    def __init__(self, q: Queue):
        self.q = q
        self.ui = UserMessageGateway("*")
        self.timers = TimerGen()
    
    @classmethod
    def register(cls, scheme: str, gateway: Type["Gateway"]):
        if scheme in cls.registry:
            raise KeyError
        cls.registry[scheme] = gateway
    
    def subscribe(self, cfg: ChannelConfig):
        assert (
            self.q
        ), "Please attach hub.q to gateway registry before any channel " \
           "subscribing"
        gid = cfg.gateway.gateway_id
        if gid not in self.gateways:
            gateway_cls = self.registry[cfg.gateway.gateway_scheme]
            self.gateways[gid] = gateway_cls()
            self.gateways[gid].start(self.q)
    
    def start_timer(self):
        self.timers.start(self.q)
    
    def start_messenger(self):
        self.ui.start(self.q)
        
    def stop(self):
        self.ui.stop()
        self.timers.stop()
        for gateway in self.gateways.values():
            gateway.stop()
    
    def on_timer(self, ts: datetime):
        for gid, last_ts in self.heartbeats.items():
            missing = (last_ts - ts).seconds
            if missing > getattr(
                self.gateways[gid], "timeout", float("inf")
            ):
                log.error(
                    f"{gid} heartbeat missing for {missing} seconds, "
                    f"reboot it!"
                )
                self.gateways[gid].reboot(self.q)
    
    def on_message(self, evt: Message):
        if evt.cmd == GATEWAY_SUBSCRIBE:
            cfg: ChannelConfig = evt.payload
            self.gateways[cfg.gateway.gateway_id].subscribe(cfg)
        elif evt.cmd == GATEWAY_UNSUBSCRIBE:
            cfg: ChannelConfig = evt.payload
            self.gateways[cfg.gateway.gateway_id].unsubscribe(cfg)
        elif evt.cmd == GATEWAY_STOP:
            cfg: GatewayConfig = evt.payload
            log.info(f"STOP: {cfg}")
            self.gateways[cfg.gateway_id].stop()
        elif evt.cmd == GATEWAY_HEARTBEAT:
            cfg: GatewayConfig = evt.payload
            self.heartbeats[cfg.gateway_id] = datetime.utcnow()
        elif evt.cmd == GATEWAY_REBOOT:
            cfg: GatewayConfig = evt.payload
            self.gateways[cfg.gateway_id].reboot(self.q)
        else:
            log.error(f"Unknown msg: {evt}")


class Gateway(abc.ABC):
    def __init_subclass__(cls, scheme: str = "", **kwargs):
        GatewayManager.register(scheme, cls)
    
    def __init__(self):
        self._state = "Init"
    
    @abc.abstractmethod
    def subscribe(self, cfg: ChannelConfig):
        pass
    
    @abc.abstractmethod
    def unsubscribe(self, cfg: ChannelConfig):
        pass
    
    @abc.abstractmethod
    def stop(self):
        pass
    
    @abc.abstractmethod
    def start(self, q: Queue):
        pass
    
    @property
    @abc.abstractmethod
    def is_running(self):
        return self._state == "Running"
    
    @abc.abstractmethod
    def reboot(self, q: Queue):
        pass
    
    def on_message(self, evt: Message):
        if evt.cmd == "SUBSCRIBE":
            if not isinstance(evt.payload, ChannelConfig):
                log.error(f"Invalid evt: {evt}")
                return
            self.subscribe(evt.payload)
        elif evt.cmd == "UNSUBSCRIBE":
            if not isinstance(evt.payload, ChannelConfig):
                log.error(f"Invalid evt: {evt}")
                return
            self.unsubscribe(evt.payload)
        else:
            log.error(f"Unknown evt: {evt}")
