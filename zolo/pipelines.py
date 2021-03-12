import abc
from contextlib import contextmanager
from typing import List, Type, Dict, Callable
from .dtypes import SinkWrapper, Message
import logging
from .dtypes import Evt, Tick, Bar, Fill, Order, Timer, Trade

log = logging.getLogger(__name__)


class PipelineRegistry:
    registry: Dict[Type[Evt], "Pipeline"] = dict()

    @classmethod
    def register(cls, evt_type: Type[Evt], pipeline: "Pipeline"):
        cls.registry[evt_type] = pipeline

    def dispatch(self, evt: Evt):
        self.registry[type(evt)].demux(evt)

    def attach_sink(self, evt_type: Type[Evt], flt: Callable, on_evt: Callable):
        self.registry[evt_type].attach_sink(flt, on_evt)


class Pipeline(abc.ABC):
    def __init__(self):
        self._busy = False
        self._sinks: List[SinkWrapper] = list()

    def __init_subclass__(cls, evt_type: Type[Evt] = None, **kwargs):
        setattr(cls, "_evt_type", evt_type)
        PipelineRegistry.register(evt_type, cls())

    @property
    def evt_type(self) -> Type[Evt]:
        return getattr(self, "_evt_type")

    @property
    def sinks(self):
        return self._sinks

    @property
    def busy(self):
        return self._busy

    @contextmanager
    def bypass_incoming_events(self):
        self._busy = True
        try:
            yield
        except Exception as e:
            log.exception(e)
            raise e
        finally:
            self._busy = False

    def demux(self, evt):
        for sink in self._sinks:
            if sink.filter(evt):
                sink.on_evt(evt)

    def attach_sink(self, flt: Callable, on_evt: Callable):
        self._sinks.append(SinkWrapper(flt, on_evt))

    def __repr__(self):
        return f"{self.__class__.__name__}"


class TickPipeline(Pipeline, evt_type=Tick):
    def demux(self, evt: Tick):
        with self.bypass_incoming_events():
            super().demux(evt)


class BarPipeline(Pipeline, evt_type=Bar):
    pass


class FillPipeline(Pipeline, evt_type=Fill):
    pass


class OrderPipeline(Pipeline, evt_type=Order):
    pass


class TimerPipeline(Pipeline, evt_type=Timer):
    def demux(self, evt: Timer):
        super().demux(evt.timestamp)


class TradePipeline(Pipeline, evt_type=Trade):
    pass


class MessagePipeline(Pipeline, evt_type=Message):
    pass
