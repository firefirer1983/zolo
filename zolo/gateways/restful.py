import logging
import concurrent.futures
import time

from datetime import datetime
from functools import partial
from threading import Thread

from ..consts import UNIX_EPOCH, RUNNING, STOPPED, INIT
from ..dtypes import Bar, Tick, ChannelConfig, ExchangeEvt
from ..exceptions import BarGetError, TickGetError
from ..adapters import Adapter
from ..consts import RESTFUL
from contextlib import contextmanager
from typing import Type, Dict, Callable, Optional
from .base import Gateway
from queue import Queue
from ..adapters import create_adapter

log = logging.getLogger(__name__)


class Executor:
    def __init__(self, workers: int):
        self._workers = workers

    class SimpleJob:
        def __init__(self, job):
            self._job = job

        def result(self):
            return self._job()

    @contextmanager
    def __call__(self):
        if self._workers > 3:
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self._workers
            ) as exe:
                yield exe
        else:
            yield self

    def submit(self, job):
        return self.SimpleJob(job)


class RestfulGateway(Gateway, gateway_scheme=RESTFUL):
    channel_registry: Dict[Type[ExchangeEvt], Type["RestfulChannel"]] = dict()

    @classmethod
    def register(cls, evt_type: Type[ExchangeEvt], channel_class):
        if evt_type in cls.channel_registry:
            raise KeyError
        cls.channel_registry[evt_type] = channel_class

    @classmethod
    def create_channel(
            cls, adapter: Adapter, cfg: ChannelConfig
    ) -> "RestfulChannel":
        return cls.channel_registry[cfg.evt_type](cfg, adapter)

    def __init__(self):
        self._state = INIT
        self._thread = None
        self._channels: Dict[str, RestfulChannel] = dict()
        self._delay = 0.1
        self._adapters: Dict[str, Adapter] = {}
        super().__init__()

    def subscribe(self, cfg: ChannelConfig):
        super().subscribe(cfg)
        if cfg.channel_id in self._channels:
            log.warning(f"{cfg} is already exist!")
            return
        if cfg.channel_id not in self._adapters:
            res = create_adapter(cfg.gateway.gateway_scheme, cfg.gateway.name, cfg.market, cfg.credential)
            self._adapters[cfg.channel_id] = res
        self._channels[cfg.channel_id] = self.create_channel(self._adapters[cfg.channel_id], cfg)

    def unsubscribe(self, cfg: ChannelConfig):
        if cfg not in self._channels:
            log.warning(f"{cfg} is not exist")
            return
        self._channels = {k: v for k, v in self._channels if k.cfg.channel_id != cfg.channel_id}

    @property
    def is_running(self):
        return self._state == RUNNING

    def _poll(self, q: Queue):
        self._state = RUNNING
        while self.is_running:
            events = self._poll_once()
            for evt in events:
                q.put(evt)
            time.sleep(self._delay)
        self._state = STOPPED

    def _poll_once(self):
        results = []
        channels = [ch for ch in self._channels.values() if ch and ch.outdated and callable(ch)]
        with Executor(len(channels)) as exe:
            jobs = [exe.submit(ch) for ch in channels]
            for p in jobs:
                try:
                    res = p.result()
                except (IOError, OSError) as e:
                    log.exception(e)
                    time.sleep(10)
                except Exception as e:
                    log.exception(e)
                    raise
                else:
                    if res:
                        results.append(res)
        return results

    def reboot(self, q: Queue):
        self.stop()
        self.start(q)

    def stop(self):
        if self.is_running:
            self._state = STOPPED
            self._thread.join(5)
            if self._state != STOPPED:
                log.error("Try to stop failed!")

    def start(self, q: Queue):
        if not self.is_running:
            self._thread = Thread(target=self._poll, args=(q,))
            self._thread.start()


class RestfulChannel:
    registry: Dict[Type[ExchangeEvt], Type["RestfulChannel"]] = dict()

    def __init__(self, cfg: ChannelConfig, adapter: Adapter):
        self._adapter = adapter
        self._last_update = UNIX_EPOCH
        self._cfg = cfg

    def __init_subclass__(cls, evt_type: Type[ExchangeEvt] = None, **kwargs):
        assert evt_type
        RestfulGateway.register(evt_type, cls)

    def update_ts(self):
        self._last_update = datetime.utcnow()

    @property
    def outdated(self):
        return (datetime.utcnow() - self._last_update).seconds < self.config.parameters["interval"]

    @property
    def adapter(self) -> Adapter:
        return self._adapter

    @property
    def config(self) -> ChannelConfig:
        return self._cfg


class BarsChannel(RestfulChannel, evt_type=Bar):
    def __init__(self, cfg: ChannelConfig, adapter: Adapter):
        super().__init__(cfg, adapter)
        granularity, instrument_id = cfg.parameters["granularity"], cfg.instrument_id
        self._restful_api = partial(
            getattr(adapter, "get_latest_bar"),
            instrument_id=instrument_id,
            granularity=granularity,
        )

    def __call__(self):
        try:
            res = self._restful_api()
        except BarGetError:
            res = None
        finally:
            self.update_ts()
        if self._last_bar and self._last_bar.timestamp == res.timestamp:
            return None
        self._last_bar = res
        self.update_ts()
        return res


class TicksChannel(RestfulChannel, evt_type=Tick):
    def __init__(self, cfg: ChannelConfig, adapter: Adapter):
        super().__init__(cfg, adapter)
        self._restful_api = partial(
            getattr(adapter, "get_tick"), instrument_id=cfg.instrument_id
        )

    def __call__(self):
        try:
            res = self._restful_api()
        except TickGetError:
            res = None
        finally:
            self.update_ts()
        if self._last_tick and self._last_tick.timestamp == res.timestamp:
            return None
        self._last_tick = res

        return res
