import abc
from datetime import datetime
from typing import Dict, Type
from ..adapters import Adapter
from ..posts import OrderPost

_executor_registry = {}


class ExecutorRegistry:

    registry: Dict[str, Type["OrderExecutor"]] = dict()

    @classmethod
    def register(cls, mode: str, exec_class: Type["OrderExecutor"]):
        cls.registry[mode] = exec_class

    @classmethod
    def create_executor(cls, mode: str, post: OrderPost, adapter: Adapter, **kwargs):
        return cls.registry[mode](post, adapter, **kwargs)


class OrderExecutor(abc.ABC):

    def __init_subclass__(cls, mode: str = "", **kwargs):
        assert mode
        ExecutorRegistry.register(mode, cls)

    def __init__(self, post: OrderPost, adapter: Adapter, timeout: float = 1):
        self._timeout = timeout
        self._client_oid: str = ""
        self._adapter = adapter
        self._post = post

    @property
    def post(self):
        return self._post

    @property
    def timeout(self) -> float:
        return self._timeout

    @property
    def adapter(self) -> Adapter:
        return self._adapter

    @abc.abstractmethod
    def result(self, ts: datetime):
        pass

    @property
    @abc.abstractmethod
    def finished(self) -> bool:
        pass

    @property
    def client_oid(self) -> str:
        return self._client_oid


create_executor = ExecutorRegistry.create_executor
