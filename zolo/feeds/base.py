import abc
from typing import Iterable, List

from ..dtypes import Tick, Bar, Fill, Trade


class DataFeed(abc.ABC):
    @abc.abstractmethod
    def __iter__(self) -> Iterable:
        pass


class TickDataFeed(DataFeed):
    @abc.abstractmethod
    def __iter__(self) -> Iterable[Tick]:
        pass


class BarDataFeed(DataFeed):
    @abc.abstractmethod
    def __iter__(self) -> Iterable[Bar]:
        pass


class FillDataFeed(DataFeed):
    @abc.abstractmethod
    def __iter__(self) -> Iterable[List[Fill]]:
        pass


class TradeDataFeed(DataFeed):
    @abc.abstractmethod
    def __iter__(self) -> Iterable[Trade]:
        pass