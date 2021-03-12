import abc
from functools import partial

from ..utils import calc_pnl_by_reverse, calc_comm_by_reverse
from ..dtypes import (
    Order,
    Bar,
    Margin,
    Position,
    Tick,
    Credential,
    InstrumentInfo
)
from datetime import datetime
import time
from . import Adapter
import logging


log = logging.getLogger(__name__)


def str_to_datetime(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")


def create_id_by_timestamp():
    return f"{int(time.time() * 10000000)}"


def timestamp_to_utc(ts: int) -> datetime:
    return datetime.utcfromtimestamp(ts / 1000)


_swap_instrument_info_registry = {}

_future_instrument_info_registry = {}

_spot_instrument_info_registry = {}


class HuobiBacktestAdapter(Adapter):
    _type = "backtest"
    exchange = "huobi"

    def __init__(self):
        super().__init__(Credential("", "", ""))

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(exchange=cls.exchange, **kwargs)

    @abc.abstractmethod
    def get_instrument_info(self, instrument_id: str):
        pass

    @abc.abstractmethod
    def create_market_order(self, instrument_id, amount, order_type, order_side) -> str:
        pass

    def get_margin(self, instrument_id) -> Margin:
        raise NotImplementedError

    def get_position(self, instrument_id) -> Position:
        raise NotImplementedError

    def get_tick(self, instrument_id) -> Tick:
        raise NotImplementedError

    @abc.abstractmethod
    def get_order_by_client_oid(self, instrument_id, client_order_id) -> Order:
        pass

    def get_latest_bar(self, instrument_id: str, granularity: int) -> Bar:
        raise NotImplementedError

    def get_bars(
        self, instrument_id: str, granularity: int, start: datetime, end: datetime
    ):
        raise NotImplementedError

    def get_last_n_bars(self, cnt: int, instrument_id: str, granularity: int):
        raise NotImplementedError

    def get_fill(
        self, instrument_id: str, before: datetime, after: datetime, limit=100
    ):
        raise NotImplementedError


class HuobiBacktestCoinMarginSwap(HuobiBacktestAdapter):
    _market = "swap@coin"

    def __init__(self):
        super().__init__()

    def get_instrument_info(self, instrument_id: str):
        return _swap_instrument_info_registry[instrument_id]


class HuobiBacktestUsdtMarginSwap(HuobiBacktestAdapter):
    _market = "swap@usdt"

    def __init__(self):
        super().__init__()

    def get_instrument_info(self, instrument_id: str) -> InstrumentInfo:
        return _swap_instrument_info_registry[instrument_id]


class HuobiBacktestCoinMarginFuture(HuobiBacktestAdapter):
    _market = "future@coin"

    def __init__(self):
        super().__init__()

    def get_instrument_info(self, instrument_id: str) -> InstrumentInfo:
        return _future_instrument_info_registry[instrument_id]


class HuobiBacktestSpot(HuobiBacktestAdapter):
    _market = "spot"

    def __init__(self):
        super().__init__()

    def get_instrument_info(self, instrument_id: str) -> InstrumentInfo:
        return _spot_instrument_info_registry[instrument_id]
