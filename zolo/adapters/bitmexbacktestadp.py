import logging
from datetime import datetime, timedelta
from functools import partial
from decimal import Decimal
from time import time
from typing import Optional, List, Callable

from .bitmexbroker import BitmexBroker
from ..backtest import (
    get_bitmex_bar_by_range,
    get_bitmex_bar_by_timestamp,
    get_bitmex_last_n_bars,
)
from ..dtypes import Margin, Position, OrderSide, OrderType, Tick

log = logging.getLogger(__name__)


def truncate_by_hour(ts: datetime):
    return datetime(ts.year, ts.month, ts.day, ts.hour)


def next_hour_beginning(ts):
    ts = ts + timedelta(hours=1)
    return datetime(ts.year, ts.month, ts.day)


class Ticker:
    def __init__(self, begin, end):
        if end - begin < timedelta(minutes=1):
            raise ValueError(f"end:{end} - begin:{begin} < 1 minute")
        self._begin = begin
        self._end = end
        # self._get_tick = tick_getter
        self._tick = None
        self._ticks = None

    def get_tick(self, instrument_id: str):
        if self._ticks is None:
            self._ticks = from_bars_2_ticks(
                get_bitmex_bar_by_range(
                    self._begin, self._end, 1, instrument_id=instrument_id
                )
            )

        try:
            self._tick = self._ticks.pop(0)
        except IndexError:
            raise EOFError(f"No more new tick")

        if self._tick and self.current_ts > self._end:
            raise EOFError(f"No more new tick")
        return self._tick

    @property
    def current_ts(self):
        if not self._tick:
            return self._begin
        return self._tick.timestamp

    @property
    def current_price(self):
        return self._tick.price

    @property
    def begin(self):
        return self._begin

    @property
    def end(self):
        return self._end


ticker = Ticker(datetime(2020, 1, 7), datetime(2020, 12, 31))

_broker = BitmexBroker(ticker)


def from_bars_2_ticks(bars):
    return [
        Tick(timestamp=bar.timestamp, price=bar.close, instrument_id=bar.instrument_id)
        for bar in bars
    ]


def create_id_by_timestamp():
    return f"crs{int(time() * 10000000)}"


def close_short(instrument_id: str, order_type: str, amount: int):
    client_oid = create_id_by_timestamp()
    order = _broker.create_order(
        instrument_id, client_oid, OrderSide.CLOSE_SHORT, order_type, amount
    )
    return order


def open_short(instrument_id: str, order_type: str, amount: int):
    client_oid = create_id_by_timestamp()
    order = _broker.create_order(
        instrument_id, client_oid, OrderSide.OPEN_SHORT, order_type, amount
    )
    return order


def open_long(instrument_id, order_type: str, amount: int):
    client_oid = create_id_by_timestamp()
    order = _broker.create_order(
        instrument_id, client_oid, OrderSide.OPEN_LONG, order_type, amount
    )
    return order


def close_long(instrument_id, order_type: str, amount: int):
    client_oid = create_id_by_timestamp()
    order = _broker.create_order(
        instrument_id, client_oid, OrderSide.CLOSE_LONG, order_type, amount
    )
    return order


def get_order_by_client_oid(instrument_id: str, client_oid: str):
    return _broker.get_order_history(instrument_id, client_oid)


def create_order(instrument_id, amount, order_type, order_side):
    client_oid = create_id_by_timestamp()
    return _broker.create_order(
        instrument_id, client_oid, order_side, order_type, amount
    )


def get_margin(instrument_id) -> Margin:
    return _broker.get_margin(instrument_id)


def get_position(instrument_id) -> Position:
    return _broker.get_position(instrument_id)


def get_contract_value(instrument_id):
    return 1


_bars = []


def get_latest_bar(instrument_id: str, granularity: int):
    global _bars
    if not _bars:
        _bars = get_bitmex_bar_by_range(
            ticker.begin, ticker.end, granularity, instrument_id
        )
    while True:
        try:
            res = _bars.pop(0)
        except IndexError:
            raise EOFError
        if res.timestamp >= ticker.current_ts:
            break
        # log.info(f"bar:{res.timestamp}")
    return res


def get_last_n_bars(cnt: int, instrument_id: str, granularity: int):
    bars = get_bitmex_last_n_bars(cnt, ticker.current_ts, instrument_id, granularity)
    return bars


def get_max_order_size(instrument_id: str):
    raise NotImplementedError()


def get_bars(instrument_id: str, granularity: int, start: datetime, end: datetime):
    return get_bitmex_bar_by_range(start, end, granularity=granularity)


def get_tick(instrument_id: str):
    res = ticker.get_tick(instrument_id)
    return res


def deposit(instrument_id: str, amount: Decimal):
    return _broker.deposit(instrument_id, amount)


def get_trade_history(instrument_id: str):
    return _broker.get_trade_history(instrument_id)
