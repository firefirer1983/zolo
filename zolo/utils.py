import json
import sqlite3
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from functools import partial, wraps
from itertools import cycle, chain
from dataclasses import asdict
from collections import Iterable
from time import time
import logging
from typing import Union
from uuid import uuid4

from zolo.consts import UNIX_EPOCH

log = logging.getLogger(__name__)


class LimitedCounter:
    def __init__(self, limit):
        self.limit = limit
        self.cnt = 0
    
    def __iadd__(self, other):
        self.cnt += other
        if self.cnt > self.limit:
            raise OverflowError
        return self


def register_sql_decimal():
    def adapt_decimal(d):
        return str(d)
    
    def convert_decimal(s):
        return Decimal(s)
    
    # Register the adapter
    sqlite3.register_adapter(Decimal, adapt_decimal)
    
    # Register the converter
    sqlite3.register_converter("decimal", convert_decimal)


def create_datatype_mapping(en_cls, dat_cls, to_enum=True):
    def _map(val):
        if to_enum:
            comp_attr = "name"
        else:
            comp_attr = "value"
        for en in en_cls:
            if getattr(en, comp_attr) == val:
                if to_enum:
                    return en
                else:
                    return getattr(dat_cls, getattr(en, "name"))
        raise ValueError(f"Unknown value:{val}")
    
    return _map


def asstr(obj):
    return json.dumps(asdict(obj))


def datetime_to_str(dt: datetime):
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


class BooleanFlip:
    def __init__(self, default=True):
        states = (True, False) if default else (False, True)
        self._it = cycle(states)
        self.state = next(self._it)
    
    def __bool__(self):
        return self.state
    
    def __next__(self):
        self.state = next(self._it)
        return self.state


granularity_registry = {
    "1m": 1, "5m": 5, "10m": 10, "15m": 15, "30m": 30, "1h": 60, "5h": 300,
    "10h": 600, "15h": 900, "1d": 24 * 60,
    "5d": 5 * 24 * 60, "10d": 10 * 24 * 60}


def granularity_in_num(g):
    return granularity_registry[g]


def granularity_in_str(g):
    for k, v in granularity_registry.items():
        if v == g:
            return k
    raise ValueError


def create_in_filter(k: str, *args):
    assert k and args
    members = (str(arg) for arg in args)
    
    def _filter(x):
        str(getattr(x, k)).lower()
        return x in members
    
    return _filter


def create_filter(**kwargs):
    def _filter(x):
        for k, v in kwargs.items():
            if str(getattr(x, k)).lower() != str(v).lower():
                return False
        return True
    
    return _filter


BYPASS_FILTER = create_filter()


def create_timer(timeout: int, trigger_begin: bool = True):
    _ts: datetime = UNIX_EPOCH
    
    def _filter(x):
        nonlocal _ts
        res = False
        if (x - _ts).seconds >= timeout:
            if _ts is UNIX_EPOCH and trigger_begin is False:
                res = False
            else:
                res = True
            _ts = x
        return res
    
    return _filter


def raise_error(e: Exception):
    raise e


def get_current_timestamp() -> str:
    return f"{int(time() * 10000000)}"


def clone_dataclass_object(obj, **kwargs):
    res = asdict(obj)
    res.update(kwargs)
    return type(obj)(**res)


def calc_entry_price(origin_price, origin_qty, price, amount) -> Decimal:
    origin_qty, amount = abs(origin_qty), abs(amount)
    res = (origin_price * origin_qty + price * amount) / (origin_qty + amount)
    return res


def calc_pnl_by_reverse(
    qty: float, price: float, avg_entry_price: float, contract_size: float
):
    return qty * contract_size * (1 / avg_entry_price - 1 / price)


def calc_pnl(
    qty: float, price: float, avg_entry_price: float, contract_size: float
):
    return qty * contract_size * (price - avg_entry_price)


def calc_comm_by_reverse(
    qty: float, price: float, rate: float, contract_size: float
):
    return rate * contract_size * abs(qty) / price


def calc_comm(
    qty: float, price: float, rate: float, contract_size: float
):
    return rate * contract_size * abs(qty) * price


def heartbeat(period=5, func=None):
    prev = datetime(1970, 1, 1)
    
    if func is None:
        return partial(heartbeat, period)
    
    @wraps(func)
    def _wrap(*args, **kwargs):
        nonlocal prev
        for v in chain(args, kwargs.values()):
            if hasattr(v, "timestamp"):
                ts = v.timestamp
                break
        else:
            ts = datetime.utcnow()
        
        if datetime.utcnow() - prev > timedelta(seconds=period):
            log.info(f"[Heartbeat]: {_wrap.__name__}, {ts} ")
            prev = datetime.utcnow()
        return func(*args, **kwargs)
    
    return _wrap


def unique_id_with_uuid4() -> str:
    return uuid4().hex


def iterable(obj: object):
    return isinstance(obj, Iterable)


def round_down(
    decimals: Union[int, str], num: Union[Decimal, float, str]
) -> Union[Decimal, float, str]:
    
    if isinstance(decimals, int):
        assert decimals > 0
        decimals = str(1 / 10 ** decimals)
    else:
        assert decimals[-1] == "1" and decimals[:2] == "0."
        
    if not isinstance(num, Decimal):
        num = Decimal(num)
        
    return Decimal(num.quantize(Decimal(decimals), rounding=ROUND_DOWN))

