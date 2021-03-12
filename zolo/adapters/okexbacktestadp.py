import logging
from datetime import datetime, timedelta
from decimal import Decimal

from xif.okex.swap.usdtmargin import get_candles

from ..consts import UNIX_EPOCH
from ..dtypes import Bar

log = logging.getLogger(__name__)


def time_interval_iter(begin: datetime, interval: int):
    hours, minutes = int(interval / 60), int(interval % 60)
    yield begin
    while True:
        cur = begin + timedelta(hours=hours, minutes=minutes)
        if cur > datetime.utcnow():
            break
        yield cur


def get_bar_iter(
    begin: datetime, instrument_id: str, granularity: int, cnt: int
):
    interval_iter = time_interval_iter(begin, interval=granularity * cnt)
    end = next(interval_iter)
    while True:
        
        try:
            begin, end = end, next(interval_iter)
        except StopIteration:
            break
        try:
            res = get_candles(
                instrument_id, begin, end, granularity=granularity * 60
                # limit=300
            )
            res.raise_for_status()
            res = res.json()
        except Exception as e:
            log.exception(e)
            break
        
        bars = [Bar(
            datetime.strptime(b[0], "%Y-%m-%dT%H:%M:%S.%fZ"),
            open=b[1],
            high=b[2],
            low=b[3],
            close=b[4],
            volume=int(b[5]),
            currency_volume=Decimal(b[6]),
        ) for b in sorted(res, key=lambda r: r[0])]
        
        while bars:
            yield bars.pop(0)


def get_latest_bar(instrument_id: str, granularity: int):
    
    try:
        return next(
            get_bar_iter(UNIX_EPOCH, instrument_id, granularity)
        )
    except StopIteration:
        raise KeyboardInterrupt(f"no available bar data")
