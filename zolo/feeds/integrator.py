from dataclasses import replace
from collections import deque
from typing import Tuple, Iterable, Union
from .base import BarDataFeed
from ..utils import granularity_in_num
from ..dtypes import Bar, Tick, Timer, BAR_EMPTY


def fill_up(bars: BarDataFeed, interval):
    bars = iter(bars)
    curr = next(bars)
    while True:
        prev = curr
        try:
            curr = next(bars)
        except EOFError as e:
            yield prev
            raise e
        else:
            while curr - prev.timestamp != interval:
                yield prev
                prev = replace(
                    prev,
                    timestamp=prev.timestamp + interval,
                    open=0,
                    close=0,
                    high=0,
                    low=0,
                    volume=0,
                    currency_volume=0,
                )
            else:
                yield prev


class HybridDataFeed:
    def __init__(self, data: BarDataFeed, periods: Tuple[str]):
        self._periods = sort_periods(*periods)
        self._bars = deque(maxlen=max(self._periods))
        self._cnt = 0
        self._base = iter(data)

    def __iter__(self) -> Iterable[Union[Tick, Bar, Timer]]:
        while True:
            bar = next(self._base)
            self._bars.appendleft(bar)
            self._cnt += 1
            for period in self._periods:
                yield Timer(bar.timestamp)
                yield Tick(
                    exchange=bar.exchange,
                    market=bar.market,
                    timestamp=bar.timestamp,
                    price=bar.close,
                    instrument_id=bar.instrument_id,
                )
                if self._cnt % period == 0:
                    yield self.integrate(period, bar)

    def integrate(self, period: int, bar: Bar):
        bars = list(self._bars)
        return replace(
            bar,
            open=bars[period - 1].open,
            close=bars[0].close,
            high=max([b.high for b in bars[:period] if b.volume] or (0,)),
            low=max([b.low for b in bars[:period] if b.volume] or (0,)),
            volume=sum([b.volume for b in bars[:period]] or (0,)),
            granularity=period,
        )


def sort_periods(*periods):
    res = map(lambda x: granularity_in_num(x) if isinstance(x, str) else x, periods)
    return sorted(res)
