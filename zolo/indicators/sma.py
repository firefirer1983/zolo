import logging

from .series import BarSeries

log = logging.getLogger(__name__)


class SimpleMovingAverage(BarSeries, alias="sma"):
    def __init__(self, period: int, granularity: int):
        super().__init__(period, granularity)
        self._val: float = 0.0

    def on_bar(self, bar):
        super().on_bar(bar)
        self._val = sum(float(bar.close) for bar in self.bars) / self.period

    @property
    def value(self):
        return self._val

    def __float__(self):
        return self._val

    def __repr__(self):
        return str(self._val)

    def __eq__(self, other):
        return self._val == float(other)

    def __gt__(self, other):
        return self._val > float(other)

    def __lt__(self, other):
        return self._val < float(other)

    def __ge__(self, other):
        return self._val >= float(other)

    def __le__(self, other):
        return self._val <= float(other)

    def __format__(self, format_spec):
        return self._val.__format__(format_spec)
