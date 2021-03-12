from .base import Indicator
from collections import deque


class BarSeries(Indicator, alias="series"):

    def __init__(self, period: int, granularity: int):
        self._period, self._granularity = period, granularity
        self._bars = deque(maxlen=period)
    
    @property
    def bars(self):
        return list(self._bars)

    def on_bar(self, bar):
        self._bars.appendleft(bar)

    @property
    def period(self):
        return self._period

    @property
    def granularity(self):
        return self._granularity
