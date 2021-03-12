from typing import TypeVar
from .series import BarSeries
from .sma import SimpleMovingAverage
from .base import Indicator, create_indicator

IndicatorType = TypeVar("IndicatorType", BarSeries, SimpleMovingAverage)


