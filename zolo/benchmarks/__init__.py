from typing import TypeVar
from .sharp import SharpRatio
from .base import create_benchmark, Benchmark
from .profitfactor import ProfitFactor
from .drawdown import Drawdown
from .timeret import TimeReturn
from .trades import TradeCounter

BenchmarkType = TypeVar("BenchmarkType", SharpRatio, ProfitFactor, Drawdown,
                        TimeReturn, TradeCounter)
