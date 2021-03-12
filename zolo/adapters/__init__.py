from .base import Adapter, create_adapter
from .huobi_restful_adapters import HuobiRestfulAdapter, HuobiRestfulCoinMarginSwap, HuobiRestfulUsdtMarginSwap, \
    HuobiRestfulCoinMarginFuture, HuobiRestfulSpot
from .hubi_backtest_adapters import HuobiBacktestCoinMarginSwap, HuobiBacktestCoinMarginFuture, \
    HuobiBacktestUsdtMarginSwap, HuobiBacktestSpot, HuobiBacktestAdapter
from .bitmex_backtest_adapters import BitmexBacktestCoinMarginSwap, BitmexBacktestCoinMarginFuture
