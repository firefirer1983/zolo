import logging

from datetime import datetime
from itertools import count
from typing import Union, Type, Iterable, Tuple, List, Dict, ItemsView, Set

from zolo.consts import INVALID
from zolo.benchmarks import BenchmarkType
from ..consts import (
    BLOCKING,
    NON_BLOCKING,
    PERIODIC,
    LONG,
    SHORT,
    DUAL,
    AIAO,
    FIFO,
    FILO,
    UNIX_EPOCH, POSITION_SIDE_EMPTY,
    DEFAULT_LEVERAGE
)
from .trdreg import create_registry
from ..orders import OrderPool, create_executor
from ..posts import OrderPost, is_blocking
from ..dtypes import CREDENTIAL_EMPTY, POSITION_EMPTY, INSTRUMENT_INVALID, \
    Qty, \
    MARGIN_EMPTY

from ..adapters import Adapter, create_adapter
from ..benchmarks.base import Benchmark, create_benchmark
from ..dtypes import Credential, InstrumentInfo, Order, Trade
from ..indicators import create_indicator, Indicator, IndicatorType

log = logging.getLogger(__name__)


class TradingContext:
    trading_id_registry: Set[str] = set()
    
    def __init__(
        self,
        unique_id: str,
        exchange: str,
        market: str,
        instrument_id: str,
        pos_side: str,
        adapter_type: str,
        registry_scheme: str,
        cred: Credential,
    ):
        if unique_id != "default" and unique_id in self.trading_id_registry:
            raise RuntimeError("Duplicated unique_id is not allow for trading")
        
        self.trading_id_registry.add(unique_id)
        
        assert pos_side in (LONG, SHORT, DUAL, POSITION_SIDE_EMPTY), \
            "Only support LONG, SHORT, DUAL"
        assert registry_scheme in (
            AIAO,
            FIFO,
            FILO,
        ), "Only support AIAO(all in all out), FIFO(first in first out), " \
           "FILO(first in last out) POSITION_SIDE_EMPTY("
        
        self._ind_cnt = count(0)
        self._bch_cnt = count(0)
        self._unique_id = unique_id
        self._exchange, self._market, self._instrument_id = (
            exchange,
            market,
            instrument_id,
        )
        self._indicators: Dict[int, Indicator] = dict()
        self._benchmarks: Dict[int, Benchmark] = dict()
        self._pos_side = pos_side
        self._cred: Credential = cred
        self._adapter = create_adapter(
            adapter_type, self.exchange, self.market, self._cred
        )
        
        self._instrument_info = INSTRUMENT_INVALID
        if self.instrument_id != INVALID:
            self._instrument_info = self._adapter.get_instrument_info(
                self.instrument_id)
        
        self._ts: datetime = UNIX_EPOCH
        self._leverage = DEFAULT_LEVERAGE
        if unique_id != "default":  # No trading for default context!
            self._leverage = self._adapter.get_leverage(instrument_id)
            self._registry = create_registry(
                registry_scheme,
                None, None,
                # self.adapter.pnl_scheme,
                # self.adapter.comm_scheme,
                self.pos_side,
            )
            self._pool = OrderPool(self._adapter)
    
    def set_leverage(self, lv: float):
        self._leverage = lv
        self._adapter.set_leverage(self.instrument_id, lv)
    
    def get_leverage(self) -> float:
        self._leverage = self._adapter.get_leverage(self.instrument_id)
        return self._leverage
    
    @property
    def registry_scheme(self) -> str:
        return getattr(self._registry, "scheme")
    
    def refresh(self, ts: datetime):
        self._pool.refresh(ts)
    
    def post_order(
        self, order: OrderPost, timeout: float, step: Qty, period: float
    ) -> Order:
        ts = self._pool.get_ts()
        assert self.credential != CREDENTIAL_EMPTY, "No valid credential!"
        if step != 0:
            mode = PERIODIC
            exe = create_executor(
                mode, order, self.adapter, timeout=timeout, step=step,
                period=period, ts=ts)
        else:
            mode = NON_BLOCKING if is_blocking(order) else BLOCKING
            exe = create_executor(
                mode, order, self.adapter, timeout=timeout,
                ts=ts)
        self._pool.add_to_pool(exe)
        return exe.result(self._pool.get_ts())
    
    def get_pending_orders(self) -> List[str]:
        return self._pool.get_pending()
    
    def get_order(self, client_oid: str, instrument_id: str) -> Order:
        res = self._pool.get_order(client_oid, instrument_id)
        return res
    
    def indicator(
        self, ind: Union[str, Type[Indicator]],
        **kwargs
    ) -> IndicatorType:
        res = create_indicator(ind, **kwargs)
        self._indicators[next(self._ind_cnt)] = res
        return res
    
    def benchmark(
        self, bch: Union[str, Type[Benchmark]], api_key: str, **kwargs
    ) -> BenchmarkType:
        kwargs["api_key"] = api_key
        res = create_benchmark(bch, **kwargs)
        self._benchmarks[next(self._bch_cnt)] = res
        return res
    
    @property
    def indicators(self) -> ItemsView[int, Indicator]:
        return self._indicators.items()
    
    @property
    def benchmarks(self) -> ItemsView[int, Benchmark]:
        return self._benchmarks.items()
    
    @property
    def instrument_info(self) -> InstrumentInfo:
        return self._instrument_info
    
    @property
    def adapter(self) -> Adapter:
        return self._adapter
    
    @property
    def credential(self) -> Credential:
        return self._cred
    
    @property
    def unique_id(self) -> str:
        return self._unique_id
    
    @property
    def exchange(self):
        return self._exchange
    
    @property
    def instrument_id(self):
        return self._instrument_id
    
    @property
    def market(self):
        return self._market
    
    @property
    def pos_side(self) -> str:
        return self._pos_side
    
    def on_order(self, order: Order):
        return self._registry.on_order(order)
    
    def get_trade(self) -> Iterable[Trade]:
        return self._registry.get_trade()
    
    # Backtest/Dryrun support only!
    def deposit(self, amount: float):
        return self.adapter.deposit(amount)
