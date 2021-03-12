from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Iterable, ItemsView, Union, Type, Callable, Dict
import logging

from .dtypes import Timer, Message
from .hub import evt_hub
from .utils import create_timer, create_filter

from .dtypes import Bar, OrderBook
from .adapters import Adapter
from .benchmarks import BenchmarkType
from .dtypes import Fill, Trade, Tick, Order, Credential, Qty, \
    Position, Margin, InstrumentInfo, Lot
from .indicators import IndicatorType
from .posts import OrderPostType

log = logging.getLogger(__name__)


class Sink(ABC):
    def on_tick(self, tick: Tick):
        raise NotImplementedError

    def on_bar(self, bar: Bar):
        raise NotImplementedError

    def on_fill(self, fill: Fill):
        raise NotImplementedError

    def on_trade(self, trade: Trade):
        raise NotImplementedError

    def on_book(self, book: OrderBook):
        raise NotImplementedError

    def on_order(self, order: Order):
        raise NotImplementedError

    def on_timer(self, ts: datetime):
        raise NotImplementedError


class TickSink(Sink):
    @abstractmethod
    def on_tick(self, tick: Tick):
        pass


class BarSink(Sink):
    @abstractmethod
    def on_bar(self, tick: Tick):
        pass


class FillSink(Sink):
    @abstractmethod
    def on_fill(self, fill: Fill):
        pass


class TradeSink(Sink):
    @abstractmethod
    def on_trade(self, trade: Trade):
        pass


class OrderSink(Sink):
    @abstractmethod
    def on_order(self, order: Order):
        pass


class TimerSink(Sink):
    @abstractmethod
    def on_timer(self, ts: datetime):
        pass


class CommissionScheme(ABC):
    @abstractmethod
    def calc_commission(self, price: float, qty: float) -> float:
        pass


class PnlScheme(ABC):
    @abstractmethod
    def calc_pnl(
        self, avg_entry_price: float, price: float, qty: float
    ) -> float:
        pass


class BrokerContext(ABC):
    @abstractmethod
    def refresh(self, ts: datetime):
        pass

    @abstractmethod
    def post_order(
        self, order: OrderPostType, timeout: float = 0, **kwargs
    ) -> Order:
        pass

    @abstractmethod
    def get_pending_orders(self) -> List[str]:
        pass

    @abstractmethod
    def get_order(self, client_oid: str) -> Order:
        pass

    @abstractmethod
    def indicator(
        self, ind: Union[str, IndicatorType], **kwargs
    ) -> IndicatorType:
        pass

    @abstractmethod
    def register_on_bar(self, granularity: int, on_bar: Callable):
        pass

    @abstractmethod
    def register_on_tick(self, on_tick: Callable):
        pass

    @abstractmethod
    def register_on_book(self, on_book: Callable):
        pass

    @abstractmethod
    def register_on_trade(self, api_key: str, on_trade: Callable):
        pass

    @abstractmethod
    def benchmark(
        self, bch: Union[str, Type[BenchmarkType]], api_key: str, **kwargs
    ) -> BenchmarkType:
        pass

    @property
    @abstractmethod
    def indicators(self) -> ItemsView[int, IndicatorType]:
        pass

    @property
    @abstractmethod
    def benchmarks(self) -> ItemsView[int, BenchmarkType]:
        pass

    @property
    @abstractmethod
    def instrument_info(self) -> InstrumentInfo:
        pass

    @property
    @abstractmethod
    def adapter(self) -> Adapter:
        pass

    @property
    @abstractmethod
    def credential(self) -> Credential:
        pass

    @property
    @abstractmethod
    def exchange(self) -> str:
        pass

    @property
    @abstractmethod
    def instrument_id(self) -> str:
        pass

    @property
    @abstractmethod
    def market(self) -> str:
        pass

    @property
    @abstractmethod
    def pos_side(self) -> str:
        pass

    @abstractmethod
    def on_order(self, order: Order):
        pass

    @abstractmethod
    def get_trade(self) -> Iterable[Trade]:
        pass

    # Backtest/Dryrun support only!
    def deposit(self, amount: float):
        raise NotImplementedError


class Broker(ABC):

    @property
    @abstractmethod
    def adapter(self):
        pass

    @property
    @abstractmethod
    def context(self) -> "BrokerContext":
        pass

    @abstractmethod
    def clone(
        self, unique_id: str, instrument_id: str, pos_side: str = "",
        credential: Credential = None, trade_registry_scheme: str = "",
    ) -> "Broker":
        pass

    @property
    @abstractmethod
    def max_optimal_depth(self) -> int:
        pass

    @abstractmethod
    def get_all_instruments(self) -> Dict[str, InstrumentInfo]:
        pass

    @abstractmethod
    def get_trade(self) -> Iterable[Trade]:
        pass

    @abstractmethod
    def get_tick(self, instrument_id: str) -> Tick:
        pass
    
    @abstractmethod
    def get_ticks(self, *instruments, pricing: str = "avg") -> List[Tick]:
        pass
    
    @abstractmethod
    def get_available_balance(self, instrument_id: str) -> float:
        pass

    @abstractmethod
    def info_instrument(self, instrument_id: str) -> InstrumentInfo:
        pass

    @abstractmethod
    def transfer_asset_to_future_margin(
        self, instrument_id: str, amount: float
    ) -> float:
        pass
    
    @abstractmethod
    def transfer_asset_to_swap_margin(
        self, instrument_id: str, amount: float
    ) -> float:
        pass
    
    @abstractmethod
    def transfer_margin_to_asset(
        self, instrument_id: str, amount: float
    ) -> float:
        pass

    @abstractmethod
    def get_book(self, instrument_id: str, depth: int) -> OrderBook:
        pass

    @abstractmethod
    def on_timer(self, ts: datetime):
        pass

    @abstractmethod
    def on_order(self, order: Order):
        pass

    @abstractmethod
    def on_fill(self, fill: Fill):
        pass

    @abstractmethod
    def register_context(
        self,
        unique_id: str,
        instrument_id: str,
        pos_side: str,
        trade_registry: str,
        credential: Credential,
    ) -> "BrokerContext":
        pass

    @abstractmethod
    def register_indicator(self, ind: IndicatorType):
        pass

    @abstractmethod
    def register_benchmark(self, sink: BenchmarkType):
        pass

    @abstractmethod
    def register_on_bar(self, granularity: int, on_bar: Callable):
        pass

    @abstractmethod
    def register_on_tick(self, on_tick: Callable):
        pass

    @abstractmethod
    def register_on_book(self, on_book: Callable):
        pass

    @abstractmethod
    def register_on_trade(self, api_key: str, on_trade: Callable):
        pass

    @abstractmethod
    def register_on_order(self, api_key: str, on_order: Callable):
        pass

    @abstractmethod
    def register_on_fill(self, api_key: str, on_fill: Callable):
        pass

    @abstractmethod
    def indicator(
        self, ind: Union[str, IndicatorType], **kwargs
    ) -> IndicatorType:
        pass

    @abstractmethod
    def benchmark(
        self, bch: Union[str, BenchmarkType], api_key: str, **kwargs
    ) -> BenchmarkType:
        pass

    @abstractmethod
    def list_active_indicators(self) -> ItemsView[int, IndicatorType]:
        pass

    @abstractmethod
    def list_active_benchmarks(self) -> ItemsView[int, BenchmarkType]:
        pass

    @abstractmethod
    def estimate_lot(self, size: float, price: float = 0) -> Lot:
        pass

    @abstractmethod
    def buy_market(
        self,
        timeout: float,
        qty: Qty,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        pass

    @abstractmethod
    def sell_market(
        self,
        timeout: float,
        qty: Qty,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        pass

    @abstractmethod
    def buy_limit(
        self, price: float, qty: Qty,
        timeout: float = 0
    ) -> Order:
        pass

    @abstractmethod
    def sell_limit(
        self, price: float, qty: Qty,
        timeout: float = 0
    ) -> str:
        pass

    @abstractmethod
    def buy_limit_ioc(
        self,
        timeout: float,
        price: float,
        qty: Qty,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        pass

    @abstractmethod
    def sell_limit_ioc(
        self,
        timeout: float,
        price: float,
        qty: Qty,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        pass

    @abstractmethod
    def buy_limit_fok(
        self,
        timeout: float,
        price: float,
        qty: Qty,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        pass

    @abstractmethod
    def sell_limit_fok(
        self,
        timeout: float,
        price: float,
        qty: Qty,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        pass

    @abstractmethod
    def buy_opponent_ioc(
        self,
        qty: Qty,
        timeout: float = 0,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        pass

    @abstractmethod
    def sell_opponent_ioc(
        self,
        qty: Qty,
        timeout: float = 0,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        pass

    @abstractmethod
    def buy_opponent_fok(
        self,
        qty: Qty,
        timeout: float = 0,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        pass

    @abstractmethod
    def sell_opponent_fok(
        self,
        qty: Qty,
        timeout: float = 0,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        pass

    @abstractmethod
    def buy_optimal_ioc(
        self,
        depth: int,
        qty: Qty,
        timeout: float = 0,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        pass

    @abstractmethod
    def sell_optimal_ioc(
        self,
        depth: int,
        qty: Qty,
        timeout: float = 0,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        pass

    @abstractmethod
    def buy_optimal_fok(
        self,
        depth: int,
        qty: Qty,
        timeout: float = 0,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        pass

    @abstractmethod
    def sell_optimal_fok(
        self,
        depth: int,
        qty: Qty,
        timeout: float = 0,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        pass

    @property
    @abstractmethod
    def exchange(self) -> str:
        pass

    @property
    @abstractmethod
    def market(self) -> str:
        pass

    def deposit(self, amount: float):
        raise RuntimeError

    @abstractmethod
    def post_order(
        self, order: OrderPostType, timeout: float = 0, **kwargs
    ) -> Order:
        pass

    @abstractmethod
    def get_pending_orders(self) -> List[str]:
        pass

    @abstractmethod
    def get_order_by_id(self, client_oid: str) -> Order:
        pass

    @abstractmethod
    def cancel_order(self, client_oid: str):
        pass

    @abstractmethod
    def cancel_all_orders(self):
        pass

    @abstractmethod
    def get_position(self) -> Position:
        pass

    @abstractmethod
    def get_margin(self) -> Margin:
        pass

    @abstractmethod
    def set_leverage(self, lv: float):
        pass
    
    @abstractmethod
    def get_leverage(self):
        pass


class Strategy(ABC):

    def __init__(self):
        self._brokers: List[Broker] = list()
        
    @abstractmethod
    def on_start(self):
        pass

    @abstractmethod
    def on_stop(self):
        pass

    def register_broker(self, *brokers: Broker):
        for brk in brokers:
            self._brokers.append(brk)
            brk.register_on_order(brk.context.credential.api_key, brk.on_order)
            brk.register_on_fill(brk.context.credential.api_key, brk.on_fill)
            # broker需要不断刷新order状态.
            evt_hub.attach_sink(Timer, create_timer(timeout=1), brk.on_timer)

    @staticmethod
    def register_timer(sink: Sink, timeout: int):
        assert timeout
        evt_hub.attach_sink(Timer, create_timer(timeout=timeout), sink.on_timer)

    @staticmethod
    def register_cmd(sink: Sink, cmd: str):
        assert cmd
        evt_hub.attach_sink(Message, create_filter(cmd=cmd), sink.on_message)

    @property
    def brokers(self) -> List[Broker]:
        return self._brokers
