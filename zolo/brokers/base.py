import abc
import logging
from datetime import datetime
from itertools import count
from typing import Iterable, List, Union, Type, ItemsView, Callable, Optional, \
    Dict

from zolo.indicators import IndicatorType
from ..base import Broker, Sink
from ..dtypes import Bar, Tick, Lot, Size, Fill, Qty, OrderBook, \
    CREDENTIAL_EMPTY, InstrumentInfo
from ..hub import evt_hub
from ..utils import create_filter
from ..dtypes import Trade
from ..consts import BUY, SELL, RESTFUL, AIAO, ON_TICK, ON_BAR, ON_TRADE, \
    ON_BOOK, LONG, DEFAULT_LEVERAGE, BLOCKING_ORDER_TIMEOUT
from .context import TradingContext
from ..benchmarks import TradeCounter, TimeReturn, ProfitFactor, SharpRatio, \
    Benchmark, BenchmarkType
from ..indicators.base import Indicator
from ..posts import MarketOrder, OpponentIocOrder, \
    OpponentFokOrder, \
    OptimalIocOrder, OptimalFokOrder, \
    LimitFokOrder, LimitIocOrder, OrderPostType, LimitOrder
from ..dtypes import Credential, Order, Position, Margin

log = logging.getLogger(__name__)


class BrokerBase(Broker):
    
    def __init__(
        self, exchange: str, market: str, adapter_type: str,
    ):
        self._exchange = exchange
        self._market = market
        self._adapter_type = adapter_type
        # 提供 default context, default context无法进行交易, 只能获取数据
        self._context: Optional[TradingContext] = None
    
    @property
    def adapter(self):
        return self.context.adapter
    
    @property
    def context(self) -> "TradingContext":
        return self._context
    
    def clone(
        self, unique_id: str, instrument_id: str, pos_side: str = "",
        credential: Credential = None, trade_registry_scheme: str = "",
        leverage: float = DEFAULT_LEVERAGE
    ) -> "Broker":
        assert self.context.unique_id != "default", \
            "Can't clone default context "
        assert unique_id == self.context.unique_id, "Can't clone yourself!"
        
        pos_side = pos_side or getattr(self, "pos_side")
        credential = credential or getattr(self, "credential")
        trade_registry_scheme = trade_registry_scheme \
                                or self.context.registry_scheme
        brk = type(self)(self.exchange, self.market, self.instrument_id)
        brk.register_context(
            unique_id,
            instrument_id,
            pos_side,
            credential,
            trade_registry_scheme,
        )
        return brk
    
    def get_all_instruments(self) -> Dict[str, InstrumentInfo]:
        return self.context.adapter.get_all_instruments()
    
    def get_tick(self, instrument_id: str = "") -> Tick:
        if not instrument_id:
            instrument_id = self.context.instrument_id
        return self.context.adapter.get_tick(instrument_id)
    
    def get_ticks(self, *instruments, pricing: str = "avg") -> List[Tick]:
        return self.context.adapter.get_ticks(*instruments, pricing=pricing)
    
    def get_book(self, instrument_id: str, depth: int) -> OrderBook:
        return self.context.adapter.get_book(instrument_id, depth)
    
    def get_trade(self) -> Iterable[Trade]:
        return self.context.get_trade()
    
    def info_instrument(self, instrument_id: str) -> InstrumentInfo:
        return self.context.adapter.get_instrument_info(instrument_id)
    
    def on_timer(self, ts: datetime):
        self.context.refresh(ts)
    
    @property
    def max_optimal_depth(self) -> int:
        return self.context.adapter.max_optimal_depth
    
    @abc.abstractmethod
    def on_order(self, order: Order):
        pass
    
    @abc.abstractmethod
    def on_fill(self, fill: Fill):
        pass
    
    def register_context(
        self,
        unique_id: str,
        instrument_id: str,
        pos_side: str,
        credential: Credential,
        trade_registry: str = AIAO,
    ) -> TradingContext:
        if self._context and unique_id == "default":
            raise RuntimeError(f"default name is reserved!")
        
        self._context = TradingContext(
            unique_id,
            self._exchange,
            self._market,
            instrument_id,
            pos_side,
            self._adapter_type,
            trade_registry,
            credential,
        )
        return self._context
    
    def register_on_bar(self, granularity: int, on_bar: Callable):
        assert granularity
        flt = create_filter(
            exchange=self.exchange,
            market=self.market,
            instrument_id=self.context.instrument_id,
            granularity=int(granularity),
        )
        evt_hub.attach_sink(Bar, flt, on_bar)
    
    def register_on_tick(self, on_tick: Callable):
        flt = create_filter(
            exchange=self.exchange,
            market=self.market,
            instrument_id=self.context.instrument_id,
        )
        evt_hub.attach_sink(Tick, flt, on_tick)
    
    def register_on_book(self, on_book: Callable):
        flt = create_filter(
            exchange=self.exchange,
            market=self.market,
            instrument_id=self.context.instrument_id,
        )
        evt_hub.attach_sink(OrderBook, flt, on_book)
    
    def register_on_order(self, api_key: str, on_order: Callable):
        flt = create_filter(
            exchange=self.exchange,
            market=self.market,
            instrument_id=self.context.instrument_id,
            account=api_key,
        )
        evt_hub.attach_sink(Order, flt, on_order)
    
    def register_on_fill(self, api_key: str, on_fill: Callable):
        flt = create_filter(
            exchange=self.exchange,
            market=self.market,
            instrument_id=self.context.instrument_id,
            account=api_key,
        )
        evt_hub.attach_sink(Fill, flt, on_fill)
    
    def register_on_trade(self, api_key: str, on_trade: Callable):
        flt = create_filter(
            exchange=self.exchange,
            market=self.market,
            instrument_id=self.context.instrument_id,
            account=api_key,
        )
        evt_hub.attach_sink(Trade, flt, on_trade)
    
    def register_indicator(self, ind: IndicatorType):
        granularity: int = getattr(ind, "granularity")
        assert granularity
        self.register_on_bar(granularity, getattr(ind, ON_BAR))
    
    def register_benchmark(self, bch: BenchmarkType):
        api_key: str = getattr(bch, "api_key", "")
        if not api_key:
            api_key: str = getattr(bch, "credential", CREDENTIAL_EMPTY).api_key
        assert api_key
        self.register_on_trade(api_key, getattr(bch, ON_TRADE))
        if callable(getattr(bch, ON_TICK, None)):
            self.register_on_trade(api_key, getattr(bch, ON_TICK))
    
    def indicator(
        self, ind: Union[str, IndicatorType], **kwargs
    ) -> IndicatorType:
        res = self.context.indicator(ind, **kwargs)
        self.register_indicator(ind)
        return res
    
    def benchmark(
        self, bch: Union[str, BenchmarkType], api_key: str, **kwargs
    ) -> BenchmarkType:
        res = self.context.benchmark(bch, api_key, **kwargs)
        return res
    
    def list_active_indicators(self) -> ItemsView[int, Indicator]:
        return self.context.indicators
    
    def list_active_benchmarks(self) -> ItemsView[int, Benchmark]:
        return self.context.benchmarks
    
    def estimate_lot(self, size: float, price: float = 0) -> Lot:
        return self.context.adapter.estimate_lot(
            self.context.instrument_id, size, price
        )
    
    @property
    def instrument_id(self):
        return self.context.instrument_id
    
    def buy_market(
        self,
        qty: Qty,
        timeout: float = BLOCKING_ORDER_TIMEOUT,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        post = MarketOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            BUY,
            self.pos_side,
            qty,
            slippage,
        )
        return self.post_order(post, timeout, step=step, period=period)
    
    def sell_market(
        self,
        qty: Qty,
        timeout: float = BLOCKING_ORDER_TIMEOUT,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        post = MarketOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            SELL,
            self.pos_side,
            qty,
            slippage,
        )
        return self.post_order(post, timeout, step=step, period=period)
    
    def buy_limit(
        self,
        price: float,
        qty: Qty,
        timeout: float
    ) -> Order:
        post = LimitOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            BUY,
            self.pos_side,
            price,
            qty,
            timeout,
        )
        return self.post_order(post, timeout)
    
    def sell_limit(
        self,
        price: float,
        qty: Qty,
        timeout: float
    ) -> Order:
        post = LimitOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            SELL,
            self.pos_side,
            price,
            qty,
            timeout,
        )
        return self.post_order(post, timeout)
    
    def buy_limit_ioc(
        self,
        price: float,
        qty: Qty,
        timeout: float,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        post = LimitIocOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            BUY,
            self.pos_side,
            price,
            qty,
        )
        return self.post_order(post, timeout, step=step, period=period)
    
    def sell_limit_ioc(
        self,
        price: float,
        qty: Qty,
        timeout: float,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        post = LimitIocOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            SELL,
            self.pos_side,
            price,
            qty,
        )
        return self.post_order(post, timeout, step=step, period=period)
    
    def buy_limit_fok(
        self,
        price: float,
        qty: Qty,
        timeout: float,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        post = LimitFokOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            BUY,
            self.pos_side,
            price,
            qty,
        )
        return self.post_order(post, timeout, step=step, period=period)
    
    def sell_limit_fok(
        self,
        price: float,
        qty: Qty,
        timeout: float,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        post = LimitFokOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            SELL,
            self.pos_side,
            price,
            qty,
        )
        return self.post_order(post, timeout, step=step, period=period)
    
    def buy_opponent_ioc(
        self,
        qty: Qty,
        timeout: float = BLOCKING_ORDER_TIMEOUT,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        post = OpponentIocOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            BUY,
            self.pos_side,
            qty,
            slippage,
        )
        return self.post_order(post, timeout, step=step, period=period)
    
    def sell_opponent_ioc(
        self,
        qty: Qty,
        timeout: float = BLOCKING_ORDER_TIMEOUT,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        post = OpponentIocOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            SELL,
            self.pos_side,
            qty,
            slippage,
        )
        return self.post_order(post, timeout, step=step, period=period)
    
    def buy_opponent_fok(
        self,
        qty: Qty,
        timeout: float = BLOCKING_ORDER_TIMEOUT,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        post = OpponentFokOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            BUY,
            self.pos_side,
            qty,
            slippage,
        )
        return self.post_order(post, timeout, step=step, period=period)
    
    def sell_opponent_fok(
        self,
        qty: Qty,
        timeout: float = BLOCKING_ORDER_TIMEOUT,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        post = OpponentFokOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            SELL,
            self.pos_side,
            qty,
            slippage,
        )
        return self.post_order(post, timeout, step=step, period=period)
    
    def buy_optimal_ioc(
        self,
        depth: int,
        qty: Qty,
        timeout: float = BLOCKING_ORDER_TIMEOUT,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        post = OptimalIocOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            BUY,
            self.pos_side,
            depth,
            qty,
            slippage,
        )
        return self.post_order(post, timeout, step=step, period=period)
    
    def sell_optimal_ioc(
        self,
        depth: int,
        qty: Qty,
        timeout: float = BLOCKING_ORDER_TIMEOUT,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        post = OptimalIocOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            SELL,
            self.pos_side,
            depth,
            qty,
            slippage,
        )
        return self.post_order(post, timeout, step=step, period=period)
    
    def buy_optimal_fok(
        self,
        depth: int,
        qty: Qty,
        timeout: float = BLOCKING_ORDER_TIMEOUT,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        post = OptimalFokOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            BUY,
            self.pos_side,
            depth,
            qty,
            slippage,
        )
        return self.post_order(post, timeout, step=step, period=period)
    
    def sell_optimal_fok(
        self,
        depth: int,
        qty: Qty,
        timeout: float = BLOCKING_ORDER_TIMEOUT,
        slippage: float = 0,
        step: Qty = 0,
        period: float = 0,
    ) -> Order:
        post = OptimalFokOrder(
            self.exchange,
            self.market,
            self.instrument_id,
            SELL,
            self.pos_side,
            depth,
            qty,
            slippage,
        )
        return self.post_order(post, timeout, step=step, period=period)
    
    @property
    def exchange(self) -> str:
        return self._exchange
    
    @property
    def market(self) -> str:
        return self._market
    
    @property
    def pos_side(self) -> str:
        return self.context.pos_side
    
    def deposit(self, amount: float):
        raise RuntimeError
    
    def post_order(
        self, order: OrderPostType, timeout: float = 0, step: Qty = 0,
        period: float = 0
    ) -> Order:
        assert order.qty > 0 and step >= 0
        #  Step quantity type must align with target
        if step:
            if type(order.qty) in (Size, Lot):
                assert type(step) in (Size, Lot)
            
            if type(order.qty) in (int, float):
                assert type(step) in (int, float)
        
        return self.context.post_order(order, timeout, step, period)
    
    def get_pending_orders(self) -> List[str]:
        return self.context.get_pending_orders()
    
    def get_order_by_id(
        self, client_oid: str, instrument_id: str = ""
    ) -> Order:
        if not instrument_id:
            instrument_id = self.context.instrument_id
        return self.context.get_order(client_oid, instrument_id)
    
    def cancel_order(self, client_oid: str):
        return self.context.adapter.cancel_order(self.instrument_id, client_oid)
    
    def cancel_all_orders(self):
        return self.context.adapter.cancel_all_orders(self.instrument_id)
    
    def get_position(self) -> Position:
        return self.context.adapter.get_position(
            self.context.instrument_id)
    
    def get_all_position(self) -> List[Position]:
        return self.context.adapter.get_position("")
    
    def get_margin(self) -> Margin:
        return self.context.adapter.get_margin(self.context.instrument_id)
    
    def get_all_margin(self) -> List[Margin]:
        return self.context.adapter.get_margin()
    
    def get_available_balance(self, symbol: str = "") -> float:
        if not symbol:
            symbol = self.info_instrument(
                self.context.instrument_id).base_currency
        return self.context.adapter.get_available_balance(symbol)
    
    def get_all_available_balance(self) -> Dict[str, float]:
        return self.context.adapter.get_available_balance("")
    
    def transfer_asset_to_future_margin(
        self, symbol: str, amount: float
    ) -> float:
        return self.context.adapter.transfer_asset_to_future_margin(
            symbol, amount)
    
    def transfer_asset_to_swap_margin(
        self, symbol: str, amount: float
    ) -> float:
        return self.context.adapter.transfer_asset_to_swap_margin(
            symbol, amount)
    
    def transfer_margin_to_asset(
        self, symbol: str, amount: float
    ) -> float:
        return self.context.adapter.transfer_margin_to_asset(
            symbol, amount)
    
    def set_leverage(self, lv: float):
        instrument_id = self.context.instrument_id
        return self.context.adapter.set_leverage(instrument_id, lv)
    
    def get_leverage(self):
        instrument_id = self.context.instrument_id
        return self.context.adapter.get_leverage(instrument_id)
