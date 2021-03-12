import logging
from abc import ABC, abstractmethod
from itertools import count
from typing import List, Optional, Dict, Type, Iterable, Callable, Tuple
from dataclasses import replace
from ..consts import LONG, SHORT, DUAL, BUY, SELL, AIAO, FIFO, FILO, CLOSE
from ..utils import calc_entry_price
from ..dtypes import Trade, Order

log = logging.getLogger(__name__)


class SchemeRegistry:
    registry: Dict[str, Type["TradeRegistry"]] = dict()
    
    @classmethod
    def register(cls, scheme: str, scheme_class: Type["TradeRegistry"]):
        assert scheme
        cls.registry[scheme] = scheme_class
        setattr(cls, "scheme", scheme)
    
    @classmethod
    def create_registry(
        cls, scheme: str, pnl_calc: Callable, comm_calc: Callable,
        pos_side: str
    ) -> "TradeRegistry":
        return cls.registry[scheme](pnl_calc, comm_calc, pos_side)


class Amounts:
    def __init__(self, name: str = ""):
        self._name = name
        self._amounts = tuple()
    
    def add(self, qty: float):
        if qty > 0:
            self._amounts += (qty,)
    
    def __repr__(self):
        _str = "|".join([str(a) for a in self._amounts])
        return f"{self._name} => {_str}  ({self.get_total()})"
    
    def get_total(self) -> float:
        return sum(self._amounts)


class TradeRegistry(ABC):
    def __init_subclass__(cls, scheme: str = "", **kwargs):
        SchemeRegistry.register(scheme, cls)
    
    def __init__(
        self,
        pnl_calc,
        comm_calc,
        pos_side: str = DUAL,
    ):
        self._trd_id_cnt = count(1, 1)
        self.pnl_calc = pnl_calc
        self.comm_calc = comm_calc
        self._avg_entry_price: float = 0
        self._trd: Optional[Trade] = None
        self._pos_side = pos_side
        self._trades = []
        self.buy = Amounts("Buys")
        self.sell = Amounts("Sells")
    
    @property
    def pos_side(self) -> str:
        return self._pos_side
    
    def gen_new_id(self) -> str:
        return str(next(self._trd_id_cnt))
    
    @abstractmethod
    def pos_increase(self, order: Order, direction: str):
        raise NotImplementedError
    
    @abstractmethod
    def pos_decrease(self, order: Order, direction: str):
        raise NotImplementedError
    
    def get_trade(self) -> Iterable[Trade]:
        while self._trades:
            yield self._trades.pop(0)
    
    def on_order_with_long_pos(self, order: Order, direction: str):
        if order.side == BUY:
            return self.pos_increase(order, direction)
        
        if order.qty <= abs(self.qty):
            return self.pos_decrease(order, direction)
        
        raise ValueError
    
    def on_order_with_short_pos(self, order: Order, direction: str):
        if order.side == SELL:
            return self.pos_increase(order, direction)
        
        if order.qty <= abs(self.qty):
            return self.pos_decrease(order, direction)
        
        raise ValueError
    
    @property
    def avg_entry_price(self) -> float:
        return self._avg_entry_price
    
    @property
    def qty(self) -> float:
        return self.buy.get_total() - self.sell.get_total()
    
    @property
    def direction(self) -> str:
        if self.qty > 0:
            return LONG
        elif self.qty < 0:
            return SHORT
        else:
            return ""
    
    def on_order_with_dual_pos(self, order: Order, direction: str):
        if not direction and order.side == BUY:
            log.info(f"[TRDREG] {direction} 开多仓: {order.qty}")
            return self.pos_increase(order, direction)
        elif not direction and order.side == SELL:
            log.info(f"[TRDREG] {direction} 开空仓: {order.qty}")
            return self.pos_increase(order, direction)
        elif direction is SHORT and order.side is BUY:
            log.info(f"[TRDREG] {direction} 减空仓: {order.qty}")
            return self.pos_decrease(order, direction)
        elif direction is LONG and order.side is SELL:
            log.info(f"[TRDREG] {direction} 减多仓: {order.qty}")
            return self.pos_decrease(order, direction)
        elif direction is SHORT and order.side is SELL:
            log.info(f"[TRDREG] {direction} 加空仓: {order.qty}")
            return self.pos_increase(order, direction)
        elif direction is LONG and order.side is BUY:
            log.info(f"[TRDREG] {direction} 加多仓: {order.qty}")
            return self.pos_increase(order, direction)
        else:
            raise ValueError
    
    def on_order(self, order: Order):
        buy, sell = (order.qty, 0) if order.side == BUY else (0, order.qty)
        
        if self.qty * ((self.buy.get_total() + buy) - (
            self.sell.get_total() + sell)) < 0:
            log.error(
                f"仓位方向反转: qty: {self.qty}, ({self.buy} + {buy}) - ("
                f"{self.sell} + {sell}) ")
            raise RuntimeError
        
        if self._pos_side == LONG:
            res = self.on_order_with_long_pos(order, self.direction)
        elif self._pos_side == SHORT:
            res = self.on_order_with_short_pos(order, self.direction)
        elif self._pos_side == DUAL:
            res = self.on_order_with_dual_pos(order, self.direction)
        else:
            raise ValueError
        if res:
            self._trades.append(res)
        
        self.buy.add(buy), self.sell.add(sell)
        log.info(f"{self.buy}")
        log.info(f"{self.sell}")
        return res


class AllInAllOut(TradeRegistry, scheme=AIAO):
    def __init__(self, pnl_calc, comm_calc, pos_side: str = DUAL):
        super().__init__(pnl_calc, comm_calc, pos_side)
    
    def pos_increase(self, order: Order, direction: str):
        if not self._trd:
            self._avg_entry_price = order.price
            self._trd = Trade(
                self.gen_new_id(),
                order.exchange,
                order.market,
                order.instrument_id,
                direction,
                order.qty,
                0,
                self.comm_calc(order.price, order.qty),
                account=order.account
            )
            self._trd.orders += tuple([order.client_oid])
            return self._trd
        
        self._avg_entry_price = calc_entry_price(
            self._avg_entry_price, abs(self.qty), order.price, order.qty
        )
        self._trd.orders += tuple([order.client_oid])
    
    def pos_decrease(self, order: Order, direction: str) -> Optional[Trade]:
        res = None
        buy, sell = (order.qty, 0) if order.side == BUY else (0, order.qty)
        pnl = self.pnl_calc(order.qty, self._avg_entry_price, order.price)
        if direction is SHORT:
            pnl = -pnl
        self._trd.pnl += pnl
        self._trd.commission += self.comm_calc(order.price, order.qty)
        self._trd.orders += tuple([order.client_oid])
        if self.buy.get_total() + buy - self.sell.get_total() + sell == 0:
            self._trd.status = CLOSE
            res = self._trd
            self._trd = None
        return res


class FirstInLastOut(TradeRegistry, scheme=FILO):
    def __init__(self, pnl_calc, comm_calc, pos_side: str = DUAL):
        super().__init__(pnl_calc, comm_calc, pos_side)
    
    def pos_increase(self, order: Order, direction: str):
        raise NotImplementedError
    
    def pos_decrease(self, order: Order, direction: str):
        raise NotImplementedError


class FirstInFirstOut(TradeRegistry, scheme=FIFO):
    def pos_increase(self, order: Order, direction: str):
        raise NotImplementedError
    
    def pos_decrease(self, order: Order, direction: str):
        raise NotImplementedError


create_registry = SchemeRegistry.create_registry
