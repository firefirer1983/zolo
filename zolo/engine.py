import abc
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Iterable, Callable, Optional
import logging
from dataclasses import replace
from .utils import calc_entry_price
from .dtypes import (
    InstrumentInfo,
    Margin,
    TICK_EMPTY,
    dot_concat,
    Tick,
    OrderBook,
    Order,
    Position,
    OrderStatus,
    POSITION_EMPTY,
    MARGIN_EMPTY,
    ORDER_BOOK_EMPTY,
    OrderType,
    Fill)
from .consts import BUY, SELL

log = logging.getLogger(__name__)

MAX_BACKTEST_VOL = 1000000000


class MatchEngine:
    entries = dict()
    books: Dict[str, OrderBook] = defaultdict(lambda: ORDER_BOOK_EMPTY)

    @classmethod
    def register_entry(cls, order_type: str, entry: "MathEntry"):
        cls.entries[order_type] = entry

    @classmethod
    def on_book(cls, book: OrderBook):
        cls.books[book.market_id] = book

    def match(self, order):
        try:
            return self.entries[order.order_type](
                order, self.books[order.market_id]
            )
        except KeyError:
            raise NotImplementedError

    def get_book(self, instrument_id: str, depth: int):
        book = self.books[instrument_id]
        return book


class MathEntry(abc.ABC):

    def __init_subclass__(cls, order_type: str = "", **kwargs):
        assert order_type
        MatchEngine.register_entry(order_type, cls())

    def __call__(self, order: Order, book: OrderBook) -> Optional[Order]:
        raise NotImplementedError


class MarketMatchEntry(MathEntry, order_type=OrderType.MARKET):
    def __call__(self, order: Order, book: OrderBook):
        if order.side is BUY:
            for price, volume in book.asks:
                if order.qty < volume:
                    return replace(
                        order,
                        price=price,
                        filled=order.qty,
                        timestamp=book.timestamp,
                        state=OrderStatus.FULFILLED,
                    )
        else:
            for price, volume in book.bids:
                if order.qty < volume:
                    return replace(
                        order,
                        price=price,
                        filled=order.qty,
                        timestamp=book.timestamp,
                        state=OrderStatus.FULFILLED,
                    )


class OptimalIocMatchEntry(MathEntry, order_type="OPTIMAL_IOC"):
    def __init__(self, depth: int = 0):
        self._depth = depth
        super().__init__()

    @property
    def depth(self) -> int:
        return self._depth

    def __call__(self, order: Order, book: OrderBook):
        if order.side is BUY:
            for depth, (price, volume) in enumerate(book.asks, start=1):
                if depth > self.depth:
                    break
                if order.qty < volume:
                    return replace(
                        order,
                        price=price,
                        filled=order.qty,
                        timestamp=book.timestamp,
                        state=OrderStatus.FULFILLED,
                    )
        else:
            for depth, (price, volume) in enumerate(book.bids, start=1):
                if depth > self.depth:
                    break
                if order.qty < volume:
                    return replace(
                        order,
                        price=price,
                        filled=order.qty,
                        timestamp=book.timestamp,
                        state=OrderStatus.FULFILLED,
                    )


class Optimal5IocMatchEntry(
    OptimalIocMatchEntry,
    order_type=OrderType.OPTIMAL_5_IOC
):
    def __init__(self):
        super().__init__(depth=5)


class AccountCenter:
    def __init__(self):
        self._deposits: Dict[str, float] = defaultdict(lambda: 0.0)
        self._positions: Dict[str, Position] = defaultdict(
            lambda: POSITION_EMPTY)
        self._margins: Dict[str, Margin] = defaultdict(lambda: MARGIN_EMPTY)
        self._buy_total: Dict[str, float] = defaultdict(lambda: 0.0)
        self._sell_total: Dict[str, float] = defaultdict(lambda: 0.0)

    def deposit(
            self,
            exchange: str,
            market: str,
            instrument_id: str,
            api_key: str,
            amount: float,
    ):
        _id = dot_concat(exchange, market, instrument_id, api_key)
        self._deposits[_id] += amount

    def withdraw(
            self,
            exchange: str,
            market: str,
            instrument_id: str,
            api_key: str,
            amount: float,
    ):
        _id = dot_concat(exchange, market, instrument_id, api_key)
        prev = self._margins[_id]
        if prev.margin_balance - amount < 0:
            raise ValueError
        self._deposits[_id] -= amount

    def get_margin(
            self, exchange: str, market: str, instrument_id: str, api_key: str
    ) -> Margin:
        _id = dot_concat(exchange, market, instrument_id, api_key)
        return self._margins[_id]

    def get_position(
            self, exchange: str, market: str, instrument_id: str, api_key: str
    ) -> Position:
        _id = dot_concat(exchange, market, instrument_id, api_key)
        return self._positions[_id]

    def do_accounting(
            self, order: Order, calc_pnl: Callable, calc_comm: Callable
    ) -> Order:
        uid = order.user_id
        pos: Position = self._positions[uid]
        commission = calc_comm(order.qty, order.price)

        res = self._buy_total[uid] - self._sell_total[uid]
        if (
                res == 0
                or (res > 0 and order.side == BUY)
                or (res < 0 and order.side == SELL)
        ):
            avg_entry_price = calc_entry_price(
                pos.avg_entry_price, pos.size, order.price, order.qty
            )
            pnl = 0
        else:
            avg_entry_price = pos.avg_entry_price
            pnl = calc_pnl(order.qty, order.price, avg_entry_price)

        if order.side == BUY:
            self._buy_total[uid] += order.qty
        else:
            self._sell_total[uid] += order.qty

        realised_pnl = pos.realised_pnl + pnl - commission

        size = self._buy_total[uid] - self._sell_total[uid]

        if size == 0:
            #  平仓,将所有的released_pnl清算到 deposit 里面.
            self._deposits[uid] += realised_pnl
            self._positions[uid] = POSITION_EMPTY
            wallet_balance = self._deposits[uid]
        else:
            self._positions[uid] = Position(
                order.exchange,
                order.market,
                order.instrument_id,
                size,
                avg_entry_price,
                realised_pnl,
                0,
                0,
            )
            wallet_balance = self._deposits[uid] + realised_pnl

        self._margins[uid] = replace(self._margins[uid],
                                     wallet_balance=wallet_balance)
        return replace(order, pnl=pnl, fee=commission, filled=order.qty)


class VirtualExchange:
    def __init__(self):
        self._ticks: Dict[str, Tick] = defaultdict(lambda: TICK_EMPTY)
        self._books = defaultdict(lambda: ORDER_BOOK_EMPTY)
        self._orders: Dict[str, Dict[str, Order]] = defaultdict(lambda: {})
        self._fills: List[Fill] = list()
        self._pending: Dict[str, Dict[str, Order]] = defaultdict(lambda: {})
        self._notify: List[Order] = list()
        self._instrument_registry = dict()
        self.accounting_center = AccountCenter()
        self.engine = MatchEngine()

    def install_instrument(
            self,
            exchange: str,
            market: str,
            instrument_id: str,
            instrument_info: InstrumentInfo,
    ):
        self._instrument_registry[
            dot_concat(exchange, market, instrument_id)
        ] = instrument_info

    def get_current_timestamp(
            self, exchange: str, market: str, instrument_id: str
    ) -> datetime:
        return self._ticks[
            dot_concat(exchange, market, instrument_id)].timestamp

    def get_current_tick(self, exchange: str, market: str,
                         instrument_id: str) -> Tick:
        return self._ticks[dot_concat(exchange, market, instrument_id)]

    def add_to_match(self, order: Order):
        _id = dot_concat(order.exchange, order.market, order.instrument_id)
        self._pending[_id][order.client_oid] = order
        self._orders[_id][order.client_oid] = order

    def on_tick(self, tick: Tick):
        _id = tick.market_id
        self._ticks[_id] = tick
        self.engine.on_book(
            OrderBook(
                exchange=tick.exchange,
                market=tick.market,
                instrument_id=tick.instrument_id,
                timestamp=tick.timestamp,
                asks=[(float(tick.price), MAX_BACKTEST_VOL)],
                bids=[(float(tick.price), MAX_BACKTEST_VOL)],
            )
        )
        self.match()

    def get_order(self) -> Iterable[Order]:
        while self._notify:
            yield self._notify.pop(0)

    def get_fill(self) -> Iterable[Fill]:
        while self._fills:
            yield self._fills.pop(0)

    def match(self):
        self._pending = self._match()

    def _match(self):
        pending: Dict[str, Dict[str, Order]] = defaultdict(lambda: dict())
        for _id, orders in self._pending.items():
            for order in orders.values():
                res = self.engine.match(order)
                if res:
                    instrument = self._instrument_registry[order.market_id]
                    calc_pnl, calc_comm = (
                        instrument.pnl_scheme,
                        instrument.comm_scheme,
                    )
                    res = self.accounting_center.do_accounting(res, calc_pnl,
                                                               calc_comm)
                    # log.info(f"完成撮合: {res}")
                    self._notify.append(res)
                    self._orders[_id][order.client_oid] = res
                else:
                    pending[_id][order.client_oid] = order

        return pending

    def update_unrealised_pnl(self, pos: Position) -> float:
        if pos == POSITION_EMPTY:
            return 0
        calc_pnl = self._instrument_registry[pos.market_id].pnl_scheme
        unrealised_pnl = calc_pnl(
            pos.size, float(self._ticks[pos.market_id].price),
            pos.avg_entry_price
        )
        return unrealised_pnl

    def get_position(
            self, exchange: str, market: str, instrument_id: str, api_key: str
    ):
        pos = self.accounting_center.get_position(
            exchange, market, instrument_id, api_key
        )
        unrealised_pnl = self.update_unrealised_pnl(pos)
        return replace(pos, unrealised_pnl=unrealised_pnl)

    def get_margin(self, exchange: str, market: str, instrument_id: str,
                   api_key: str):
        pos = self.accounting_center.get_position(
            exchange, market, instrument_id, api_key
        )
        mrg = self.accounting_center.get_margin(
            exchange, market, instrument_id, api_key
        )

        unrealised_pnl = self.update_unrealised_pnl(pos)
        return replace(
            mrg,
            margin_balance=mrg.wallet_balance + unrealised_pnl,
            unrealised_pnl=unrealised_pnl,
        )

    def get_order_by_client_oid(
            self, exchange: str, market: str, instrument_id: str,
            client_oid: str
    ):
        _id = dot_concat(exchange, market, instrument_id)
        return self._orders[_id].get(client_oid)

    def deposit(
            self,
            exchange: str,
            market: str,
            instrument_id: str,
            api_key: str,
            amount: float,
    ):
        return self.accounting_center.deposit(
            exchange, market, instrument_id, api_key, amount
        )

    def withdraw(
            self,
            exchange: str,
            market: str,
            instrument_id: str,
            api_key: str,
            amount: float,
    ):
        return self.accounting_center.withdraw(
            exchange, market, instrument_id, api_key, amount
        )

    # for backtest to exit datafeed loop only!
    def poll(self):
        if self._notify:
            raise InterruptedError


vtx = VirtualExchange()
