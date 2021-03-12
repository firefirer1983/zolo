import logging
from collections import OrderedDict, defaultdict
from decimal import Decimal
from itertools import count


from ..dtypes import (
    OrderType,
    Order,
    OrderStatus,
    Margin,
    Position,
    Trade,
    clone_dataclass_object,
)

log = logging.getLogger(__file__)


# 只有加仓,才需要重新计算 avg_entry_price, 减仓不会影响平均入仓价
def calc_entry_price(origin_price, origin_qty, price, amount) -> Decimal:
    # origin_qty, amount = Decimal(abs(origin_qty)), Decimal(abs(amount))
    # origin_price, price = Decimal(origin_price), Decimal(price)
    origin_qty, amount = abs(origin_qty), abs(amount)
    res = (origin_price * origin_qty + price * amount) / (origin_qty + amount)
    return res


def calc_pnl(direction, amount, avg_entry_price, close_price):
    if direction is None:
        raise ValueError(f"Invalid direction!")
    amount = -abs(amount) if direction == "SHORT" else abs(amount)
    # amount, close_price = Decimal(amount), Decimal(close_price)
    res = Decimal(amount * (1 / avg_entry_price - 1 / close_price))
    return res


def calc_commission(rate, price, size):
    # rate, price, size = Decimal(rate), Decimal(price), Decimal(size)
    res = rate * size / price
    return res


class TradesStack:
    trd_id = count(1, 1)

    def __init__(self, exchange: str, instrument_id: str, init_dir: str = None):
        self._dir = init_dir
        self._open_orders, self._close_orders = list(), list()
        self._avg_entry_price = Decimal(0)
        self._trades_history = []
        self._trd: Trade = None
        self._exchange = exchange
        self._instrument_id = instrument_id
    
    @property
    def direction(self):
        if self._trd:
            return self._trd.direction
        return None
    
    def fill(self, order: Order):
        if not isinstance(order.price, Decimal):
            raise ValueError(f"order price must be decimal")
        offset, direction = order.side.split("_")

        if offset == "OPEN" and not self._trd:
            self._trd = Trade(
                trade_id=next(self.trd_id),
                exchange=self._exchange,
                instrument_id=self._instrument_id,
                status="OPEN",
                direction=direction,
            )
        
        if offset == "OPEN":
            self._avg_entry_price = calc_entry_price(
                self.avg_entry_price, self.qty, order.price, order.qty
            )
            self._trd.size += Decimal(order.qty)
            self._open_orders.append(order)
        else:
            self._close_orders.append(order)
            if self.qty < 0:
                raise RuntimeError(f"No enough position to close!")
            
            self._trd.pnl += calc_pnl(
                self.direction, order.qty, self.avg_entry_price, order.price
            )
        self._trd.commission += Decimal(order.fee)
        
        if self.qty == 0 and self._trd and self._trd.status == "OPEN":
            self._open_orders.clear()
            self._close_orders.clear()
            self._trd.status = "CLOSE"
            self._trd.close_ts = order.timestamp
            self._trades_history.append(self._trd)
            self._trd = None
            return self._trades_history[-1]
        return None
    
    @property
    def qty(self):
        return sum(order.size for order in self._open_orders) - sum(
            order.size for order in self._close_orders
        )
    
    @property
    def avg_entry_price(self):
        if not isinstance(self._avg_entry_price, Decimal):
            raise ValueError(
                f"avg_entry_price:{self.avg_entry_price} " f"is not decimal!"
            )
        return self._avg_entry_price
    
    @property
    def instrument_id(self):
        return self._instrument_id
    
    def calc_unrealised_pnl(self, price):
        if self.qty != 0:
            return calc_pnl(self.direction, self.qty, self.avg_entry_price,
                            price)
        return Decimal(0)
    
    def get_trade_history(self):
        return self._trades_history
    
    @property
    def net_pnl(self):
        net = self.gross_pnl - self.total_commission
        if not isinstance(net, Decimal):
            raise ValueError(f"{net} is not decimal")
        return net
    
    @property
    def gross_pnl(self):
        pnl = sum(trd.pnl for trd in self._trades_history)
        if self._trd:
            pnl += self._trd.pnl
        if not pnl:
            return Decimal(0)
        if not isinstance(pnl, Decimal):
            raise ValueError(f"pnl:{pnl} is not decimal")
        return pnl
    
    @property
    def total_commission(self):
        commission = sum(trd.commission for trd in self._trades_history)
        if self._trd:
            commission += self._trd.commission
        return commission


class BitmexBroker:
    def __init__(self, ticker):
        self._orders_history = defaultdict(lambda: OrderedDict())
        self._ticker = ticker
        self._trades_stacks = dict()
        self._deposit_histories = defaultdict(lambda: list())
        self._withdraw_histories = defaultdict(lambda: list())
    
    def get_contract_value(self, instrument_id: str):
        return {
            "XBTUSD": 1
        }[instrument_id]
    
    def get_stack(self, instrument_id: str) -> TradesStack:
        return self._trades_stacks.setdefault(
            instrument_id, TradesStack("bitmex", instrument_id)
        )
    
    def withdraw(self, instrument_id: str, amount):
        self._withdraw_histories[instrument_id].append(Decimal(amount))
    
    def deposit(self, instrument_id: str, amount):
        self._deposit_histories[instrument_id].append(Decimal(amount))
    
    def get_total_deposit(self, instrument_id: str):
        if self._deposit_histories:
            return sum(self._deposit_histories[instrument_id])
        return Decimal(0)
    
    def get_total_withdraw(self, instrument_id: str):
        if self._withdraw_histories:
            return sum(self._withdraw_histories[instrument_id])
        return Decimal(0)
    
    @property
    def fee_rate(self):
        return Decimal(0.00075)

    def get_trade_history(self, instrument_id: str):
        return self.get_stack(instrument_id).get_trade_history()

    def get_order_history(self, instrument_id, client_oid):
        return self._orders_history[instrument_id].get(client_oid)
    
    def create_order(
        self, instrument_id: str, client_oid: str, side: str, order_type: str,
        size: int
    ):
        if instrument_id != "XBTUSD":
            raise NotImplementedError(f"暂时只支持XBTUSD")
        
        size = Decimal(abs(size))
        
        order = Order(
            exchange="bitmex",
            timestamp=self._ticker.current_ts,
            side=side,
            price=self._ticker.current_price,
            size=size,
            instrument_id=instrument_id,
            client_oid=client_oid,
            order_id=client_oid,
            fee=calc_commission(
                self.fee_rate, self._ticker.current_price, size
            ),  # 以btc作为手续费
            order_type=order_type,
            contract_size=0,
            state=OrderStatus.PREPARING,
            filled=0,
            errmsg="",
            account="",
        )
        self.get_stack(instrument_id).fill(order)
        self._orders_history[instrument_id][order.order_id] = clone_dataclass_object(
            order, state=OrderStatus.FULFILLED, filled=order.qty
        )
        return order
    
    def get_position(self, instrument_id: str):
        pos = self.get_stack(instrument_id)
        if not pos.qty:
            return None
        return Position(
            instrument_id=instrument_id,
            size=pos.qty if pos.direction == "LONG" else -pos.qty,
            avg_entry_price=pos.avg_entry_price,
            realised_pnl=pos.net_pnl,
            unrealised_pnl=pos.calc_unrealised_pnl(self._ticker.current_price),
            home_notional=abs(pos.qty) * self.get_contract_value(instrument_id) / self._ticker.current_price
        )
    
    def get_margin(self, instrument_id):
        pos = self.get_stack(instrument_id)
        wallet_balance = self.get_wallet_balance(instrument_id)
        margin_balance = self.get_margin_balance(instrument_id)
        return Margin(
            instrument_id=instrument_id,
            unrealised_pnl=wallet_balance - margin_balance,
            realised_pnl=pos.net_pnl,
            wallet_balance=wallet_balance,
            margin_balance=margin_balance,
        )
    
    def get_wallet_balance(self, instrument_id: str) -> Decimal:
        wallet_balance = (
            self.get_total_deposit(instrument_id)
            - self.get_total_withdraw(instrument_id)
            + self.get_stack(instrument_id).net_pnl
        )
        if not isinstance(wallet_balance, Decimal):
            raise ValueError(f"wallet balance is not decimal!")
        return Decimal(wallet_balance)
    
    def get_margin_balance(self, instrument_id: str) -> Decimal:
        
        return self.get_wallet_balance(
            instrument_id) + self.get_stack(
            instrument_id
        ).calc_unrealised_pnl(self._ticker.current_price)
