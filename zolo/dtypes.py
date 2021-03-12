from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Tuple, Callable, Union, TypeVar, List, Any

from zolo.consts import INVALID
from .consts import UNIX_EPOCH, OPEN, MAX_FLOAT, DEFAULT_LEVERAGE
from .utils import datetime_to_str


class TradeStatus:
    Opened = "Opened"
    Closed = "Closed"


class OrderStatus:
    FULFILLED = "FULFILLED"
    PARTIAL = "PARTIAL"
    CANCELED = "CANCELED"
    CANCELING = "CANCELING"
    ONGOING = "ONGOING"
    FAIL = "FAIL"
    PARTIAL_FILED_OTHER_CANCELED = "PARTIAL_FILED_OTHER_CANCELED"
    PREPARING = "PREPARING"
    UNKNOWN = "UNKNOWN"


class OrderSide:
    OPEN_LONG = "OPEN_LONG"  # 开多
    OPEN_SHORT = "OPEN_SHORT"  # 开空
    CLOSE_LONG = "CLOSE_LONG"  # 平多
    CLOSE_SHORT = "CLOSE_SHORT"  # 平空


class OrderType:
    MARKET = "MARKET"
    LIMIT_GTC = "LIMIT_GTC"
    POST_ONLY = "POST_ONLY"
    LIMIT_IOC = "LIMIT_IOC"
    LIMIT_FOK = "LIMIT_FOK"
    OPPONENT_GTC = "OPPONENT_GTC"
    OPPONENT_IOC = "OPPONENT_IOC"
    OPPONENT_FOK = "OPPONENT_FOK"
    OPTIMAL_5_IOC = "OPTIMAL_5_IOC"
    OPTIMAL_GTC = "OPTIMAL_GTC"
    OPTIMAL_10_IOC = "OPTIMAL_10_IOC"
    OPTIMAL_15_IOC = "OPTIMAL_15_IOC"
    OPTIMAL_20_IOC = "OPTIMAL_20_IOC"
    OPTIMAL_25_IOC = "OPTIMAL_25_IOC"
    OPTIMAL_5_FOK = "OPTIMAL_5_FOK"
    OPTIMAL_10_FOK = "OPTIMAL_10_FOK"
    OPTIMAL_15_FOK = "OPTIMAL_15_FOK"
    OPTIMAL_20_FOK = "OPTIMAL_20_FOK"
    OPTIMAL_25_FOK = "OPTIMAL_25_FOK"
    OPTIMAL_5_GTC = "OPTIMAL_5_GTC"
    OPTIMAL_10_GTC = "OPTIMAL_10_GTC"
    OPTIMAL_15_GTC = "OPTIMAL_15_GTC"
    OPTIMAL_20_GTC = "OPTIMAL_20_GTC"
    OPTIMAL_25_GTC = "OPTIMAL_25_GTC"


@dataclass(frozen=True)
class Timer:
    timestamp: datetime


@dataclass(frozen=True)
class Position:
    exchange: str
    market: str
    instrument_id: str
    size: float
    avg_entry_price: float
    realised_pnl: float
    unrealised_pnl: float
    home_notional: float
    leverage: float
    
    def __repr__(self):
        return f"""
        Position(
            instrument_id: {self.instrument_id}
            size: {self.size}
            avg_entry_price: {self.avg_entry_price:.2f}
            realised_pnl: {self.realised_pnl}
            unrealised_pnl: {self.unrealised_pnl}
            home_notional: {self.home_notional}
            leverage: {self.leverage}
        )
        """
    
    @property
    def market_id(self):
        return parse_market_id(self)


# wallet_balance = deposits - withdraws + realised_pnl (你的钱包仓位清算前的总余额)
# margin_balance = wallet_balance + unrealised_pnl (钱包仓位清算后的总余额)
# available_margin = margin_balance - maint_margin - init_margin (可用于做开仓保证金的余额)


@dataclass(frozen=True)
class Margin:
    exchange: str
    market: str
    instrument_id: str
    wallet_balance: float
    unrealised_pnl: float
    realised_pnl: float
    init_margin: float
    maint_margin: float
    margin_balance: float
    leverage: float
    
    @property
    def market_id(self):
        return parse_market_id(self)
    
    @property
    def available_margin(self):
        return self.margin_balance - self.init_margin - self.maint_margin
    
    def __repr__(self):
        return f"""
        Margin(
            wallet_balance: {self.wallet_balance: .6f}
            unrealised_pnl: {self.unrealised_pnl: .6f}
            realised_pnl: {self.realised_pnl: .6f}
            init_margin: {self.init_margin}
            maint_margin: {self.maint_margin}
            instrument_id: {self.instrument_id}
            margin_balance: {self.margin_balance: .6f}
        )
        """


@dataclass(frozen=True)
class Order:
    exchange: str
    market: str
    side: str
    pos_side: str
    qty: float
    instrument_id: str
    client_oid: str
    order_type: str
    leverage: float
    price: float = 0
    avg_entry_price: float = 0
    fee: float = 0
    fee_asset: str = ""
    pnl: float = 0
    order_id: str = ""
    created_at: datetime = UNIX_EPOCH
    finished_at: datetime = UNIX_EPOCH
    contract_size: float = 0
    state: str = OrderStatus.UNKNOWN
    filled: float = 0
    slippage: float = 0
    errmsg: str = ""
    account: str = ""
    
    @property
    def market_id(self):
        return parse_market_id(self)
    
    @property
    def user_id(self):
        return parse_user_id(self)
    
    @property
    def done(self) -> bool:
        return self.state in (
            OrderStatus.FULFILLED,
            OrderStatus.PARTIAL_FILED_OTHER_CANCELED,
            OrderStatus.CANCELED,
            OrderStatus.FAIL,
        )
    
    def __repr__(self):
        return f"""
        Order(
            exchange: {self.exchange}
            created_at: {datetime_to_str(self.created_at)}
            finished_at: {datetime_to_str(self.finished_at)}
            side: {self.side}
            pos_side: {self.pos_side}
            price: {self.price:.2f}
            qty: {self.qty}
            instrument_id: {self.instrument_id}
            client_oid: {self.client_oid}
            fee: {self.fee:.5f}
            order_type: {self.order_type}
            contract_size: {self.contract_size:.2f}
            state: {self.state}
            filled: {self.filled}
            errmsg: {self.errmsg}
            account: {self.account}
        )
        """


@dataclass(frozen=True)
class Tick:
    exchange: str
    market: str
    instrument_id: str
    timestamp: datetime
    price: float
    
    @property
    def market_id(self):
        return parse_market_id(self)
    
    def __repr__(self):
        return (
            f"Tick(instrument_id:{self.instrument_id}, "
            f"timestamp:{datetime_to_str(self.timestamp)}, price:{self.price})"
        )


@dataclass(frozen=True)
class Bar:
    exchange: str
    market: str
    instrument_id: str
    timestamp: datetime
    open: float
    close: float
    high: float
    low: float
    volume: float
    currency_volume: float
    granularity: int
    
    @property
    def market_id(self):
        return parse_market_id(self)
    
    def __repr__(self):
        return f"""
        Bar(
            exchange: {self.exchange}
            instrument_id: {self.instrument_id}
            timestamp: {datetime_to_str(self.timestamp)}
            open: {self.open: .4f}
            close: {self.close: .4f}
            high: {self.high: .4f}
            low: {self.low: .4f}
            volume: {self.volume}
            currency_volume: {self.currency_volume}
            granularity: {self.granularity}
        )
        """


DepthTuple = Tuple[float, Union[int, float]]


@dataclass(frozen=True)
class OrderBook:
    exchange: str
    market: str
    instrument_id: str
    timestamp: datetime
    asks: List[DepthTuple]
    bids: List[DepthTuple]
    
    @property
    def market_id(self):
        return parse_market_id(self)


@dataclass
class Trade:
    trade_id: str
    exchange: str
    market: str
    instrument_id: str
    pos_side: str
    size: float
    pnl: float
    commission: float
    close_ts: datetime = None
    orders: Tuple[str] = tuple()
    status: str = OPEN
    account: str = ""
    
    @property
    def market_id(self):
        return parse_market_id(self)
    
    @property
    def user_id(self):
        return parse_user_id(self)


@dataclass(frozen=True)
class Basis:
    exchange: str
    market: str
    instrument_id: str
    contract_price: float
    basis: float
    basis_rate: float
    ts: datetime


@dataclass(frozen=True)
class IndexBasis(Basis):
    index_price: float


@dataclass(frozen=True)
class SpotBasis(Basis):
    price: float


@dataclass(frozen=True)
class Fill:
    fill_id: str
    exchange: str
    instrument_id: str
    price: float
    side: str
    pos_side: str
    filled_ts: datetime
    pnl: float
    size: float
    commission: float
    order_id: str
    client_oid: str = ""
    
    @property
    def market_id(self):
        return parse_market_id(self)


@dataclass(frozen=True)
class Message:
    cmd: str
    payload: Any
    
    def __repr__(self):
        return str(self)
    
    def __str__(self):
        return f"Message(cmd={self.cmd}, payload={self.payload})"


@dataclass(frozen=True)
class Credential:
    api_key: str
    secret_key: str
    passphrase: str


@dataclass(frozen=True)
class SinkWrapper:
    filter: Callable
    on_evt: Callable


@dataclass(frozen=True)
class GatewayConfig:
    gateway_scheme: str
    name: str
    
    @property
    def gateway_id(self):
        return dot_concat(self.gateway_scheme, self.name)


@dataclass(frozen=True)
class ChannelConfig:
    gateway: GatewayConfig
    market: str
    instrument_id: str
    credential: Credential
    evt_type: "ExchangeEvt"
    parameters: Dict
    
    @property
    def channel_id(self):
        return dot_concat(
            self.market, self.instrument_id, self.credential.api_key,
            self.evt_type
        )


@dataclass(frozen=True)
class InstrumentInfo:
    instrument_type: str
    instrument_id: str
    underlying: str
    commission: str
    base_currency: str
    quote_currency: str
    settle_currency: str
    contract_value: str
    contract_value_currency: str
    option_type: str
    strike_price: str
    list_time: datetime
    expire_time: datetime
    leverage: str
    tick_size: str
    lot_size: str
    min_size: str
    contract_type: str
    alias: str
    state: bool
    
    @property
    def market_type(self):
        return self.instrument_type


class Lot(int):
    def __add__(self, other):
        return Lot(int(self) + other)
    
    def __sub__(self, other):
        return Lot(int(self) - other)
    
    def __mul__(self, other):
        return Lot(int(self) * other)
    
    def __divmod__(self, other):
        a, b = int.__divmod__(int(self), other)
        return Lot(a), Lot(b)
    
    def __repr__(self):
        return f"Lot({self})"


class Size(float):
    def __add__(self, other):
        return Size(float(self) + other)
    
    def __sub__(self, other):
        return Size(float(self) - other)
    
    def __mul__(self, other):
        return Size(float(self) * other)
    
    def __divmod__(self, other):
        a, b = float.__divmod__(float(self), other)
        return Size(a), Size(b)
    
    def __repr__(self):
        return f"Size({self})"


@dataclass(frozen=True)
class OptionInstrumentInfo(InstrumentInfo):
    available: bool
    
    @property
    def market_type(self):
        return "option"


Qty = TypeVar("Qty", int, float, Lot, Size)

# 公共行情消息
MarketEvt = TypeVar(
    "MarketEvt", Tick, Bar, OrderBook
)
# 交易私有消息
TradeEvt = TypeVar(
    "TradeEvt", Order, Fill, Trade, Position, Margin
)
# 交易所消息
ExchangeEvt = TypeVar(
    "ExchangeEvt", Tick, Bar, OrderBook, Order, Fill, Trade, Position,
    Margin
)

# 消息总类
Evt = TypeVar(
    "Evt", Tick, Bar, OrderBook, Order, Fill, Trade, Position, Margin,
    Timer, Message
)


def dot_concat(*args) -> str:
    return ".".join(str(arg).lower() for arg in args)


def parse_market_id(
    obj: ExchangeEvt
) -> str:
    return dot_concat(obj.exchange, obj.market, obj.instrument_id)


def parse_user_id(obj: TradeEvt) -> str:
    return dot_concat(obj.exchange, obj.market, obj.instrument_id, obj.account)


def by_lot(qty: Qty) -> bool:
    return type(qty) is Lot


def by_size(qty: Qty) -> bool:
    return type(qty) is Size


CURRENCY_EMPTY = "INVALID_CURRENCY"
POSITION_EMPTY = Position("", "", "", 0.0, 0.0, 0.0, 0.0, 0.0, DEFAULT_LEVERAGE)
MARGIN_EMPTY = Margin("", "", "", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                      DEFAULT_LEVERAGE)
TICK_EMPTY = Tick("", "", "", UNIX_EPOCH, 0)
CREDENTIAL_EMPTY = Credential("", "", "")
ORDER_BOOK_EMPTY = OrderBook(
    "", "", "", UNIX_EPOCH, [(0.0, 0.0)], [(0.0, 0.0)]
)
COVER_SIZE = Size(float("inf"))
BAR_EMPTY = Bar("", "", "", UNIX_EPOCH, 0, 0, 0, 0, 0, 0, 0)

INSTRUMENT_INVALID = InstrumentInfo(
    instrument_type=INVALID,
    instrument_id=INVALID,
    underlying=INVALID,
    commission=INVALID,
    base_currency=CURRENCY_EMPTY,
    quote_currency=CURRENCY_EMPTY,
    settle_currency=CURRENCY_EMPTY,
    contract_value=MAX_FLOAT,
    contract_value_currency=CURRENCY_EMPTY,
    option_type=INVALID,
    strike_price=MAX_FLOAT,
    list_time=UNIX_EPOCH,
    expire_time=UNIX_EPOCH,
    leverage=MAX_FLOAT,
    tick_size=MAX_FLOAT,
    lot_size=MAX_FLOAT,
    min_size=MAX_FLOAT,
    contract_type=INVALID,
    alias=INVALID,
    state=False
)

