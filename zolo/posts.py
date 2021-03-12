import abc
from dataclasses import dataclass
from typing import Union, TypeVar

from .dtypes import Size, Lot, OrderType, Qty

__all__ = [
    "OrderPost",
    "MarketOrder",
    "PostOnlyOrder",
    "OptimalOrder",
    "OpponentOrder",
    "LimitOrder",
    "OpponentFokOrder",
    "OpponentIocOrder",
    "OptimalFokOrder",
    "OptimalIocOrder",
    "LimitIocOrder",
    "LimitFokOrder",
    "is_blocking",
    "OrderPostType"
]


class OrderPost(abc.ABC):

    def __post_init__(self):
        market = getattr(self, "market", None)
        qty_type = type(getattr(self, "qty"))
        if market in ("swap@coin", "swap@usdt", "future@coin", "future@usdt"):
            if qty_type not in (Size, Lot):
                raise ValueError(f"Contract market only support Size and Lot")
        elif market is "spot":
            if qty_type not in (int, float):
                raise ValueError(f"Spot market only support int and flot")


@dataclass(frozen=True)
class MarketOrder(OrderPost):
    exchange: str
    market: str
    instrument_id: str
    side: str
    pos_side: str
    qty: Qty
    slippage: float = 0
    timeout: float = 12

    @property
    def order_type(self) -> str:
        return OrderType.MARKET


@dataclass(frozen=True)
class OpponentOrder(OrderPost):
    exchange: str
    market: str
    instrument_id: str
    side: str
    pos_side: str
    qty: Qty
    slippage: float = 0
    timeout: float = 0

    @property
    def order_type(self) -> str:
        return OrderType.OPPONENT_GTC


@dataclass(frozen=True)
class OpponentIocOrder(OrderPost):
    exchange: str
    market: str
    instrument_id: str
    side: str
    pos_side: str
    qty: Qty
    slippage: float = 0
    timeout: float = 12

    @property
    def order_type(self) -> str:
        return OrderType.OPPONENT_IOC


@dataclass(frozen=True)
class OpponentFokOrder(OrderPost):
    exchange: str
    market: str
    instrument_id: str
    side: str
    pos_side: str
    qty: Qty
    slippage: float = 0
    timeout: float = 12

    @property
    def order_type(self) -> str:
        return OrderType.OPPONENT_FOK


@dataclass(frozen=True)
class OptimalOrder(OrderPost):
    exchange: str
    market: str
    instrument_id: str
    side: str
    pos_side: str
    depth: int
    qty: Qty
    slippage: float = 0
    timeout: float = 0

    @property
    def order_type(self) -> str:
        return f"OPTIMAL_{self.depth}_GTC"


@dataclass(frozen=True)
class OptimalIocOrder(OrderPost):
    exchange: str
    market: str
    instrument_id: str
    side: str
    pos_side: str
    depth: int
    qty: Qty
    slippage: float = 0
    timeout: float = 12

    @property
    def order_type(self) -> str:
        return f"OPTIMAL_{self.depth}_IOC"


@dataclass(frozen=True)
class OptimalFokOrder(OrderPost):
    exchange: str
    market: str
    instrument_id: str
    side: str
    pos_side: str
    depth: int
    qty: Qty
    slippage: float = 0
    timeout: float = 12

    @property
    def order_type(self) -> str:
        return f"OPTIMAL_{self.depth}_FOK"


@dataclass(frozen=True)
class PostOnlyOrder(OrderPost):
    exchange: str
    market: str
    instrument_id: str
    side: str
    pos_side: str
    price: float
    qty: Qty
    timeout: float = 0

    @property
    def order_type(self) -> str:
        return OrderType.POST_ONLY


@dataclass(frozen=True)
class LimitOrder(OrderPost):
    exchange: str
    market: str
    instrument_id: str
    side: str
    pos_side: str
    price: float
    qty: Qty
    timeout: float = 0

    @property
    def order_type(self) -> str:
        return OrderType.LIMIT_GTC


@dataclass(frozen=True)
class LimitFokOrder(OrderPost):
    exchange: str
    market: str
    instrument_id: str
    side: str
    pos_side: str
    price: float
    qty: Qty
    timeout: float = 12

    @property
    def order_type(self) -> str:
        return OrderType.LIMIT_FOK


@dataclass(frozen=True)
class LimitIocOrder(OrderPost):
    exchange: str
    market: str
    instrument_id: str
    side: str
    pos_side: str
    price: float
    qty: Qty
    timeout: float = 12

    @property
    def order_type(self) -> str:
        return OrderType.LIMIT_IOC


def is_blocking(post: OrderPost):
    if type(post) in (LimitOrder, OpponentOrder, OptimalOrder):
        return True
    return False


OrderPostType = TypeVar(
    "OrderPostType", MarketOrder, OpponentOrder,
    OpponentIocOrder, OpponentFokOrder, LimitOrder,
    LimitIocOrder, LimitFokOrder, OptimalOrder,
    OptimalIocOrder, OptimalFokOrder
)
