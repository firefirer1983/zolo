from enum import Enum


class OKexSwapOrderType(Enum):
    """
    0：普通委托（order type不填或填0都是普通委托）
    1：只做Maker（Post only）
    2：全部成交或立即取消（FOK）
    3：立即成交并取消剩余（IOC）
    4: 市价
    """
    NORMAL = "0"
    POSTONLY = "1"
    FOK = "2"
    IOC = "3"
    MARKET = "4"


class OKexSwapOrderStatus(Enum):
    FAILED = "-2"
    CANCELED = "-1"
    WAITING = "0"
    PARTIAL = "1"
    FULFILLED = "2"
    ONGOING = "3"
    CANCELING = "4"


class OKexSwapOrderSide(Enum):
    OPEN_LONG = "1"  # 开多
    OPEN_SHORT = "2"  # 开空
    CLOSE_LONG = "3"  # 平多
    CLOSE_SHORT = "4"  # 平空


class HuobiSwapOrderType(Enum):
    OPPONENT_IOC = "opponent_ioc"
    OPTIMAL_5_IOC = "optimal_5_ioc"
    OPTIMAL_10_IOC = "optimal_10_ioc"
    OPTIMAL_20_IOC = "optimal_20_ioc"


class HuobiSwapOrderStatus(Enum):
    CANCELED = "7"  # 已撤单
    WAITING = "1"  # 准备提交
    PREPARING = "2"  # 准备提交
    PARTIAL = "4"  # 部分成交
    FULFILLED = "6"  # 全部成交
    ONGOING = "3"  # 已提交
    CANCELING = "11"  # 撤单中
    PARTIAL_FILED_OTHER_CANCELED = "5"  # 部分成交已撤单


class HuobiSwapOrderOffset(Enum):
    OPEN = "open"
    CLOSE = "close"


class HuobiSwapOrderDirection(Enum):
    BUY = "buy"
    SELL = "sell"


class HuobiSwapOrderSide(Enum):
    OPEN_LONG = (
        HuobiSwapOrderOffset.OPEN.value, HuobiSwapOrderDirection.BUY.value
    )  # 开多
    OPEN_SHORT = (
        HuobiSwapOrderOffset.OPEN.value, HuobiSwapOrderDirection.SELL.value
    )  # 开空
    CLOSE_LONG = (
        HuobiSwapOrderOffset.CLOSE.value, HuobiSwapOrderDirection.SELL.value
    )  # 平多
    CLOSE_SHORT = (
        HuobiSwapOrderOffset.CLOSE.value, HuobiSwapOrderDirection.BUY.value
    )  # 平空
