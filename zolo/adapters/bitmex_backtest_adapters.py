import abc
from functools import partial
from typing import Union, Tuple
from ..utils import unique_id_with_uuid4
from ..posts import MarketOrder, OpponentOrder, OpponentIocOrder, OpponentFokOrder, OptimalOrder, OptimalIocOrder, \
    OptimalFokOrder, PostOnlyOrder, LimitOrder, LimitFokOrder, LimitIocOrder
from ..engine import vtx
from ..dtypes import (
    Order,
    Bar,
    Margin,
    Position,
    Tick,
    Credential,
    OrderStatus,
    OrderType,
    Lot, OrderBook, INSTRUMENT_INVALID, InstrumentInfo)
from zolo.consts import INVALID
from ..consts import FOK, IOC, BACKTEST, INVALID
from datetime import datetime
from ..exceptions import OrderGetError
from . import Adapter
from ..utils import calc_pnl, calc_comm, calc_comm_by_reverse, calc_pnl_by_reverse
import logging


log = logging.getLogger(__name__)


def str_to_datetime(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")


def timestamp_to_utc(ts: int) -> datetime:
    return datetime.utcfromtimestamp(ts / 1000)


_SWAP_INSTRUMENT_REGISTRY = {
    # "XBTUSD": InstrumentInfo(
    #     True,
    #     "btc-usd",
    #     "usd",
    #     "btc",
    #     100,
    #     1,
    #     100000,
    #     1,
    #     partial(calc_pnl_by_reverse, contract_size=1),
    #     partial(calc_comm_by_reverse, rate=0.00035, contract_size=1),
    # )
}
_FUTURE_INSTRUMENT_REGISTRY = {}
_SPOT_INSTRUMENT_REGISTRY = {}


class BitmexBacktestAdapter(Adapter, mode=BACKTEST, exchange="bitmex"):
    def __init__(self, *args):
        super().__init__(*args)

    @abc.abstractmethod
    def get_instrument_info(
        self, instrument_id: str
    ) -> InstrumentInfo:
        raise NotImplementedError

    def estimate_lot(self, instrument_id: str, size: float, price: float = 0) -> Lot:
        if not price:
            price = float(self.get_tick(instrument_id).price)

        return Lot(size * price / self.get_instrument_info(instrument_id).contract_size)

    def transfer_asset(self, market: str, instrument_id: str, size: float):
        raise RuntimeError

    def create_order(
        self,
        post: Union[
            MarketOrder,
            PostOnlyOrder,
            LimitOrder,
            LimitIocOrder,
            LimitFokOrder,
            OpponentIocOrder,
            OpponentFokOrder,
            OpponentOrder,
            OptimalOrder,
            OptimalIocOrder,
            OptimalFokOrder,
        ],
    ) -> str:
        assert self.exchange == post.exchange and self.market == post.market
        ts = vtx.get_current_timestamp(self.exchange, self.market, post.instrument_id)
        client_oid = unique_id_with_uuid4()
        price = getattr(post, "price", 0)
        slippage = getattr(post, "slippage", 0)
        contract_size = getattr(
            self.get_instrument_info(post.instrument_id), "contract_size", 0
        )
        order = Order(
            self.exchange,
            self.market,
            post.side,
            post.pos_side,
            price,
            float(post.qty),
            post.instrument_id,
            client_oid,
            post.order_type,
            0,
            0,
            client_oid,
            ts,
            contract_size,
            slippage=slippage,
            account=self.credential.api_key,
        )
        vtx.add_to_match(order)
        vtx.match()
        return client_oid

    def get_margin(self, instrument_id) -> Margin:
        return vtx.get_margin(
            self.exchange, self.market, instrument_id, self.credential.api_key
        )

    def get_position(self, instrument_id) -> Position:
        return vtx.get_position(
            self.exchange, self.market, instrument_id, self.credential.api_key
        )

    def get_tick(self, instrument_id) -> Tick:
        return vtx.get_current_tick(self.exchange, self.market, instrument_id)

    def get_latest_bar(self, instrument_id: str, granularity: int) -> Bar:
        raise RuntimeError

    def get_bars(
        self, instrument_id: str, granularity: int, start: datetime, end: datetime
    ):
        raise RuntimeError

    def get_last_n_bars(self, cnt: int, instrument_id: str, granularity: int):
        raise RuntimeError

    def get_fill(
        self, instrument_id: str, before: datetime, after: datetime, limit=100
    ):
        raise RuntimeError

    def get_order_by_client_oid(self, instrument_id, client_order_id) -> Order:
        res = vtx.get_order_by_client_oid(
            self.exchange, self.market, instrument_id, client_order_id
        )
        if res:
            return res
        raise OrderGetError

    def deposit(self, instrument_id: str, amount: float):
        return vtx.deposit(
            self.exchange, self.market, instrument_id, self.credential.api_key,
            amount
        )

    def get_book(self, instrument_id: str, depth: int) -> OrderBook:
        return vtx.get_book(instrument_id, depth)


class BitmexBacktestCoinMarginSwap(BitmexBacktestAdapter, market="swap@coin"):
    def __init__(self, *args):
        super().__init__(*args)
        for instrument_id, instrument in _SWAP_INSTRUMENT_REGISTRY.items():
            vtx.install_instrument(
                self.exchange, self.market, instrument_id, instrument
            )

    def get_instrument_info(self, instrument_id: str) -> InstrumentInfo:
        if instrument_id == INVALID:
            return INSTRUMENT_INVALID
        return _SWAP_INSTRUMENT_REGISTRY[instrument_id]


class BitmexBacktestCoinMarginFuture(BitmexBacktestAdapter, market="future@coin"):
    def __init__(self, *args):
        super().__init__(*args)
        for instrument_id, instrument in _FUTURE_INSTRUMENT_REGISTRY.items():
            vtx.install_instrument(
                self.exchange, self.market, instrument_id, instrument
            )

    def get_instrument_info(self, instrument_id: str) -> InstrumentInfo:
        if instrument_id == INVALID:
            return INSTRUMENT_INVALID
        return _FUTURE_INSTRUMENT_REGISTRY[instrument_id]
