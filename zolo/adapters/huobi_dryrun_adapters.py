import abc
import math
from pprint import pprint
from ..dtypes import Order, Bar, Trade, Fill, Margin, Position, Tick, \
    Credential, \
    SwapInstrumentInfo, FutureInstrumentInfo, SpotInstrumentInfo
from datetime import datetime, timedelta
import time
from . import Adapter
from ..consts import DRYRUN
from ..engine import vtx
from ..posts import OrderPostType
from huobi_restful.clients import HuobiCoinMarginSwap, HuobiUsdtMarginSwap, \
    HuobiCoinMarginFuture, HuobiSpot
import logging

from ..exceptions import TickGetError
from ..utils import unique_id_with_uuid4

log = logging.getLogger(__name__)


def str_to_datetime(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")


def create_id_by_timestamp():
    return f"{int(time.time() * 10000000)}"


def timestamp_to_utc(ts: int) -> datetime:
    return datetime.utcfromtimestamp(ts / 1000)


class HuobiDryrunAdapter(Adapter, mode=DRYRUN, exchange="huobi"):

    def __init__(self, *args):
        super().__init__(*args)

    @abc.abstractmethod
    def get_instrument_info(self, instrument_id: str):
        pass

    def create_order(
        self,
        post: OrderPostType,
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

    def create_market_order(self, instrument_id, amount, order_type,
                            order_side) -> str:
        pass

    def get_margin(self, instrument_id) -> Margin:
        pass

    def get_position(self, instrument_id) -> Position:
        pass

    def get_tick(self, instrument_id) -> Tick:
        try:
            res = self.client.get_market_trade(instrument_id)
            if res["status"] != "ok":
                raise ValueError(f"get tick failed!")
            data = res["tick"]["data"][0]
            price = data["price"]
            ts = data["ts"]
        except Exception as e:
            log.exception(e)
            raise TickGetError(str(e))

        return Tick(
            exchange=self.exchange,
            market=self.market,
            timestamp=timestamp_to_utc(ts),
            instrument_id=instrument_id,
            price=float(price),
        )

    def get_order_by_client_oid(self, instrument_id, client_order_id) -> Order:
        pass

    def get_contract_value(self, instrument_id):
        pass

    def get_latest_bar(self, instrument_id: str, granularity: int) -> Bar:
        granularity_sym = f"{granularity}min"
        prev = datetime.utcnow() - timedelta(minutes=granularity)
        try:
            res = self.client.get_market_history_kline(
                instrument_id, granularity_sym, size="2", from_ts=prev
            )
        except Exception as e:
            log.exception(e)
            raise e
        res = sorted(res["data"], key=lambda r: r["id"])[0]
        return Bar(
            self.exchange, self.market, instrument_id,
            datetime.utcfromtimestamp(int(res["id"])),
            float(res["open"]), float(res["close"]), float(res["high"]),
            float(res["low"]), volume=int(res["vol"]),
            currency_volume=float(res["amount"]),
            granularity=granularity
        )

    def get_max_order_size(self, instrument_id: str):
        pass

    def get_bars(
            self, instrument_id: str, granularity: int, start: datetime,
            end: datetime
    ):
        pass

    def get_last_n_bars(self, cnt: int, instrument_id: str, granularity: int):
        pass

    def get_fill(
            self, instrument_id: str, before: datetime, after: datetime,
            limit=100
    ):
        pass


class HuobiDryrunCoinMarginSwap(HuobiDryrunAdapter, market="swap@coin"):

    def __init__(self, *args):
        super().__init__(*args)

        self._client = HuobiCoinMarginSwap(
            self.credential.api_key, self.credential.secret_key
        )

    def get_instrument_info(self, instrument_id: str):
        try:
            res = self.client.get_swap_contract_info(instrument_id)
        except Exception as e:
            raise e
        if res["status"] != "ok":
            raise ValueError(f"Get contract info failed!")
        res = res["data"][0]
        base, quote = res["contract_code"].split("-")
        return SwapInstrumentInfo(
            available=bool(res["contract_status"]),
            underlying=f"{base}-{quote}".lower(),
            quote=quote,
            margin_by=base,
            price_tick=res["price_tick"],
            contract_size=res["contract_size"],
            max_size=10000000
        )


class HuobiDryrunUsdtMarginSwap(HuobiDryrunAdapter, market="swap@usdt"):

    def __init__(self, *args):
        super().__init__(*args)
        self._client = HuobiUsdtMarginSwap(
            self.credential.api_key, self.credential.secret_key
        )

    def get_instrument_info(self, instrument_id: str):
        return None


class HuobiDryrunCoinMarginFuture(HuobiDryrunAdapter, market="future@coin"):

    def __init__(self, *args):
        super().__init__(*args)
        self._client = HuobiCoinMarginFuture(
            self.credential.api_key, self.credential.secret_key)

    def get_instrument_info(self, instrument_id: str):
        return None


class HuobiDryrunSpot(HuobiDryrunAdapter, market="spot"):

    def __init__(self, *args):
        super().__init__(*args)
        self._client = HuobiSpot(
            self.credential.api_key, self.credential.secret_key
        )

    def get_instrument_info(self, instrument_id: str):
        return None
