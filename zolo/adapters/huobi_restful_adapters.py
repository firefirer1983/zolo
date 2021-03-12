import abc
import math
from decimal import Decimal
from functools import partial
from pprint import pprint
from typing import Dict, Tuple, Union, List

from zolo.dtypes import Lot, CREDENTIAL_EMPTY, POSITION_EMPTY, MARGIN_EMPTY
from zolo.posts import OrderPostType, MarketOrder, LimitOrder, LimitIocOrder, \
    LimitFokOrder, OpponentIocOrder, OptimalFokOrder, OptimalIocOrder, \
    OptimalOrder, OpponentOrder, OpponentFokOrder
from zolo.utils import round_down
from ..dtypes import Order, Bar, Trade, Fill, Margin, Position, Tick, \
    Credential, InstrumentInfo, OrderType, OrderStatus, OrderBook
from zolo.consts import INVALID, MAX_INT
from datetime import datetime, timedelta
import time
from . import Adapter
from ..consts import RESTFUL, MAX_FLOAT, UNIX_EPOCH, BUY, SELL, LONG, SHORT, \
    OPEN, CLOSE, DEFAULT_LEVERAGE
from huobi_restful.clients import HuobiCoinMarginSwap, HuobiUsdtMarginSwap, \
    HuobiCoinMarginFuture, HuobiSpot
import logging

from ..exceptions import TickGetError, OrderBookGetError, OrderGetError, \
    PositionGetError, MarginGetError, BalanceGetError, OrderPostError, \
    AssetTransferError

log = logging.getLogger(__name__)


# 1. 火币的 equity 就是 margin balance
# 2. margin frozen就是在下单期间被冻结的保证金,也就是 init margin
# 3. available margin = margin balance - frozen margin - position margin
# 4. margin balance = deposit + realised pnl + unrealised pnl
# 5. wallet balance = margin balance - unrealised pnl = deposit + realised pnl


def str_to_datetime(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")


def create_id_by_timestamp():
    return f"{int(time.time() * 10000000)}"


def timestamp_to_utc(ts: Union[str, int]) -> datetime:
    if isinstance(ts, str):
        ts = int(ts)
    return datetime.utcfromtimestamp(ts / 1000)


MAX_OPTIMAL_DEPTH = 150


class HuobiRestfulAdapter(Adapter, mode=RESTFUL, exchange="huobi"):
    
    def __init__(self, *args):
        super().__init__(*args)
    
    def get_position(self, instrument_id) -> Position:
        raise NotImplementedError
    
    def get_margin(self, instrument_id) -> Margin:
        raise NotImplementedError
    
    def get_available_balance(self, symbol: str) -> float:
        raise NotImplementedError
    
    def get_order_by_client_oid(self, instrument_id, client_order_id) -> Order:
        pass
    
    def get_contract_value(self, instrument_id):
        pass
    
    def get_latest_bar(self, instrument_id: str, granularity: int) -> Bar:
        granularity_sym = f"{granularity}min"
        prev = datetime.utcnow() - timedelta(minutes=granularity)
        try:
            res = self._client.get_market_history_kline(
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
        self, instrument_id: str, before: datetime, after: datetime, limit=100
    ):
        pass


class HuobiRestfulCoinMarginSwap(HuobiRestfulAdapter, market="swap@coin"):
    
    def __init__(self, *args):
        super().__init__(*args)
        
        self._client = HuobiCoinMarginSwap(
            self.credential.api_key, self.credential.secret_key
        )

    @property
    def max_optimal_depth(self) -> int:
        return MAX_OPTIMAL_DEPTH
   
    def get_instrument_info(self, instrument_id: str):
        raise NotImplementedError


class HuobiRestfulUsdtMarginSwap(HuobiRestfulAdapter, market="swap@usdt"):
    
    def __init__(self, *args):
        super().__init__(*args)
        self._client = HuobiUsdtMarginSwap(
            self.credential.api_key, self.credential.secret_key
        )
    
    @property
    def max_optimal_depth(self) -> int:
        return MAX_OPTIMAL_DEPTH
    
    def get_instrument_info(self, instrument_id: str):
        raise NotImplementedError


class HuobiRestfulCoinMarginFuture(HuobiRestfulAdapter, market="future@coin"):
    
    def estimate_lot(
        self, instrument_id: str, size: float, price: float = 0
    ) -> Lot:
        contract_value = float(
            self._instrument_registry[instrument_id].contract_value)
        return Lot(int(size * price / contract_value))
    
    def __init__(self, *args):
        super().__init__(*args)
        self._client = HuobiCoinMarginFuture(
            self.credential.api_key, self.credential.secret_key)
        self._instrument_registry = self.get_all_instruments()
        self._leverage = DEFAULT_LEVERAGE
        assert self._instrument_registry

    @property
    def max_optimal_depth(self) -> int:
        return MAX_OPTIMAL_DEPTH

    def get_tick(self, instrument_id) -> Tick:
        try:
            res = self._client.get_ticker(instrument_id)
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

    def get_ticks(self, *instruments, pricing: str = "avg") -> List[Tick]:
        try:
            res = self._client.get_market_detail_merged()
            if res["status"] != "ok":
                raise ValueError(f"get ticks failed!")
            ts = timestamp_to_utc(res["ts"])
            res = res["ticks"]
        except Exception as e:
            log.exception(e)
            raise TickGetError
        ret = []
        for r in res:
            if pricing == "avg":
                price = (float(r["bid"][0]) + float(r["ask"][0])) / 2
            elif pricing == "ask":
                price = float(r["ask"][0])
            elif pricing == "bid":
                price = float(r["bid"][0])
            else:
                raise ValueError
            ret.append(Tick(
                self.exchange, self.market, r["symbol"], ts, price
            ))
        return ret
    
    def transfer_margin_to_asset(self, symbol: str, amount: float):
        amount = str(round_down(8, amount))
        try:
            res = self._client.transfer_margin_to_asset(symbol, amount)
            if res["status"] != "ok":
                log.error(f"{res}")
                raise AssetTransferError
        except Exception as e:
            log.exception(e)
            raise AssetTransferError
        
    def transfer_asset_to_future_margin(self, symbol: str, amount: float):
        raise NotImplementedError(f"asset to margin is not avail in future "
                                  f"adapter")
    
    def transfer_asset_to_swap_margin(self, symbol: str, amount: float):
        raise NotImplementedError(f"asset to margin is not avail in future "
                                  f"adapter")
    
    @staticmethod
    def _get_underlying(symbol: str):
        return f"{symbol}-USD"
    
    @staticmethod
    def _format_delivery_date(s: str) -> datetime:
        assert s
        return datetime.strptime(s, "%Y%m%d")
    
    @staticmethod
    def _get_sys_delivery_type(s: str):
        if s == "this_week":
            return "CW"
        elif s == "next_week":
            return "NW"
        elif s == "quarter":
            return "CQ"
        elif s == "next_quarter":
            return "NQ"
        raise ValueError
    
    def get_all_instruments(self) -> Dict[str, InstrumentInfo]:
        ret = dict()
        res = self._client.get_contract_contract_info()
        assert res["status"] == "ok"
        res = res["data"]
        for r in res:
            base = r["symbol"]
            delivery_type = self._get_sys_delivery_type(r['contract_type'])
            alias = f"{base}_{delivery_type}"
            ret[r["contract_code"]] = InstrumentInfo(
                instrument_type="futures",
                instrument_id=r["contract_code"],
                underlying=self._get_underlying(r["symbol"]),
                commission="",
                base_currency=base,
                quote_currency="USD",
                settle_currency=r["symbol"],
                contract_value=r["contract_size"],
                contract_value_currency="USD",
                option_type="",
                strike_price="",
                list_time=self._format_delivery_date(r["create_date"]),
                expire_time=timestamp_to_utc(int(r["delivery_time"])),
                leverage="",
                tick_size=r["price_tick"],
                lot_size="1",
                min_size="1",
                contract_type="linear",
                alias=alias,
                state=bool(r["contract_status"])
            )
        return ret
    
    def get_instrument_info(self, instrument_id: str) -> InstrumentInfo:
        assert instrument_id
        instrument_id = instrument_id.upper()
        return self._instrument_registry[instrument_id]
    
    def get_book(
        self, instrument_id: str, depth: int
    ) -> OrderBook:
        if depth not in (20, 150):
            raise ValueError
        if depth == 150:
            depth_type = "step0"
        else:
            depth_type = "step6"
        try:
            res = self._client.get_market_depth(instrument_id, depth_type)
            assert res["status"] == "ok"
            asks, bids = res["tick"]["asks"], res["tick"]["bids"]
        except Exception as e:
            log.exception(e)
            raise OrderBookGetError
        else:
            res = OrderBook(
                exchange=self.exchange, market=self.market,
                instrument_id=instrument_id,
                timestamp=timestamp_to_utc(res["ts"]),
                asks=[tuple(d) for d in asks],
                bids=[tuple(d) for d in bids])
            return res
    
    @staticmethod
    def get_sys_order_type(r: Union[str, OrderPostType]) -> str:
        if isinstance(r, str):
            if r == "limit":
                return OrderType.LIMIT_GTC
            elif r == "opponent":
                return OrderType.OPPONENT_GTC
            elif r == "optimal_5":
                return OrderType.OPTIMAL_5_GTC
            elif r == "optimal_10":
                return OrderType.OPTIMAL_10_GTC
            elif r == "optimal_20":
                return OrderType.OPTIMAL_20_GTC
            else:
                raise NotImplementedError
        else:
            if isinstance(r, LimitOrder):
                return OrderType.LIMIT_GTC
            elif isinstance(r, OpponentOrder):
                return OrderType.OPPONENT_GTC
            elif isinstance(r, OpponentIocOrder):
                return OrderType.OPPONENT_IOC
            elif isinstance(r, OptimalFokOrder):
                return f"OPTIMAL_{r.depth}_FOK"
            elif isinstance(r, OptimalIocOrder):
                return f"OPTIMAL_{r.depth}_IOC"
            elif isinstance(r, OptimalOrder):
                return f"OPTIMAL_{r.depth}_GTC"
            else:
                raise NotImplementedError
    
    @staticmethod
    def get_order_type(post: OrderPostType) -> str:
        if isinstance(post, LimitOrder):
            return f"limit"
        elif isinstance(post, OpponentOrder):
            return f"opponent"
        elif isinstance(post, OptimalOrder):
            return f"optimal_{post.depth}"
        elif isinstance(post, OpponentFokOrder):
            return f"opponent_{post.depth}_fok"
        elif isinstance(post, OpponentIocOrder):
            return f"opponent_{post.depth}_ioc"
        elif isinstance(post, OptimalFokOrder):
            return f"optimal_{post.depth}_fok"
        elif isinstance(post, OptimalIocOrder):
            return f"optimal_{post.depth}_ioc"
        else:
            raise NotImplementedError

    @staticmethod
    def _get_direction_and_offset(post: OrderPostType) -> Tuple[str, str]:
        post: LimitOrder = post
        if post.pos_side == LONG and post.side == BUY:
            return "buy", "open"
        elif post.pos_side == LONG and post.side == SELL:
            return "sell", "close"
        elif post.pos_side == SHORT and post.side == SELL:
            return "buy", "close"
        elif post.pos_side == SHORT and post.side == BUY:
            return "sell", "open"
        else:
            raise NotImplementedError
    
    @staticmethod
    def _get_sys_side_and_pos_side(
        direction: str, offset: str
    ) -> Tuple[str, str]:
        if direction == "buy" and offset == "open":
            return BUY, LONG
        elif direction == "sell" and offset == "close":
            return SELL, LONG
        elif direction == "sell" and offset == "open":
            return SELL, SHORT
        elif direction == "buy" and offset == "close":
            return BUY, SHORT
        else:
            raise NotImplementedError
    
    @staticmethod
    def _get_sys_order_status(status: int):
        if status == 1:
            return OrderStatus.PREPARING
        elif status == 2:
            return OrderStatus.PREPARING
        elif status == 3:
            return OrderStatus.ONGOING
        elif status == 4:
            return OrderStatus.PARTIAL
        elif status == 5:
            return OrderStatus.PARTIAL_FILED_OTHER_CANCELED
        elif status == 6:
            return OrderStatus.FULFILLED
        elif status == 7:
            return OrderStatus.CANCELED
        elif status == 11:
            return OrderStatus.CANCELING
        else:
            raise NotImplementedError
    
    def get_leverage(self, instrument_id: str) -> float:
        log.warning(f"all the future@coin use one leverage")
        assert instrument_id
        instrument_id = instrument_id.upper()
        mrg = self.get_margin(instrument_id)
        if mrg != MARGIN_EMPTY:
            self._leverage = mrg.leverage
        return self._leverage
    
    def set_leverage(self, instrument_id: str, lv: float):
        log.warning(f"all the future@coin use one leverage")
        assert instrument_id
        instrument_id = instrument_id.upper()
        base_sym = self._instrument_registry[instrument_id].base_currency
        self._leverage = lv
        self._client.post_contract_switch_lever_rate(base_sym, int(lv))
    
    def create_order(self, post: OrderPostType) -> str:
        if self._leverage == DEFAULT_LEVERAGE:
            raise RuntimeError(f"Please set leverage first!")
        
        assert self.exchange == post.exchange and self.market == post.market
        instrument_id = post.instrument_id.upper()
        instrument = self._instrument_registry[instrument_id]
        price = round_down(str(instrument.tick_size), getattr(post, "price", 0))
        qty = post.qty
        direction, offset = self._get_direction_and_offset(post)
        if qty < Decimal(instrument.min_size):
            raise ValueError
        
        client_oid = create_id_by_timestamp()
        order_type = self.get_order_type(post)
        try:
            res = self._client.post_order(
                contract_code=instrument_id, price=str(price), volume=str(qty),
                order_price_type=order_type, client_order_id=client_oid,
                direction=direction, offset=offset, lever_rate=self._leverage
            )
            if res["status"] != "ok":
                log.error(f"{res}")
                raise OrderPostError
        except OrderPostError:
            raise
        except Exception as e:
            log.exception(e)
        return client_oid
    
    def get_order_by_client_oid(self, instrument_id, client_order_id) -> Order:
        assert instrument_id
        instrument_id = instrument_id.upper()
        symbol = self._instrument_registry[instrument_id].base_currency
        try:
            res = self._client.get_contract_order_info(
                symbol=symbol, client_order_id=client_order_id)
            assert res["status"] == "ok"
            ts = timestamp_to_utc(res["ts"])
            res = res["data"][0]
        except Exception as e:
            log.exception(e)
            raise OrderGetError
        else:
            qty = float(res["volume"])
            side, pos_side = self._get_sys_side_and_pos_side(
                res["direction"], res["offset"])
            order_type = self.get_sys_order_type(res["order_price_type"])
            price = res.get("price", None) or 0
            avg_entry_price = res.get("trade_avg_price", None) or 0
            fee = float(res["fee"])
            status = self._get_sys_order_status(int(res["status"]))
            if status in (OrderStatus.FULFILLED, OrderStatus.CANCELED,
                          OrderStatus.PARTIAL_FILED_OTHER_CANCELED):
                finished_at = ts
            else:
                finished_at = UNIX_EPOCH
            
            if res["lever_rate"] != self._leverage:
                raise RuntimeError()
            
            return Order(
                exchange=self.exchange, market=self.market, side=side,
                pos_side=LONG, price=float(price), qty=qty,
                avg_entry_price=float(avg_entry_price),
                leverage=float(res["lever_rate"]),
                instrument_id=instrument_id, client_oid=client_order_id,
                order_type=order_type, fee=fee, fee_asset=res["fee_asset"],
                pnl=0, order_id=res["order_id"], created_at=ts,
                finished_at=finished_at, contract_size=0,
                state=status, filled=qty, slippage=0
            )
    
    def get_position(self, instrument_id: str) -> Position:
        assert instrument_id
        instrument_id = instrument_id.upper()
        base = self._instrument_registry[instrument_id].base_currency
        try:
            res = self._client.get_contract_position_info(base)
            assert res["status"] == "ok"
            res = res["data"]
        except Exception as e:
            log.exception(e)
            raise PositionGetError
        pos = POSITION_EMPTY
        for r in res:
            if r["contract_code"] == instrument_id:
                size = int(res["volume"])
                if res["direction"] == "sell":
                    size = -int(res["volume"])
                home_notional = float(res["last_price"]) * float(res["volume"])
                pos = Position(
                    exchange=self.exchange,
                    market=self.market,
                    instrument_id=instrument_id,
                    size=size,
                    avg_entry_price=res["cost_hold"],
                    realised_pnl=res["profit"],
                    unrealised_pnl=res["profit_unreal"],
                    home_notional=home_notional,
                    leverage=res["lever_rate"],
                )
        return pos

    def get_margin(self, instrument_id: str) -> Margin:
        assert instrument_id
        instrument_id = instrument_id.upper()
        symbol = self._instrument_registry[instrument_id].base_currency
        try:
            res = self._client.get_contract_account_info(symbol)
            assert res["status"] == "ok"
            res = res["data"][0]
        except Exception as e:
            log.exception(e)
            raise MarginGetError
            
        margin_balance = float(res["margin_balance"])
        unrealised_pnl = float(res["profit_unreal"])
        wallet_balance = margin_balance - unrealised_pnl
        return Margin(
            exchange=self.exchange,
            market=self.market,
            instrument_id=instrument_id,
            wallet_balance=wallet_balance,
            unrealised_pnl=unrealised_pnl,
            realised_pnl=res["profit_real"],
            init_margin=res["margin_frozen"],
            maint_margin=res["margin_position"],
            margin_balance=margin_balance,
            leverage=float(res["lever_rate"]),
        )
    
    def cancel_order(self, instrument_id: str, client_oid: str):
        assert instrument_id
        instrument_id = instrument_id.upper()
        symbol = self._instrument_registry[instrument_id].base_currency
        return self._client.cancel_order(symbol, client_oid)
    
    def cancel_all_orders(self, instrument_id: str):
        symbol = self._instrument_registry[instrument_id].base_currency
        return self._client.cancel_all_orders(symbol)


class HuobiRestfulSpot(HuobiRestfulAdapter, market="spot"):
    
    def __init__(self, *args):
        super().__init__(*args)
        self._client = HuobiSpot(
            self.credential.api_key, self.credential.secret_key
        )
        self._instrument_registry: Dict[str, InstrumentInfo] = dict()
        self._account_id: str = ""
        res = self._client.get_symbols()
        assert res["status"] == "ok"
        for r in res["data"]:
            self._instrument_registry[r["symbol"]] = InstrumentInfo(
                instrument_type="spot",
                instrument_id=r["symbol"],
                underlying=INVALID,
                commission=INVALID,
                base_currency=r["base-currency"],
                quote_currency=r["quote-currency"],
                settle_currency=INVALID,
                contract_value=INVALID,
                contract_value_currency=INVALID,
                option_type=INVALID,
                strike_price=MAX_FLOAT,
                list_time=UNIX_EPOCH,
                expire_time=UNIX_EPOCH,
                leverage=MAX_FLOAT,
                tick_size=r["price-precision"],
                lot_size=r["amount-precision"],
                min_size=r["min-order-amt"],
                contract_type=INVALID,
                alias=INVALID,
                state=bool(r["state"] == "online"))
        
        self._account_id = INVALID
        if self.credential != CREDENTIAL_EMPTY:
            res = self._client.get_accounts()
            assert res["status"] == "ok"
            for r in res["data"]:
                if r["type"] == "spot" and r["state"] == "working":
                    self._account_id = r["id"]
            assert self._account_id
    
    @property
    def max_optimal_depth(self) -> int:
        return MAX_INT
    
    def get_all_instruments(self) -> Dict[str, InstrumentInfo]:
        return self._instrument_registry.copy()
    
    def get_instrument_info(self, instrument_id: str) -> InstrumentInfo:
        return self._instrument_registry[instrument_id]
    
    def estimate_lot(self, instrument_id: str, size: float, price: float = 0):
        raise NotImplementedError("Lot is for delivery or swap")
    
    def get_tick(self, instrument_id: str) -> Tick:
        try:
            res = self._client.get_market_trade(instrument_id)
            assert res["status"] == "ok"
            ts = timestamp_to_utc(res["ts"])
            res = res["tick"]["data"][0]
        except Exception as e:
            log.exception(e)
            raise TickGetError
        price = float(res["price"])
        
        return Tick(self.exchange, self.market, instrument_id, timestamp=ts,
                    price=price)
    
    def get_ticks(self, *instruments, pricing: str = "avg"):
        instruments = [s.lower() for s in instruments]
        try:
            res = self._client.get_tickers()
            assert res["status"] == "ok"
            ts = timestamp_to_utc(res["ts"])
            res = res["data"]
        except Exception as e:
            log.exception(e)
            raise TickGetError
        ret = []
        for r in res:
            if instruments and r["symbol"] not in instruments:
                continue
            if pricing == "avg":
                price = (float(r["ask"]) + float(r["bid"])) / 2
            elif pricing == "ask":
                price = float(r["ask"])
            elif pricing == "bid":
                price = float(r["bid"])
            else:
                raise ValueError
            ret.append(Tick(self.exchange, self.market, r["symbol"], ts, price))
        return ret
    
    def get_book(self, instrument_id: str, depth: int) -> OrderBook:
        if depth not in (5, 10, 20, 150):
            raise ValueError
        if depth == 150:
            depth_type = "step0"
            depth = 0
        else:
            depth_type = "step1"
        try:
            res = self._client.get_market_depth(
                instrument_id, _type=depth_type, depth=depth)
            assert res["status"] == "ok"
            ts = timestamp_to_utc(res["ts"])
            asks, bids = res["tick"]["asks"], res["tick"]["bids"]
        except Exception as e:
            log.exception(e)
            raise OrderBookGetError
        else:
            return OrderBook(
                exchange=self.exchange, market=self.market,
                instrument_id=instrument_id, timestamp=ts,
                asks=[tuple(d) for d in asks],
                bids=[tuple(d) for d in bids]
            )
    
    def create_order(self, post: OrderPostType) -> str:
        assert self.exchange == post.exchange and self.market == post.market
        instrument_id = post.instrument_id
        instrument = self._instrument_registry[instrument_id]
        price = round_down(instrument.tick_size, getattr(post, "price", 0))
        qty = round_down(instrument.lot_size, float(post.qty))
        
        if qty < Decimal(instrument.min_size):
            raise ValueError
        
        client_oid = create_id_by_timestamp()
        order_type = self.get_order_type(post)
        try:
            res = self._client.post_spot_order(
                account_id=self._account_id, symbol=post.instrument_id,
                order_type=order_type, amount=str(qty), price=str(price),
                source="spot-api", client_order_id=client_oid,
                stop_price="", operator=""
            )
            if res["status"] != "ok":
                log.error(f"{res}")
                raise OrderPostError
        except OrderPostError:
            raise
        except Exception as e:
            log.exception(e)
        return client_oid
    
    def get_order_by_client_oid(self, instrument_id, client_order_id) -> Order:
        try:
            res = self._client.get_order_by_client_oid(client_order_id)
            if res.get("status") != "ok":
                log.error(f"{res}")
                raise OrderGetError
            res = res["data"]
        except Exception as e:
            log.exception(e)
            raise OrderGetError
        else:
            price = float(res["price"])
            if price == 0.0:
                price = float(res["field-amount"]) / \
                        float(res["field-cash-amount"])
            qty = float(res["field-amount"])
            side = self._get_order_side(res["type"])
            order_type = self.get_sys_order_type(res["type"])
            fee = float(res["field-fees"])
            created_at, finished_at = timestamp_to_utc(
                res["created-at"]), timestamp_to_utc(res["finished-at"])
            status = self._get_sys_order_status(res["state"])
            return Order(
                exchange=self.exchange, market=self.market, side=side,
                pos_side=LONG, price=price, qty=qty,
                leverage=float(res.get("lever_rate", DEFAULT_LEVERAGE)),
                instrument_id=instrument_id, client_oid=client_order_id,
                order_type=order_type, fee=fee, pnl=0, order_id=str(res["id"]),
                created_at=created_at, finished_at=finished_at, contract_size=0,
                state=status, filled=qty, slippage=0
            )
    
    @staticmethod
    def get_order_type(post: OrderPostType) -> str:
        side = "buy" if post.side == BUY else "sell"
        if isinstance(post, MarketOrder):
            return f"{side}-market"
        elif isinstance(post, LimitOrder):
            return f"{side}-limit"
        elif isinstance(post, LimitIocOrder):
            return f"{side}-ioc"
        elif isinstance(post, LimitFokOrder):
            return f"{side}-limit_fok"
        else:
            raise NotImplementedError
    
    @staticmethod
    def get_sys_order_type(order_type: str) -> str:
        if order_type in ("buy-market", "sell-market"):
            return OrderType.MARKET
        elif order_type in ("buy-limit", "sell-limit"):
            return OrderType.LIMIT_GTC
        elif order_type in ("buy-ioc", "sell-ioc"):
            return OrderType.LIMIT_IOC
        elif order_type in ("buy-limit-fok", "sell-limit-fok"):
            return OrderType.LIMIT_FOK
        else:
            raise NotImplementedError

    @staticmethod
    def _get_order_side(order_type: str) -> str:
        if order_type in (
            "buy-market", "buy-limit", "buy-ioc", "buy-limit-fok"
        ):
            return BUY
        elif order_type in (
            "sell-market", "sell-limit", "sell-ioc", "sell-limit-fok"
        ):
            return SELL
        else:
            raise NotImplementedError
    
    @staticmethod
    def _get_sys_order_status(status: str):
        if status == "created":
            return OrderStatus.PREPARING
        elif status == "filled":
            return OrderStatus.FULFILLED
        elif status == "submitted":
            return OrderStatus.ONGOING
        elif status == "partial-filled":
            return OrderStatus.PARTIAL
        elif status == "partial-canceled":
            return OrderStatus.PARTIAL_FILED_OTHER_CANCELED
        elif status == "canceling":
            return OrderStatus.CANCELING
        elif status == "canceled":
            return OrderStatus.CANCELED
        else:
            raise NotImplementedError
    
    def transfer_margin_to_asset(self, symbol: str, amount: float):
        raise NotImplementedError(f"margin to asset not avail in spot adapter")
    
    def transfer_asset_to_future_margin(self, symbol: str, amount: float):
        amount = round_down(8, amount)
        try:
            res = self._client.transfer_asset_to_future_margin(symbol, amount)
            if res["status"] != "ok":
                log.error(f"{res}")
                raise AssetTransferError
        except Exception as e:
            log.exception(e)
            raise e
    
    def transfer_asset_to_swap_margin(self, symbol: str, amount: float):
        amount = round_down(8, amount)
        try:
            res = self._client.transfer_asset_to_swap_margin(symbol, amount)
            if res["status"] != "ok":
                log.error(f"{res}")
                raise AssetTransferError
        except Exception as e:
            log.exception(e)
            raise e
    
    def get_leverage(self, instrument_id: str):
        raise NotImplementedError
    
    def set_leverage(self, instrument_id: str, lv: float):
        raise NotImplementedError
    
    def get_available_balance(self, symbol: str) -> float:
        assert symbol
        symbol = symbol.lower()
        try:
            res = self._client.get_accounts_balance(self._account_id)
            assert res["status"] == "ok"
            res = res["data"]["list"]
        except Exception as e:
            log.exception(e)
            raise BalanceGetError
        for r in res:
            if r["currency"] == symbol and r["type"] == "trade":
                balance = float(r["balance"])
                if balance < 0.00000001:
                    balance = float(0)
                return balance
        return 0
    
    def cancel_order(self, instrument_id: str, client_oid: str):
        return self._client.post_cancel_order_by_client_oid(client_oid)
    
    def cancel_all_orders(self, instrument_id: str):
        return self._client.cancel_all_orders(instrument_id)
    
    def set_leverage(self, instrument_id: str, leverage: float):
        raise NotImplementedError
    
    def get_leverage(self, instrument_id: str) -> float:
        return DEFAULT_LEVERAGE
