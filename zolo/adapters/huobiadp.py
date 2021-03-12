import json
import logging
import time
from datetime import datetime, timedelta
from decimal import Decimal

from xif.huobi.swap.coinmargin import Client as CoinMarginSwapClient
from xif.huobi.swap.usdtmargin import Client as UsdtMarginSwapClient
from xif.huobi.future.coinmargin import Client as UsdtMarginFutureClient
from .enums import HuobiSwapOrderType, HuobiSwapOrderSide, HuobiSwapOrderStatus, \
    HuobiSwapOrderDirection
from ..dtypes import Bar, Order, Margin, Position, Tick
from ..dtypes import OrderType, OrderSide, OrderStatus
from ..exceptions import OrderPostError, MarginGetError, PositionGetError, \
    TickGetError, BarGetError
from ..utils import create_datatype_mapping, asstr

log = logging.getLogger(__name__)

order_type_to_hb = create_datatype_mapping(HuobiSwapOrderType, OrderType)
order_status_to_hb = create_datatype_mapping(HuobiSwapOrderStatus, OrderStatus)
order_side_to_hb = create_datatype_mapping(HuobiSwapOrderSide, OrderSide)
hb_to_order_type = create_datatype_mapping(
    HuobiSwapOrderType, OrderType, to_enum=False)
hb_to_order_status = create_datatype_mapping(
    HuobiSwapOrderStatus, OrderStatus, to_enum=False)
hb_to_order_side = create_datatype_mapping(
    HuobiSwapOrderSide, OrderSide, to_enum=False)


def str_to_datetime(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")


def create_id_by_timestamp():
    return f"{int(time.time() * 10000000)}"


def timestamp_to_utc(ts: int) -> datetime:
    return datetime.utcfromtimestamp(ts / 1000)


def contract_value_config_proxy():
    configs = {}
    
    def getter(instrument_id: str):
        nonlocal configs
        if not configs:
            res = get_swap_contract_info().json()
            if res["status"] != "ok":
                raise ValueError(f"Get contract info failed!")
            res = res["data"]
            configs = {r["contract_code"]: r["contract_size"] for r in
                       res}
        return configs[instrument_id]
    
    return getter


get_contract_value = contract_value_config_proxy()


def close_short(instrument_id: str, order_type: str, amount: int):
    order = create_order(
        instrument_id, amount, order_type, OrderSide.CLOSE_SHORT
    )
    return order


def open_short(instrument_id: str, order_type: str, amount: int):
    order = create_order(
        instrument_id, amount, order_type, OrderSide.OPEN_SHORT
    )
    return order


def open_long(instrument_id, order_type: str, amount: int):
    order = create_order(
        instrument_id, amount, order_type, OrderSide.OPEN_LONG
    )
    return order


def close_long(instrument_id, order_type: str, amount: int):
    order = create_order(
        instrument_id, amount, order_type, OrderSide.CLOSE_LONG
    )
    return order


def get_order_by_client_oid(instrument_id: str, client_oid: str):
    try:
        res = get_swap_order_info(
            instrument_id,
            client_order_id=client_oid,
        )
        res.raise_for_status()
        res = res.json()
    except Exception as e:
        log.error("get order by id failed!")
        log.exception(e)
        return None
    if res["status"] == "ok" and res["data"] and res["data"][0] and \
        res["data"][0]["client_order_id"] == int(client_oid):
        ts = timestamp_to_utc(int(res["ts"]))
        res = res["data"][0]
        log.info(f"order get :{res}")
        order_side = hb_to_order_side(
            (str(res["offset"]), str(res["direction"])))
        if res["trade_avg_price"]:
            price = float(res["trade_avg_price"])
        else:
            price = float(res["price"])
        return Order(
            exchange="huobi",
            instrument_id=res["contract_code"],
            size=int(res["volume"]),
            order_id=str(res["order_id"]),
            client_oid=str(res["client_order_id"]),
            price=price,
            order_type=hb_to_order_type(res["order_price_type"]),
            contract_val=get_contract_value(res["contract_code"]),
            state=hb_to_order_status(str(res["status"])),
            side=order_side,
            fee=Decimal(res["fee"]),
            filled=int(res["trade_volume"]),
            timestamp=ts,
        )


def create_order(instrument_id, amount, order_type, order_side):
    client_oid = create_id_by_timestamp()
    order_type = order_type_to_hb(order_type).value
    offset, direction = order_side_to_hb(order_side).value
    try:
        res = post_swap_order(
            contract_code=instrument_id,
            client_order_id=client_oid,
            order_price_type=order_type,
            offset=offset,
            direction=direction,
            lever_rate=1,
            volume=amount,
            price=None,
        )
        res.raise_for_status()
        res = res.json()
    except Exception as e:
        log.exception(e)
        raise OrderPostError(
            asstr(Order(
                instrument_id=instrument_id,
                client_oid=client_oid,
                errmsg=f"API Call Failed! {str(e)}",
                order_type=order_type,
                side=order_side,
            ))
        )
    else:
        if res:
            if res.get("status") != "ok":
                raise OrderPostError(
                    asstr(Order(
                        instrument_id=instrument_id,
                        client_oid=client_oid,
                        errmsg=f"Create Order Failed! {json.dumps(res)}",
                        order_type=order_type,
                        side=order_side,
                    ))
                )
            else:
                res = res.get("data")
        else:
            raise OrderPostError(
                asstr(Order(
                    instrument_id=instrument_id,
                    client_oid=client_oid,
                    errmsg=f"API Return None!",
                    order_type=order_type,
                    side=order_side,
                ))
            )
    
    return Order(
        instrument_id=instrument_id,
        order_id=str(res["order_id"]),
        client_oid=str(res["client_order_id"]),
        order_type=order_type,
        side=order_side,
    )


def get_margin(instrument_id) -> Margin:
    try:
        res = get_swap_account_info(instrument_id)
        res.raise_for_status()
        res = res.json()
    except Exception as e:
        log.exception(e)
        raise MarginGetError(str(e))
    if res["status"] == "ok" and res["data"] and res["data"][0]:
        res = res["data"][0]
        return Margin(
            wallet_balance=Decimal(res["withdraw_available"] or 0),
            unrealised_pnl=Decimal(res["profit_unreal"] or 0),
            maint_margin=Decimal(res["margin_position"] or 0),
            init_margin=Decimal(res["margin_frozen"] or 0),
            instrument_id=res["contract_code"],
            margin_balance=Decimal(res["margin_balance"] or 0),
            realised_pnl=Decimal(res["profit_real"] or 0),
        )
    raise MarginGetError(str(res.text))


def get_position(instrument_id) -> Position:
    try:
        res = get_swap_position_info(instrument_id)
        res.raise_for_status()
        res = res.json()
    except Exception as e:
        log.exception(e)
        raise PositionGetError(str(e))
    if res["status"] == "ok":
        if res["data"] and res["data"][0]:
            res = res["data"][0]
            size = int(res["volume"] or 0)
            if res["direction"] == HuobiSwapOrderDirection.SELL.value:
                size = -abs(size)
            realised_pnl = Decimal(res["profit"] or 0)
            unrealised_pnl = Decimal(res["profit_unreal"] or 0)
            home_notional = abs(size) * get_contract_value(instrument_id) \
                            / float(res["last_price"])
            return Position(
                instrument_id=res["contract_code"],
                size=size,
                avg_entry_price=float(res["cost_open"]),
                realised_pnl=Decimal(realised_pnl),
                unrealised_pnl=Decimal(unrealised_pnl),
                home_notional=Decimal(home_notional),
            )
        return None
    raise PositionGetError(str(json.dumps(res)))


def get_latest_bar(instrument_id: str, granularity: int):
    granularity_sym = f"{granularity}min"
    prev = datetime.utcnow() - timedelta(minutes=granularity)
    try:
        res = get_market_history_kline(
            instrument_id, granularity_sym, size="2", from_ts=prev
        )
        res.raise_for_status()
        res = res.json()
    except Exception as e:
        log.exception(e)
        raise e
    res = sorted(res["data"], key=lambda r: r["id"])[0]
    return Bar(
        "huobi", instrument_id,
        datetime.utcfromtimestamp(int(res["id"])),
        float(res["open"]), float(res["close"]), float(res["high"]),
        float(res["low"]), volume=int(res["vol"]),
        currency_volume=float(res["amount"]),
        granularity=granularity
    )


def get_last_n_bars(cnt: int, instrument_id: str, granularity: int):
    granularity_sym = f"{granularity}min"
    prev = datetime.utcnow() - timedelta(minutes=cnt * granularity + 1)
    try:
        res = get_market_history_kline(
            instrument_id, granularity_sym, size=f"{cnt + 1}", from_ts=prev
        )
        res.raise_for_status()
        res = res.json()
    except Exception as e:
        log.exception(e)
        raise BarGetError
    res = sorted(res["data"], key=lambda r: r["id"])
    del res[-1]
    return [Bar(
        "huobi", instrument_id,
        datetime.utcfromtimestamp(int(r["id"])),
        float(r["open"]), float(r["close"]), float(r["high"]), float(r["low"]),
        volume=int(r["vol"]),
        currency_volume=float(r["amount"]),
        granularity=granularity
    ) for r in res]


def get_max_order_size(instrument_id: str):
    raise NotImplementedError()


def get_tick(instrument_id: str):
    try:
        res = get_market_trade(instrument_id)
        res.raise_for_status()
        res = res.json()
        if res["status"] != "ok":
            raise ValueError(f"get tick failed!")
        data = res["tick"]["data"][0]
        price = data["price"]
        ts = data["ts"]
    except Exception as e:
        log.exception(e)
        raise TickGetError(str(e))
    
    return Tick(
        timestamp=timestamp_to_utc(ts),
        instrument_id=instrument_id,
        price=float(price),
    )
