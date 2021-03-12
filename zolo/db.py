import logging
from dataclasses import asdict

from sqlalchemy import create_engine, insert, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from .dtypes import Order, Margin, Bar, Trade
from .model import OrderJournal, AccountJournal, metadata, HistoryBar, TradeJournal
from .utils import register_sql_decimal

log = logging.getLogger(__name__)


_URL = ""
_ECHO = False
_POOL_PRE_PING = True
_POOL_RECYCLE = 600


def config_db_engine(url, echo=False) -> Engine:
    global _URL, _ECHO
    _URL, _ECHO = url, echo


def db_engine() -> Engine:
    eng = None
    if not eng:
        eng = create_engine(
            url=_URL,
            echo=_ECHO,
            pool_pre_ping=_POOL_PRE_PING,
            pool_recycle=_POOL_RECYCLE,
        )
        if eng.name == "sqlite":
            register_sql_decimal()
        create_table_if_not_exists()
    return eng


def log_order_to_db(exchange_name, uid, order: Order, slippage):
    order = asdict(order)
    order.update(dict(exchange=exchange_name, uid=uid, slippage=slippage))
    try:
        with db_engine().begin() as c:
            c.execute(insert(OrderJournal), order)
    except IntegrityError:
        log.warning(f"duplicate order: {order}")
    except Exception as e:
        log.error(f"log order: {order} to db failed!")
        log.exception(e)
        raise


def log_account_to_db(
    exchange,
    uid,
    instrument_id,
    timestamp,
    mrg: Margin,
    qty,
    price,
    home_notional: float,
    client_oid: str,
    currency: str = "",
):
    account = dict(
        price=price,
        timestamp=timestamp,
        wallet_balance=mrg.wallet_balance,
        pos_qty=qty,
        instrument_id=instrument_id,
        realised_pnl=mrg.realised_pnl,
        unrealised_pnl=mrg.unrealised_pnl,
        currency=currency,
        uid=uid,
        exchange=exchange,
        home_notional=home_notional,
        margin_balance=mrg.margin_balance,
        init_margin=mrg.init_margin,
        maint_margin=mrg.maint_margin,
        client_oid=client_oid,
    )
    try:
        with db_engine().begin() as c:
            c.execute(insert(AccountJournal), account)
    except IntegrityError as e:
        log.exception(e)
        log.error(f"Duplicated Account entry: {account}")
    except Exception as e:
        log.exception(e)
        log.error(f"log account: {account} to db failed!")
        raise


def log_bar_to_db(bar: Bar):
    try:
        with db_engine().begin() as c:
            c.execute(insert(HistoryBar), asdict(bar))
    except IntegrityError as e:
        log.error(f"Duplicated Bar entry: {bar}")
    except Exception as e:
        log.exception(e)
        log.error(f"log bar: {bar} to db failed!")
        raise


def log_trade_to_db(
    uid: str, exchange: str, market: str, instrument_id: str, trade: Trade
):
    try:
        with db_engine().begin() as c:
            c.execute(insert(TradeJournal), asdict(trade))
    except IntegrityError:
        try:
            with db_engine().begin() as c:
                trade = asdict(trade)
                trade.update(
                    dict(
                        uid=uid,
                        exchange=exchange,
                        market=market,
                        instrument_id=instrument_id,
                    )
                )
                c.execute(update(TradeJournal), trade)
        except Exception as e:
            log.exception(e)
            raise
    except Exception as e:
        raise e


def create_table_if_not_exists():
    metadata.create_all(db_engine(), checkfirst=True)
