from sqlalchemy import MetaData
from sqlalchemy import (
    Table,
    Column,
    DECIMAL,
    DateTime,
    Integer,
    String,
    UniqueConstraint,
)

metadata = MetaData()

OrderJournal = Table(
    "order_journal",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("timestamp", DateTime),
    Column("order_id", String(32), comment="订单id"),
    Column("client_oid", String(128), comment="订单cl id"),
    Column("instrument_id", String(128)),
    Column("side", String(32)),
    Column("size", Integer),
    Column("price", DECIMAL(24, 12)),
    Column("slippage", DECIMAL(24, 12)),
    Column("state", String(32)),
    Column("contract_val", DECIMAL(12, 6)),
    Column("order_type", String(16)),
    Column("fee", DECIMAL(12, 6)),
    Column("filled", Integer),
    Column("errmsg", String(255)),
    Column("exchange", String(16)),
    Column("uid", String(128)),
    UniqueConstraint("exchange", "order_id", "instrument_id", name="unique_key"),
)

HistoryBar = Table(
    "history_bar",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("exchange", String(32)),
    Column("instrument_id", String(32)),
    Column("timestamp", DateTime),
    Column("open", DECIMAL(20, 7)),
    Column("close", DECIMAL(20, 7)),
    Column("high", DECIMAL(20, 7)),
    Column("low", DECIMAL(20, 7)),
    Column("volume", Integer),
    Column("currency_volume", DECIMAL(20, 7)),
    Column("granularity", Integer),
)

AccountJournal = Table(
    "account_journal",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("timestamp", DateTime),
    Column("wallet_balance", DECIMAL(20, 7)),
    Column("margin_balance", DECIMAL(20, 4)),
    Column("client_oid", String(128)),
    Column("pos_qty", Integer),
    Column("instrument_id", String(32)),
    Column("currency", String(16)),
    Column("unrealised_pnl", DECIMAL(20, 7)),
    Column("realised_pnl", DECIMAL(20, 7)),
    Column("uid", String(128)),
    Column("exchange", String(16), nullable=False),
    Column("home_notional", DECIMAL(20, 7)),
    Column("price", DECIMAL(20, 4)),
    Column("init_margin", DECIMAL(20, 7)),
    Column("maint_margin", DECIMAL(20, 7)),
    Column("total_pnl", DECIMAL(20, 7)),
    UniqueConstraint(
        "exchange", "uid", "instrument_id", "client_oid", name="unique_key"
    ),
)


TradeJournal = Table(
    "trade_journal",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("uid", String(128)),
    Column("trade_id", Integer, unique=True),
    Column("exchange", String(16)),
    Column("market", String(16)),
    Column("instrument_id", String(16)),
    Column("close_ts", DateTime),
    Column("status", String(8)),
    Column("pnl", DECIMAL(64)),
    Column("pos_side", String(8)),
    Column("position", DECIMAL(20, 7)),
    Column("commission", DECIMAL(20, 7)),
    UniqueConstraint(
        "exchange", "uid", "market", "instrument_id", "trade_id",
        name="unique_key"
    ),
)
