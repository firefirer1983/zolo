from datetime import datetime, timedelta
from .base import BarDataFeed
from ..utils import register_sql_decimal
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy import MetaData, Column, INTEGER, DateTime, DECIMAL, Table, select, asc

from ..dtypes import Bar
from ..utils import granularity_in_num

_URL = ""
_ECHO = False


def config_db_engine(url, echo=False) -> Engine:
    global _URL, _ECHO
    _URL, _ECHO = url, echo


def db_engine() -> Engine:
    eng = None
    if not eng:
        eng = create_engine(_URL, echo=_ECHO)
        register_sql_decimal()
    return eng


def get_schema(name):
    if not db_engine().dialect.has_table(db_engine(), name):
        metadata = MetaData(db_engine())
        # Create a table with the appropriate Columns
        table = Table(
            name,
            metadata,
            Column("id", INTEGER, primary_key=True),
            Column("timestamp", DateTime),
            Column("open", DECIMAL(20, 7)),
            Column("close", DECIMAL(20, 7)),
            Column("high", DECIMAL(20, 7)),
            Column("low", DECIMAL(20, 7)),
            Column("volume", INTEGER),
        )
        table.create(bind=db_engine(), checkfirst=True)
        return table
    else:
        meta = MetaData()
        meta.reflect(bind=db_engine())
        return meta.tables[name]


class BarSQLDataFeed(BarDataFeed):
    buf_size = 7 * 24 * 60

    def __init__(
        self,
        exchange: str,
        market: str,
        instrument_id: str,
        granularity: str,
        start: datetime,
        end: datetime = None,
    ):
        self._exchange = exchange.lower()
        self._market = market.lower()
        self._instrument_id = instrument_id.lower()
        year, self._start, self._end = start.year, start, end
        if not self._end:
            self._end = datetime.utcnow()
        self._schema = get_schema(f"{self._instrument_id}_{granularity}_bar_{year}")
        self._granularity = granularity_in_num(granularity)
        self._bars = []

    @property
    def exchange(self):
        return self._exchange

    def __iter__(self):
        while True:
            try:
                bar = self._bars.pop(0)
            except IndexError:
                self._bars = self.reload(self._start)
                if self._bars:
                    self._start = self._bars[-1].timestamp + timedelta(minutes=1)
                else:
                    raise EOFError
                bar = self._bars.pop(0)

            if bar.timestamp >= self._end:
                raise EOFError

            yield bar

    def reload(self, start: datetime, cnt: int = buf_size):
        with db_engine().connect() as c:
            bars = c.execute(
                select([self._schema])
                .where(self._schema.c.timestamp >= start)
                .order_by(asc(self._schema.c.timestamp))
                .limit(cnt)
            ).fetchall()
            return [
                Bar(
                    exchange=self._exchange,
                    market=self._market,
                    instrument_id=self._instrument_id,
                    timestamp=bar[1],
                    open=bar[2],
                    close=bar[4],
                    high=bar[3],
                    low=bar[5],
                    volume=bar[6],
                    currency_volume=0,
                    granularity=self._granularity,
                )
                for bar in sorted(bars, key=lambda x: x[1])
            ]
