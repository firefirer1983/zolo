from zolo.dtypes import Bar, Tick
from zolo.feeds.sql import BarSQLDataFeed
from zolo.feeds.integrator import HybridDataFeed
from sqlalchemy import create_engine
from datetime import datetime
from os.path import dirname
base_path = dirname(dirname(__file__))
engine = create_engine(f"sqlite:////{base_path}/bitmex.sqlite3")


def test_bar_feed():
    feed = iter(BarSQLDataFeed(engine, "bitmex", "xbtusd", "1m", datetime(2020, 1, 1)))
    bar: Bar = next(feed)
    assert bar.timestamp.date() == datetime(2020, 1, 1).date()


def test_tick_integrator():
    feed = iter(HybridDataFeed(BarSQLDataFeed(engine, "bitmex", "xbtusd", "1m", datetime(2020, 1, 1)), periods=("1h",)))
    bar: Bar = next(feed)
    breakpoint()