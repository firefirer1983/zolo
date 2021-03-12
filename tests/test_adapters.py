import os
from zolo.credential import load_credential_from_env
from zolo.adapters import create_adapter
from zolo.dtypes import Credential
cred = Credential("", "", "")# load_credential_from_env()

os.environ["HTTP_PROXY"] = "socks5h://127.0.0.1:17720"
os.environ["HTTPS_PROXY"] = "socks5h://127.0.0.1:17720"

adapter_type = "restful"
exchange = "huobi"
market = "swap@coin"
instrument_id = "BTC-USD"
granularity = 60

adapter = create_adapter(adapter_type, exchange, market, cred)


def test_create():
    assert adapter.credential == cred
    assert bool(adapter.client)


def test_get_tick():
    tick = adapter.get_tick(instrument_id)
    assert tick.exchange == exchange
    assert tick.instrument_id == instrument_id


def test_get_latest_bar():
    bar = adapter.get_latest_bar(instrument_id, granularity)
    assert bar.exchange == exchange
    assert bar.instrument_id == instrument_id
    assert bar.granularity == granularity


def test_get_instrument_info():
    info = adapter.get_instrument_info(instrument_id)
