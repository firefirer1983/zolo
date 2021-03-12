#
# This file base on python stdlib only!
#
import sys
from datetime import datetime

# order executors type:
BLOCKING = "BLOCKING"
NON_BLOCKING = "NON_BLOCKING"
PERIODIC = "PERIODIC"

# stable currencies registry
STABLE_CURRENCIES = ("usdt", "USDT")

# order time in force type
FOK = "FullOrKill"
IOC = "ImmediateOrCancel"
GTC = "GoodTillCancel"

# position side:
LONG = "Long"
SHORT = "Short"
DUAL = "Dual"
POSITION_SIDE_EMPTY = "EMPTY_POSITION_SIDE"

# order side:
BUY = "Buy"
SELL = "Sell"

# gateways types:
WEB_SOCKET = "WebSocket"
RESTFUL = "Restful"
ZMQ = "Zmq"
BACKTEST = "Backtest"
DRYRUN = "Dryrun"

# trade generate schemes:
AIAO = "AIAO"
FIFO = "FIFO"
FILO = "FILO"

# trade status
CLOSE = "Close"
OPEN = "Open"

# event handler function names:
ON_START = "on_start"
ON_STOP = "on_stop"
ON_TICK = "on_tick"
ON_BAR = "on_bar"
ON_FILL = "on_fill"
ON_TRADE = "on_trade"
ON_ORDER = "on_order"
ON_TIMER = "on_timer"
ON_MESSAGE = "on_message"
ON_BOOK = "on_book"

# default datetime
UNIX_EPOCH = datetime(1970, 1, 1)

# exchange registry
EXCHANGE_REGISTRY = ("bitmex", "huobi", "okex", "binance")

# running status
STOPPED = "STOPPED"
INIT = "INIT"
RUNNING = "RUNNING"
PAUSED = "PAUSED"

# gateway control command
GATEWAY_START = "GATEWAY_START"
GATEWAY_STOP = "GATEWAY_STOP"
GATEWAY_PAUSE = "GATEWAY_PAUSE"
GATEWAY_HEARTBEAT = "GATEWAY_HEARTBEAT"
GATEWAY_REBOOT = "GATEWAY_REBOOT"
GATEWAY_SUBSCRIBE = "GATEWAY_SUBSCRIBE"
GATEWAY_UNSUBSCRIBE = "GATEWAY_UNSUBSCRIBE"

# limit constant
MAX_INT = sys.maxsize
MIN_INT = -sys.maxsize - 1

MAX_FLOAT = sys.float_info.max
MIN_FLOAT = sys.float_info.min

LAMBER_RET_ZERO = lambda: float(0)

DEFAULT_LEVERAGE = -1.0
INVALID = "INVALID"


USER_MSG_GATEWAY = "tcp://127.0.0.1:5555"
