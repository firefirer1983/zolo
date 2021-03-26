from abc import abstractmethod, ABC
from typing import Union, Tuple, List, Dict, Optional

from zolo.consts import MAX_INT
from zolo.posts import OrderPost
from ..dtypes import (
    Margin,
    Position,
    Order,
    Bar,
    Credential,
    Tick,
    InstrumentInfo,
    Lot,
    dot_concat,
    OrderBook)
from datetime import datetime

_adapter_registry = {}


class Adapter(ABC):
    def __init__(self, mode: str, exchange: str, market: str, cred: Credential):
        self._client = None
        self._mode = mode
        self._exchange = exchange
        self._market = market
        self._api_key = cred.api_key if cred else ""
        self._secret_key = cred.secret_key if cred else ""
        self._passphrase = cred.passphrase if cred else ""
    
    def __init_subclass__(cls, **kwargs):
        if len(cls.__bases__) != 1:
            raise ValueError
        parent_cls = cls.__bases__[0]
        for k, v in _adapter_registry.items():
            if v == parent_cls:
                d = {k + tuple(kwargs.values()): cls}
                break
        else:
            d = {tuple(kwargs.values()): cls}
        _adapter_registry.update(d)
    
    @property
    def credential(self) -> Credential:
        return Credential(self._api_key, self._secret_key, self._passphrase)
    
    @property
    def max_optimal_depth(self) -> int:
        return MAX_INT
    
    @abstractmethod
    def get_all_instruments(self) -> Dict[str, InstrumentInfo]:
        pass
    
    @abstractmethod
    def get_instrument_info(
        self, instrument_id: str
    ) -> InstrumentInfo:
        pass
    
    @abstractmethod
    def estimate_lot(self, instrument_id: str, size: float,
                     price: float = 0) -> Lot:
        pass
    
    @abstractmethod
    def create_order(self, post: OrderPost) -> str:
        pass
    
    @abstractmethod
    def transfer_margin_to_asset(self, symbol: str, amount: float) -> float:
        pass
    
    @abstractmethod
    def transfer_asset_to_future_margin(
        self, symbol: str, amount: float) -> float:
        pass
    
    @abstractmethod
    def transfer_asset_to_swap_margin(
        self, symbol: str, amount: float) -> float:
        pass
    
    @abstractmethod
    def get_margin(
        self, instrument_id: str = ""
    ) -> Union[List[Margin], Margin]:
        pass
    
    @abstractmethod
    def get_position(
        self, instrument_id: str = ""
    ) -> Union[Position, List[Position]]:
        pass
    
    @abstractmethod
    def get_tick(self, instrument_id: str) -> Tick:
        pass
    
    @abstractmethod
    def get_ticks(
        self, *instruments: str, pricing: str = "avg"
    ) -> List[Tick]:
        pass
    
    @abstractmethod
    def get_book(self, instrument_id: str, depth: int) -> OrderBook:
        pass
    
    @abstractmethod
    def get_order_by_client_oid(self, instrument_id, client_order_id) -> Order:
        pass
    
    @staticmethod
    @abstractmethod
    def get_sys_order_type(post: OrderPost) -> str:
        pass
    
    @abstractmethod
    def get_latest_bar(self, instrument_id: str, granularity: int) -> Bar:
        pass
    
    @abstractmethod
    def get_bars(
        self, instrument_id: str, granularity: int, start: datetime,
        end: datetime
    ):
        pass
    
    @abstractmethod
    def set_leverage(self, instrument_id: str, lv: float):
        pass
    
    @abstractmethod
    def get_leverage(self, instrument_id: str) -> float:
        pass
    
    @abstractmethod
    def get_last_n_bars(self, cnt: int, instrument_id: str, granularity: int):
        pass
    
    @abstractmethod
    def get_fill(
        self, instrument_id: str, before: datetime, after: datetime, limit=100
    ):
        pass
    
    @abstractmethod
    def get_available_balance(
        self, symbol: str = ""
    ) -> Union[Dict[str, float], float]:
        pass
    
    @abstractmethod
    def cancel_order(self, instrument_id: str, client_oid: str):
        pass
    
    @abstractmethod
    def cancel_all_orders(self, instrument_id: str):
        pass
    
    @property
    def mode(self) -> str:
        return self._mode
    
    @property
    def exchange(self) -> str:
        return self._exchange
    
    @property
    def market(self) -> str:
        return self._market
    
    @property
    def aid(self):
        return dot_concat(
            self.mode, self.exchange, self.market, self.credential.api_key
        )


def create_adapter(
    mode: str, exchange: str, market: str, credential: Credential
) -> "Adapter":
    return _adapter_registry[(mode, exchange, market)](
        mode, exchange, market, credential
    )
