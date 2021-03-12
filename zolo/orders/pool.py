from datetime import datetime
from typing import List

from ..consts import UNIX_EPOCH
from ..adapters import Adapter
from .base import OrderExecutor


class OrderPool:
    def __init__(self, adapter: Adapter):
        self._history: List[OrderExecutor] = list()
        self._adapter = adapter
        self._ts = UNIX_EPOCH
        self._pending: List[OrderExecutor] = list()

    def add_to_pool(self, entry: OrderExecutor):
        self._history.append(entry)
        self._pending.append(entry)

    def refresh(self, ts: datetime):
        self._ts = ts
        pending = list()
        for entry in self._pending:
            entry.result(self._ts)
            if entry.finished:
                continue
            pending.append(entry)
        self._pending = pending

    def get_pending(self) -> List[str]:
        return [entry.client_oid for entry in self._pending]

    def get_order(self, client_oid: str, instrument_id: str):
        for entry in self._history:
            if entry.client_oid == client_oid:
                if self._ts == UNIX_EPOCH:
                    return entry.result(datetime.utcnow())
                return entry.result(self._ts)
        # return self._adapter.get_order_by_client_oid(instrument_id, client_oid)
    
    def get_ts(self) -> datetime:
        if self._ts == UNIX_EPOCH:
            return datetime.utcnow()
        return self._ts
