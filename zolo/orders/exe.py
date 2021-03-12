import time
import logging
from dataclasses import replace
from datetime import datetime
from typing import Union, List, Optional, Dict, Tuple

from uuid import uuid4

from ..dtypes import Lot, Size, by_lot, by_size
from ..consts import BLOCKING, NON_BLOCKING, PERIODIC, DEFAULT_LEVERAGE
from .base import OrderExecutor
from ..adapters import Adapter
from ..dtypes import Order, OrderStatus
from ..exceptions import OrderGetError
from ..posts import MarketOrder, OpponentOrder, OpponentIocOrder, OpponentFokOrder, OptimalOrder, OptimalIocOrder, \
    OptimalFokOrder, LimitOrder, LimitFokOrder, LimitIocOrder

log = logging.getLogger(__name__)


class BlockingExecutor(OrderExecutor, mode=BLOCKING):
    def __init__(
        self,
        post: Union[
            MarketOrder,
            LimitFokOrder,
            LimitIocOrder,
            OpponentFokOrder,
            OpponentIocOrder,
            OptimalIocOrder,
            OptimalFokOrder,
        ],
        adapter: Adapter,
        ts: datetime,
        timeout: float = 12,
        retry: int = 10,
        refresh_interval: float = 0.5,
    ):
        assert type(post) in (
            MarketOrder,
            LimitFokOrder,
            LimitIocOrder,
            OpponentFokOrder,
            OpponentIocOrder,
            OptimalIocOrder,
            OptimalFokOrder,
        )
        super().__init__(post, adapter, timeout)
        self._retry = retry
        self._refresh_interval = refresh_interval
        self._result: Optional[Order] = None
        self._begin_ts = ts
        self._post = post
        self._client_oid = self.adapter.create_order(post)
        self._refresh_interval = refresh_interval

    def result(self, ts: datetime):
        if self.finished:
            return self._result

        while self._retry:
            time.sleep(self._refresh_interval)
            if self.timeout and (ts - self._begin_ts).seconds >= self.timeout:
                break

            try:
                res = self.adapter.get_order_by_client_oid(
                    self._post.instrument_id, self.client_oid
                )
            except Exception as e:
                log.exception(e)
                continue
            finally:
                self._retry -= 1

            if res.done:
                self._result = res
                return self._result

        raise TimeoutError

    @property
    def finished(self) -> bool:
        return self._result and self._result.done


class NonBlockingExecutor(OrderExecutor, mode=NON_BLOCKING):
    def __init__(
        self,
        post: Union[LimitOrder, OpponentOrder, OptimalOrder],
        adapter: Adapter,
        ts: datetime,
        timeout: float = 0,
    ):
        super().__init__(post, adapter, timeout)
        self._result: Optional[Order] = None
        self._ts = ts
        assert type(post) in (LimitOrder, OpponentOrder, OptimalOrder)
        self._post = post
        self._client_oid = self.adapter.create_order(self._post)

    def result(self, ts: datetime) -> Order:

        if self.finished:
            return self._result

        try:
            self._result = self.adapter.get_order_by_client_oid(
                self._post.instrument_id, self.client_oid
            )
        except OrderGetError as e:
            log.exception(e)
            self._result = Order(
                exchange=self._post.exchange,
                market=self.adapter.market,
                instrument_id=self._post.instrument_id,
                client_oid=self.client_oid,
                created_at=datetime.utcnow(),
                side=self._post.side,
                pos_side=self._post.pos_side,
                leverage=DEFAULT_LEVERAGE,
                state=OrderStatus.UNKNOWN,
                account=self.adapter.credential.api_key,
                qty=0,
                slippage=0,
                price=0,
                contract_size=0,
                order_type=self._post.order_type,
            )
        finally:
            if self.timeout and (ts - self._ts).seconds > self.timeout:
                raise TimeoutError
        return self._result

    @property
    def finished(self) -> bool:
        return self._result and self._result.done


class PeriodicExecutor(OrderExecutor, mode=PERIODIC):
    def __init__(
        self,
        post: Union[
            MarketOrder,
            OptimalIocOrder,
            OptimalFokOrder,
            OpponentIocOrder,
            OpponentFokOrder,
        ],
        adapter: Adapter,
        period: float,
        step: Union[float, Size, Lot],
        ts: datetime,
        timeout: float = 0,
    ):
        super().__init__(post, adapter, timeout)
        self._period = period
        self._ts = ts
        self._step = step
        self._result: Dict[str, Order] = dict()
        self._client_oid = uuid4().hex
        assert type(post) in (
            MarketOrder,
            OptimalIocOrder,
            OptimalFokOrder,
            OpponentIocOrder,
            OpponentFokOrder,
        )
        self._post = post
        self._filled = 0

    def result(self, ts: datetime) -> Tuple[Order]:
        if (ts - self._ts).seconds < self._period or self.finished:
            return tuple(self._result.values())

        step = self._step
        if by_size(step):
            step = self.adapter.estimate_lot(self._post.instrument_id, step)

        left = self._post.qty - self._filled
        if by_size(self._post.qty):
            left = (
                self.adapter.estimate_lot(self._post.instrument_id, self._post.qty)
                - self._filled
            )

        if step > left:
            step = left

        exe = BlockingExecutor(replace(self._post, qty=step), self.adapter, ts=ts)
        res = exe.result(ts)
        self._result[res.client_oid] = res
        self._filled += res.qty

        return tuple(self._result.values())

    @property
    def finished(self) -> bool:
        if by_lot(self._post.qty):
            left_over = self._post.qty - self._filled
        elif by_size(self._post.qty):
            left_over = (
                    self.adapter.estimate_lot(self._post.instrument_id, self._post.qty)
                    - self._filled
            )
        else:
            left_over = self._post.qty - self._filled
        # log.info(f"left over: {left_over}")
        return (
                left_over
                <= self.adapter.get_instrument_info(self._post.instrument_id).min_size
        )
