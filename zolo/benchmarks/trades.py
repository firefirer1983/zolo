from ..consts import CLOSE, MAX_FLOAT, LONG
from ..dtypes import Trade
from .base import Benchmark
from typing import Set, Optional
from collections import namedtuple

PnlMarker = namedtuple("Marker", ("trd_id", "max_val", "min_val"))
PnlAccumulator = namedtuple("Accumulator", (
    "profit", "loss", "wins", "losses", "long", "short"
))


def max_pnl_statistics():
    loss_mark = PnlMarker("", 0, MAX_FLOAT)
    profit_mark = PnlMarker("", 0, MAX_FLOAT)

    def _flt(trd: Trade = None):
        nonlocal loss_mark, profit_mark
        if trd:
            if trd.pnl < 0:
                if abs(trd.pnl) > loss_mark.max_val:
                    loss_mark = PnlMarker(
                        trd.trade_id, trd.pnl, loss_mark.min_val)
                if abs(trd.pnl) < loss_mark.min_val:
                    loss_mark = PnlMarker(
                        trd.trade_id, loss_mark.max_val, trd.pnl)
            else:
                if abs(trd.pnl) > profit_mark.max_val:
                    profit_mark = PnlMarker(
                        trd.trade_id, trd.pnl, profit_mark.min_val)
                if abs(trd.pnl) < loss_mark.min_val:
                    profit_mark = PnlMarker(
                        trd.trade_id, profit_mark.max_val, trd.pnl)

        return loss_mark, profit_mark

    return _flt


def accumulate_pnl_statistics():
    acc = PnlAccumulator(0, 0, 0, 0, 0, 0)

    def _flt(trd: Trade = None):
        nonlocal acc
        if trd:
            profit, loss, wins, losses, long, short = \
                acc.profit, acc.loss, acc.wins, acc.losses, acc.long, acc.short

            if trd.pnl < 0:
                loss += trd.pnl
                losses += 1
            else:
                profit += trd.pnl
                wins += 1

            if trd.pos_side == LONG:
                long += 1
            else:
                short += 1
            acc = PnlAccumulator(profit, loss, wins, losses, long, short)
        return acc

    return _flt


class TradeCounter(Benchmark, alias="trades"):

    def __init__(self, api_key: str):
        super().__init__(api_key)
        self._trades: Set[str] = set()
        self._ongoing: Optional[Trade] = None
        self.marker = max_pnl_statistics()
        self.accumulator = accumulate_pnl_statistics()

    def on_trade(self, trade: Trade):
        if trade.status is CLOSE:
            self.process_on_close(trade)
        else:
            self.process_on_going(trade)

    def process_on_going(self, trd: Trade):
        if trd.trade_id not in self._trades:
            self._trades.add(trd.trade_id)

        if not self._ongoing:
            self._ongoing = trd

    def process_on_close(self, trd: Trade):
        self.marker(trd)
        self.accumulator(trd)
        self._ongoing = None

    @property
    def max_profit(self) -> float:
        _, profit = self.marker()
        return profit.max_val

    @property
    def min_profit(self) -> float:
        _, profit = self.marker()
        return profit.min_val

    @property
    def max_loss(self) -> float:
        loss, _ = self.marker()
        return loss.min_val

    @property
    def min_loss(self) -> float:
        _, loss = self.marker()
        return loss.max_val

    @property
    def pnl_ratio(self) -> float:
        try:
            return self.accumulator().profit / self.accumulator().loss
        except ZeroDivisionError:
            return 0

    @property
    def profit(self) -> float:
        return self.accumulator().profit

    @property
    def loss(self) -> float:
        return self.accumulator().loss

    @property
    def net(self) -> float:
        return self.profit - self.loss

    @property
    def wins(self) -> int:
        return self.accumulator().wins

    @property
    def losses(self) -> int:
        return self.accumulator().loss

    @property
    def long_count(self) -> int:
        return self.accumulator().long

    @property
    def short_count(self) -> int:
        return self.accumulator().short

    @property
    def profit_average(self) -> float:
        try:
            return self.accumulator().profit / self.accumulator().wins
        except ZeroDivisionError:
            return 0

    @property
    def loss_average(self):
        try:
            return self.accumulator().loss / self.accumulator().losses
        except ZeroDivisionError:
            return 0

    def __repr__(self):
        return f"Trades(profit:{self.profit}, loss:{self.loss}, " \
               f"net:{self.net} wins:{self.wins}, losses:{self.losses}, " \
               f"longs:" \
               f"{self.long_count}, shorts:{self.short_count}, profit_avg:" \
               f"{self.profit_average}, loss_average:{self.loss_average}, " \
               f"pnl_ratio: {self.pnl_ratio})"
