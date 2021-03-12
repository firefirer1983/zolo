from typing import List, Type, Union
import importlib
from .base import PnlScheme, CommissionScheme, Strategy
from .cfg import AppConfig
from .brokers import BacktestBroker, CryptoBroker, DryrunBroker, BrokerBase
from dataclasses import dataclass
from .runs import BacktestRunner, CryptoRunner, DryRunner
import logging

log = logging.getLogger(__name__)


@dataclass
class AppContext:
    brokers: List[BrokerBase]
    runner_cls: Union[Type[BacktestRunner], Type[CryptoRunner], Type[DryRunner]]
    strategy_cls: Strategy


#
# def instantiate_backtest_context(cfg: AppConfig, pnl: PnlScheme, comm: CommissionScheme) -> AppContext:
#     res = []
#     brk = BacktestBroker(cfg.exchange, pnl, comm)
#     mod, cls = cfg.script.split(".")
#     strategy_cls = getattr(importlib.import_module(mod), cls)
#     for sect in cfg.securities:
#         security_id = brk.register_security(sect.market, sect.instrument_id, sect.credential)
#         log.info(f"{security_id} registered and activated!")
#         for idt in sect.indicators:
#             brk.indicator(idt.type, **idt.parameters)
#         for bch in sect.benchmarks:
#             brk.benchmark(bch)
#     res.append(brk)
#     return AppContext(res, BacktestRunner, strategy_cls)
#
#
# def instantiate_dryrun_context(cfg: AppConfig, pnl: PnlScheme, comm: CommissionScheme) -> AppContext:
#     res = []
#     brk = DryrunBroker(cfg.exchange, pnl, comm)
#     mod, cls = cfg.script.split(".")
#     strategy_cls = getattr(importlib.import_module(mod), cls)
#     for sect in cfg.securities:
#         security_id = brk.register_security(sect.market, sect.instrument_id, sect.credential)
#         log.info(f"{security_id} registered and activated!")
#         for idt in sect.indicators:
#             brk.indicator(idt.type, **idt.parameters)
#         for bch in sect.benchmarks:
#             brk.benchmark(bch)
#     res.append(brk)
#     return AppContext(res, DryRunner, strategy_cls)
#
#
# def instantiate_app_context(cfg: AppConfig) -> AppContext:
#     res = []
#     brk = CryptoBroker(cfg.exchange)
#     mod, cls = cfg.script.split(".")
#     strategy_cls = getattr(importlib.import_module(mod), cls)
#     for sect in cfg.securities:
#         security_id = brk.register_security(sect.market, sect.instrument_id, sect.credential)
#         log.info(f"{security_id} registered and activated!")
#         for idt in sect.indicators:
#             brk.indicator(idt.type, **idt.parameters)
#         for bch in sect.benchmarks:
#             brk.benchmark(bch)
#     res.append(brk)
#     return AppContext(res, CryptoRunner, strategy_cls)
