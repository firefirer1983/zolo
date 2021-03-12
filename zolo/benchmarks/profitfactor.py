from ..dtypes import Trade
from .base import Benchmark
import logging


log = logging.getLogger(__name__)


class ProfitFactor(Benchmark, alias="profitfactor"):
    
    def __init__(self):
        pass
    
    def on_trade(self, trade: Trade):
        log.info(f"profitfactor on trade: {trade}")
