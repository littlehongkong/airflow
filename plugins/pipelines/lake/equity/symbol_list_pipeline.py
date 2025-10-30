from typing import List, Dict
from plugins.pipelines.base_equity_pipeline import BaseEquityPipeline
from plugins.hooks.eodhd_hook import EODHDHook
from psycopg2.extras import execute_values

class SymbolListPipeline(BaseEquityPipeline):
    """EODHD 거래소별 심볼리스트 수집 파이프라인"""

    def __init__(self, data_domain: str, exchange_code: str, trd_dt: str):
        super().__init__(data_domain=data_domain, exchange_code=exchange_code, trd_dt=trd_dt)
        self.hook = EODHDHook()

    def fetch(self, **kwargs):
        data = self.hook.get_exchange_symbols(exchange_code=self.exchange_code)
        return data