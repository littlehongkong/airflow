# plugins/pipelines/equity_dividend_pipeline.py
from datetime import datetime
from typing import Any, Dict, List
from plugins.hooks.eodhd_hook import EODHDHook
from plugins.pipelines.base_equity_pipeline import BaseEquityPipeline

class EquityDividendPipeline(BaseEquityPipeline):
    """
    /eod-bulk-last-day/{EXCHANGE}?type=dividends API 기반
    일일 배당(Dividend) 데이터 수집 및 저장
    """

    def __init__(self, data_domain: str, exchange_code: str, trd_dt: str):
        super().__init__(data_domain, exchange_code, trd_dt)
        self.hook = EODHDHook()

    def fetch(self, **kwargs):
        data = self.hook.get_dividends(exchange_code=self.exchange_code, trd_dt=self.trd_dt)
        return self._standardize_fetch_output(data)