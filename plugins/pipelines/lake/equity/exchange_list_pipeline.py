from typing import Any, Dict, List
from plugins.hooks.eodhd_hook import EODHDHook
from plugins.pipelines.base_equity_pipeline import BaseEquityPipeline

class ExchangeInfoPipeline(BaseEquityPipeline):
    """
    거래소 데이터 수집 및 저장
    """

    def __init__(self, data_domain: str, exchange_code: str, trd_dt: str):
        super().__init__(data_domain, exchange_code, trd_dt)
        self.hook = EODHDHook()

    def fetch(self, **kwargs):
        data = self.hook.get_ex(exchange_code=self.exchange_code, trd_dt=self.trd_dt)
        return self._standardize_fetch_output(data)
