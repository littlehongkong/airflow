from typing import Any, Dict, List
from plugins.hooks.eodhd_hook import EODHDHook
from plugins.pipelines.base_equity_pipeline import BaseEquityPipeline

class ExchangeInfoPipeline(BaseEquityPipeline):
    """
    거래소 데이터 수집 및 저장
    """

    def __init__(self, domain: str, exchange_code: str, trd_dt: str, domain_group: str, allow_empty: bool):
        super().__init__(domain, exchange_code, trd_dt, domain_group=domain_group, allow_empty=allow_empty)
        self.hook = EODHDHook()

    def fetch(self, **kwargs):
        data = self.hook.get_exchanges_list()
        return data
