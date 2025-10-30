from plugins.hooks.eodhd_hook import EODHDHook
from plugins.pipelines.base_equity_pipeline import BaseEquityPipeline
from typing import List, Dict


class EquityPricePipeline(BaseEquityPipeline):
    """
    EODHD 일별 종가 데이터 파이프라인
    - 국가 및 거래소 단위로 수집
    - BaseEquityPipeline.fetch_and_load() 공통화 구조 적용
    """

    def __init__(self, data_domain: str, exchange_code: str, trd_dt: str):
        super().__init__(data_domain=data_domain, exchange_code=exchange_code, trd_dt=trd_dt)
        self.hook = EODHDHook()


    def fetch(self, **kwargs):
        data = self.hook.get_prices(exchange_code=self.exchange_code, trd_dt=self.trd_dt)
        return data
