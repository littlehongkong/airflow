from typing import Dict, List
from plugins.hooks.eodhd_hook import EODHDHook
from plugins.pipelines.base_equity_pipeline import BaseEquityPipeline


class ExchangeHolidayPipeline(BaseEquityPipeline):
    """
    EODHD 거래소 휴장일 파이프라인 (Data Lake File 적재 버전)
    - 거래소 기본정보 + 휴장일 정보를 통합 수집 및 개별 JSON 파일로 적재
    """

    def __init__(self, domain: str, exchange_code: str, trd_dt: str, domain_group: str):
        super().__init__(domain=domain, exchange_code=exchange_code, trd_dt=trd_dt, domain_group=domain_group)
        self.hook = EODHDHook()
        # self.pg_hook = PostgresHook(postgres_conn_id="postgres_default") # 제거

    # ------------------------------------------------------------------
    # 2️⃣ Fetch: EODHD API 호출
    # ------------------------------------------------------------------
    def fetch(self, **kwargs):
        data = self.hook.get_exchange_holidays(exchange_code=self.exchange_code)
        return data
