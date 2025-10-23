from typing import Dict, List
from plugins.hooks.eodhd_hook import EODHDHook
from plugins.pipelines.base_equity_pipeline import BaseEquityPipeline


class ExchangeHolidayPipeline(BaseEquityPipeline):
    """
    EODHD 거래소 휴장일 파이프라인 (Data Lake File 적재 버전)
    - 거래소 기본정보 + 휴장일 정보를 통합 수집 및 개별 JSON 파일로 적재
    """

    def __init__(self, data_domain: str, exchange_code: str, trd_dt: str):
        super().__init__(data_domain=data_domain, exchange_code=exchange_code, trd_dt=trd_dt)
        self.hook = EODHDHook()
        # self.pg_hook = PostgresHook(postgres_conn_id="postgres_default") # 제거

    # ------------------------------------------------------------------
    # 2️⃣ Fetch: EODHD API 호출
    # ------------------------------------------------------------------
    def fetch(self, **kwargs):
        data = self.hook.get_exchange_holidays(exchange_code=self.exchange_code)
        return self._standardize_fetch_output(data)

    # ------------------------------------------------------------------
    # 3️⃣ Load: File Lake 적재 (개별 JSON)
    # ------------------------------------------------------------------
    def load(self, records: List[Dict], **kwargs):
        """
        수집된 거래소 휴장일 정보를 trd_dt 파티션 아래 개별 JSON 파일로 적재합니다.
        (records는 fetch_and_load에서 [fetch_result] 형태로 전달됨)
        """

        # 1. 경로 및 메타데이터 획득 (Base 클래스 호출)
        kwargs['partition_key_name'] = 'trd_dt'
        kwargs['geo_key_name'] = kwargs.get('geo_key_name', 'exchange_code')

        target_dir, base_metadata = self._get_lake_path_and_metadata(
            **kwargs  # 🔴 kwargs 전체를 전달
        )

        file_name = f"{self.data_domain}.jsonl"

        # 3. 공통 적재 함수 호출 (개별 JSON 파일 적재)
        return self._write_records_to_lake(
            records=records,
            target_dir=target_dir,
            base_metadata=base_metadata,
            file_name=file_name,
        )
