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

# ---------------------------------
    # 2️⃣ Load (DB Bulk Insert)
    # ---------------------------------
    def load(self, records: List[Dict], **kwargs):
        """
        수집된 데이터를 trd_dt 파티션 아래 JSON Lines 파일로 적재합니다.
        """

        kwargs['partition_key_name'] = 'trd_dt'
        kwargs['geo_key_name'] = kwargs.get('geo_key_name', 'exchange_code')  # 기본값 유지

        target_dir, base_metadata = self._get_lake_path_and_metadata(
            **kwargs  # 🔴 kwargs 전체를 전달
        )

        file_name = f"{self.data_domain}.jsonl"

        # 🔴 2. 공통 적재 함수 호출: 파일명 문자열을 직접 전달하여 함수 정의를 생략
        return self._write_records_to_lake(
            records=records,
            target_dir=target_dir,
            base_metadata=base_metadata,
            file_name=file_name,
        )