from typing import List, Dict
from plugins.pipelines.base_equity_pipeline import BaseEquityPipeline
from plugins.hooks.eodhd_hook import EODHDHook
from psycopg2.extras import execute_values

class SymbolListPipeline(BaseEquityPipeline):
    """EODHD 거래소별 심볼리스트 수집 파이프라인"""

    def __init__(self, data_domain: str, exchange_code: str, trd_dt: str):
        super().__init__(data_domain=data_domain, exchange_code=exchange_code, trd_dt=trd_dt)
        self.hook = EODHDHook()

    # ----------------------------
    # Fetch
    # ----------------------------
    def fetch(self, exchange_code: str, **kwargs) -> List[Dict]:

        data = self.hook.get_exchange_symbols(exchange_code=exchange_code)

        if not data:
            raise ValueError(f"{exchange_code} 거래소의 심볼 데이터가 없습니다.")
        return data

    # ----------------------------
    # Load
    # ----------------------------
    def load(self, records: List[Dict], **kwargs):
        """
        수집된 일별 종가 데이터를 trd_dt 파티션 아래 JSON Lines 파일로 적재합니다.
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

    # ----------------------------
    # Fetch + Load 통합
    # ----------------------------
    def fetch_and_load(self, **kwargs):
        exchange_code = kwargs.get("exchange_code")
        result = self.fetch(exchange_code=exchange_code)
        if not result:
            return {"record_count": 0}

        # ✅ 데이터 적재
        load_info = self.load(result, exchange_code=exchange_code)
        return self._enforce_record_count(load_info, records=result)

    def validate(self, records: List[Dict], **kwargs) -> bool:
        """심볼 리스트 데이터: 필수 필드 (Code, Name, Exchange) 검증"""
        if not records:
            self.log.warning("⚠️ Symbol List validation: Records list is empty.")
            return False

        required_keys = ['Code', 'Name', 'Country', 'Exchange']

        for i, rec in enumerate(records[:100]):
            for key in required_keys:
                if rec.get(key) is None:
                    raise ValueError(f"Symbol List validation failed (Index {i}): Missing critical key '{key}'.")

            if not isinstance(rec['Code'], str) or not rec['Code'].strip():
                raise ValueError(f"Symbol List validation failed (Index {i}): Invalid or empty 'Code' field.")

        self.log.info(f"✅ SymbolList validation passed for {len(records):,} records.")
        return True