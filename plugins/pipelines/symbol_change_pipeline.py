# plugins/pipelines/symbol_change_pipeline.py
from datetime import datetime
from typing import Any, Dict, List, Tuple

from plugins.pipelines.base_equity_pipeline import BaseEquityPipeline
from plugins.hooks.eodhd_hook import EODHDHook
from psycopg2.extras import execute_values
import json

class SymbolChangePipeline(BaseEquityPipeline):
    """
    거래소별 심볼 변경 이력 수집 파이프라인
    - EODHD: /api/symbol-change-history/{EXCHANGE_CODE}
    - 결과는 symbol_change_history 테이블에 upsert
    """

    def __init__(self, data_domain: str, exchange_code: str, trd_dt: str):
        super().__init__(data_domain=data_domain, exchange_code=exchange_code, trd_dt=trd_dt)
        self.hook = EODHDHook()

    # ---- Fetch -------------------------------------------------
    def fetch(self, trd_dt: str, **kwargs) -> List[Dict[str, Any]]:
        """
        심볼 변경 이력 수집
        """
        data = self.hook.get_symbol_changes(trd_dt=trd_dt)
        if not data:
            self.log.info(f"[{trd_dt}] 변경 이력 없음")
            return []
        return data

    # ---- Load --------------------------------------------------
    def load(self, records: List[Dict], **kwargs):
        """
        수집된 일별 종가 데이터를 trd_dt 파티션 아래 JSON Lines 파일로 적재합니다.
        """

        kwargs['exchange_code'] = 'US'
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

    def fetch_and_load(self, **kwargs):
        trd_dt = kwargs.get("trd_dt")
        result = self.fetch(trd_dt=trd_dt)
        if not result:
            return {"record_count": 0}

        # ✅ 데이터 적재
        load_info = self.load(result, trd_dt=trd_dt)
        return self._enforce_record_count(load_info, records=result)


    # ------------------------------------------------------------------
    # Validate (데이터 검증)
    # ------------------------------------------------------------------
    def validate(self, records: List[Dict], **kwargs) -> bool:
        """심볼 변경 이력: 필수 필드 (OldCode, NewCode, EffectiveDate) 검증"""
        if not records:
            self.log.info("ℹ️ Symbol Change validation: No records found (may be normal).")
            return True

        exchange_code = kwargs.get("exchange_code", "N/A")
        required_keys = ['OldCode', 'NewCode', 'EffectiveDate', 'Exchange']

        for i, rec in enumerate(records[:100]):
            for key in required_keys:
                if rec.get(key) is None:
                    raise ValueError(f"Symbol Change validation failed (Index {i}): Missing critical key '{key}'.")

            # 날짜 형식 검증 (유효한 ISO 형식인지 확인)
            try:
                datetime.fromisoformat(str(rec['EffectiveDate']).split('T')[0])
            except ValueError:
                raise ValueError(f"Symbol Change validation failed (Index {i}): Invalid 'EffectiveDate' format.")

        self.log.info(f"✅ Symbol Change validation passed for {len(records):,} records.")
        return True