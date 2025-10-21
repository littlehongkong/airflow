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

    def fetch(self, exchange_code: str, trd_dt: str) -> List[Dict[str, Any]]:
        data = self.hook.get_dividends(exchange_code=exchange_code, trd_dt=trd_dt)
        if isinstance(data, list):
            return data
        self.log.warning("⚠️ Dividend API 응답이 list 형식이 아닙니다.")
        return []

    def fetch_and_load(self, **kwargs):
        exchange_code = kwargs.get("exchange_code")
        trd_dt = kwargs.get("trd_dt")
        result = self.fetch(exchange_code=exchange_code, trd_dt=trd_dt)
        if not result:
            self.log.info(f"ℹ️ {exchange_code} Dividend 데이터 없음 ({trd_dt})")
            return {"record_count": 0}

        load_info = self.load(result, exchange_code=exchange_code, trd_dt=trd_dt)
        return self._enforce_record_count(load_info, records=result)


# ---------------------------------
    # 2️⃣ Load (DB Bulk Insert)
    # ---------------------------------
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