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
    def fetch(self, exchange_code: str, **kwargs) -> Dict:
        """
        거래소 휴장일 및 기본정보 조회 (단일 Dict 객체 반환)
        """
        # ... (API 호출 로직은 이전과 동일)
        data = self.hook.get_exchange_holidays(exchange_code)
        if not data:
            raise ValueError(f"{exchange_code} 거래소의 휴장일 데이터가 없습니다.")

        return data

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

    # ------------------------------------------------------------------
    # 4️⃣ Fetch + Load 통합
    # ------------------------------------------------------------------
    def fetch_and_load(self, **kwargs):
        exchange_code = kwargs.get("exchange_code")
        result = self.fetch(exchange_code=exchange_code)

        if not result:
            return {"exchange_code": exchange_code, "status": "empty"}

        # ✅ 데이터 적재
        load_info = self.load([result], exchange_code=exchange_code)
        return self._enforce_record_count(load_info, records=[result])

    def validate(self, records: List[Dict], **kwargs) -> bool:
        """휴장일 데이터: 단일 레코드, 필수 필드 (Code, ExchangeHolidays) 검증"""
        if not records or len(records) != 1:
            raise ValueError(f"Holiday validation failed: Expected exactly 1 record, got {len(records)}.")

        rec = records[0]
        exchange_code = kwargs.get('exchange_code', 'N/A')

        # 1. 필수 식별자 및 기본 정보 검증
        if rec.get("Code") != exchange_code:
            raise ValueError(f"Holiday validation failed: Expected Code '{exchange_code}', got '{rec.get('Code')}'.")

        # 2. 휴장일 목록 검증 (Dict 형태)
        holidays = rec.get("ExchangeHolidays")
        if not holidays:
            self.log.info(f"ℹ️ Exchange {exchange_code} reports no holidays.")
            return True

        if not isinstance(holidays, dict):
            raise ValueError(
                f"Holiday validation failed (Exchange {exchange_code}): 'ExchangeHolidays' is not a dictionary.")

        # 3. 휴장일 개별 항목 구조 검증
        sample_holiday = next(iter(holidays.values()), None)
        if sample_holiday:
            required_holiday_keys = ['Holiday', 'Date', 'Type']
            for key in required_holiday_keys:
                if sample_holiday.get(key) is None:
                    raise ValueError(
                        f"Holiday validation failed (Exchange {exchange_code}): Sample holiday missing key '{key}'.")

        self.log.info(f"✅ Exchange Holiday validation passed for {exchange_code}.")
        return True