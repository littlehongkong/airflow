from plugins.hooks.eodhd_hook import EODHDHook
from plugins.pipelines.base_equity_pipeline import BaseEquityPipeline
from psycopg2.extras import execute_values
from typing import List, Dict

class EquityPricePipeline(BaseEquityPipeline):
    """
    EODHD 일별 종가 데이터 파이프라인
    - 국가 및 거래소 단위로 수집
    - Bulk Insert 기반으로 고속 적재 수행
    """

    def __init__(self, data_domain: str, exchange_code: str, trd_dt: str):
        super().__init__(data_domain=data_domain, exchange_code=exchange_code, trd_dt=trd_dt)
        self.hook = EODHDHook()


    # ---------------------------------
    # 1️⃣ Fetch (API 호출)
    # ---------------------------------
    def fetch(self, exchange_code: str = None, trd_dt: str = None, **kwargs):
        """
        EODHD에서 종가 데이터 수집
        """
        # 국가 단위 또는 거래소 단위 구분 처리
        data = self.hook.get_prices(trd_dt=trd_dt, exchange_code=exchange_code)

        if not data:
            raise ValueError(f"{exchange_code} {trd_dt} 데이터가 없습니다.")

        return data


    # -----------------------------------------
    # 🔹 확장용 Template Method (선택적 override)
    # -----------------------------------------
    def fetch_and_load(self, **kwargs):
        exchange_code = kwargs.get("exchange_code")
        trd_dt = kwargs.get("trd_dt")
        result = self.fetch(exchange_code=exchange_code, trd_dt=trd_dt)
        if not result:
            return {"record_count": 0}

        # ✅ 데이터 적재
        load_info = self.load(result, exchange_code=exchange_code)
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

    # ---------------------------------
    # 3️⃣ Validate (데이터 검증)
    # ---------------------------------
    def validate(self, records: List[Dict], **kwargs) -> bool:
        """시세 데이터: 필수 필드, 데이터 타입, 비즈니스 규칙(High > Low) 검증"""
        if not records:
            self.log.warning("⚠️ Price validation: Records list is empty.")
            return False

        required_keys = ['code', 'date', 'close', 'high', 'low', 'volume', 'MarketCapitalization']

        for i, rec in enumerate(records[:100]):  # 첫 100개만 샘플링
            # 1. 필수 키 검증
            for key in required_keys:
                if rec.get(key) is None:
                    raise ValueError(f"Price validation failed (Index {i}): Missing key '{key}'.")

            # 2. 타입 및 논리 검증: High >= Low, 가격 > 0
            try:
                high = float(rec['high'])
                low = float(rec['low'])
                close = float(rec['close'])
            except (ValueError, TypeError):
                raise ValueError(f"Price validation failed (Index {i}): Price fields are not numeric.")

            if high < low:
                raise ValueError(
                    f"Price validation failed (Index {i}): High price ({high}) is less than Low price ({low}).")
            if close <= 0:
                raise ValueError(f"Price validation failed (Index {i}): Close price ({close}) must be positive.")

        self.log.info(f"✅ EquityPrice validation passed for {len(records):,} records.")
        return True
