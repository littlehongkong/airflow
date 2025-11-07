from plugins.validators.base_data_validator import BaseDataValidator
from plugins.config import constants as C
import pandas as pd
import json

class ExchangeHolidayValidator(BaseDataValidator):
    """Exchange Holiday 전용 Validator (nested JSON → string 변환 + allow_empty 지원)"""

    def __init__(self, domain: str, domain_group: str, trd_dt: str, exchange_code: str, vendor: str, allow_empty: bool = False, **kwargs):
        """
        Data Lake 구조:
        /opt/airflow/data/data_lake/raw/equity/exchange_holiday/
            vendor={vendor}/exchange_code={exchange_code}/trd_dt={trd_dt}/exchange_holiday.jsonl
        """
        self.allow_empty = allow_empty
        self.vendor = vendor
        self.exchange_code = exchange_code
        self.domain_group = domain_group

        dataset_path = (
            C.DATA_LAKE_ROOT
            / "raw"
            / domain_group
            / domain
            / f"vendor={vendor.lower()}"
            / f"exchange_code={exchange_code}"
            / f"trd_dt={trd_dt}"
            / f"{domain}.jsonl"
        )

        super().__init__(
            domain=domain,
            domain_group=domain_group,
            layer="lake",
            trd_dt=trd_dt,
            dataset_path=dataset_path,
            vendor=vendor,
            exchange_code=exchange_code,
            allow_empty=allow_empty,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # 1️⃣ Load JSONL (원본 유지)
    # -------------------------------------------------------------------------
    def _load_dataset(self) -> pd.DataFrame:
        try:
            self.log.info(f"📂 Loading JSONL: {self.dataset_path}")
            with open(self.dataset_path, "r", encoding="utf-8") as f:
                records = [json.loads(line) for line in f]
        except FileNotFoundError:
            self.log.error(f"🚫 Exchange detail file missing: {self.dataset_path}")
            return pd.DataFrame()

        if not records:
            self.log.warning(f"⚠️ Empty exchange detail for {self.exchange_code}")
            return pd.DataFrame()

        df = pd.DataFrame(records)
        if df.empty:
            self.log.warning(f"⚠️ No records in exchange detail for {self.exchange_code}")
            return df

        # ✅ 모든 nested dict 컬럼을 JSON 문자열로 변환 (Parquet 호환)
        df = self._normalize_nested_fields(df)

        return df

    # -------------------------------------------------------------------------
    # 2️⃣ Nested dict → JSON 문자열 변환
    # -------------------------------------------------------------------------
    def _normalize_nested_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parquet 저장 시 PyArrow struct 오류 방지용
        - nested dict를 문자열로 직렬화
        - 빈 dict도 '{}' 문자열로 강제 변환
        """
        nested_cols = ["ExchangeHolidays", "ExchangeEarlyCloseDays", "TradingHours"]
        for col in nested_cols:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: (
                        json.dumps(x if x else {}, ensure_ascii=False)
                        if isinstance(x, (dict, list))
                        else x
                    )
                )
        return df
