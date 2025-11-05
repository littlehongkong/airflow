from plugins.validators.base_data_validator import BaseDataValidator
from plugins.config import constants as C
import pandas as pd
import json

class ExchangeHolidayValidator(BaseDataValidator):
    """Exchange Holiday 전용 Validator (flatten + allow_empty 지원)"""

    def __init__(self, domain: str, trd_dt: str, exchange_code: str, vendor: str, allow_empty: bool = False, **kwargs):
        """
        Data Lake 구조:
        /opt/airflow/data/data_lake/raw/exchange_holiday/
            vendor={vendor}/exchange_code={exchange_code}/trd_dt={trd_dt}/exchange_holiday.jsonl
        """
        self.allow_empty = allow_empty
        self.vendor = vendor
        self.exchange_code = exchange_code

        dataset_path = (
            C.DATA_LAKE_ROOT
            / "raw"
            / domain
            / f"vendor={vendor.lower()}"
            / f"exchange_code={exchange_code}"
            / f"trd_dt={trd_dt}"
            / f"{domain}.jsonl"
        )

        super().__init__(
            domain=domain,
            layer="lake",
            trd_dt=trd_dt,
            dataset_path=dataset_path,
            vendor=vendor,
            exchange_code=exchange_code,
            allow_empty=allow_empty,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # 1️⃣ Load + Flatten
    # -------------------------------------------------------------------------
    def _load_dataset(self) -> pd.DataFrame:
        """원본 JSONL 로드 + flatten"""
        try:
            with open(self.dataset_path, "r", encoding="utf-8") as f:
                records = [json.loads(line) for line in f]
        except Exception as e:
            print(f"❌ Failed to read JSONL: {self.dataset_path} ({e})")
            return pd.DataFrame()

        if not records:
            print(f"⚠️ No records found in JSONL file: {self.dataset_path}")
            return pd.DataFrame()

        df = pd.DataFrame(records)

        if "ExchangeHolidays" in df.columns:
            df = self._flatten_exchange_holiday(df)

        return df

    # -------------------------------------------------------------------------
    # 2️⃣ Flatten nested ExchangeHolidays → rows
    # -------------------------------------------------------------------------
    def _flatten_exchange_holiday(self, df: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for _, row in df.iterrows():
            holidays = row.get("ExchangeHolidays", {})
            if not isinstance(holidays, dict) or not holidays:
                continue

            for h in holidays.values():
                rows.append({
                    "exchange_code": row.get("Code") or row.get("exchange_code"),
                    "holiday_name": h.get("Holiday"),
                    "date": h.get("Date"),
                    "type": h.get("Type"),
                    "is_open": row.get("isOpen", False),
                    "country": row.get("Country"),
                    "timezone": row.get("Timezone"),
                })

        if not rows:
            print("⚠️ No holiday rows extracted during flattening.")
            return pd.DataFrame(columns=[
                "exchange_code", "holiday_name", "date", "type", "is_open", "country", "timezone"
            ])

        df_out = pd.DataFrame(rows)

        # ✅ 날짜 형식 변환 (문자열 → datetime)
        if "date" in df_out.columns:
            df_out["date"] = pd.to_datetime(df_out["date"], errors="coerce")

        return df_out
