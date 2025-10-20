import os
import json
import pandera as pa
from pandera import Column, Check
import pandas as pd
from plugins.validators.base_validator import BaseDataValidator



class ExchangeHolidayValidator(BaseDataValidator):
    """
    거래소별 휴장일(ExchangeHoliday) 검증기
    - 구조: Holiday, Date, Type
    - 상위 필드: Name, Code, Country, Currency, Timezone
    """

    def __init__(self, exchange_code: str, trd_dt: str, data_domain: str = "exchange_holidays"):
        super().__init__(exchange_code, trd_dt, data_domain)
        self.schema = self._get_schema()

    # ------------------------------------------------------------------
    # ✅ Pandera 스키마 정의
    # ------------------------------------------------------------------
    @staticmethod
    def _get_schema() -> pa.DataFrameSchema:
        return pa.DataFrameSchema(
            columns={
                "Code": Column(str, nullable=False, checks=Check(lambda s: s.str.len() > 0)),
                "Country": Column(str, nullable=False),
                "Currency": Column(str, nullable=False, checks=Check(lambda s: s.str.len() == 3)),
                "Timezone": Column(str, nullable=False),
                "Holiday": Column(str, nullable=False),
                "Date": Column(str, nullable=False, checks=Check(lambda s: s.str.match(r"^\d{4}-\d{2}-\d{2}$"))),
                "Type": Column(str, nullable=False),
            },
            coerce=True,
        )

    # ------------------------------------------------------------------
    # ✅ ExchangeHolidays 중첩 데이터 정규화
    # ------------------------------------------------------------------
    def _flatten_exchange_holidays(self, data: dict) -> pd.DataFrame:
        """
        API 구조에서 ExchangeHolidays 필드를 DataFrame으로 변환
        """
        if not data or "ExchangeHolidays" not in data:
            raise ValueError("⚠️ 'ExchangeHolidays' 필드가 누락되었습니다.")

        records = []
        for _, item in data["ExchangeHolidays"].items():
            records.append({
                "Code": data.get("Code"),
                "Country": data.get("Country"),
                "Currency": data.get("Currency"),
                "Timezone": data.get("Timezone"),
                "Holiday": item.get("Holiday"),
                "Date": item.get("Date"),
                "Type": item.get("Type"),
            })

        return pd.DataFrame(records)

    # ------------------------------------------------------------------
    # ✅ 검증 실행 (Airflow DAG에서 호출)
    # ------------------------------------------------------------------
    def validate(self, **kwargs):
        print(f"🚀 [EXCHANGE HOLIDAY] 검증 시작 ({self.exchange_code})")

        # 1️⃣ 파일 로드 (raw 경로 기준)
        target_dir = self._get_lake_path(layer=self.layer)
        files = [f for f in os.listdir(target_dir) if f.endswith(".json") or f.endswith(".jsonl")]
        if not files:
            raise AssertionError(f"⚠️ 휴장일 데이터 파일이 없습니다: {target_dir}")

        file_path = os.path.join(target_dir, files[0])
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        df = self._flatten_exchange_holidays(data)
        print(f"✅ 데이터 변환 완료: {len(df):,}건")

        # 2️⃣ Pandera 검증
        try:
            self.schema.validate(df, lazy=True)
            print("✅ Pandera 검증 완료")
        except pa.errors.SchemaErrors as err:
            print("❌ Pandera 검증 실패 상세:")
            print(err.failure_cases.head(10))
            raise AssertionError("Pandera 검증 실패")

        # 3️⃣ Soda 검증
        self.soda_check_file = os.path.join("/opt/airflow/plugins/soda/checks", "exchange_holiday_checks.yml")
        self._run_soda(layer=self.layer)

        print(f"🎯 Exchange Holiday 검증 완료 ({self.exchange_code})\n")
