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

    def __init__(self, exchange_code: str, trd_dt: str, data_domain: str = "exchange_holidays", **kwargs):
        super().__init__(exchange_code, trd_dt, data_domain, **kwargs)
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
        allow_empty = kwargs.get("allow_empty", getattr(self, "allow_empty", False))
        self.run(context=kwargs, allow_empty=allow_empty)