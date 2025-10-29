import os
import pandera.pandas as pa
from pandera import Column, Check
from plugins.validators.base_validator import BaseDataValidator



class SymbolListValidator(BaseDataValidator):
    """
    거래소별 Symbol List 검증기
    - 구조: Code, Name, Country, Exchange, Currency, Type, Isin
    """

    def __init__(self, exchange_code: str, trd_dt: str, data_domain: str = "symbol_list", **kwargs):
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
                "Name": Column(str, nullable=False, checks=Check(lambda s: s.str.len() > 0)),
                "Country": Column(str, nullable=False, checks=Check(lambda s: s.str.len() > 0)),
                "Exchange": Column(str, nullable=False, checks=Check(lambda s: s.str.len() > 0)),
                "Currency": Column(str, nullable=False, checks=Check(lambda s: s.str.len() == 3)),
                "Type": Column(str, nullable=False),
                "Isin": Column(str, nullable=True, checks=Check(lambda s: s.str.match(r"^[A-Za-z]{2}[0-9A-Za-z]{9}[0-9]$") | s.isna()))
            },
            coerce=True
        )

    # ------------------------------------------------------------------
    # ✅ 검증 실행 (Airflow DAG에서 호출)
    # ------------------------------------------------------------------
    def validate(self, **kwargs):
        allow_empty = kwargs.get("allow_empty", getattr(self, "allow_empty", False))
        return self.run(context=kwargs, allow_empty=allow_empty)