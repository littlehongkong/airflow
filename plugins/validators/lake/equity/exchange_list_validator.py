import os
import pandera as pa
from pandera import Column, Check
from plugins.validators.base_validator import BaseDataValidator


class ExchangeInfoValidator(BaseDataValidator):
    """
    거래소 List 검증기
    """

    def __init__(self, exchange_code: str, trd_dt: str, data_domain: str, **kwargs):
        super().__init__(exchange_code, trd_dt, data_domain, **kwargs)
        self.schema = self._get_schema()

    # ------------------------------------------------------------------
    # ✅ Pandera 스키마 정의
    # ------------------------------------------------------------------
    @staticmethod
    def _get_schema() -> pa.DataFrameSchema:
        return pa.DataFrameSchema(
            columns={
                "Name": Column(str, nullable=False, checks=Check(lambda s: s.str.len() > 0, error="Empty Name")),
                "Code": Column(str, nullable=False, checks=Check(lambda s: s.str.match(r"^[A-Z0-9]+$")), coerce=True),
                "OperatingMIC": Column(str, nullable=True, coerce=True),
                "Country": Column(str, nullable=False, checks=Check(lambda s: s.str.len() > 0)),
                "Currency": Column(str, nullable=True, checks=Check(lambda s: s.str.len().between(2, 6)), coerce=True),
                "CountryISO2": Column(str, nullable=False, checks=Check(lambda s: s.str.match(r"^[A-Z]{2}$")),
                                      coerce=True),
                "CountryISO3": Column(str, nullable=False, checks=Check(lambda s: s.str.match(r"^[A-Z]{3}$")),
                                      coerce=True),
            },
            strict=False,
            coerce=True,
        )

    # ------------------------------------------------------------------
    # ✅ 검증 실행 (Airflow DAG에서 호출)
    # ------------------------------------------------------------------
    def validate(self, **kwargs):
        allow_empty = kwargs.get("allow_empty", getattr(self, "allow_empty", False))
        return self.run(context=kwargs, allow_empty=allow_empty)