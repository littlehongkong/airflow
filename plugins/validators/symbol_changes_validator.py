import os
import pandera.pandas as pa
from pandera import Column, Check
import pandas as pd
from plugins.validators.base_validator import BaseDataValidator


class SymbolChangeValidator(BaseDataValidator):
    """
    거래소별 Symbol Change 검증기
    - 데이터가 없을 경우 gracefully skip
    """

    def __init__(self, exchange_code: str, trd_dt: str, data_domain: str = "symbol_changes", **kwargs):
        super().__init__(exchange_code, trd_dt, data_domain, **kwargs)
        self.schema = self._get_schema()

    @staticmethod
    def _get_schema() -> pa.DataFrameSchema:
        return pa.DataFrameSchema(
            columns={
                "exchange": Column(str, nullable=False),
                "old_symbol": Column(str, nullable=False),
                "new_symbol": Column(str, nullable=False),
                "company_name": Column(str, nullable=False),
                "effective": Column(str, nullable=False, checks=Check(lambda s: s.str.match(r"^\d{4}-\d{2}-\d{2}$"))),
            },
            coerce=True,
        )

    def validate(self, **kwargs):
        allow_empty = kwargs.get("allow_empty", getattr(self, "allow_empty", False))
        return self.run(context=kwargs, allow_empty=allow_empty)