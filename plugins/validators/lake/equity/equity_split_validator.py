# plugins/validators/split_validator.py
import os
import json
import tempfile
import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check
from plugins.validators.base_validator import BaseDataValidator
from soda.scan import Scan
import yaml

class EquitySplitValidator(BaseDataValidator):
    """거래소별 액면분할(Splits) 데이터 검증 Validator"""

    def __init__(self, exchange_code: str, trd_dt: str, data_domain: str = "splits", **kwargs):
        super().__init__(exchange_code, trd_dt, data_domain, **kwargs)
        self.schema = self._get_schema()

    # ============================================================
    # 1️⃣ Pandera 스키마 정의
    # ============================================================
    def _get_schema(self):
        # eod-bulk-last-day ?type=splits 결과: code, exchange, date, split
        return pa.DataFrameSchema(
            columns={
                "code": Column(str, nullable=False, coerce=True),
                "exchange": Column(str, nullable=False, coerce=True),
                "date": Column(str, nullable=False, coerce=True, checks=[
                    # YYYY-MM-DD 포맷 검증
                    Check(lambda s: pd.to_datetime(s, format="%Y-%m-%d", errors="coerce").notna(),
                          error="❌ 'date' must be YYYY-MM-DD"),
                ]),
                "split": Column(str, nullable=False, coerce=True, checks=[
                    # "A/B" 형태(소수 허용) 검증
                    Check.str_matches(r"^\d+(\.\d+)?/\d+(\.\d+)?$",
                                      error="❌ 'split' must look like '1/10' or '1.000000/10.000000'")
                ]),
            },
            strict=False,
        )

    # ============================================================
    # 2️⃣ 전체 검증 실행
    # ============================================================
    def validate(self, **kwargs):
        allow_empty = kwargs.get("allow_empty", getattr(self, "allow_empty", False))
        return self.run(context=kwargs, allow_empty=allow_empty)