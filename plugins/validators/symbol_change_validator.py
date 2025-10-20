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

    def __init__(self, exchange_code: str, trd_dt: str, data_domain: str = "symbol_change"):
        super().__init__(exchange_code, trd_dt, data_domain)
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
        print(f"🚀 [SYMBOL CHANGE] 검증 시작 ({self.exchange_code})")

        # 1️⃣ 데이터 로드
        try:
            df = self._load_records(layer=self.layer)
        except AssertionError as e:
            print(f"⚠️ 데이터 로드 실패 또는 파일 없음: {e}")
            return

        # 2️⃣ 빈 데이터셋인 경우 graceful skip
        if df.empty or len(df) == 0:
            print(f"⚠️ {self.exchange_code} 거래소의 Symbol Change 데이터가 비어있어 검증을 건너뜁니다.")
            return

        # 3️⃣ Pandera 검증
        try:
            self.schema.validate(df, lazy=True)
            print(f"✅ Pandera 검증 완료: {len(df):,}건")
        except pa.errors.SchemaErrors as err:
            print("❌ Pandera 검증 실패 상세:")
            print(err.failure_cases.head(10))
            raise AssertionError("Pandera 검증 실패")

        # 4️⃣ Soda 검증 (선택적)
        self.soda_check_file = os.path.join("/opt/airflow/plugins/soda/checks", "symbol_change_checks.yml")
        if os.path.exists(self.soda_check_file):
            self._run_soda(layer=self.layer)
        else:
            print("⚠️ Soda 체크파일 없음, 건너뜁니다.")

        print(f"🎯 Symbol Change 검증 완료 ({self.exchange_code})\n")
