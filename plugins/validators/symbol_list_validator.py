import os
import pandera as pa
from pandera import Column, Check
from plugins.validators.base_validator import BaseDataValidator


class SymbolListValidator(BaseDataValidator):
    """
    거래소별 Symbol List 검증기
    - 구조: Code, Name, Country, Exchange, Currency, Type, Isin
    """

    def __init__(self, exchange_code: str, trd_dt: str, data_domain: str = "symbol_list"):
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
        print(f"🚀 [SYMBOL LIST] 검증 시작 ({self.exchange_code})")

        df = self._load_records(layer=self.layer)
        if df.empty:
            raise AssertionError("⚠️ 검증 대상 데이터가 비어 있습니다.")

        # Pandera 검증
        try:
            self.schema.validate(df, lazy=True)
            print(f"✅ Pandera 검증 완료: {len(df):,}건")
        except pa.errors.SchemaErrors as err:
            print("❌ Pandera 검증 실패 상세:")
            print(err.failure_cases.head(10))
            raise AssertionError("Pandera 검증 실패")

        # Soda 검증
        self.soda_check_file = os.path.join("/opt/airflow/plugins/soda/checks", "symbol_list_checks.yml")
        self._run_soda(layer=self.layer)

        print(f"🎯 Symbol List 검증 완료 ({self.exchange_code})\n")
