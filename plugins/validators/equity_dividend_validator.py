# plugins/validators/dividend_validator.py
import os
import json
import tempfile
import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check
from plugins.validators.base_validator import BaseDataValidator
from soda.scan import Scan
import yaml
import numpy as np


class EquityDividendValidator(BaseDataValidator):
    """거래소별 배당(Dividends) 데이터 검증 Validator"""

    def __init__(self, exchange_code: str, trd_dt: str, data_domain: str = "dividends"):
        super().__init__(exchange_code, trd_dt, data_domain)
        self.schema = self._get_schema()

    # ============================================================
    # 1️⃣ Pandera 스키마 정의
    # ============================================================
    def _get_schema(self):
        return pa.DataFrameSchema(
            columns={
                "code": Column(
                    str,
                    nullable=False,
                    checks=[
                        Check(lambda s: s.str.len() > 0,
                              error="❌ code는 빈 문자열일 수 없음"),
                    ]
                ),
                "date": Column(
                    str,
                    nullable=False,
                    checks=[
                        Check(lambda s: s.str.match(r'^\d{4}-\d{2}-\d{2}$').all(),
                              error="❌ date 형식 오류 (YYYY-MM-DD 형식이어야 함)"),
                        Check(lambda s: pd.to_datetime(s, errors='coerce').notna().all(),
                              error="❌ date가 유효한 날짜가 아님"),
                    ]
                ),
                "dividend": Column(
                    str,
                    nullable=False,
                    checks=[
                        Check(lambda s: s.str.match(r'^\d+(\.\d+)?$').all(),
                              error="❌ dividend 형식 오류 (숫자 형식이어야 함)"),
                        Check(lambda s: s.astype(float) > 0,
                              error="❌ dividend는 양수여야 함"),
                    ]
                ),
                "currency": Column(
                    str,
                    nullable=False,
                    checks=[
                        Check(lambda s: s.str.match(r'^[A-Z]{3}$').all(),
                              error="❌ currency 형식 오류 (3자리 대문자 통화 코드)"),
                    ]
                ),
                "declarationDate": Column(
                    str,
                    nullable=True,
                    checks=[
                        Check(lambda s: s.isna() | s.str.match(r'^\d{4}-\d{2}-\d{2}$'),
                              error="❌ declarationDate 형식 오류 (YYYY-MM-DD 또는 null)"),
                    ]
                ),
                "recordDate": Column(
                    str,
                    nullable=True,
                    checks=[
                        Check(lambda s: s.isna() | s.str.match(r'^\d{4}-\d{2}-\d{2}$'),
                              error="❌ recordDate 형식 오류 (YYYY-MM-DD 또는 null)"),
                    ]
                ),
                "paymentDate": Column(
                    str,
                    nullable=True,
                    checks=[
                        Check(lambda s: s.isna() | s.str.match(r'^\d{4}-\d{2}-\d{2}$'),
                              error="❌ paymentDate 형식 오류 (YYYY-MM-DD 또는 null)"),
                    ]
                ),
                "period": Column(
                    str,
                    nullable=True,
                ),
                "unadjustedValue": Column(
                    str,
                    nullable=True,
                    checks=[
                        Check(lambda s: s.isna() | s.str.match(r'^\d+(\.\d+)?$'),
                              error="❌ unadjustedValue 형식 오류 (숫자 또는 null)"),
                        Check(lambda s: s.isna() | (pd.to_numeric(s, errors='coerce') > 0),
                              error="❌ unadjustedValue는 양수여야 함"),
                    ]
                ),
            },
            checks=[
                # DataFrame 레벨 검증: code + date 조합 중복 체크
                Check(lambda df: ~df.duplicated(subset=['code', 'date']).any(),
                      error="❌ 중복된 (code, date) 조합이 존재함"),


                # 날짜 순서 검증: declarationDate <= recordDate
                Check(lambda df: (
                        df['declarationDate'].isna() |
                        df['recordDate'].isna() |
                        (pd.to_datetime(df['declarationDate'], errors='coerce') <=
                         pd.to_datetime(df['recordDate'], errors='coerce'))
                ).all(),
                      error="❌ declarationDate가 recordDate보다 늦음"),

                # 날짜 순서 검증: recordDate <= paymentDate
                Check(lambda df: (
                        df['recordDate'].isna() |
                        df['paymentDate'].isna() |
                        (pd.to_datetime(df['recordDate'], errors='coerce') <=
                         pd.to_datetime(df['paymentDate'], errors='coerce'))
                ).all(),
                      error="❌ recordDate가 paymentDate보다 늦음"),

                # 미래 날짜 체크
                Check(lambda df: (pd.to_datetime(df['date']) <= pd.Timestamp.now()).all(),
                      error="❌ 미래 날짜가 포함되어 있음"),

                # 너무 오래된 날짜 체크
                Check(lambda df: (pd.to_datetime(df['date']) >= pd.Timestamp('1900-01-01')).all(),
                      error="❌ 1900년 이전 날짜가 포함되어 있음"),
            ],
            strict=False,  # 추가 컬럼 허용
            coerce=False,  # 타입 강제 변환 안 함
        )

    # ============================================================
    # 2️⃣ 전체 검증 실행
    # ============================================================
    def validate(self, **kwargs):
        raw_dir = self._get_lake_path("raw")
        files = [f for f in os.listdir(raw_dir) if f.endswith(".json") or f.endswith(".jsonl")]
        if not files:
            print(f"⚠️ 검증 대상 JSON 파일이 없습니다: {raw_dir}")
            return

        all_records = []
        for file in files:
            path = os.path.join(raw_dir, file)
            with open(path, "r", encoding="utf-8") as f:
                try:
                    if file.endswith(".jsonl"):
                        all_records.extend([json.loads(line) for line in f if line.strip()])
                    else:
                        all_records.extend(json.load(f))
                except Exception as e:
                    print(f"❌ 파일 로드 실패: {file} | {e}")

        df = pd.DataFrame(all_records)
        if df.empty:
            raise AssertionError("❌ 검증할 데이터가 없습니다.")

        # ✅ Pandera 스키마 검증
        try:
            self.schema.validate(df, lazy=True)
            print(f"✅ Pandera 검증 통과 ({len(df)} records)")
        except pa.errors.SchemaErrors as e:
            print(f"❌ Pandera 검증 실패:")
            print(e.failure_cases)
            raise AssertionError(f"❌ Pandera 검증 실패: {e}")

        # ✅ Soda Core 검증 실행
        temp_path = os.path.join(raw_dir, "_dividends_temp.parquet")
        df.to_parquet(temp_path, index=False)
        self._run_soda_on_parquet(temp_path, mode="dividends")

    # ============================================================
    # 3️⃣ Soda 실행 로직
    # ============================================================
    def _run_soda_on_parquet(self, parquet_path: str, mode: str = "dividends"):
        base_dir = "/opt/airflow/plugins/soda/checks"
        soda_check_file = os.path.join(base_dir, f"dividends_checks.yml")

        if not os.path.exists(soda_check_file):
            print(f"⚠️ {soda_check_file} 없음 — 건너뜀.")
            return

        tmp_config = {"data_source my_duckdb": {"type": "duckdb", "path": ":memory:"}}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as tmp_file:
            yaml.dump(tmp_config, tmp_file)
            tmp_config_path = tmp_file.name

        scan = Scan()
        scan.set_data_source_name("my_duckdb")
        scan.add_configuration_yaml_file(tmp_config_path)
        scan.add_sodacl_yaml_files(soda_check_file)
        scan._data_source_manager.get_data_source("my_duckdb").connection.execute(
            f"CREATE TABLE dividends AS SELECT * FROM read_parquet('{parquet_path}')"
        )

        exit_code = scan.execute()
        print(f"🧪 Soda Scan 완료 (exit_code={exit_code}, mode={mode})")
        if exit_code != 0:
            raise AssertionError(f"❌ Soda 검증 실패: exit_code={exit_code}, mode={mode}")

        os.unlink(tmp_config_path)