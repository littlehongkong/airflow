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

    def __init__(self, exchange_code: str, trd_dt: str, data_domain: str = "splits"):
        super().__init__(exchange_code, trd_dt, data_domain)
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
            self.schema.validate(df)
            print(f"✅ Pandera 검증 통과 ({len(df)} records)")
        except Exception as e:
            raise AssertionError(f"❌ Pandera 검증 실패: {e}")

        # ✅ Soda Core 검증 실행
        temp_path = os.path.join(raw_dir, "_splits_temp.parquet")

        df.to_parquet(temp_path, index=False)
        self._run_soda_on_parquet(temp_path, mode="splits")

    # ============================================================
    # 3️⃣ Soda 실행 로직
    # ============================================================
    def _run_soda_on_parquet(self, parquet_path: str, mode: str = "splits"):
        base_dir = "/opt/airflow/plugins/soda/checks"
        soda_check_file = os.path.join(base_dir, f"splits_checks.yml")

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
            f"CREATE TABLE splits AS SELECT * FROM read_parquet('{parquet_path}')"
        )

        exit_code = scan.execute()
        print(f"🧪 Soda Scan 완료 (exit_code={exit_code}, mode={mode})")
        if exit_code != 0:
            raise AssertionError(f"❌ Soda 검증 실패: exit_code={exit_code}, mode={mode}")

        os.unlink(tmp_config_path)
