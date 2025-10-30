"""
Warehouse Base Validator (Refactored)
-----------------------------------------
✅ 목적:
- Warehouse 적재 후 정합성 검증을 위한 공통 베이스 클래스
- BaseWarehousePipeline과 동일한 도메인 구조로 동작
- 하위 클래스에서는 `_define_checks()`만 구현

기능 요약:
1️⃣ Parquet 파일 로드 (snapshot 기반)
2️⃣ Pandera / Soda Core / Custom Rule 기반 검증
3️⃣ 검증 결과 저장 및 상태 요약
"""

import duckdb
import pandas as pd
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from plugins.validators.base_validator_interface import BaseValidatorInterface
from plugins.config.constants import DATA_WAREHOUSE_ROOT
from datetime import datetime, timezone


class BaseWarehouseValidator(BaseValidatorInterface):
    """
    ✅ Warehouse 공통 Validator
    """

    def __init__(self, domain: str, snapshot_dt: str, warehouse_root: Path = DATA_WAREHOUSE_ROOT):
        super().__init__()
        self.domain = domain
        self.snapshot_dt = snapshot_dt
        self.warehouse_root = warehouse_root
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.log.info(f"Initialized validator for domain={domain}, snapshot_dt={snapshot_dt}")

    # -------------------------------------------------------------------------
    # 1️⃣ Validation Entry Point
    # -------------------------------------------------------------------------
    def validate(self, context: Optional[Dict] = None, **kwargs) -> Dict[str, Any]:
        """
        검증 메인 함수 (Airflow Operator에서 직접 호출)
        """
        dag_run_id, task_id = self._extract_airflow_context(context)
        parquet_path = self._get_parquet_path()

        if not parquet_path.exists():
            self.log.warning(f"❌ Parquet file not found: {parquet_path}")
            return self._generate_result(
                status="skipped",
                checks={},
                record_count=0,
                message="File not found",
                dag_run_id=dag_run_id,
                task_id=task_id,
            )

        # ✅ Load data
        df = pd.read_parquet(parquet_path)
        self.log.info(f"📦 Loaded {len(df):,} rows from {parquet_path}")

        # ✅ Run checks
        checks = self._define_checks(df)
        status = self._aggregate_status(checks)
        self.log.info(f"✅ Validation complete | Status: {status}")

        # ✅ Build result
        result = self._generate_result(
            status=status,
            checks=checks,
            record_count=len(df),
            snapshot_dt=self.snapshot_dt,
            parquet_path=str(parquet_path),
            dag_run_id=dag_run_id,
            task_id=task_id,
        )

        # ✅ Save result JSON
        saved_path = self._save_validation_result(result)
        result["result_path"] = str(saved_path)
        return result

    # -------------------------------------------------------------------------
    # 2️⃣ Abstracts for Subclasses
    # -------------------------------------------------------------------------
    def _get_parquet_path(self) -> Path:
        """
        snapshot 기준 warehouse 경로 반환
        하위 클래스에서 domain 기준으로 자동 접근
        """
        return self.warehouse_root / self.domain / f"snapshot_dt={self.snapshot_dt}" / f"{self.domain}.parquet"

    def _define_checks(self, df: pd.DataFrame) -> Dict[str, Any]:
        """하위 클래스에서 검증 규칙 정의"""
        raise NotImplementedError("❌ _define_checks() must be implemented in subclass")

    # -------------------------------------------------------------------------
    # 3️⃣ Utility Checks
    # -------------------------------------------------------------------------
    def _check_row_count(self, df: pd.DataFrame, min_rows: int = 1) -> Dict[str, Any]:
        count = len(df)
        return {"passed": count >= min_rows, "value": count, "expected": f">={min_rows}", "message": f"row_count={count}"}

    def _check_no_nulls(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        null_count = df[column].isnull().sum()
        return {"passed": null_count == 0, "value": null_count, "expected": 0, "message": f"nulls in {column}: {null_count}"}

    def _check_no_duplicates(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        dup_count = df[column].duplicated().sum()
        return {"passed": dup_count == 0, "value": dup_count, "expected": 0, "message": f"duplicates in {column}: {dup_count}"}

    def _check_foreign_key(self, df: pd.DataFrame, ref_df: pd.DataFrame, fk_col: str, ref_col: str) -> Dict[str, Any]:
        missing = df[~df[fk_col].isin(ref_df[ref_col])]
        miss_count = len(missing)
        return {"passed": miss_count == 0, "value": miss_count, "expected": 0, "message": f"missing FK in {fk_col}: {miss_count}"}

    def _aggregate_status(self, checks: Dict[str, Any]) -> str:
        if not checks:
            return "skipped"
        if any(not c.get("passed", False) for c in checks.values()):
            return "failed"
        return "success"

    # -------------------------------------------------------------------------
    # 4️⃣ Result Handling
    # -------------------------------------------------------------------------
    def _generate_result(
        self,
        status: str,
        checks: Dict[str, Any],
        record_count: int,
        snapshot_dt: Optional[str] = None,
        parquet_path: Optional[str] = None,
        dag_run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """검증 메타데이터 결과 생성"""
        result = {
            "dataset": self.domain,
            "snapshot_dt": snapshot_dt or self.snapshot_dt,
            "status": status,
            "record_count": record_count,
            "checks": checks,
            "validated_file": parquet_path,
            "validated_at": datetime.now(timezone.utc).isoformat(),
        }
        if message:
            result["message"] = message
        if dag_run_id:
            result["dag_run_id"] = dag_run_id
        if task_id:
            result["task_id"] = task_id
        return result

    def _save_validation_result(self, result: Dict[str, Any]) -> Path:
        """검증 결과 JSON 저장"""
        result_dir = self.warehouse_root / self.domain / "validation"
        result_dir.mkdir(parents=True, exist_ok=True)
        file_name = f"{self.domain}_{self.snapshot_dt}_validation.json"
        result_path = result_dir / file_name
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        self.log.info(f"💾 Validation result saved: {result_path}")
        return result_path

    # -------------------------------------------------------------------------
    # 5️⃣ Optional: Pandera / Soda Core 연동 (확장 포인트)
    # -------------------------------------------------------------------------
    def _validate_with_pandera(self, df: pd.DataFrame, schema_def: dict) -> Dict[str, Any]:
        """
        Optional: Pandera 스키마 기반 검증 (warehouse_schemas/{domain}.json)
        """
        import pandera as pa
        from pandera import DataFrameSchema, Column

        try:
            columns = {
                c["name"]: Column(
                    eval(f"pa.{c['type'].capitalize()}"),
                    nullable=c.get("nullable", True),
                )
                for c in schema_def.get("columns", [])
            }
            schema = DataFrameSchema(columns)
            schema.validate(df)
            self.log.info(f"✅ Pandera validation passed for {self.domain}")
            return {"passed": True, "message": "Pandera schema validation passed"}
        except Exception as e:
            self.log.error(f"❌ Pandera validation failed: {e}")
            return {"passed": False, "message": str(e)}

    def _validate_with_soda(self, df: pd.DataFrame, checks: Optional[List[str]] = None):
        """
        Optional: Soda Core 기반 검증 (데이터 품질 리포트 생성)
        """
        try:
            from soda.scan import Scan
            scan = Scan()
            scan.add_pandas_df(df, self.domain)
            if checks:
                for rule in checks:
                    scan.add_check(rule)
            scan.execute()
            if scan.has_check_failures():
                raise ValueError(f"Soda checks failed for {self.domain}")
            self.log.info(f"✅ Soda validation passed for {self.domain}")
        except Exception as e:
            self.log.error(f"❌ Soda validation failed: {e}")
            raise e

    # -------------------------------------------------------------------------
    # 6️⃣ Cleanup
    # -------------------------------------------------------------------------
    def cleanup(self):
        """리소스 정리"""
        if self.conn:
            self.conn.close()
            self.conn = None
