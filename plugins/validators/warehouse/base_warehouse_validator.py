"""
Warehouse Base Validator
-----------------------------------------
DuckDB 기반 Warehouse 검증용 공통 베이스 클래스
- BaseValidatorInterface 구현
- Parquet 로드 + DuckDB 검증
- 참조 무결성 및 비즈니스 규칙 점검 유틸리티 포함
"""

import duckdb
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional, List
from plugins.validators.base_validator_interface import BaseValidatorInterface
from plugins.config.constants import DATA_LAKE_ROOT
import json


class BaseWarehouseValidator(BaseValidatorInterface):
    def __init__(
        self,
        domain: str,
        snapshot_dt: str,
        partition_key: str,
        partition_value: str,
        warehouse_root: Path = Path("/opt/airflow/data/data_warehouse"),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.domain = domain
        self.snapshot_dt = snapshot_dt
        self.partition_key = partition_key
        self.partition_value = partition_value
        self.warehouse_root = warehouse_root
        self.duckdb_path = warehouse_root / "duckdb" / f"{domain}.duckdb"
        self.conn = None


    # ------------------------------------------------------
    # ✅ Core entrypoint
    # ------------------------------------------------------
    def build(self, context: Optional[Dict] = None, **kwargs) -> Dict[str, Any]:
        self.log.info(f"🔍 Running validation for {self.domain} | {self.partition_value} | {self.snapshot_dt}")
        dag_run_id, task_id = self._extract_airflow_context(context)
        parquet_path = self._get_parquet_path()

        if not parquet_path.exists():
            self.log.warning(f"❌ Parquet file not found: {parquet_path}")
            result = self._get_validation_metadata(
                status="skipped",
                checks={},
                dag_run_id=dag_run_id,
                task_id=task_id,
                message="Parquet not found",
                record_count=0,
            )
            return result

        # Load data
        df = pd.read_parquet(parquet_path)
        self.log.info(f"📦 Loaded {len(df)} records from {parquet_path}")

        # Run checks
        checks = self._define_checks(df)
        status = self._aggregate_status(checks)
        self.log.info(f"✅ Validation status: {status}")

        # Save result
        result = self._get_validation_metadata(
            status=status,
            checks=checks,
            dag_run_id=dag_run_id,
            task_id=task_id,
            record_count=len(df),
            snapshot_dt=self.snapshot_dt,
            parquet_path=str(parquet_path),
        )

        saved_path = self._save_validation_result(result)
        result["result_path"] = str(saved_path)
        return result

    # ------------------------------------------------------
    # ✅ Abstracts for subclasses
    # ------------------------------------------------------
    def _get_parquet_path(self) -> Path:
        """하위 클래스에서 구현"""
        raise NotImplementedError

    def _define_checks(self, df: pd.DataFrame) -> Dict[str, Any]:
        """하위 클래스에서 구현"""
        raise NotImplementedError



    def cleanup(self):
        """리소스 정리"""
        if self.conn:
            self.conn.close()
            self.conn = None

    # ------------------------------------------------------
    # ✅ Validation utility methods
    # ------------------------------------------------------
    def _check_row_count(self, df: pd.DataFrame, min_rows: int = 1) -> Dict[str, Any]:
        return {
            "passed": len(df) >= min_rows,
            "value": len(df),
            "expected": f">={min_rows}",
            "message": f"Row count = {len(df)}",
        }

    def _check_no_nulls(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        null_count = df[column].isnull().sum()
        return {
            "passed": null_count == 0,
            "value": null_count,
            "expected": 0,
            "message": f"Null count in {column}: {null_count}",
        }

    def _check_no_duplicates(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        dup_count = df[column].duplicated().sum()
        return {
            "passed": dup_count == 0,
            "value": dup_count,
            "expected": 0,
            "message": f"Duplicate count in {column}: {dup_count}",
        }

    def _check_foreign_key(
        self, df: pd.DataFrame, ref_df: pd.DataFrame, fk_column: str, ref_column: str
    ) -> Dict[str, Any]:
        missing = df[~df[fk_column].isin(ref_df[ref_column])]
        missing_count = len(missing)
        return {
            "passed": missing_count == 0,
            "value": missing_count,
            "expected": 0,
            "message": f"Missing foreign key values in {fk_column}: {missing_count}",
        }

    def _aggregate_status(self, checks: Dict[str, Any]) -> str:
        if not checks:
            return "skipped"
        if any(not c["passed"] for c in checks.values()):
            return "failed"
        return "success"

    # ------------------------------------------------------
    # ✅ DB Context Management
    # ------------------------------------------------------
    def _open_duckdb(self):
        self.conn = duckdb.connect(str(self.duckdb_path))
        self.log.debug(f"Connected to DuckDB: {self.duckdb_path}")

    def _close_duckdb(self):
        if self.conn:
            self.conn.close()
            self.log.debug("Closed DuckDB connection")

    # ------------------------------------------------------
    # ✅ Result saving
    # ------------------------------------------------------
    def _save_validation_result(self, result: Dict[str, Any], **kwargs) -> Path:
        result_dir = self.warehouse_root / self.domain / "validation"
        result_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{self.partition_value}_{self.snapshot_dt}_validation.json"
        output_path = result_dir / filename
        with open(output_path, "w") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        self.log.info(f"💾 Saved validation result: {output_path}")
        return output_path
