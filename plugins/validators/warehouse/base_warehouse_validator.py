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
import numpy as np
from pathlib import Path
from typing import Dict, Any, Optional, List
from plugins.validators.base_validator_interface import BaseValidatorInterface
from plugins.config.constants import DATA_WAREHOUSE_ROOT
from datetime import datetime, timezone


class BaseWarehouseValidator(BaseValidatorInterface):
    def __init__(
        self,
        domain: str,
        snapshot_dt: str,
        partition_key: str = "snapshot_dt",
        partition_value: Optional[str] = None,
        warehouse_root: Path = Path("/opt/airflow/data/data_warehouse"),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.domain = domain
        self.snapshot_dt = snapshot_dt
        self.partition_key = partition_key
        self.partition_value = partition_value  # ✅ 이제 optional
        self.warehouse_root = warehouse_root
        self.duckdb_path = warehouse_root / "duckdb" / f"{domain}.duckdb"
        self.conn = None

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
        value = int(len(df))
        return {
            "passed": bool(value >= min_rows),
            "value": value,
            "expected": f">={min_rows}",
            "message": f"Row count = {value}",
        }

    def _check_no_nulls(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        null_count = int(df[column].isnull().sum())
        return {
            "passed": bool(null_count == 0),
            "value": null_count,
            "expected": 0,
            "message": f"Null count in {column}: {null_count}",
        }

    def _check_no_duplicates(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        dup_count = int(df[column].duplicated().sum())
        return {
            "passed": bool(dup_count == 0),
            "value": dup_count,
            "expected": 0,
            "message": f"Duplicate count in {column}: {dup_count}",
        }

    def _check_foreign_key(self, df: pd.DataFrame, ref_df: pd.DataFrame, fk_column: str, ref_column: str) -> Dict[
        str, Any]:
        missing_count = int(len(df[~df[fk_column].isin(ref_df[ref_column])]))
        return {
            "passed": bool(missing_count == 0),
            "value": missing_count,
            "expected": 0,
            "message": f"Missing foreign key values in {fk_column}: {missing_count}",
        }

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

    def _save_validation_result(self, result: Dict[str, Any], **kwargs) -> Path:

        def _make_json_serializable(obj):
            import numpy as np

            if isinstance(obj, (np.bool_, bool)):
                return bool(obj)
            elif isinstance(obj, (np.integer, int)):
                return int(obj)
            elif isinstance(obj, (np.floating, float)):
                return float(obj)
            elif isinstance(obj, dict):
                return {k: _make_json_serializable(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [_make_json_serializable(v) for v in obj]
            else:
                return obj

        # ✅ 변환 수행
        serializable_result = _make_json_serializable(result)

        # ✅ 저장
        result_dir = self.warehouse_root / self.domain / "validation"
        result_dir.mkdir(parents=True, exist_ok=True)

        # ✅ 파일명 규칙: {domain}_{partition(optional)}_{snapshot_dt}_validation.json
        if self.partition_value:
            filename = f"{self.domain}_{self.partition_value}_{self.snapshot_dt}_validation.json"
        else:
            filename = f"{self.domain}_{self.snapshot_dt}_validation.json"

        output_path = result_dir / filename
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(serializable_result, f, indent=2, ensure_ascii=False)

        self.log.info(f"💾 Saved validation result: {output_path}")
        return output_path

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
