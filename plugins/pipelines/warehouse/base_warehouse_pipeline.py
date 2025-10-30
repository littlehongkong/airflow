import json
import logging
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone

from plugins.config.constants import (
    DATA_LAKE_ROOT, DATA_WAREHOUSE_ROOT,
    VENDORS, WAREHOUSE_SOURCE_MAP, SCHEMA_DIR
)
from plugins.utils.snapshot_registry import update_global_snapshot_registry


class BaseWarehousePipeline(ABC):
    """
    ✅ Warehouse 공통 파이프라인 베이스 클래스

    주요 기능:
    - Data Lake validated → Warehouse 통합 변환 파이프라인의 표준 인터페이스
    - 데이터 로딩, 파케 저장, 메타데이터 기록, 스냅샷 레지스트리 갱신 담당
    - 비즈니스 로직은 하위 클래스의 `_transform_business_logic()`에서 수행
    """

    # -------------------------------------------------------------------------
    # 1️⃣ 초기화 및 경로 설정
    # -------------------------------------------------------------------------
    def __init__(
        self,
        domain: str,
        snapshot_dt: str,
        vendor_priority: Optional[List[str]] = None,
    ):
        self.domain = domain
        self.snapshot_dt = snapshot_dt
        self.vendor_priority = vendor_priority or [VENDORS.get("EODHD", "eodhd")]
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self.output_dir = DATA_WAREHOUSE_ROOT / self.domain / f"snapshot_dt={self.snapshot_dt}"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / f"{self.domain}.parquet"
        self.meta_file = self.output_dir / "_build_meta.json"

        self.conn = None  # duckdb 연결 객체

    # -------------------------------------------------------------------------
    # 2️⃣ 공통 데이터 로딩
    # -------------------------------------------------------------------------
    def _get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """DuckDB 연결 반환"""
        if self.conn is None:
            self.conn = duckdb.connect(database=":memory:")
        return self.conn

    def _load_source_datasets(self, warehouse_domain: str) -> Dict[str, pd.DataFrame]:
        """
        ✅ 공통 Data Lake → Warehouse 데이터 로더

        constants.WAREHOUSE_SOURCE_MAP 기반으로 validated 폴더에서 parquet 자동 로드
        """
        source_map = WAREHOUSE_SOURCE_MAP.get(warehouse_domain)
        if not source_map:
            raise ValueError(f"❌ No source mapping defined for {warehouse_domain}")

        conn = self._get_duckdb_connection()
        results = {}

        for lake_domain in source_map:
            domain_dir = DATA_LAKE_ROOT / "validated" / lake_domain
            parquet_files = list(domain_dir.rglob("*.parquet"))

            if not parquet_files:
                self.log.warning(f"⚠️ No parquet files found for {lake_domain}")
                continue

            files_expr = ", ".join([f"'{p.as_posix()}'" for p in parquet_files])
            df = conn.execute(f"SELECT * FROM read_parquet([{files_expr}])").df()

            self.log.info(f"📊 Loaded {len(df):,} records from {lake_domain}")
            results[lake_domain] = df

        return results

    # -------------------------------------------------------------------------
    # 3️⃣ 스키마 로드 및 컬럼 순서 정렬
    # -------------------------------------------------------------------------
    def _load_schema_definition(self) -> dict:
        """📘 warehouse_schemas 폴더에서 domain별 스키마 정의 JSON을 로드"""
        schema_path = SCHEMA_DIR / f"{self.domain}.json"
        if not schema_path.exists():
            self.log.warning(f"⚠️ Schema file not found for {self.domain}")
            return {}
        with open(schema_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _get_preferred_columns(self) -> list[str]:
        schema = self._load_schema_definition()
        return [c["name"] for c in schema.get("columns", [])]

    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """컬럼 순서 재정렬"""
        preferred = self._get_preferred_columns()
        if not preferred:
            return df
        for col in preferred:
            if col not in df.columns:
                df[col] = None
        return df[preferred + [c for c in df.columns if c not in preferred]]

    # -------------------------------------------------------------------------
    # 4️⃣ 저장 및 메타데이터 관리
    # -------------------------------------------------------------------------
    def save_parquet(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Parquet 저장"""
        table = pa.Table.from_pandas(df)
        pq.write_table(table, self.output_file.as_posix())
        size = self.output_file.stat().st_size
        self.log.info(f"✅ Parquet saved: {self.output_file} ({len(df):,} rows, {size:,} bytes)")
        return {"file_path": str(self.output_file), "row_count": len(df), "file_size": size}

    def save_metadata(
        self,
        row_count: int,
        source_info: Optional[Dict[str, Any]] = None,
        metrics: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Warehouse 단계 메타데이터 저장"""
        meta = {
            "dataset": self.domain,
            "snapshot_dt": self.snapshot_dt,
            "status": "success",
            "record_count": row_count,
            "output_file": str(self.output_file),
            "sources": source_info or {},
            "metrics": metrics or {},
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if context:
            ti = context.get("task_instance")
            if ti:
                meta["dag_run_id"] = context.get("run_id")
                meta["task_id"] = ti.task_id

        with open(self.meta_file, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)
        self.log.info(f"📘 Metadata saved: {self.meta_file}")
        return meta

    def update_registry(self):
        """전역 스냅샷 레지스트리 갱신"""
        update_global_snapshot_registry(
            domain=self.domain,
            snapshot_dt=self.snapshot_dt,
            meta_file=self.meta_file
        )
        self.log.info(f"📝 Registry updated: {self.domain} / {self.snapshot_dt}")

    # -------------------------------------------------------------------------
    # 5️⃣ 추상 메서드 (하위 클래스 구현부)
    # -------------------------------------------------------------------------
    @abstractmethod
    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """도메인별 정규화 로직 (하위 클래스 구현)"""
        pass

    @abstractmethod
    def _transform_business_logic(self, **kwargs) -> pd.DataFrame:
        """
        도메인별 비즈니스 로직 구현부
        예시:
          - exchange: 거래소 + 휴장일 병합
          - asset: symbol_list + symbol_changes 병합
        """
        pass

    @abstractmethod
    def build(self, **kwargs) -> Dict[str, Any]:
        """메인 빌드 프로세스 (하위 클래스 구현)"""
        pass

    # -------------------------------------------------------------------------
    # 6️⃣ 리소스 정리
    # -------------------------------------------------------------------------
    def cleanup(self):
        """DuckDB 연결 종료"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
