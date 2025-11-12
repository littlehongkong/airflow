import json
import logging
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any
from datetime import datetime, timezone
from pathlib import Path

from plugins.utils.path_manager import DataPathResolver
from plugins.config.constants import (
    DATA_WAREHOUSE_ROOT,
    VALIDATOR_SCHEMA_LAKE, VALIDATOR_CHECKS_LAKE, DATA_LAKE_ROOT,
    VALIDATOR_SCHEMA_WAREHOUSE, VALIDATOR_CHECKS_WAREHOUSE,
    WAREHOUSE_DOMAINS,
)


class BaseWarehousePipeline(ABC):
    """
    ✅ Warehouse 공통 파이프라인 베이스 클래스

    - Data Lake validated → Warehouse snapshot 변환
    - 스키마 기반 컬럼 정렬, Parquet 저장, 메타데이터 관리
    """

    def __init__(
        self,
        domain: str,
        domain_group: str,
        trd_dt: str,
        vendor: str = None,
        country_code: Optional[str] = None,
    ):
        self.layer = "warehouse"
        self.domain = domain
        self.domain_group = domain_group
        self.trd_dt = trd_dt
        self.vendor = vendor
        self.country_code = country_code
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self.conn = None  # duckdb 연결
        self.exchanges: list = []

        # ✅ 경로 설정
        self._setup_output_paths()

    # -------------------------------------------------------------------------
    # 1️⃣ 공통 경로 설정
    # -------------------------------------------------------------------------
    def _setup_output_paths(self):
        """
        ✅ DataPathResolver 기반으로 Warehouse 출력 경로 자동 설정
        """
        if self.domain not in WAREHOUSE_DOMAINS:
            raise ValueError(f"❌ Unknown warehouse domain: {self.domain}")

        # 1️⃣ PathResolver 통해 snapshot 경로 생성
        snapshot_dir = DataPathResolver.warehouse_snapshot(
            domain_group=self.domain_group,
            domain=self.domain,
            country_code=self.country_code,
            trd_dt=self.trd_dt,
        )
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        # 2️⃣ 물리 파일명
        domain_name = WAREHOUSE_DOMAINS[self.domain]

        # 3️⃣ 경로 속성
        self.output_dir = snapshot_dir
        self.output_file = snapshot_dir / f"{domain_name}.parquet"
        self.meta_file = snapshot_dir / "_build_meta.json"
        self.domain_meta_file = (
                Path(DATA_WAREHOUSE_ROOT)
                / "snapshot"
                / self.domain_group
                / domain_name
                / "_warehouse_meta.json"
        )

        self.log.info(f"📦 Output path configured: {self.output_file}")

    # -------------------------------------------------------------------------
    # 2️⃣ Parquet 저장
    # -------------------------------------------------------------------------
    def save_parquet(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Parquet 저장 (object → string 변환 포함)"""
        try:
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].astype(str)

            string_cols = df.select_dtypes(include=["object", "string"]).columns
            df[string_cols] = df[string_cols].replace(
                ["None", "none", "NULL", "null", "NaN", "nan"], pd.NA
            )

            table = pa.Table.from_pandas(df)
            pq.write_table(table, self.output_file.as_posix())

            file_size = self.output_file.stat().st_size
            self.log.info(
                f"✅ Parquet saved: {self.output_file} ({len(df):,} rows, {file_size:,} bytes)"
            )

            return {
                "file_path": self.output_file.as_posix(),
                "row_count": len(df),
                "file_size_bytes": file_size,
            }

        except Exception as e:
            self.log.error(f"❌ Failed to save parquet: {e}")
            raise

    # -------------------------------------------------------------------------
    # 3️⃣ 메타데이터 저장
    # -------------------------------------------------------------------------
    def save_metadata(self, row_count: int, **kwargs) -> Dict[str, Any]:
        """각 스냅샷(trd_dt별) 메타 저장"""
        meta = {
            "domain": self.domain,
            "snapshot_dt": self.trd_dt,
            "row_count": row_count,
            "timestamp": datetime.utcnow().isoformat(),
            **kwargs,
        }

        meta_path = self.output_dir / "_validation_meta.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)

        self.log.info(f"🧾 Metadata saved → {meta_path}")
        return meta

    def _update_domain_metadata(self, record_count: int):
        """도메인별 최신 snapshot 메타 갱신"""
        meta_path = self.domain_meta_file
        now = datetime.now(timezone.utc).isoformat()

        new_meta = {
            "domain": self.domain,
            "latest_snapshot": self.trd_dt,
            "record_count": record_count,
            "last_build_meta": str(self.meta_file),
            "updated_at": now,
        }

        if meta_path.exists():
            try:
                old = json.load(open(meta_path))
                new_meta["first_ingested"] = old.get("first_ingested", self.trd_dt)
                new_meta["total_snapshots"] = old.get("total_snapshots", 0) + 1
            except Exception:
                new_meta["first_ingested"] = self.trd_dt
                new_meta["total_snapshots"] = 1
        else:
            new_meta["first_ingested"] = self.trd_dt
            new_meta["total_snapshots"] = 1

        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(new_meta, f, indent=2, ensure_ascii=False)

        self.log.info(f"📘 Domain meta updated: {meta_path}")

    # -------------------------------------------------------------------------
    # 4️⃣ 스키마 관련
    # -------------------------------------------------------------------------
    def _load_schema_definition(self) -> dict:
        """
        📘 Warehouse 전용 Schema 로더
        - constants.py 기반으로 schema_root 설정
        - WAREHOUSE_DOMAINS 매핑 자동 적용
        """
        # ✅ warehouse 전용 schema root 설정 (고정)
        self.schema_root = VALIDATOR_SCHEMA_WAREHOUSE / self.domain_group

        # ✅ WAREHOUSE_DOMAINS 매핑 적용 (예: asset → asset_master)
        domain_name = WAREHOUSE_DOMAINS.get(self.domain, self.domain)

        schema_path = self.schema_root / f"{domain_name}.json"

        if not schema_path.exists():
            self.log.warning(f"⚠️ Schema file not found for {self.domain} | {schema_path}")
            return {}

        self.log.info(f"🆗 Schema file loaded: {schema_path}")
        with open(schema_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _get_preferred_columns(self) -> list[str]:
        schema = self._load_schema_definition()
        return [c["name"] for c in schema.get("columns", [])]

    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        preferred = self._get_preferred_columns()
        if not preferred:
            return df
        for col in preferred:
            if col not in df.columns:
                df[col] = None
        return df[[c for c in preferred if c in df.columns]]


    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """선택적 스키마 표준화 (기본은 그대로 반환)"""
        return df

    # -------------------------------------------------------------------------
    # 5️⃣ 추상 메서드
    # -------------------------------------------------------------------------
    @abstractmethod
    def _load_source_datasets(self) -> dict[str, pd.DataFrame]:
        pass


    @abstractmethod
    def _transform_business_logic(self, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def build(self, **kwargs) -> Dict[str, Any]:
        pass


    # -------------------------------------------------------------------------
    # 6️⃣ 리소스 정리
    # -------------------------------------------------------------------------
    def cleanup(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
