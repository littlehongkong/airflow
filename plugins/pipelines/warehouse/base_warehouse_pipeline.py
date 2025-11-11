import json
import logging
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any
from datetime import datetime, timezone

from plugins.config.constants import (
    DATA_WAREHOUSE_ROOT, VALIDATOR_SCHEMA_LAKE, VALIDATOR_CHECKS_LAKE, DATA_LAKE_ROOT,
    VALIDATOR_SCHEMA_WAREHOUSE, VALIDATOR_CHECKS_WAREHOUSE
)

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
            domain_group: str,
            trd_dt: str,
            vendor: str = None,
            country_code: Optional[str] = None,
    ):
        self.layer:str ='warehouse'
        self.domain = domain
        self.domain_group = domain_group
        self.trd_dt = trd_dt
        self.vendor = vendor
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.country_code = country_code # 국가단위 파티셔닝에 사용

        # ✅ 경로 설정 통합
        self._setup_output_paths()

        self.conn = None  # duckdb 연결 객체
        self.exchanges: list = []

        # ✅ constants 기반 경로 설정
        if self.layer == "lake":
            self.schema_root = VALIDATOR_SCHEMA_LAKE / domain_group / vendor.lower()
            self.check_root = VALIDATOR_CHECKS_LAKE / domain_group / vendor.lower()
            self.data_root = DATA_LAKE_ROOT

        elif self.layer == "warehouse":
            # ✅ equity 도메인 폴더를 포함하도록 수정
            self.schema_root = VALIDATOR_SCHEMA_WAREHOUSE / domain_group
            self.check_root = VALIDATOR_CHECKS_WAREHOUSE / domain_group
            self.data_root = DATA_WAREHOUSE_ROOT

    # -------------------------------------------------------------------------
    # 2️⃣ 공통 데이터 로딩
    # -------------------------------------------------------------------------
    def _get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """DuckDB 연결 반환"""
        if self.conn is None:
            self.conn = duckdb.connect(database=":memory:")
        return self.conn


    @abstractmethod
    def _load_source_datasets(self) -> dict[str, pd.DataFrame]:
        """✅ Domain별 Loader를 직접 호출하는 명시적 버전"""
        pass

    # -------------------------------------------------------------------------
    # 3️⃣ 스키마 로드 및 컬럼 순서 정렬
    # -------------------------------------------------------------------------
    def _load_schema_definition(self) -> dict:
        """📘 warehouse_schemas 폴더에서 domain별 스키마 정의 JSON을 로드"""
        schema_path = self.schema_root / f"{self.domain}.json"
        if not schema_path.exists():
            self.log.warning(f"⚠️ Schema file not found for {self.domain}")
            return {}
        else:
            self.log.info(f"🆗️ Schema file exists! {schema_path}")
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

        return df[[c for c in preferred if c in df.columns]]

    # -------------------------------------------------------------------------
    # 4️⃣ 저장 및 메타데이터 관리
    # -------------------------------------------------------------------------
    def save_parquet(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Parquet 파일 저장
        - object 컬럼은 문자열로 변환 (PyArrow ArrowTypeError 방지)
        """
        try:
            # 🔹 모든 object 타입 컬럼을 문자열로 변환
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].astype(str)

            string_cols = df.select_dtypes(include=["object", "string"]).columns
            if len(string_cols) > 0:
                df[string_cols] = df[string_cols].replace(
                    ["None", "none", "NULL", "null", "NaN", "nan"], pd.NA
                )

            table = pa.Table.from_pandas(df)

            # ✅ 국가 파티션 디렉토리 추가
            if self.country_code:
                output_dir = self.output_file.parent / f"country_code={self.country_code}"
                output_dir.mkdir(parents=True, exist_ok=True)
                self.output_file = output_dir / self.output_file.name  # 예: /asset_master/snapshot_dt=2025-11-05/country_code=US/asset_master.parquet

            pq.write_table(table, self.output_file.as_posix())

            file_size = self.output_file.stat().st_size
            self.log.info(
                f"✅ Parquet saved: {self.output_file} "
                f"({len(df):,} rows, {file_size:,} bytes)")

            return {
                "file_path": self.output_file.as_posix(),
                "row_count": len(df),
                "file_size_bytes": file_size,
            }

        except Exception as e:
            self.log.error(f"❌ Failed to save parquet: {e}")
            raise

    def _setup_output_paths(self):
        """
        ✅ 출력 디렉토리 및 파일 경로 설정
        - snapshot 레이어에 우선 저장
        - validated는 validator가 promote 시 생성
        """
        if self.domain == "fundamentals":
            # 하위 pipeline에서 직접 정의하므로 skip
            return

        snapshot_root = (
                DATA_WAREHOUSE_ROOT
                / "snapshot"
                / self.domain_group
                / self.domain
                / f"trd_dt={self.trd_dt}"
        )
        snapshot_root.mkdir(parents=True, exist_ok=True)

        self.output_dir = snapshot_root
        self.output_file = snapshot_root / f"{self.domain}.parquet"
        self.meta_file = snapshot_root / "_build_meta.json"

        # 📘 도메인 전역 메타파일 경로 (예: /data_warehouse/exchange/_warehouse_meta.json)
        self.domain_meta_file = DATA_WAREHOUSE_ROOT / self.domain / "_warehouse_meta.json"

    def save_metadata(self, row_count: int, **kwargs) -> Dict[str, Any]:
        """
        메타데이터 파일(_metadata.json) 기록
        """
        import json
        meta = {
            "domain": self.domain,
            "snapshot_dt": self.trd_dt,
            "row_count": row_count,
            "timestamp": datetime.utcnow().isoformat(),
            **kwargs,  # ✅ 추가적인 모든 인자 포함
        }

        meta_path = (
                DATA_WAREHOUSE_ROOT
                / self.domain
                / "snapshot"
                / f"trd_dt={self.trd_dt}"
                / "_metadata.json"
        )

        meta_path.parent.mkdir(parents=True, exist_ok=True)
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)

        self.log.info(f"🧾 Metadata saved → {meta_path.as_posix()}")
        return meta

    # -------------------------------------------------------------------------
    # 5️⃣ 추상 메서드 (하위 클래스 구현부)
    # -------------------------------------------------------------------------    @abstractmethod
    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """도메인별 정규화 로직 (하위 클래스 구현)"""
        pass

    @abstractmethod
    def _transform_business_logic(self, **kwargs) -> pd.DataFrame:
        """
        도메인별 비즈니스 로직 구현부
        예시:
          - exchange: 거래소 + 휴장일 병합
        """
        pass

    def _update_domain_metadata(self, record_count: int):
        """
        ✅ 도메인 루트에 `_warehouse_meta.json` 생성/갱신
        - 최신 스냅샷 일자, 최초 적재일자, 총 적재 횟수 등을 관리
        """
        meta_path = DATA_WAREHOUSE_ROOT / self.domain / "_warehouse_meta.json"
        now = datetime.now(timezone.utc).isoformat()

        new_meta = {
            "domain": self.domain,
            "latest_snapshot": self.trd_dt,
            "record_count": record_count,
            "last_build_meta": str(self.meta_file),
            "updated_at": now,
        }

        # 이전 메타 유지 (최초 적재일, 누적 카운트)
        if meta_path.exists():
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    old = json.load(f)
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