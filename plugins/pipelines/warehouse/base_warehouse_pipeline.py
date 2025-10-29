# plugins/pipelines/warehouse/base_warehouse_pipeline.py
import json
import logging
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any, Tuple

from plugins.config.constants import (
    DATA_LAKE_ROOT, DATA_WAREHOUSE_ROOT,
    LAYERS, DATA_DOMAINS, VENDORS
)
from plugins.utils.partition_finder import latest_trd_dt_path, parquet_file_in
from plugins.utils.snapshot_registry import update_global_snapshot_registry


class BaseWarehousePipeline(ABC):
    """
    ✅ Warehouse 공통 파이프라인 베이스 클래스

    주요 기능:
    1. Data Lake validated 레이어에서 데이터 로딩 (vendor 우선순위 지원)
    2. 데이터 정규화 및 변환
    3. Entity ID 생성 (optional)
    4. Parquet 저장 (snapshot 기반)
    5. 메타데이터 관리 및 전역 레지스트리 갱신

    하위 클래스가 구현해야 할 메서드:
    - _normalize_dataframe(): 도메인별 정규화 로직
    - _generate_entity_ids(): Entity ID 생성 로직 (optional)
    - _get_preferred_columns(): 출력 컬럼 순서 정의
    """

    def __init__(
            self,
            domain: str,
            snapshot_dt: str,
            partition_key: str = "country_code",
            partition_value: Optional[str] = None,
            vendor_priority: Optional[List[str]] = None,
            layer: str = "warehouse"
    ):
        """
        Args:
            domain: Warehouse 도메인명 (e.g. 'asset_master', 'exchange_master')
            snapshot_dt: 스냅샷 날짜 (YYYY-MM-DD)
            partition_key: 파티셔닝 키 (default: country_code)
            partition_value: 파티셔닝 값 (e.g. 'KOR', 'USA')
            vendor_priority: 벤더 우선순위 리스트
        """
        self.domain = domain
        self.snapshot_dt = snapshot_dt
        self.partition_key = partition_key
        self.partition_value = partition_value.upper() if partition_value else None
        self.vendor_priority = vendor_priority or [VENDORS.get("EODHD", "eodhd")]
        self.layer = layer

        self.log = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # 출력 경로 설정
        self._setup_output_paths()

        # DuckDB 연결
        self.conn = None

    def _setup_output_paths(self):
        """출력 디렉토리 및 파일 경로 설정"""
        if self.partition_value:
            self.output_dir = (
                    DATA_WAREHOUSE_ROOT
                    / self.domain
                    / f"{self.partition_key}={self.partition_value}"
                    / f"snapshot_dt={self.snapshot_dt}"
            )
        else:
            self.output_dir = (
                    DATA_WAREHOUSE_ROOT
                    / self.domain
                    / f"snapshot_dt={self.snapshot_dt}"
            )

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / f"{self.domain}.parquet"
        self.meta_file = self.output_dir / "_build_meta.json"



    def _get_validated_files(self, lake_domain: str, filename: str = None) -> List[Path]:
        """
        Data Lake validated/<lake_domain> 경로에서 parquet 파일 목록을 안전하게 조회

        Args:
            lake_domain: validated 레이어 내의 도메인명 (예: 'exchange_list')
            filename: 특정 파일명 (기본값: <lake_domain>.parquet)

        Returns:
            parquet 파일 경로 리스트
        """
        layer_root = DATA_LAKE_ROOT / "validated" / lake_domain
        layer_root = layer_root.resolve()

        if not layer_root.exists():
            raise FileNotFoundError(f"❌ validated path not found: {layer_root}")

        parquet_name = filename or f"{lake_domain}.parquet"
        parquet_files = list(layer_root.glob(f"**/{parquet_name}"))

        if not parquet_files:
            raise FileNotFoundError(f"❌ No parquet files found under {layer_root}")

        self.log.info(f"📂 Found {len(parquet_files)} {lake_domain} files in {layer_root}")
        return parquet_files

    # ==============================
    # Data Lake 데이터 로딩
    # ==============================

    def _get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """DuckDB 연결 반환 (싱글톤 패턴)"""
        if self.conn is None:
            self.conn = duckdb.connect(database=":memory:")
        return self.conn

    def _choose_best_partition(
            self,
            lake_domain: str,
            exchange_code: str,
            layer: str = "validated"
    ) -> Optional[Path]:
        """
        벤더 우선순위에 따라 최신 validated 파티션 선택

        Args:
            lake_domain: Data Lake 도메인 (e.g. 'symbol_list', 'fundamentals')
            exchange_code: 거래소 코드
            layer: 레이어명 (default: 'validated')

        Returns:
            선택된 파티션 디렉토리 경로 또는 None
        """
        for vendor in self.vendor_priority:
            partition_dir = latest_trd_dt_path(
                domain=lake_domain,
                exchange_code=exchange_code,
                vendor=vendor,
                layer=LAYERS.get(layer.upper(), layer)
            )
            if partition_dir:
                self.log.info(f"✅ Selected partition: {partition_dir} (vendor={vendor})")
                return partition_dir

        self.log.warning(f"⚠️ No partition found for {lake_domain} / {exchange_code}")
        return None

    def _load_parquet_from_partition(
            self,
            partition_dir: Path,
            domain: str,
            additional_columns: Optional[Dict[str, Any]] = None
    ) -> Optional[pd.DataFrame]:
        """
        파티션 디렉토리에서 parquet 파일 로드

        Args:
            partition_dir: 파티션 디렉토리 경로
            domain: 도메인명 (파일명 결정용)
            additional_columns: 추가할 컬럼 딕셔너리

        Returns:
            DataFrame 또는 None
        """
        pq_file = parquet_file_in(partition_dir, domain)

        assert pq_file, f"⚠️ Parquet file not found in {partition_dir}"

        try:
            conn = self._get_duckdb_connection()
            df = conn.execute(
                f"SELECT * FROM read_parquet('{pq_file.as_posix()}')"
            ).df()

            # 추가 컬럼 설정
            if additional_columns:
                for col_name, col_value in additional_columns.items():
                    df[col_name] = col_value

            self.log.info(f"✅ Loaded {len(df):,} rows from {pq_file.name}")
            return df

        except Exception as e:
            self.log.error(f"❌ Failed to load parquet: {pq_file} - {e}")
            return None

    def load_lake_data(
            self,
            lake_domain: str,
            exchange_codes: List[str],
            layer: str = "validated",
            additional_columns: Optional[Dict[str, Any]] = None
    ) -> List[pd.DataFrame]:
        """
        여러 거래소의 Data Lake 데이터 로드

        Args:
            lake_domain: Data Lake 도메인
            exchange_codes: 거래소 코드 리스트
            layer: 레이어명
            additional_columns: 각 거래소별 추가 컬럼

        Returns:
            DataFrame 리스트
        """
        dataframes = []

        for ex_code in exchange_codes:
            partition_dir = self._choose_best_partition(lake_domain, ex_code, layer)
            if not partition_dir:
                continue

            # 거래소별 추가 컬럼 설정
            extra_cols = {"exchange_code": ex_code}
            if additional_columns:
                extra_cols.update(additional_columns)

            df = self._load_parquet_from_partition(
                partition_dir,
                lake_domain,
                extra_cols
            )

            if df is not None:
                dataframes.append(df)

        return dataframes

    # ==============================
    # 데이터 변환 및 정규화
    # ==============================

    @abstractmethod
    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        도메인별 DataFrame 정규화 로직

        하위 클래스에서 반드시 구현:
        - 컬럼명 표준화
        - 데이터 타입 변환
        - 필수 필드 검증
        - 기본값 설정

        Args:
            df: 원본 DataFrame

        Returns:
            정규화된 DataFrame
        """
        pass

    def _generate_entity_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Entity ID 생성 (Optional)

        필요한 경우 하위 클래스에서 오버라이드

        Args:
            df: DataFrame

        Returns:
            entity_id 컬럼이 추가된 DataFrame
        """
        return df

    def _get_preferred_columns(self) -> List[str]:
        """
        출력 컬럼 순서 정의 (Optional)

        필요한 경우 하위 클래스에서 오버라이드

        Returns:
            컬럼명 리스트
        """
        return []

    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """컬럼 순서 재정렬"""
        preferred = self._get_preferred_columns()
        if not preferred:
            return df

        # 누락된 컬럼은 None으로 추가
        for col in preferred:
            if col not in df.columns:
                df[col] = None

        # 기존 컬럼 중 preferred에 없는 것 추가
        remaining = [c for c in df.columns if c not in preferred]
        final_order = preferred + remaining

        return df[final_order]

    # ==============================
    # 병합 및 저장
    # ==============================

    def merge_dataframes(
            self,
            dataframes: List[pd.DataFrame],
            deduplicate_on: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        여러 DataFrame 병합 및 중복 제거

        Args:
            dataframes: DataFrame 리스트
            deduplicate_on: 중복 제거 기준 컬럼 리스트

        Returns:
            병합된 DataFrame
        """
        if not dataframes:
            raise ValueError("❌ No dataframes to merge")

        merged = pd.concat(dataframes, ignore_index=True)
        self.log.info(f"📊 Merged {len(dataframes)} dataframes: {len(merged):,} total rows")

        if deduplicate_on:
            before = len(merged)
            merged = merged.drop_duplicates(subset=deduplicate_on, keep="first")
            after = len(merged)
            if before != after:
                self.log.info(f"🔄 Deduplicated: {before:,} → {after:,} rows ({before - after:,} removed)")

        return merged.reset_index(drop=True)

    def save_parquet(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Parquet 파일 저장

        Args:
            df: 저장할 DataFrame

        Returns:
            저장 결과 딕셔너리
        """
        try:
            table = pa.Table.from_pandas(df)
            pq.write_table(table, self.output_file.as_posix())

            file_size = self.output_file.stat().st_size
            self.log.info(
                f"✅ Parquet saved: {self.output_file} "
                f"({len(df):,} rows, {file_size:,} bytes)"
            )

            return {
                "file_path": self.output_file.as_posix(),
                "row_count": len(df),
                "file_size_bytes": file_size
            }

        except Exception as e:
            self.log.error(f"❌ Failed to save parquet: {e}")
            raise

    # ==============================
    # 메타데이터 관리
    # ==============================

    def save_metadata(
            self,
            row_count: int,
            source_info: Optional[Dict[str, Any]] = None,
            metrics: Optional[Dict[str, Any]] = None,
            context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Warehouse 단계 메타데이터 저장

        Args:
            row_count: 최종 Parquet 레코드 수
            source_info: validated 데이터셋별 lineage 정보 (dict)
            metrics: 도메인별 통계 정보
            context: Airflow context (dag_run_id, task_id 등)
        Returns:
            meta dict (Warehouse 표준 포맷)
        """
        from datetime import datetime, timezone

        meta = {
            "dataset": self.domain,
            "snapshot_dt": self.snapshot_dt,
            "status": "success",
            "record_count": row_count,
            "output_file": self.output_file.as_posix(),
            "sources": source_info or {},
            "metrics": metrics or {},
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # DAG/Task context 추가
        if context:
            meta["dag_run_id"] = context.get("run_id")
            ti = context.get("task_instance")
            if ti:
                meta["task_id"] = ti.task_id

        # 파티션 정보
        if self.partition_value:
            meta[self.partition_key] = self.partition_value

        # 메타 저장
        with open(self.meta_file, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)

        self.log.info(f"📘 Metadata saved: {self.meta_file}")
        return meta

    def update_registry(self, additional_key: Optional[str] = None):
        """
        전역 스냅샷 레지스트리 갱신

        Args:
            additional_key: 추가 키 (e.g. country_code)
        """
        kwargs = {
            "domain": self.domain,
            "snapshot_dt": self.snapshot_dt,
            "meta_file": self.meta_file,
        }

        if additional_key and self.partition_value:
            kwargs[additional_key] = self.partition_value

        update_global_snapshot_registry(**kwargs)
        self.log.info(f"📝 Registry updated: {self.domain} / {self.snapshot_dt}")

    # ==============================
    # 메인 빌드 프로세스
    # ==============================

    @abstractmethod
    def build(self) -> Dict[str, Any]:
        """
        Warehouse 빌드 메인 로직

        표준 프로세스:
        1. Data Lake에서 데이터 로드
        2. 정규화 및 변환
        3. Entity ID 생성 (optional)
        4. 병합 및 중복 제거
        5. 컬럼 순서 정렬
        6. Parquet 저장
        7. 메타데이터 저장
        8. 레지스트리 갱신

        Returns:
            빌드 결과 메타데이터
        """
        pass

    def cleanup(self):
        """리소스 정리"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()