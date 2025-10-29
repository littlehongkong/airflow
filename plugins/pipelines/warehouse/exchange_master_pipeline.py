# plugins/pipelines/warehouse/exchange_master_pipeline.py
import pandas as pd
from typing import Dict, Any, List
from plugins.pipelines.warehouse.base_warehouse_pipeline import BaseWarehousePipeline
from datetime import datetime, timezone

class ExchangeMasterPipeline(BaseWarehousePipeline):
    """
    ✅ 거래소 마스터 파이프라인

    입력: Data Lake validated/exchange_list
    출력: warehouse/exchange_master/snapshot_dt=YYYY-MM-DD/exchange_master.parquet

    특징:
    - 전역 거래소 목록 구축 (파티션 없음)
    - 국가별 거래소 매핑 생성
    """

    def __init__(self, snapshot_dt: str, vendor_priority: List[str] = None):
        from plugins.config.constants import WAREHOUSE_DOMAIN

        super().__init__(
            domain=WAREHOUSE_DOMAIN.get("EXCHANGE_MASTER"),
            snapshot_dt=snapshot_dt,
            vendor_priority=vendor_priority
        )

    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        거래소 데이터 정규화

        필수 컬럼:
        - country_code (국가 코드)
        - exchange_code (거래소 코드)
        - exchange_name (거래소명)
        """
        df = df.copy()
        df.columns = df.columns.str.lower()

        # 필수 컬럼 매핑
        code_col = next(
            (c for c in ("code", "exchange_code", "mic", "operatingmic")
             if c in df.columns),
            None
        )

        if not code_col:
            raise ValueError("❌ 거래소 데이터에 code/exchange_code 컬럼이 없습니다.")

        # 표준 컬럼 생성
        normalized = pd.DataFrame({
            "country_code": df.get("countryiso3", df.get("country_code", "")),
            "country_name": df.get("country", df.get("country_name", "")),
            "exchange_code": df[code_col],
            "exchange_name": df.get("name", df.get("exchange_name", "")),
            "currency": df.get("currency", ""),
            "country_iso2": df.get("countryiso2", ""),
            "timezone": df.get("timezone", ""),
            "operating_mic": df.get("operatingmic", df.get(code_col, "")),
        })

        # 데이터 정제
        normalized["country_code"] = (
            normalized["country_code"]
            .astype(str).str.strip().str.upper()
        )
        normalized["exchange_code"] = (
            normalized["exchange_code"]
            .astype(str).str.strip().str.upper()
        )
        normalized["exchange_name"] = (
            normalized["exchange_name"]
            .astype(str).str.strip()
        )

        # 필수값 필터링
        normalized = normalized[
            (normalized["country_code"] != "") &
            (normalized["exchange_code"] != "")
            ]

        return normalized

    def _get_preferred_columns(self) -> List[str]:
        """출력 컬럼 순서"""
        return [
            "country_code",
            "country_name",
            "exchange_code",
            "exchange_name",
            "currency",
            "country_iso2",
            "timezone",
            "operating_mic",
        ]

    def _build_country_exchange_map(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """
        국가별 거래소 코드 매핑 생성

        Returns:
            {"KOR": ["KS", "KQ"], "USA": ["US", "NASDAQ"], ...}
        """
        mapping = (
            df.groupby("country_code")["exchange_code"]
            .apply(lambda s: sorted(set(s.dropna().astype(str))))
            .to_dict()
        )
        return mapping

    # ✅ 최종 build() 구조 (불필요한 유효성 검증 코드 제거 후)
    def build(self, **kwargs) -> Dict[str, Any]:
        """
        거래소 마스터 빌드

        프로세스:
        1. validated/exchange_list의 모든 파티션 로드
        2. 정규화 및 병합
        3. 중복 제거
        4. 국가별 거래소 매핑 생성
        5. Parquet 저장
        6. 메타데이터 저장
        7. 레지스트리 갱신
        """
        self.log.info(f"🏗️ Building exchange_master for snapshot_dt={self.snapshot_dt}")

        parquet_files = self._get_validated_files(self.domain)
        self.log.info(f"📂 Found {len(parquet_files)} exchange_list files")

        # 2️⃣ 모든 parquet 파일 병합
        conn = self._get_duckdb_connection()
        files_expr = ", ".join([f"'{str(p)}'" for p in parquet_files])
        raw_df = conn.execute(f"SELECT * FROM read_parquet([{files_expr}])").df()
        self.log.info(f"📊 Loaded {len(raw_df):,} raw records")

        # 3️⃣ 정규화
        normalized = self._normalize_dataframe(raw_df)

        # 4️⃣ 중복 제거
        deduplicated = normalized.drop_duplicates(subset=["exchange_code"], keep="first").reset_index(drop=True)
        self.log.info(f"🔄 Deduplicated: {len(normalized):,} → {len(deduplicated):,} rows")

        # 5️⃣ 컬럼 순서 정렬
        final_df = self._reorder_columns(deduplicated)

        # 6️⃣ 국가별 거래소 매핑 생성
        country_map = self._build_country_exchange_map(final_df)
        self.log.info(f"🗺️ Country-Exchange mapping: {len(country_map)} countries")

        # 7️⃣ Parquet 저장
        save_result = self.save_parquet(final_df)

        # 8️⃣ 메타데이터 저장
        meta = self.save_metadata(
            row_count=len(final_df),
            source_info={
                "datasets": [self.domain],
                "validated_files": [str(p) for p in parquet_files],
                "record_counts": {"exchange_list": len(final_df)},
                "last_validated_timestamps": {
                    "exchange_list": datetime.now(timezone.utc).isoformat()
                },
            },
            metrics={"country_count": len(country_map), "vendor_priority": ["eodhd"]},
            context=kwargs.get("context"),
        )

        # 9️⃣ 레지스트리 갱신
        self.update_registry()

        self.log.info(
            f"✅ [BUILD COMPLETE] exchange_master | "
            f"{len(final_df):,} exchanges | {len(country_map)} countries"
        )

        return meta
