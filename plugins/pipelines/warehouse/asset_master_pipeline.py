# plugins/pipelines/warehouse/asset_master_pipeline.py
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime, timezone

from plugins.pipelines.warehouse.base_warehouse_pipeline import BaseWarehousePipeline
from plugins.config.constants import WAREHOUSE_DOMAINS, WAREHOUSE_SOURCE_MAP, VENDORS


class AssetMasterPipeline(BaseWarehousePipeline):
    """
    ✅ 종목 마스터 파이프라인 (Asset Master)
    입력: validated/symbol_list, symbol_changes
    출력: warehouse/asset/snapshot_dt=YYYY-MM-DD/asset.parquet
    """

    def __init__(self, snapshot_dt: str, vendor_priority: List[str] = None):
        super().__init__(
            domain=WAREHOUSE_DOMAINS["ASSET"],
            snapshot_dt=snapshot_dt,
            vendor_priority=vendor_priority,
        )

    # =============================
    # 데이터 정규화
    # =============================
    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = df.columns.str.lower()

        # 필수 컬럼 매핑
        symbol_col = next((c for c in ["code", "symbol", "ticker"] if c in df.columns), None)
        if not symbol_col:
            raise ValueError("❌ 종목 데이터에 symbol/code 컬럼이 없습니다.")

        normalized = pd.DataFrame({
            "symbol": df[symbol_col].astype(str).str.strip().str.upper(),
            "name": df.get("name", df.get("symbol_name", "")),
            "exchange_code": df.get("exchange", df.get("exchange_code", "")),
            "currency": df.get("currency", ""),
            "isin": df.get("isin", ""),
            "figi": df.get("figi", ""),
            "sector": df.get("sector", ""),
            "industry": df.get("industry", ""),
            "country": df.get("country", ""),
        })

        # 필수값 필터링
        normalized = normalized[normalized["symbol"] != ""].reset_index(drop=True)
        return normalized

    def _get_preferred_columns(self) -> List[str]:
        return [
            "symbol",
            "name",
            "exchange_code",
            "currency",
            "isin",
            "figi",
            "sector",
            "industry",
            "country",
        ]

    # =============================
    # 메인 빌드 로직
    # =============================
    def build(self, **kwargs) -> Dict[str, Any]:
        """
        종목 마스터 빌드
        1️⃣ symbol_list + symbol_changes validated 데이터 로드
        2️⃣ 병합 및 정규화
        3️⃣ 중복 제거
        4️⃣ Parquet 저장 + 메타데이터 기록
        """
        self.log.info(f"🏗️ Building {self.domain} for snapshot_dt={self.snapshot_dt}")

        # 1️⃣ 소스 파일 로드
        source_domains = WAREHOUSE_SOURCE_MAP[self.domain]  # ["symbol_list", "symbol_changes"]
        parquet_files = self._get_validated_files(source_domains)

        if not parquet_files:
            raise FileNotFoundError(f"❌ No validated parquet files found for {source_domains}")
        self.log.info(f"📂 Found {len(parquet_files)} parquet files from {source_domains}")

        # 2️⃣ 파일 병합
        conn = self._get_duckdb_connection()
        files_expr = ", ".join([f"'{str(p)}'" for p in parquet_files])
        raw_df = conn.execute(f"SELECT * FROM read_parquet([{files_expr}])").df()
        self.log.info(f"📊 Loaded {len(raw_df):,} raw records")

        # 3️⃣ 정규화 및 중복 제거
        normalized = self._normalize_dataframe(raw_df)
        deduped = normalized.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
        self.log.info(f"🔄 Deduplicated: {len(normalized)} → {len(deduped)}")

        final_df = self._reorder_columns(deduped)

        # 4️⃣ 저장
        save_result = self.save_parquet(final_df)

        # 5️⃣ 메타데이터
        meta = self.save_metadata(
            row_count=len(final_df),
            source_info={
                "datasets": source_domains,
                "validated_files": [str(p) for p in parquet_files],
                "record_counts": {self.domain: len(final_df)},
                "last_validated_timestamps": {
                    d: datetime.now(timezone.utc).isoformat() for d in source_domains
                },
            },
            metrics={
                "symbol_count": len(final_df),
                "partial_records": int(final_df[["sector", "industry"]].isna().any(axis=1).sum()),
                "vendor_priority": [VENDORS["EODHD"]]
            },
            context=kwargs.get("context"),
        )

        self.update_registry()
        self.log.info(f"✅ [BUILD COMPLETE] {self.domain} | {len(final_df)} symbols")

        return meta
