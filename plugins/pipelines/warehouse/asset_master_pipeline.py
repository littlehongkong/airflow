import pandas as pd
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

from plugins.pipelines.warehouse.base_warehouse_pipeline import BaseWarehousePipeline
from plugins.config.constants import (
    WAREHOUSE_DOMAINS,
    DOMAIN_GROUPS,
    VENDORS
)
from plugins.utils.transform_utils import normalize_columns, safe_merge
from plugins.utils.id_generator import generate_entity_id
from plugins.utils.exchange_loader import load_exchanges_by_country


class AssetMasterPipeline(BaseWarehousePipeline):
    """
    ✅ 자산 마스터 파이프라인 (Warehouse Domain: asset_master)
    --------------------------------------------------------
    - Symbol / Fundamentals / Exchange 데이터를 병합하여 국가 단위 마스터 생성
    - 고유 ID 생성은 id_generator 유틸 사용
    - Exchange 매핑은 latest_snapshot_meta.json 기반 동적 로드
    """

    def __init__(
        self,
        trd_dt: str,
        domain_group: str,
        country_code: Optional[str] = None,
        vendor_priority: Optional[List[str]] = None,
    ):
        super().__init__(
            domain=WAREHOUSE_DOMAINS["asset"],
            domain_group=domain_group,
            trd_dt=trd_dt,
            vendor_priority=vendor_priority,
        )
        self.country_code = country_code
        self.exchanges = []

    # ============================================================
    # 📘 1️⃣ 거래소 매핑 로드 (latest_snapshot_meta 기반)
    # ============================================================
    def _load_exchange_codes(self) -> List[str]:
        """latest_snapshot_meta.json에서 해당 국가의 거래소 목록 로드"""
        if not self.country_code:
            raise ValueError("❌ country_code 값이 누락되었습니다. (예: 'KOR', 'USA')")
        try:
            mapping = load_exchanges_by_country([self.country_code])
            exchanges = mapping.get(self.country_code, [])
            if not exchanges:
                raise ValueError(f"❌ {self.country_code} 국가에 대한 거래소 정보가 없습니다.")
            self.log.info(f"🌍 {self.country_code} 거래소 코드: {exchanges}")
            return exchanges
        except Exception as e:
            raise RuntimeError(f"⚠️ 거래소 매핑 로드 실패: {e}")

    # ============================================================
    # 📘 2️⃣ Symbol 리스트 정규화
    # ============================================================
    def _normalize_symbol_list(self, df: pd.DataFrame) -> pd.DataFrame:
        df = normalize_columns(df)
        return pd.DataFrame({
            "ticker": df.get("code", df.get("symbol", "")).astype(str).str.upper(),
            "security_name": df.get("name", ""),
            "exchange_code": df.get("exchange", df.get("exchange_code", "")),
            "currency_code": df.get("currency", ""),
            "country_code": df.get("countryiso2", self.country_code),
            "isin": df.get("isin", ""),
        }).drop_duplicates(subset=["ticker", "exchange_code"])

    # ============================================================
    # 📘 3️⃣ Fundamentals 정규화
    # ============================================================
    def _normalize_fundamentals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = normalize_columns(df)
        base = pd.DataFrame({
            "ticker": df.get("code", ""),
            "exchange_code": df.get("exchange", ""),
            "sector": df.get("sector", ""),
            "industry": df.get("industry", ""),
            "gic_sector": df.get("gicsector", ""),
            "gic_group": df.get("gicgroup", ""),
            "gic_industry": df.get("gicindustry", ""),
            "gic_sub_industry": df.get("gicsubindustry", ""),
            "ipo_date": df.get("ipodate", None),
            "is_delisted": df.get("isdelisted", None),
            "currency_code": df.get("currencycode", df.get("currency", "")),
            "isin": df.get("isin", ""),
            "cusip": df.get("cusip", ""),
            "lei": df.get("lei", ""),
            "last_fundamental_update": df.get("updatedat", None),
        })
        base["ticker"] = base["ticker"].astype(str).str.strip().str.upper()
        return base.drop_duplicates(subset=["ticker", "exchange_code"], keep="first")

    # ============================================================
    # 📘 4️⃣ Exchange 리스트 정규화
    # ============================================================
    def _normalize_exchange_list(self, df: pd.DataFrame) -> pd.DataFrame:
        df = normalize_columns(df)
        return pd.DataFrame({
            "exchange_code": df.get("code", ""),  # ✅ 명시적으로 변경
            "exchange_name": df.get("name", ""),
            "country_code": df.get("countryiso2", ""),
            "currency_code": df.get("currency", ""),
        })

    # ============================================================
    # 📘 5️⃣ 병합 및 변환 로직
    # ============================================================
    def _transform_business_logic(
        self,
        symbol_list: pd.DataFrame,
        fundamentals: Optional[pd.DataFrame] = None,
        exchange_list: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        merged = symbol_list.copy()

        # ✅ fundamentals 병합
        if fundamentals is not None and not fundamentals.empty:
            merged = safe_merge(merged, fundamentals, on=["ticker", "exchange_code"], how="left")

        # ✅ exchange 정보 병합
        if exchange_list is not None and not exchange_list.empty:
            merged = safe_merge(merged, exchange_list, on=["exchange_code"], how="left")

        merged = merged.drop_duplicates(subset=["ticker", "exchange_code"])

        # ✅ 고유 ID 생성 (AST_{b32hash})
        merged["security_id"] = merged.apply(
            lambda x: generate_entity_id(
                prefix="AST",
                country=x.get("country_code") or self.country_code or "XX",
                exchange=x.get("exchange_code", ""),
                ticker=x.get("ticker", "")
            ),
            axis=1,
        )

        # ✅ 날짜 메타필드
        merged["trd_dt"] = self.trd_dt
        merged["last_symbol_update"] = self.trd_dt
        merged["ingested_at"] = datetime.now(timezone.utc).isoformat()
        merged["source_vendor"] = VENDORS["EODHD"]

        # ✅ 컬럼 순서 정렬
        preferred_cols = [
            "security_id", "ticker", "security_name", "exchange_code", "country_code",
            "security_type", "isin", "cusip", "lei",
            "sector", "industry", "gic_sector", "gic_group",
            "gic_industry", "gic_sub_industry", "ipo_date", "is_delisted",
            "currency_code", "last_fundamental_update", "last_symbol_update",
            "ingested_at", "source_vendor", "snapshot_date"
        ]
        for col in preferred_cols:
            if col not in merged.columns:
                merged[col] = None
        return merged[preferred_cols]

    # ============================================================
    # 📘 6️⃣ 전체 빌드 실행
    # ============================================================
    def build(self, **kwargs) -> Dict[str, Any]:
        self.log.info(f"🏗️ Building AssetMasterPipeline | trd_dt={self.trd_dt}, country={self.country_code}")

        # ✅ 거래소 매핑 로드
        self.exchanges = self._load_exchange_codes()


        # ✅ 데이터 로드 (BaseWarehousePipeline 공통)
        sources = self._load_source_datasets(self.domain)
        symbol_df = sources.get("symbol_list")
        fundamental_df = sources.get("fundamentals")
        exchange_df = sources.get("exchange_list")

        if symbol_df is None or symbol_df.empty:
            raise FileNotFoundError("❌ symbol_list 데이터가 없습니다.")

        # ✅ 병합 및 변환
        final_df = self._transform_business_logic(
            symbol_list=symbol_df,
            fundamentals=fundamental_df,
            exchange_list=exchange_df,
        )

        # ✅ 저장 및 메타 기록
        self.save_parquet(final_df)
        meta = self.save_metadata(
            row_count=len(final_df),
            source_datasets=list(sources.keys()),
            metrics={
                "symbol_count": len(final_df),
                "vendor_priority": self.vendor_priority,
                "exchanges": self.exchanges,
            },
            context=kwargs.get("context"),
        )

        self.log.info(f"✅ [BUILD COMPLETE] asset_master | {len(final_df):,} symbols ({self.country_code})")
        return meta
