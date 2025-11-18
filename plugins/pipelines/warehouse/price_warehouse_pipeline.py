import pandas as pd
from typing import Dict, Any
from pathlib import Path

from plugins.pipelines.warehouse.base_warehouse_pipeline import BaseWarehousePipeline
from plugins.utils.loaders.equity.price_loader import load_prices
from plugins.utils.loaders.lake.exchange_loader import load_exchange_list
from plugins.utils.loaders.warehouse.asset_master_loader import load_asset_master_latest
from plugins.utils.id_generator import generate_or_reuse_entity_id


class PriceWarehousePipeline(BaseWarehousePipeline):
    """
    📘 Equity Prices Warehouse Pipeline
    ----------------------------------------------------
    ✅ 역할:
      - Data Lake (validated/prices) → Warehouse (snapshot/prices)
      - 종목별 일자 단위 시세 데이터 적재 (OHLC 정규화)
      - security_id 부여 및 스키마 표준화
    """

    def __init__(self, domain: str, domain_group: str, trd_dt: str, vendor: str = "eodhd", country_code: str = None):
        super().__init__(
            domain=domain,
            domain_group=domain_group,
            trd_dt=trd_dt,
            vendor=vendor,
            country_code=country_code,
        )

    # -------------------------------------------------------------------------
    # 1️⃣ Load Source Datasets
    # -------------------------------------------------------------------------
    def _load_source_datasets(self) -> Dict[str, pd.DataFrame]:
        """✅ validated layer parquet 파일 로드"""
        exchange_df = load_exchange_list(
            self.domain_group, vendor=self.vendor, trd_dt=self.trd_dt
        )
        exchanges = exchange_df[exchange_df["CountryISO3"] == self.country_code]["Code"].tolist()
        price_df = load_prices(
            domain_group=self.domain_group,
            vendor=self.vendor,
            exchange_codes=exchanges,
            trd_dt=self.trd_dt,
        )

        # 로그
        dup_tickers = price_df[price_df.duplicated(subset=["code"], keep=False)]["code"].unique().tolist()
        print(
            f"🚨 Duplicate tickers detected: {dup_tickers[:20]}..."
            if dup_tickers else "✅ No duplicate tickers found across exchanges."
        )

        self.log.info(f"✅ Loaded {len(price_df):,} rows from {len(exchanges)} exchanges")
        return {"prices": price_df}

    # -------------------------------------------------------------------------
    # 2️⃣ Normalize
    # -------------------------------------------------------------------------
    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """✅ 컬럼명 정규화 및 스키마 표준화"""
        df.columns = [c.strip().lower().replace(".", "_") for c in df.columns]
        df = df.loc[:, ~df.columns.duplicated()]

        rename_map = {
            "code": "ticker",
            "marketcapitalization": "market_cap",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        df["ticker"] = df["ticker"].astype(str)

        return df

    # -------------------------------------------------------------------------
    # 3️⃣ Business Logic
    # -------------------------------------------------------------------------
    def _transform_business_logic(self, **kwargs) -> pd.DataFrame:
        """✅ 스키마 필드 구성 및 ID 매핑"""
        df = kwargs["prices"]
        df = self._normalize_dataframe(df)

        # ✅ security_id 부여
        df = self._assign_persistent_security_id(df)

        # ✅ OHLC 기준 컬럼 필터링 (스키마 준수)
        required_cols = [
            "security_id",
            "ticker",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            # "market_cap",
        ]
        existing_cols = [c for c in required_cols if c in df.columns]
        df = df[existing_cols]

        # 결측값/타입 정리
        df = df.fillna({"volume": 0, "market_cap": 0})

        # ✅ 스키마 순서 맞추기
        schema_order = [
            "security_id",
            "ticker",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "market_cap"
        ]
        df = df.reindex(columns=schema_order)

        return df

    # -------------------------------------------------------------------------
    # 4️⃣ Build
    # -------------------------------------------------------------------------
    def build(self, **kwargs) -> Dict[str, Any]:
        """✅ 전체 프로세스 실행: Load → Transform → Save"""
        self.log.info(f"🚀 Building Price Warehouse snapshot for {self.trd_dt}")

        datasets = self._load_source_datasets()
        df = self._transform_business_logic(**datasets)

        # 저장
        save_info = self.save_parquet(df)

        # 메타데이터 기록
        meta_info = self.save_metadata(
            row_count=len(df),
            file_path=save_info["file_path"],
            vendor=self.vendor,
            layer=self.layer,
            snapshot_date=self.trd_dt,
        )

        self._update_domain_metadata(record_count=len(df))
        self.log.info(f"🏁 Price Warehouse build complete: {save_info['file_path']}")
        return meta_info

    # -------------------------------------------------------------------------
    # 🔑 Persistent ID Assignment
    # -------------------------------------------------------------------------
    def _assign_persistent_security_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """asset_master 기반 security_id 매핑"""
        if df.empty:
            self.log.warning("⚠️ No records to assign security_id (empty DataFrame).")
            return df

        master_df = load_asset_master_latest(domain_group=self.domain_group, country_code=self.country_code)

        # ✅ 국가별 join key 설정
        # todo :  join_keys = ["ticker"] 로만 코드 일원화예정(eodhd 오류 해결 완료시)
        if self.country_code in ["USA", "US"]:
            join_keys = ["ticker"]  # 🇺🇸 미국은 ticker 단위 매핑
        else:
            join_keys = ["ticker", "exchange_code"]  # 🇰🇷 등은 거래소별

        self.log.info(f"🧩 Using join keys for mapping: {join_keys}")

        # ✅ key 정규화 (대소문자/공백)
        for col in join_keys:
            df[col] = df[col].astype(str).str.upper().str.strip()
            master_df[col] = master_df[col].astype(str).str.upper().str.strip()

        # ✅ 매핑 수행 (벡터화)
        df = df.merge(
            master_df[join_keys + ["security_id"]],
            on=join_keys,
            how="inner"
        )

        self.log.info(
            f"🔑 Assigned security_id for {len(df):,} rows "
        )
        return df


