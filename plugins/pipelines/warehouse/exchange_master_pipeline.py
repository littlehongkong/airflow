"""
💾 거래소 마스터 파이프라인
- Data Lake(validated) → Data Warehouse(exchange_master)
"""

import pandas as pd
import json
from typing import Dict, Any

from plugins.pipelines.warehouse.base_warehouse_pipeline import BaseWarehousePipeline
from plugins.utils.transform_utils import normalize_columns, safe_merge
from plugins.utils.loaders.lake.exchange_loader import load_exchange_list
from plugins.utils.warehouse_utils import load_exchange_details_by_country

class ExchangeMasterPipeline(BaseWarehousePipeline):
    """
    ✅ 거래소 마스터 파이프라인 (Warehouse Domain: exchange_master)

    [파이프라인 구조]
    1️⃣ Data Lake validated 데이터 로드
    2️⃣ 거래소 리스트 + 상세데이터 정규화
    3️⃣ TradingHours 등 flatten 후 병합
    4️⃣ Pandera 스키마 정렬 및 저장
    """

    def __init__(self, domain: str, domain_group: str, trd_dt: str, vendor: str = None, **kwargs):
        super().__init__(domain=domain, domain_group=domain_group, trd_dt=trd_dt, vendor=vendor)
        self.layer = "warehouse"
        self.master_countries = kwargs.get("master_countries")

        # ============================================================
    # 1️⃣ 거래소 리스트 정규화
    # ============================================================
    def _normalize_exchange_list(self, df: pd.DataFrame) -> pd.DataFrame:
        df = normalize_columns(df)

        code_col = "code"

        exclude_exchanges = ["FOREX", "CC", "MONEY", "EUFUND", "GBOND"]
        df = df[~df[code_col].astype(str).str.upper().isin(exclude_exchanges)]

        normalized = pd.DataFrame({
            "country_code": df.get("countryiso3", df.get("country_code", "")),
            "exchange_code": df[code_col].astype(str).str.strip().str.upper(),
            "exchange_name": df.get("name", ""),
            "currency_code": df.get("currency", ""),
            "country_iso2": df.get("countryiso2", ""),
            "operating_mic": df.get("operatingmic", df.get(code_col, "")),
        })

        return normalized.dropna(subset=["exchange_code"]).reset_index(drop=True)

    # ============================================================
    # 2️⃣ 거래소 상세데이터 정규화 (TradingHours 중심)
    # ============================================================
    def _normalize_exchange_detail(self, df: pd.DataFrame) -> pd.DataFrame:
        df = normalize_columns(df)
        if df.empty:
            return pd.DataFrame()

        exch_col = next((c for c in ("code") if c in df.columns), None)
        if exch_col:
            df = df.rename(columns={exch_col: "exchange_code"})

        self.log.info(df.iloc[0])

        # TradingHours flatten
        df["open_time"] = df["tradinghours"].apply(
            lambda x: json.loads(x).get("Open") if isinstance(x, str) else x.get("Open") if isinstance(x,
                                                                                                       dict) else None
        )

        df["close_time"] = df["tradinghours"].apply(
            lambda x: json.loads(x).get("Close") if isinstance(x, str) else x.get("Close") if isinstance(x,
                                                                                                         dict) else None
        )

        df["open_time_utc"] = df["tradinghours"].apply(
            lambda x: json.loads(x).get("OpenUTC") if isinstance(x, str) else x.get("OpenUTC") if isinstance(x,
                                                                                                             dict) else None
        )

        df["close_time_utc"] = df["tradinghours"].apply(
            lambda x: json.loads(x).get("CloseUTC") if isinstance(x, str) else x.get("CloseUTC") if isinstance(x,
                                                                                                               dict) else None
        )

        df["working_days"] = df["tradinghours"].apply(
            lambda x: json.loads(x).get("WorkingDays") if isinstance(x, str) else x.get("WorkingDays") if isinstance(x,
                                                                                                                     dict) else None
        )
        keep_cols = [
            "exchange_code", "timezone", "workingdays",
            "open_time", "close_time", "open_time_utc", "close_time_utc", "working_days",
            "activetickers", "previousdayupdatedtickers", "updatedtickers"
        ]
        df = df[[c for c in keep_cols if c in df.columns]].copy()
        df = df.rename(columns={
            "activetickers": "active_tickers",
            "previousdayupdatedtickers":"previous_day_updated_tickers",
            "updatedtickers": "updated_tickers"
        })

        df = df.loc[:, ~df.columns.duplicated(keep="first")]

        df["exchange_code"] = df["exchange_code"].astype(str).str.upper().str.strip()
        return df.drop_duplicates(subset=["exchange_code"])

    # ============================================================
    # 3️⃣ 도메인 변환 로직
    # ============================================================
    def _transform_business_logic(self, exchange_list: pd.DataFrame, exchange_detail: pd.DataFrame) -> pd.DataFrame:

        merged = safe_merge(
            df1=exchange_list,
            df2=exchange_detail,
            on="exchange_code",
            how="inner",
        )

        self.log.info(f"merged.columns : {merged.columns}")

        # 컬럼 정리 및 타입 보정
        for col in ["active_tickers", "updated_tickers"]:
            if col in merged.columns:
                merged[col] = pd.to_numeric(merged[col], errors="coerce")

        final_df = self._reorder_columns(merged)
        return final_df

    # ============================================================
    # 4️⃣ 데이터 로드 및 빌드
    # ============================================================
    def _load_source_datasets(self) -> dict[str, pd.DataFrame]:

        # 1) exchange_list 로드
        exchange_df = load_exchange_list(self.domain_group, self.vendor, self.trd_dt)

        # 2) master_countries 기준으로 exchange_detail 로드
        all_details = []

        for country in self.master_countries:
            detail_by_country = load_exchange_details_by_country(
                domain_group=self.domain_group,
                vendor=self.vendor,
                trd_dt=self.trd_dt,
                country_code=country
            )
            if detail_by_country is not None and not detail_by_country.empty:
                all_details.append(detail_by_country)

        if all_details:
            exchange_detail_df = pd.concat(all_details, ignore_index=True)
        else:
            exchange_detail_df = pd.DataFrame()

        return {
            "exchange_list": exchange_df,
            "exchange_detail": exchange_detail_df
        }

    def build(self, **kwargs) -> Dict[str, Any]:
        self.log.info(f"🏗️ Building ExchangeMasterPipeline | snapshot_dt={self.trd_dt}")

        sources = self._load_source_datasets()
        if sources["exchange_list"].empty:
            raise FileNotFoundError("❌ exchange_list 데이터가 없습니다.")

        norm_list = self._normalize_exchange_list(sources["exchange_list"])
        norm_detail = self._normalize_exchange_detail(sources["exchange_detail"])
        final_df = self._transform_business_logic(norm_list, norm_detail)

        self.save_parquet(final_df)
        meta = self.save_metadata(
            row_count=len(final_df),
            source_datasets=list(sources.keys()),
            vendor=self.vendor,
        )

        self.log.info(f"✅ [BUILD COMPLETE] exchange_master | {len(final_df):,} rows")
        return meta
