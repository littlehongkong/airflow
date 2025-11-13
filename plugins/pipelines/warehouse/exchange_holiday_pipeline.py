"""
💾 거래소 휴장일 마스터 파이프라인 (최종본)
- AssetMasterPipeline 스타일로 재작성됨
- 국가별 holiday_master 저장 (비어 있어도 저장)
"""

import pandas as pd
import json
from typing import Dict, Any

from plugins.pipelines.warehouse.base_warehouse_pipeline import BaseWarehousePipeline
from plugins.utils.transform_utils import normalize_columns
from plugins.utils.warehouse_utils import load_exchange_details_by_country


class ExchangeHolidayPipeline(BaseWarehousePipeline):
    """
    Warehouse Domain: holiday_master
    국가별 holiday_master 스냅샷 생성
    """

    def __init__(self, domain: str, domain_group: str, trd_dt: str, vendor: str = None, **kwargs):
        super().__init__(
            domain=domain,            # "holiday"
            domain_group=domain_group,
            trd_dt=trd_dt,
            vendor=vendor,
            country_code=kwargs.get("country_code")
        )
        self.layer = "warehouse"
        self.master_countries = kwargs.get("master_countries")  # airflow variable 기반 (예: ["USA", "KOR"])

    # ------------------------------------------------------------
    # 1️⃣ 국가별 exchange_detail 로드
    # ------------------------------------------------------------
    def _load_source_datasets(self, country: str) -> pd.DataFrame:
        df = load_exchange_details_by_country(
            domain_group=self.domain_group,
            vendor=self.vendor,
            trd_dt=self.trd_dt,
            country_code=country
        )
        if df is None:
            return pd.DataFrame()
        return df

    # ------------------------------------------------------------
    # 2️⃣ 휴장일 추출
    # ------------------------------------------------------------
    def _extract_holidays(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        df = normalize_columns(df)
        holidays_json_col = "exchangeholidays"

        if holidays_json_col not in df.columns:
            return pd.DataFrame()

        self.log.info(f"df.columns = {df.columns}")

        records = []
        for _, row in df.iterrows():
            exch = row.get("code") or row.get("exchange_code")
            country = row.get("country") or row.get("country_code")

            holidays = json.loads(row.get("exchangeholidays", {}))
            for k in holidays.keys():
                records.append(
                    {
                        "country_code": country,
                        "exchange_code": exch,
                        "holiday_name": holidays.get(k).get("Holiday"),
                        "holiday_date": holidays.get(k).get("Date"),
                        "holiday_type": holidays.get(k).get("Type"),
                    }
                )

        if not records:
            return pd.DataFrame()

        df_out = pd.DataFrame(records)
        df_out["holiday_date"] = pd.to_datetime(df_out["holiday_date"], errors="coerce")
        df_out = df_out.dropna(subset=["holiday_date"])

        return df_out.sort_values(["exchange_code", "holiday_date"]).reset_index(drop=True)


    def _transform_business_logic(self, **kwargs):
        """
        Holiday pipeline은 AssetMaster 스타일로 build() 내부에서 모든 로직을 수행하므로
        이 메서드는 사용되지 않는다.
        """
        return None

    # ------------------------------------------------------------
    # 3️⃣ 전체 빌드 실행 (AssetMasterPipeline 스타일)
    # ------------------------------------------------------------
    def build(self, **kwargs) -> Dict[str, Any]:

        self.log.info(f"🏗️ Building ExchangeHolidayPipeline | trd_dt={self.trd_dt}, country={self.country_code}")

        saved = []

        # ------------------------------------------------------------
        # 1) Load source
        # ------------------------------------------------------------
        df = self._load_source_datasets(country=self.country_code)
        holiday_df = self._extract_holidays(df)

        # ------------------------------------------------------------
        # 2) No holidays? Save empty DF
        # ------------------------------------------------------------
        if holiday_df.empty:
            self.log.warning(f"⚠️ No holiday data for {self.country_code}. Saving empty parquet")
            holiday_df = pd.DataFrame(
                columns=[
                    "exchange_code", "country_code",
                    "holiday_name", "holiday_date", "holiday_type"
                ]
            )

        # ------------------------------------------------------------
        # 3) Save parquet (country partition 자동 적용)
        # ------------------------------------------------------------
        parquet_path = self.save_parquet(holiday_df)

        # ------------------------------------------------------------
        # 4) Save metadata
        # ------------------------------------------------------------
        meta = self.save_metadata(
            row_count=len(holiday_df),
            source_datasets=[f"exchange_detail_{self.country_code}"],
            metrics={"vendor": self.vendor, "country": self.country_code},
            context=kwargs.get("context"),
        )

        # ------------------------------------------------------------
        # 5) 결과 저장 (saved 리스트에 append)
        # ------------------------------------------------------------
        saved.append({
            "country": self.country_code,
            "rows": len(holiday_df),
            "parquet": str(parquet_path),
            "meta": meta,
        })

        # ------------------------------------------------------------
        # 6) 전체 결과 반환
        # ------------------------------------------------------------
        return {
            "snapshot_dt": self.trd_dt,
            "row_count_total": len(holiday_df),
            "saved": saved,
        }
