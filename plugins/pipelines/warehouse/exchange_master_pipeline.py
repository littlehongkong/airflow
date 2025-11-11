"""
plugins/pipelines/warehouse/exchange_master_pipeline.py

💾 거래소 마스터 파이프라인
- Data Lake(validated) → Data Warehouse(exchange)
- BaseWarehousePipeline + transform_utils 기반 표준형
"""

import pandas as pd
from typing import Dict, Any, Optional, List

from plugins.pipelines.warehouse.base_warehouse_pipeline import BaseWarehousePipeline
from plugins.config.constants import WAREHOUSE_DOMAINS, DOMAIN_GROUPS
from plugins.utils.transform_utils import normalize_columns, safe_merge

from plugins.utils.loaders.lake.exchange_loader import load_exchange_list
from plugins.utils.loaders.lake.exchange_holiday_loader import load_exchange_holiday_list


class ExchangeMasterPipeline(BaseWarehousePipeline):
    """
    ✅ 거래소 마스터 파이프라인 (Warehouse Domain: exchange)

    [파이프라인 구조]
    1️⃣ Data Lake validated 데이터 로드
    2️⃣ 거래소 데이터 정규화
    3️⃣ 휴장일 데이터 병합
    4️⃣ Pandera 스키마 기준 컬럼 정렬 및 저장
    """

    def __init__(self, trd_dt: str, vendor: str = None, **kwargs):
        super().__init__(
            domain=WAREHOUSE_DOMAINS["exchange"],
            domain_group=DOMAIN_GROUPS["equity"],
            trd_dt=trd_dt,
            vendor=vendor,
        )
        self.trigger_source = kwargs.get("trigger_source", None)  # ✅ 로그용으로 저장

    # ============================================================
    # 📘 1️⃣ 거래소 리스트 정규화
    # ============================================================
    def _normalize_exchange_list(self, df: pd.DataFrame) -> pd.DataFrame:
        df = normalize_columns(df)

        code_col = next(
            (c for c in ("code", "exchange_code", "mic", "operatingmic") if c in df.columns),
            None
        )
        if not code_col:
            raise ValueError("❌ 거래소 데이터에 code/exchange_code 컬럼이 없습니다.")

        normalized = pd.DataFrame({
            "country_code": df.get("countryiso3", df.get("country_code", "")),
            "country_name": df.get("country", df.get("country_name", "")),
            "exchange_code": df[code_col],
            "exchange_name": df.get("name", df.get("exchange_name", "")),
            "currency": df.get("currency", ""),
            "country_iso2": df.get("countryiso2", ""),
            "timezone": df.get("timezone", ""),
            "operating_mic": df.get("operatingmic", df.get(code_col, "")),
            "active_tickers": df.get("activetickers", df.get("active_tickers", None)),
            "previous_day_updated_tickers": df.get(
                "previous_day_updated_tickers", df.get("previousdayupdatedtickers", None)
            ),
            "updated_tickers": df.get("updated_tickers", df.get("updatedtickers", None)),
        })

        for col in ["country_code", "exchange_code", "exchange_name"]:
            normalized[col] = normalized[col].astype(str).str.strip().str.upper()

        return normalized[
            (normalized["country_code"] != "") &
            (normalized["exchange_code"] != "")
        ]

    # ============================================================
    # 📘 2️⃣ 휴장일 정규화
    # ============================================================
    def _normalize_exchange_holiday(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        휴장일 데이터 표준화 + 컬럼 슬림화
        - exchange_code, holiday_name, holiday_date 3개만 남김
        """
        df = normalize_columns(df)

        # 1) 거래소 식별 컬럼 정규화
        exch_col = next(
            (c for c in ("exchange", "exchange_code", "code", "mic", "operatingmic") if c in df.columns),
            None
        )
        if exch_col is None:
            self.log.warning("⚠️ holiday_df에 거래소 식별 컬럼이 없습니다. (병합 생략)")
            df["exchange_code"] = None
        else:
            df = df.rename(columns={exch_col: "exchange_code"})

        # 2) 필요한 컬럼만 남김
        #   - 원본에 따라 holiday_date가 date/Datetime/string 혼재 가능 → pandas datetime 캐스팅
        keep_cols = []
        if "holiday_name" in df.columns:
            keep_cols.append("holiday_name")
        if "holiday_date" in df.columns:
            keep_cols.append("holiday_date")

        # 없으면 생성(널)
        if "holiday_name" not in df.columns:
            df["holiday_name"] = None
        if "holiday_date" not in df.columns:
            df["holiday_date"] = None

        df = df[["exchange_code", "holiday_name", "holiday_date"]]

        # 3) 타입 보정
        try:
            df["holiday_date"] = pd.to_datetime(df["holiday_date"], errors="coerce", utc=False)
        except Exception:
            pass

        # exchange_code 대문자/트림
        df["exchange_code"] = df["exchange_code"].astype(str).str.strip().str.upper()


        return df

    # ============================================================
    # 📘 3️⃣ 도메인 변환 로직 (Pandera 스키마 정합성 반영)
    # ============================================================
    def _transform_business_logic(
            self,
            exchange_list: pd.DataFrame,
            exchange_holiday: Optional[pd.DataFrame] = None,
            **kwargs
    ) -> pd.DataFrame:
        """
        거래소 + (축약된) 휴장일 통합 변환
        - 휴장일은 기준일(self.trd_dt) 이후 '가장 가까운 휴장일' 1건만 남겨서 join
        """
        # 1) 거래소 정규화 & 중복 제거
        normalized = self._normalize_exchange_list(exchange_list)
        deduped = normalized.drop_duplicates(subset=["exchange_code"], keep="first")

        # 2) 휴장일 축약: 기준일 이후 첫 휴장일만 남김
        if exchange_holiday is not None and not exchange_holiday.empty:
            holiday_df = self._normalize_exchange_holiday(exchange_holiday)

            # 기준일 파싱
            ref_dt = pd.to_datetime(self.trd_dt)

            # 기준일 이후(>=)만 필터
            future_holidays = holiday_df.loc[
                (holiday_df["holiday_date"].notna()) & (holiday_df["holiday_date"] >= ref_dt)
                ].copy()

            # 가장 가까운 휴장일 1건/거래소
            #   sort asc → groupby().head(1) or idxmin
            future_holidays = future_holidays.sort_values(["exchange_code", "holiday_date"], ascending=[True, True])
            next_holiday = future_holidays.groupby("exchange_code", as_index=False).first()
            # 필요 컬럼만 보장
            next_holiday = next_holiday[["exchange_code", "holiday_name", "holiday_date"]]

            # 3) 병합 (슬림해진 df만 JOIN)
            merged = safe_merge(
                left=deduped,
                right=next_holiday,
                left_on="exchange_code",
                right_on="exchange_code",
                how="left",
                suffixes=("", "_holiday"),
            )
        else:
            merged = deduped

        # 4) Pandera 스키마 누락 컬럼 보정
        for col in ["open_time", "close_time", "working_days"]:
            if col not in merged.columns:
                merged[col] = None

        # 5) 컬럼명 표준화 (원본 api 표기 → 표준 스키마 표기)
        rename_map = {
            "activetickers": "active_tickers",
            "previousdayupdatedtickers": "previous_day_updated_tickers",
            "updatedtickers": "updated_tickers",
        }
        merged = merged.rename(columns=rename_map)

        # ✅ 수치 컬럼 형 변환
        for col in ["active_tickers", "previous_day_updated_tickers", "updated_tickers"]:
            if col in merged.columns:
                merged[col] = pd.to_numeric(merged[col], errors="coerce")

        # 6) 완전 방어: 컬럼 중복 제거 + 여분 컬럼 드랍(holiday_* 접미사 등)
        merged = merged.loc[:, ~merged.columns.duplicated(keep="first")]
        drop_extras = [c for c in merged.columns if c.endswith("_holiday")]
        if drop_extras:
            merged = merged.drop(columns=drop_extras, errors="ignore")

        # 7) 컬럼 순서 표준화 (Pandera 스키마 기준)
        final_df = self._reorder_columns(merged)
        return final_df


    def _load_source_datasets(self) -> dict[str, pd.DataFrame]:
        """✅ Domain별 Loader를 직접 호출하는 명시적 버전"""


        exchange_df = load_exchange_list(
            domain_group=self.domain_group,
            vendor=self.vendor,
            trd_dt=self.trd_dt
        )

        exchanges = exchange_df[exchange_df['CountryISO3'] == self.country_code]['Code'].tolist()

        for exchange_code in exchanges:
            exchange_holiday_df = load_exchange_holiday_list(
                domain_group=self.domain_group,
                vendor=self.vendor,
                trd_dt=self.trd_dt,
                exchange_code=exchange_code
            )


        return {
            "exchange_holiday": exchange_holiday_df,
            "exchange_list": exchange_df
        }


    # ============================================================
    # 📘 5️⃣ 전체 빌드 실행
    # ============================================================
    def build(self, **kwargs) -> Dict[str, Any]:
        self.log.info(f"🏗️ Building ExchangeMasterPipeline | snapshot_dt={self.trd_dt}")

        sources = self._load_source_datasets()
        exchange_df = sources.get("exchange_list")
        holiday_df = sources.get("exchange_holiday")

        if exchange_df is None or exchange_df.empty:
            raise FileNotFoundError("❌ exchange_list 데이터가 없습니다.")

        final_df = self._transform_business_logic(
            exchange_list=exchange_df,
            exchange_holiday=holiday_df,
        )

        # ✅ 저장 + 메타 기록
        self.save_parquet(final_df)
        meta = self.save_metadata(
            row_count=len(final_df),
            source_datasets=list(sources.keys()),
            metrics={"vendor": self.vendor},
            context=kwargs.get("context"),
        )

        self.log.info(f"✅ [BUILD COMPLETE] exchange_master | {len(final_df):,} rows")
        return meta
