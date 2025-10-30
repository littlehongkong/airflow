# plugins/pipelines/warehouse/exchange_master_pipeline.py
import pandas as pd
from typing import Dict, Any, List

from plugins.pipelines.warehouse.base_warehouse_pipeline import BaseWarehousePipeline
from plugins.config.constants import WAREHOUSE_DOMAINS


class ExchangeMasterPipeline(BaseWarehousePipeline):
    """
    ✅ 거래소 마스터 파이프라인 (표준 템플릿형)

    [공통 구조]
    1️⃣ 데이터 로드 (Data Lake validated)
    2️⃣ 정규화 / 병합
    3️⃣ 도메인별 비즈니스 로직 적용 (_transform_business_logic)
    4️⃣ 중복 제거 / 정렬
    5️⃣ 저장 + 메타 기록

    다른 도메인 파이프라인도 이 구조를 그대로 상속받아 사용 가능
    """

    def __init__(self, snapshot_dt: str, vendor_priority: List[str] = None):
        super().__init__(
            domain=WAREHOUSE_DOMAINS["exchange"],
            snapshot_dt=snapshot_dt,
            vendor_priority=vendor_priority,
        )

    # ============================================================
    # 📘 1️⃣ 데이터 정규화 (공통 형태로 반환)
    # ============================================================
    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        거래소 데이터 표준화
        - 컬럼명 소문자 변환
        - 코드/이름 컬럼 정규화
        """
        df = df.copy()
        df.columns = df.columns.str.lower()

        # 필수 컬럼 매핑
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
        })

        for col in ["country_code", "exchange_code", "exchange_name"]:
            normalized[col] = normalized[col].astype(str).str.strip().str.upper()

        return normalized[
            (normalized["country_code"] != "") &
            (normalized["exchange_code"] != "")
        ]

    # ============================================================
    # 📘 2️⃣ 비즈니스 로직 (각 도메인별 오버라이드)
    # ============================================================
    def _transform_business_logic(
        self,
        exchange_df: pd.DataFrame,
        holiday_df: pd.DataFrame | None = None
    ) -> pd.DataFrame:
        """
        도메인 전용 가공 로직
        - 거래소 기준으로 휴장일 데이터 병합
        - 국가별 거래소 매핑 등 필요 시 확장 가능
        """
        # 1️⃣ 정규화
        normalized = self._normalize_dataframe(exchange_df)

        # 2️⃣ 중복 제거
        deduped = normalized.drop_duplicates(subset=["exchange_code"], keep="first")

        # 3️⃣ 휴장일 병합 (선택적)
        if holiday_df is not None and not holiday_df.empty:
            merged = deduped.merge(
                holiday_df,
                how="left",
                left_on="exchange_code",
                right_on="exchange",
                suffixes=("", "_holiday"),
            )
        else:
            merged = deduped

        # 4️⃣ 컬럼 순서 정렬 (Base에 정의된 기본 순서 활용)
        final_df = self._reorder_columns(merged)
        return final_df

    # ============================================================
    # 📘 3️⃣ 빌드 실행 (BaseWarehousePipeline 표준화)
    # ============================================================
    def build(self, **kwargs) -> Dict[str, Any]:
        """
        공통 빌드 로직
        """
        self.log.info(f"🏗️ Building {self.domain} for snapshot_dt={self.snapshot_dt}")

        # ✅ 1. 관련 도메인 데이터 자동 로드
        sources = self._load_source_datasets(self.domain)
        exchange_df = sources.get("exchange_list")
        holiday_df = sources.get("exchange_holiday")

        if exchange_df is None or exchange_df.empty:
            raise FileNotFoundError("❌ exchange_list 데이터가 없습니다.")

        # ✅ 2. 도메인별 변환 로직 실행
        final_df = self._transform_business_logic(exchange_df, holiday_df)

        # ✅ 3. 저장 + 메타데이터 기록
        save_result = self.save_parquet(final_df)
        meta = self.save_metadata(
            row_count=len(final_df),
            source_info={
                "datasets": list(sources.keys()),
                "record_counts": {k: len(v) for k, v in sources.items()},
            },
            metrics={"vendor_priority": self.vendor_priority},
            context=kwargs.get("context")
        )

        # ✅ 4. 레지스트리 갱신
        self.update_registry()

        self.log.info(
            f"✅ [BUILD COMPLETE] {self.domain} | "
            f"{len(final_df):,} rows successfully saved."
        )

        return meta
