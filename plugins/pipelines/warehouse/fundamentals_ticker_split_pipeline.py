from pathlib import Path
import json
import pandas as pd
from typing import Dict, Any
from plugins.pipelines.warehouse.base_warehouse_pipeline import BaseWarehousePipeline
from plugins.utils.loaders.warehouse.asset_master_loader import load_asset_master_latest
from plugins.config.constants import DATA_LAKE_ROOT, DATA_WAREHOUSE_ROOT, DATA_LAKE_VALIDATED


class FundamentalsTickerSplitPipeline(BaseWarehousePipeline):
    """
    ✅ Fundamentals Warehouse Builder (asset_master 기반)
    - asset_master에 존재하는 종목만 탐색하여 fundamentals JSON 적재
    - 비상장/비주류/비활성 종목은 자동 스킵
    """

    def __init__(self, domain_group: str, vendor: str, exchange_code: str, trd_dt: str, **kwargs):
        super().__init__(
            domain="fundamental",
            domain_group=domain_group,
            trd_dt=trd_dt,
            vendor=vendor,
            country_code=kwargs.get("country_code", "USA"),
        )
        self.exchange_code = exchange_code
        self.trigger_source = kwargs.get("trigger_source")

    # ----------------------------------------------------------------------
    # Dummy abstract methods
    # ----------------------------------------------------------------------
    def _load_source_datasets(self) -> Dict[str, pd.DataFrame]:
        return {}

    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    # ----------------------------------------------------------------------
    # Business Logic
    # ----------------------------------------------------------------------
    def _transform_business_logic(self, **kwargs) -> Dict[str, Any]:
        """
        ✅ asset_master 종목 기준으로 fundamentals JSON split
        """
        self.log.info(
            f"🏗️ Building FundamentalsTickerSplitPipeline | country={self.country_code}, trd_dt={self.trd_dt}"
        )

        # ✅ 1️⃣ asset_master 로드 (이미 비주류 거래소 제외됨)
        master_df = load_asset_master_latest(self.domain_group, country_code=self.country_code)
        master_df = master_df[["ticker", "exchange_code", "security_id"]].drop_duplicates()
        master_df = master_df.dropna(subset=["ticker", "exchange_code", "security_id"])
        master_df = master_df.astype(str).apply(lambda x: x.str.upper().str.strip())

        self.log.info(f"📦 Loaded {len(master_df):,} asset_master records for {self.country_code}")

        # ✅ 2️⃣ Lake validated fundamentals 폴더 기준
        lake_root = (
            Path(DATA_LAKE_VALIDATED)
            / self.domain_group
            / "fundamentals"
            / f"vendor={self.vendor}"
        )
        base_out = self.output_dir
        base_out.mkdir(parents=True, exist_ok=True)

        # ✅ 4️⃣ ticker별 fundamentals 탐색 및 적재
        ticker_count, section_count, skipped = 0, 0, 0

        for _, row in master_df.iterrows():
            ticker = row["ticker"]
            exchange_code = row["exchange_code"]
            security_id = row["security_id"]

            # 🔍 fundamentals JSON 파일 탐색
            json_path = (
                lake_root
                / f"exchange_code={self.exchange_code}"
                / f"trd_dt={self.trd_dt}"
                / f"{ticker}.json"
            )
            print(f"json_path : {json_path}")
            if not json_path.exists():
                skipped += 1
                self.log.warning(f"⚠️ Fundamentals not found for {ticker} ({exchange_code}) → skipped")
                continue

            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    skipped += 1
                    self.log.warning(f"⚠️ Invalid JSON structure for {ticker}")
                    continue

                # ✅ 보관 경로: security_id 기준
                sec_dir = base_out / f"security_id={security_id}"
                sec_dir.mkdir(parents=True, exist_ok=True)

                # 상위 key별 분리 저장
                for section, section_data in data.items():
                    out_path = sec_dir / f"{section}.json"
                    with open(out_path, "w", encoding="utf-8") as out_f:
                        json.dump(section_data, out_f, ensure_ascii=False, indent=2)
                    section_count += 1

                ticker_count += 1
                self.log.info(f"📄 Saved {len(data)} sections for {security_id} ({ticker})")

            except Exception as e:
                skipped += 1
                self.log.warning(f"⚠️ Failed to process {ticker}: {e}")

        # ✅ 5️⃣ 결과 요약
        self.log.info(
            f"✅ Fundamentals split complete | {ticker_count} tickers processed | {skipped} skipped | {section_count} sections | path={base_out}"
        )

        meta = self.save_metadata(
            row_count=ticker_count,
            skipped=skipped,
            section_count=section_count,
            country_code=self.country_code,
            vendor=self.vendor,
        )
        return meta

    # ----------------------------------------------------------------------
    # Build Entrypoint
    # ----------------------------------------------------------------------
    def build(self, **kwargs) -> Dict[str, Any]:
        """Airflow entrypoint"""
        self.log.info(f"🚀 [START] FundamentalsTickerSplitPipeline ({self.country_code})")
        result = self._transform_business_logic(**kwargs)
        self._update_domain_metadata(result["row_count"])
        self.log.info(f"🏁 [COMPLETE] fundamentals_ticker_split | {self.trd_dt}")
        return result
