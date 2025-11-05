"""
plugins/pipelines/warehouse/fundamentals_ticker_split_pipeline.py
"""

import json
from pathlib import Path
from typing import Dict, Any
from plugins.pipelines.warehouse.base_warehouse_pipeline import BaseWarehousePipeline
from plugins.config.constants import DATA_LAKE_ROOT, DATA_WAREHOUSE_ROOT


class FundamentalsTickerSplitPipeline(BaseWarehousePipeline):
    """
    ✅ Fundamentals Warehouse Builder (Ticker + Section Split)
    Data Lake → Warehouse (per ticker, per section JSON)

    Example output:
    data_warehouse/snapshot/equity/fundamentals/trd_dt=2025-11-05/exchange_code=KQ/ticker=AAPL/General.json
    """

    def __init__(self, domain_group: str, vendor: str, exchange_code: str, trd_dt: str, **kwargs):
        super().__init__(
            domain="fundamentals",
            domain_group=domain_group,
            trd_dt=trd_dt,
            country_code=None,
        )
        self.vendor = vendor
        self.exchange_code = exchange_code
        self.trigger_source = kwargs.get("trigger_source", None)

    # ------------------------------------------------------------------
    def _transform_business_logic(self, **kwargs) -> Dict[str, Any]:
        """
        💡 fundamentals JSON을 ticker별 / section별 파일로 쪼개서 저장
        """
        self.log.info(f"🏗️ Building FundamentalsTickerSplitPipeline | exchange_code={self.exchange_code}, trd_dt={self.trd_dt}")

        # Lake 경로 설정
        lake_dir = (
            Path(DATA_LAKE_ROOT)
            / "validated"
            / self.domain_group
            / "fundamentals"
            / f"vendor={self.vendor}"
            / f"exchange_code={self.exchange_code}"
            / f"trd_dt={self.trd_dt}"
        )

        if not lake_dir.exists():
            raise FileNotFoundError(f"❌ Source directory not found: {lake_dir}")

        json_files = list(lake_dir.glob("*.json"))
        if not json_files:
            raise FileNotFoundError(f"❌ No JSON files found in {lake_dir}")

        # Warehouse 경로 설정
        base_out = (
            Path(DATA_WAREHOUSE_ROOT)
            / "snapshot"
            / self.domain_group
            / "fundamentals"
            / f"trd_dt={self.trd_dt}"
            / f"exchange_code={self.exchange_code}"
        )
        base_out.mkdir(parents=True, exist_ok=True)

        ticker_count, section_count = 0, 0

        # ----------------------------------------------------------
        # 각 ticker별 JSON 읽기
        # ----------------------------------------------------------
        for jf in json_files:
            try:
                with open(jf, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    continue

                ticker = data.get("General", {}).get("Code", jf.stem)
                ticker_dir = base_out / f"ticker={ticker}"
                ticker_dir.mkdir(parents=True, exist_ok=True)

                # 상위 key들(General, Highlights, Valuation, 등)
                for section_name, section_data in data.items():
                    out_path = ticker_dir / f"{section_name}.json"
                    with open(out_path, "w", encoding="utf-8") as out_f:
                        json.dump(section_data, out_f, ensure_ascii=False, indent=2)

                    section_count += 1

                ticker_count += 1

                self.log.info(f"📄 Saved sections for {ticker} ({len(data.keys())} sections)")

            except Exception as e:
                self.log.warning(f"⚠️ Failed to process {jf.name}: {e}")

        self.log.info(
            f"✅ Fundamentals ticker-split build complete | {ticker_count} tickers | {section_count} total sections | path={base_out}"
        )

        # 메타데이터 저장
        meta = self.save_metadata(
            row_count=ticker_count,
            exchange_code=self.exchange_code,
            vendor=self.vendor,
            section_count=section_count,
            context=kwargs.get("context"),
        )

        return meta

    # ------------------------------------------------------------------
    def build(self, **kwargs) -> Dict[str, Any]:
        """메인 빌드"""
        self.log.info("🚀 [START] FundamentalsTickerSplitPipeline")
        result = self._transform_business_logic(**kwargs)
        self.log.info(
            f"✅ [BUILD COMPLETE] fundamentals_ticker_split | {self.exchange_code} | trd_dt={self.trd_dt}"
        )
        return result
