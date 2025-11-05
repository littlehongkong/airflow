"""
plugins/pipelines/warehouse/fundamentals_ticker_split_pipeline.py

💾 펀더멘털 티커별 Key-Split 파이프라인
- Data Lake(validated) → Data Warehouse(snapshot)
- ticker 단위 JSON을 key별 Parquet으로 분리 저장
- OOM 방지를 위해 파일 단위 순차 변환 수행
"""

import json
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from pathlib import Path

from plugins.pipelines.warehouse.base_warehouse_pipeline import BaseWarehousePipeline
from plugins.config.constants import DATA_LAKE_ROOT, DATA_WAREHOUSE_ROOT, WAREHOUSE_DOMAINS, DOMAIN_GROUPS


class FundamentalsTickerSplitPipeline(BaseWarehousePipeline):
    """
    ✅ Fundamentals Ticker-Split Warehouse Pipeline
    ----------------------------------------------------------------------
    [파이프라인 구조]
    1️⃣ Data Lake validated fundamentals JSON 파일 로드
    2️⃣ 각 티커별 폴더(ticker=XXXX) 생성
    3️⃣ JSON 최상위 Key(General, Highlights, Financials 등)를 각각 Parquet으로 저장
    4️⃣ 메타정보(_build_meta.json) 기록
    """

    def __init__(
        self,
        trd_dt: str,
        exchange_code: str,
        vendor: Optional[str] = "eodhd",
        domain_group: Optional[str] = DOMAIN_GROUPS["equity"],
        **kwargs
    ):
        super().__init__(
            domain=WAREHOUSE_DOMAINS["fundamentals"],
            domain_group=domain_group,
            trd_dt=trd_dt,
            vendor_priority=[vendor],
        )
        self.exchange_code = exchange_code
        self.vendor = vendor
        self.trigger_source = kwargs.get("trigger_source", None)

    # ============================================================
    # 📘 1️⃣ 티커 단위 Fundamentals → Warehouse 변환 로직
    # ============================================================
    def _transform_business_logic(self, **kwargs) -> pd.DataFrame:
        """
        fundamentals JSON을 ticker별 key-split Parquet으로 변환
        """
        vendor = self.vendor
        exchange_code = self.exchange_code
        domain_group = self.domain_group
        trd_dt = self.trd_dt

        self.log.info(
            f"🏗️ Building FundamentalsTickerSplitPipeline | "
            f"exchange_code={exchange_code}, trd_dt={trd_dt}"
        )

        # ✅ 입력 / 출력 경로
        lake_dir = (
            Path(DATA_LAKE_ROOT)
            / "validated"
            / domain_group
            / "fundamentals"
            / f"vendor={vendor}"
            / f"exchange_code={exchange_code}"
            / f"trd_dt={trd_dt}"
        )

        warehouse_dir = (
            Path(DATA_WAREHOUSE_ROOT)
            / "snapshot"
            / domain_group
            / "fundamentals"
            / f"trd_dt={trd_dt}"
            / f"exchange_code={exchange_code}"
        )
        warehouse_dir.mkdir(parents=True, exist_ok=True)

        if not lake_dir.exists():
            raise FileNotFoundError(f"❌ Source directory not found: {lake_dir}")

        json_files = list(lake_dir.glob("*.json"))
        if not json_files:
            raise FileNotFoundError(f"❌ No fundamentals JSON files found in {lake_dir}")

        total_tickers = 0
        total_keys = set()

        # =======================================================
        # 🔁 파일 단위 변환 (OOM 방지)
        # =======================================================
        for f in json_files:
            ticker = f.stem
            try:
                data = json.load(open(f, "r", encoding="utf-8"))
                if not isinstance(data, dict):
                    self.log.warning(f"⚠️ Invalid format for {f.name} (skip)")
                    continue

                ticker_dir = warehouse_dir / f"ticker={ticker}"
                ticker_dir.mkdir(parents=True, exist_ok=True)

                # 각 key별로 parquet 저장
                for key, value in data.items():
                    if not isinstance(value, (dict, list)):
                        continue

                    key_lower = key.lower()
                    total_keys.add(key_lower)
                    out_path = ticker_dir / f"{key_lower}.parquet"

                    df = pd.DataFrame([value]) if isinstance(value, dict) else pd.DataFrame(value)
                    if df.empty:
                        continue

                    df["ticker"] = ticker
                    df["exchange_code"] = exchange_code
                    df["trd_dt"] = trd_dt

                    # 객체형 컬럼은 문자열로 변환
                    for col in df.select_dtypes(include=["object"]).columns:
                        df[col] = df[col].astype(str)

                    df.to_parquet(out_path, index=False)
                total_tickers += 1

                if total_tickers % 100 == 0:
                    self.log.info(f"📦 Processed {total_tickers:,} tickers so far...")

            except Exception as e:
                self.log.warning(f"⚠️ Failed to process {f.name}: {e}")

        # =======================================================
        # 🧾 메타파일 기록
        # =======================================================
        meta_info = {
            "snapshot_dt": trd_dt,
            "exchange_code": exchange_code,
            "total_tickers": total_tickers,
            "keys_generated": sorted(list(total_keys)),
            "build_time": datetime.now(timezone.utc).isoformat(),
            "source_path": str(lake_dir),
            "output_path": str(warehouse_dir),
        }

        meta_path = warehouse_dir / "_build_meta.json"
        with open(meta_path, "w", encoding="utf-8") as mf:
            json.dump(meta_info, mf, indent=2, ensure_ascii=False)

        self.log.info(
            f"✅ Fundamentals ticker-split build complete "
            f"| {total_tickers:,} tickers | {len(total_keys)} key types | path={warehouse_dir}"
        )
        return pd.DataFrame([meta_info])

    # ============================================================
    # 📘 2️⃣ 전체 빌드 실행 (Entry Point)
    # ============================================================
    def build(self, **kwargs) -> Dict[str, Any]:
        self.log.info("🚀 [START] FundamentalsTickerSplitPipeline")
        try:
            result_df = self._transform_business_logic(**kwargs)
            meta = self.save_metadata(
                row_count=len(result_df),
                exchange_code=self.exchange_code,
                vendor=self.vendor,
                context=kwargs.get("context"),
            )

            self.log.info(
                f"✅ [BUILD COMPLETE] fundamentals_ticker_split | "
                f"{self.exchange_code} | trd_dt={self.trd_dt}"
            )
            return meta
        except Exception as e:
            self.log.error(f"❌ Build failed: {e}")
            raise
        finally:
            self.cleanup()
