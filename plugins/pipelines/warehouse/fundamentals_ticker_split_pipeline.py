import json
from pathlib import Path
from typing import Dict, Any
import pandas as pd

from plugins.pipelines.warehouse.base_warehouse_pipeline import BaseWarehousePipeline
from plugins.utils.loaders.warehouse.asset_master_loader import load_asset_master_latest
from plugins.config.constants import DATA_LAKE_ROOT, DATA_WAREHOUSE_ROOT


class FundamentalsTickerSplitPipeline(BaseWarehousePipeline):
    """
    ✅ Fundamentals Warehouse Builder (Ticker + Section Split)
    Data Lake → Warehouse (per security_id, per section JSON)

    Example output:
    data_warehouse/snapshot/equity/fundamentals/
        trd_dt=2025-11-05/
        exchange_code=KQ/
        security_id=AST_KOR_KQ_AAPL/
        General.json
    """

    def __init__(self, domain_group: str, vendor: str, exchange_code: str, trd_dt: str, country_code: str, **kwargs):
        super().__init__(
            domain="fundamentals",
            domain_group=domain_group,
            trd_dt=trd_dt,
            country_code=country_code,
        )
        self.vendor = vendor
        self.exchange_code = exchange_code
        self.trigger_source = kwargs.get("trigger_source", None)

    # ------------------------------------------------------------------
    def _load_source_datasets(self) -> Dict[str, pd.DataFrame]:
        """
        ✅ BaseWarehousePipeline의 abstractmethod 구현
        - asset_master 로드
        - fundamentals JSON 파일 목록 로드 (Path만 반환)
        """
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
        else:
            self.log.info(f"처리할 대상 파일 목록 : {len(json_files)}")

        # 🧩 마스터 로드
        master_df = load_asset_master_latest(self.domain_group)

        # 기본 키 정규화
        master_df["ticker"] = master_df["ticker"].astype(str).str.upper().str.strip()
        master_df["exchange_code"] = master_df["exchange_code"].astype(str).str.upper().str.strip()

        return {"json_files": json_files, "master_df": master_df}

    def _setup_output_paths(self):
        """
        ✅ Fundamentals 전용 출력 디렉토리
        - country_code / trd_dt 파티션 구조 유지
        - BaseWarehousePipeline 기본 설정은 비활성화
        """
        snapshot_root = (
                DATA_WAREHOUSE_ROOT
                / "snapshot"
                / self.domain_group
                / "fundamentals"
                / f"country_code={self.country_code}"
                / f"trd_dt={self.trd_dt}"
        )
        snapshot_root.mkdir(parents=True, exist_ok=True)

        self.output_dir = snapshot_root
        self.output_file = snapshot_root / f"{self.domain}.parquet"
        self.meta_file = snapshot_root / "_build_meta.json"

        # (옵션) 공용 메타파일 필요 시만 유지
        self.domain_meta_file = (
                DATA_WAREHOUSE_ROOT
                / self.domain_group
                / self.domain
                / "_warehouse_meta.json"
        )

        self.log.info(f"📦 Output path configured: {snapshot_root}")


    # ------------------------------------------------------------------
    def _transform_business_logic(self, **kwargs) -> Dict[str, Any]:
        """
        💡 asset_master를 기준으로 fundamentals JSON을 security_id별 / section별로 저장
        - master_df에는 이미 비주류 거래소 종목이 제외된 상태
        - master에 존재하는 ticker만 JSON 매핑 대상
        """
        json_files = kwargs["json_files"]
        master_df = kwargs["master_df"]

        self.log.info(
            f"🏗️ Building FundamentalsTickerSplitPipeline | exchange_code={self.exchange_code}, trd_dt={self.trd_dt}"
        )

        # JSON 파일 인덱스 (ticker → Path)
        json_index = {f.stem.upper(): f for f in json_files}

        # Warehouse 경로 설정 (국가 단위)
        base_out = (
                Path(DATA_WAREHOUSE_ROOT)
                / "snapshot"
                / self.domain_group
                / "fundamentals"
                / f"country_code={self.country_code}"
                / f"trd_dt={self.trd_dt}"
        )
        base_out.mkdir(parents=True, exist_ok=True)

        ticker_count, section_count, skipped = 0, 0, 0

        # ----------------------------------------------------------
        # 마스터 기준으로 ticker 매핑
        # ----------------------------------------------------------
        for _, row in master_df.iterrows():
            ticker = str(row["ticker"]).upper().strip()
            security_id = row["security_id"]

            # 해당 ticker의 fundamentals JSON 존재 여부 확인
            jf = json_index.get(ticker)
            if jf is None:
                skipped += 1
                continue  # JSON이 없으면 skip

            try:
                with open(jf, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    continue

                # 보관 경로: security_id 기준으로 생성
                security_dir = base_out / f"security_id={security_id}"
                security_dir.mkdir(parents=True, exist_ok=True)

                # 섹션별 JSON 저장
                for section_name, section_data in data.items():
                    out_path = security_dir / f"{section_name}.json"
                    with open(out_path, "w", encoding="utf-8") as out_f:
                        json.dump(section_data, out_f, ensure_ascii=False, indent=2)
                    section_count += 1

                ticker_count += 1
                self.log.info(f"📄 Saved {len(data.keys())} sections for {security_id} ({ticker})")

            except Exception as e:
                skipped += 1
                self.log.warning(f"⚠️ Failed to process {ticker}: {e}")

        self.log.info(
            f"✅ Fundamentals ticker-split build complete | {ticker_count} saved | {skipped} skipped | {section_count} total sections | path={base_out}"
        )

        # 메타데이터 저장
        meta = self.save_metadata(
            row_count=ticker_count,
            country_code=self.country_code,
            vendor=self.vendor,
            section_count=section_count,
            skipped=skipped,
            context=kwargs.get("context"),
        )

        return meta

    # ------------------------------------------------------------------
    def build(self, **kwargs) -> Dict[str, Any]:
        """메인 빌드"""
        self.log.info("🚀 [START] FundamentalsTickerSplitPipeline")
        datasets = self._load_source_datasets()
        result = self._transform_business_logic(**datasets)
        self.log.info(
            f"✅ [BUILD COMPLETE] fundamentals_ticker_split | {self.exchange_code} | trd_dt={self.trd_dt}"
        )
        return result
