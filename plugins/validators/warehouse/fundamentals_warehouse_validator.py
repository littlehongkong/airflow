import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, Any
import pandas as pd
import logging

from plugins.validators.base_data_validator import BaseDataValidator
from plugins.config.constants import DATA_WAREHOUSE_ROOT, WAREHOUSE_DOMAINS


class FundamentalsWarehouseValidator(BaseDataValidator):
    """
    🧩 Fundamentals Warehouse Validator (constants 기반 개선판)
    ---------------------------------------------------------------
    ✅ 개선 포인트:
      - constants.WAREHOUSE_DOMAINS 기반 경로 자동 구성
      - section별 Pandera 검증 (Type별 스키마 자동 선택)
      - Snapshot 단위 Soda Core 검증 (optional)
      - 병렬 처리 (security_id 단위)
    """

    def __init__(
        self,
        trd_dt: str,
        country_code: str,
        domain_group: str,
        vendor: str = "eodhd",
        allow_empty: bool = False,
    ):
        super().__init__(
            domain="fundamental",
            layer="warehouse",
            trd_dt=trd_dt,
            vendor=vendor,
            domain_group=domain_group,
            allow_empty=allow_empty,
        )

        self.country_code = country_code
        self.log = logging.getLogger(__name__)

        # ✅ constants 기반 dataset_path 구성
        domain_conf = WAREHOUSE_DOMAINS["fundamental"]
        relative_path = domain_conf["path"].format(
            country_code=country_code,
            trd_dt=trd_dt,
        )
        self.dataset_path = Path(DATA_WAREHOUSE_ROOT) / relative_path

        self.log.info(f"📂 Fundamentals dataset path: {self.dataset_path}")

    # ------------------------------------------------------------------
    def _detect_fund_type(self, general_data: dict) -> str:
        """General 블록의 Type 기준으로 카테고리 자동 감지"""
        t = (general_data.get("Type") or general_data.get("General_Type") or "").lower()
        if "etf" in t:
            return "etf"
        elif "fund" in t or "mutual" in t:
            return "fund"
        return "stock"

    # ------------------------------------------------------------------
    def _validate_security_folder(self, security_dir: Path) -> Dict[str, Any]:
        """
        ✅ 단일 security_id 폴더 검증
        - General.json 존재 여부 확인
        - Type 감지
        - 각 Section 파일별 Pandera 검증 수행
        """
        try:
            general_path = security_dir / "General.json"
            if not general_path.exists():
                return {"security_id": security_dir.name, "status": "skipped", "reason": "No General.json"}

            with open(general_path, "r", encoding="utf-8") as f:
                general_data = json.load(f)

            fund_type = self._detect_fund_type(general_data)
            schema_dir = self.schema_root / "fundamentals"  # e.g. /schemas/warehouse/equity/fundamentals
            check_dir = self.check_root / "fundamentals"

            errors = []
            sections = [p for p in security_dir.glob("*.json")]

            for section_file in sections:
                section = section_file.stem
                # ✅ Type별 스키마 로드
                schema_path = schema_dir / f"{fund_type}_{section}.json"
                if not schema_path.exists():
                    self.log.debug(f"⚠️ No schema for {fund_type}/{section}, skipping")
                    continue

                try:
                    df = pd.json_normalize(json.load(open(section_file, "r", encoding="utf-8")), sep="_")
                    with open(schema_path, "r", encoding="utf-8") as f:
                        schema_def = json.load(f)

                    result = self._validate_with_pandera(df, schema_def)
                    if not result.get("passed", False):
                        errors.append(f"{section}: {result.get('message')}")

                except Exception as e:
                    errors.append(f"{section}: {str(e)}")

            # ✅ 결과 요약
            status = "success" if not errors else "failed"
            return {
                "security_id": security_dir.name,
                "fund_type": fund_type,
                "status": status,
                "errors": errors,
            }

        except Exception as e:
            return {"security_id": security_dir.name, "status": "failed", "errors": [str(e)]}

    # ------------------------------------------------------------------
    def validate(self, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        ⚙️ Warehouse 전체 검증 프로세스
        1️⃣ 병렬 security_id 단위 Pandera 검증
        2️⃣ Snapshot 단위 Soda Core 검증 (type별)
        """
        total_checked, failed_tickers = 0, []
        all_results = []

        if not self.dataset_path.exists():
            raise FileNotFoundError(f"❌ Dataset path not found: {self.dataset_path}")

        security_dirs = [p for p in self.dataset_path.glob("security_id=*") if p.is_dir()]
        if not security_dirs:
            self.log.warning("⚠️ No fundamentals security_id folders found")
            return {"status": "empty", "checked_tickers": 0}

        self.log.info(f"📊 Validating {len(security_dirs)} fundamentals records")

        # ✅ 병렬 실행
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(self._validate_security_folder, d): d for d in security_dirs}
            for future in as_completed(futures):
                res = future.result()
                all_results.append(res)
                total_checked += 1
                if res["status"] != "success":
                    failed_tickers.append(res["security_id"])

        # ✅ Snapshot 단위 Soda 검증 (type별)
        df_summary = pd.DataFrame(all_results)
        soda_results = {}

        for fund_type in df_summary["fund_type"].dropna().unique():
            soda_path = self.check_root / f"fundamentals_{fund_type}.yml"
            if not soda_path.exists():
                self.log.warning(f"⚠️ No Soda check found for {fund_type}")
                continue
            self.log.info(f"🧪 Running Soda validation for {fund_type}")
            soda_results[fund_type] = self._run_soda_duckdb_validation(df_summary, soda_path)

        # ✅ 결과 저장
        failed = len(failed_tickers)
        result = {
            "dataset": self.domain,
            "layer": self.layer,
            "country_code": self.country_code,
            "trd_dt": self.trd_dt,
            "checked_tickers": total_checked,
            "failed_tickers": failed_tickers,
            "failed_count": failed,
            "status": "failed" if failed > 0 else "success",
            "validated_at": datetime.now(timezone.utc).isoformat(),
        }

        meta_path = self.dataset_path / "_last_validated.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        self.log.info(
            f"🎯 Fundamentals warehouse validation complete | total={total_checked} | failed={failed} | path={meta_path}"
        )

        if failed > 0:
            raise ValueError(f"❌ Fundamentals warehouse validation failed: {failed} tickers")

        return result
