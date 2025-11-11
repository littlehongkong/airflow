import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, Any
import pandas as pd
import logging
from plugins.config.constants import DATA_WAREHOUSE_SNAPSHOT

from plugins.validators.base_data_validator import BaseDataValidator


class FundamentalsWarehouseValidator(BaseDataValidator):
    """
    🧩 Fundamentals Warehouse Validator
    - Type별(fundamental 종류별) Pandera + Soda Core 검증
    - security_id 단위 병렬 처리
    - snapshot/country_code/type/security_id 구조 대응
    """

    def __init__(self, trd_dt: str, country_code: str, domain_group: str, vendor: str = "eodhd",
            dataset_path: str | None = None,
            allow_empty: bool = False):
        super().__init__(
            domain="fundamentals",
            layer="warehouse",
            trd_dt=trd_dt,
            vendor=vendor,
            domain_group=domain_group,
            dataset_path=dataset_path,
            allow_empty=allow_empty
        )

        self.country_code = country_code
        self.dataset_path = (
            self.data_root
            / "snapshot"
            / self.domain_group
            / self.domain
            / f"country_code={country_code}"
            / f"trd_dt={trd_dt}"
        )
        self.log = logging.getLogger(__name__)

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
    def _validate_ticker_folder(self, security_dir: Path) -> Dict[str, Any]:
        """
        ✅ 단일 security_id 폴더 검증
        - type 감지
        - section별 Pandera 검증
        """
        try:
            general_path = security_dir / "General.json"
            if not general_path.exists():
                return {"security_id": security_dir.name, "status": "skipped", "reason": "No General.json"}

            with open(general_path, "r", encoding="utf-8") as f:
                general_data = json.load(f)

            fund_type = self._detect_fund_type(general_data)
            schema_dir = self.schema_root
            check_dir = self.check_root

            errors = []
            sections = [p for p in security_dir.glob("*.json")]
            for section_file in sections:
                section = section_file.stem
                schema_path = schema_dir / f"fundamentals_{fund_type}.json"
                if not schema_path.exists():
                    self.log.warning(f"⚠️ No Pandera schema for {fund_type} → {section_file.name} skipped")
                    continue

                df = pd.json_normalize(json.load(open(section_file)), sep="_")
                with open(schema_path, "r", encoding="utf-8") as f:
                    schema_def = json.load(f)
                result = self._validate_with_pandera(df, schema_def)
                if not result.get("passed", False):
                    errors.append(f"{section}: {result.get('message')}")

            # ✅ Soda Core snapshot-level 검증은 후단에서 수행
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
        1️⃣ 병렬 ticker-level Pandera 검증
        2️⃣ snapshot 단위 Soda Core 검증 (type별)
        """
        type_dirs = [d for d in self.dataset_path.glob("type=*") if d.is_dir()]
        if not type_dirs:
            raise FileNotFoundError(f"❌ No fundamentals type folders found in {self.dataset_path}")

        total_checked, failed_tickers = 0, []
        all_results = []

        for type_dir in type_dirs:
            type_name = type_dir.name.split("=")[-1]
            security_dirs = [p for p in type_dir.glob("security_id=*") if p.is_dir()]
            self.log.info(f"📂 Validating {len(security_dirs)} fundamentals ({type_name.upper()})")

            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = {executor.submit(self._validate_ticker_folder, d): d for d in security_dirs}
                for future in as_completed(futures):
                    res = future.result()
                    all_results.append(res)
                    total_checked += 1
                    if res["status"] != "success":
                        failed_tickers.append(res["security_id"])

            # ✅ Snapshot-level Soda 검증 (type별)
            soda_filename = f"fundamentals_{type_name}.yml"
            soda_path = self.check_root / soda_filename

            if soda_path.exists():
                df_summary = pd.DataFrame(all_results)
                checks = self._run_soda_duckdb_validation(df_summary, soda_path)
            else:
                self.log.warning(f"⚠️ Soda check file not found for type={type_name}")
                checks = {}

        # ---------------------------------------------------------------
        # ✅ 최종 결과 저장
        # ---------------------------------------------------------------
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

        meta_path = (
            self.dataset_path / "_last_validated.json"
        )
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        self.log.info(
            f"🎯 Fundamentals warehouse validation done | total={total_checked} | failed={failed} | path={meta_path}"
        )

        if failed > 0:
            raise ValueError(f"❌ Fundamentals warehouse validation failed: {failed} tickers")

        return result
