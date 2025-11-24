import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, Any
import pandas as pd
import logging

from plugins.config.constants import WAREHOUSE_DOMAINS
from plugins.validators.base_data_validator import BaseDataValidator
from plugins.utils.path_manager import DataPathResolver


class FundamentalsWarehouseValidator(BaseDataValidator):
    """
    🧩 Fundamentals Warehouse Validator (DataPathResolver 기반 최신버전)
    -----------------------------------------------------------------
    - snapshot/fundamental_master/ 구조 기반
    - Type(stock/etf) 자동 감지
    - 각 Section JSON Pandera 검증
    - security_id=* 단위 병렬 검증
    """

    def __init__(
        self,
        trd_dt: str,
        country_code: str,
        domain_group: str = "equity",
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

        # ─────────────────────────────────────────────
        # 📁 warehouse/snapshot/equity/fundamental_master/country_code=USA/trd_dt=xxxx
        # ─────────────────────────────────────────────
        self.dataset_path = DataPathResolver.warehouse_snapshot(
            domain_group=domain_group,
            domain="fundamental",
            country_code=country_code,
            trd_dt=trd_dt
        )

        self.log.info(f"📂 Warehouse Fundamentals dataset root = {self.dataset_path}")

    # ------------------------------------------------------------------
    def _detect_fund_type(self, general_data: dict) -> str:
        """General 블록의 Type으로 stock / etf 자동 감지"""
        t = (general_data.get("Type") or general_data.get("General_Type") or "").lower()
        if "etf" in t:
            return "etf"
        if "fund" in t or "mutual" in t:
            return "fund"
        return "stock"

    # ------------------------------------------------------------------
    def _validate_security_folder(self, security_dir: Path) -> Dict[str, Any]:
        """
        security_id 단위 검증
        - General.json 존재 체크
        - stock/etf 구분
        - Section JSON 파일 Pandera 검증
        """
        try:
            general_path = security_dir / "General.json"
            if not general_path.exists():
                return {"security_id": security_dir.name, "status": "skipped", "reason": "No General.json"}

            general_data = json.loads(general_path.read_text(encoding="utf-8"))
            fund_type = self._detect_fund_type(general_data)

            schema_dir = self.schema_root / "fundamentals"
            errors = []

            for section_file in security_dir.glob("*.json"):
                section = section_file.stem

                schema_path = schema_dir / f"{fund_type}_{section}.json"
                if not schema_path.exists():
                    self.log.debug(f"⚠️ Schema 없음 → skip: {schema_path}")
                    continue

                try:
                    df = pd.json_normalize(json.loads(section_file.read_text()), sep="_")
                    schema_def = json.loads(schema_path.read_text(encoding="utf-8"))

                    result = self._validate_with_pandera(df, schema_def)
                    if not result.get("passed", False):
                        errors.append(f"{section}: {result.get('message')}")
                except Exception as e:
                    errors.append(f"{section}: {str(e)}")

            status = "success" if not errors else "failed"

            return {
                "security_id": security_dir.name,
                "fund_type": fund_type,
                "status": status,
                "errors": errors,
            }

        except Exception as e:
            return {
                "security_id": security_dir.name,
                "status": "failed",
                "errors": [str(e)],
            }

    # ------------------------------------------------------------------
    def validate(self, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        전체 fundamentals warehouse 검증 프로세스
        - security_id 단위 병렬 Pandera 검증
        - 결과 JSON 저장
        """
        if not self.dataset_path.exists():
            raise FileNotFoundError(f"❌ warehouse path not found: {self.dataset_path}")

        security_dirs = [p for p in self.dataset_path.glob("security_id=*") if p.is_dir()]

        if not security_dirs:
            self.log.warning("⚠️ No fundamentals folders found")
            return {"status": "empty", "checked_tickers": 0}

        self.log.info(f"📊 {len(security_dirs)} fundamentals records 검증 시작")

        all_results = []
        failed = []
        total = len(security_dirs)

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(self._validate_security_folder, d): d for d in security_dirs}
            for future in as_completed(futures):
                res = future.result()
                all_results.append(res)
                if res["status"] != "success":
                    failed.append(res["security_id"])

        # 결과 저장
        summary = {
            "dataset": "fundamentals",
            "layer": "warehouse",
            "country_code": self.country_code,
            "trd_dt": self.trd_dt,
            "checked_tickers": total,
            "failed_tickers": failed,
            "failed_count": len(failed),
            "status": "failed" if failed else "success",
            "validated_at": datetime.now(timezone.utc).isoformat(),
        }

        last_validated = self.dataset_path / "_last_validated.json"
        last_validated.write_text(json.dumps(summary, indent=2, ensure_ascii=False))

        self.log.info(
            f"🎯 Fundamentals warehouse validation 완료 | total={total} | failed={len(failed)} | path={last_validated}"
        )

        if failed:
            raise ValueError(f"❌ Validation failed: {len(failed)} tickers")

        return summary
