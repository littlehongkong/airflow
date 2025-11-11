from plugins.validators.base_data_validator import BaseDataValidator
from plugins.config import constants as C
from plugins.utils.name_utils import normalize_field_names   # ✅ 새 유틸 사용
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import pandas as pd, json, logging
import gc
import shutil
import psutil, tracemalloc
import traceback
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
_parquet_lock = Lock()

process = psutil.Process()
tracemalloc.start()

class FundamentalDataValidator(BaseDataValidator):
    """
    🧩 Fundamental 전용 Validator
    - 파일 단위 검증 (OOM 방지)
    - 각 JSON별 Pandera/Soda 적용
    - validated 결과 JSONL append
    """

    def __init__(
        self,
        trd_dt: str,
        domain: str,
        exchange_code: str,
        vendor: str = "eodhd",
        domain_group: str = "equity",
    ):
        self.dataset_path = (
            C.DATA_LAKE_ROOT
            / "raw"
            / domain_group
            / domain
            / f"vendor={vendor}"
            / f"exchange_code={exchange_code}"
            / f"trd_dt={trd_dt}"
        )

        super().__init__(
            domain=domain,
            layer="lake",
            trd_dt=trd_dt,
            vendor=vendor,
            dataset_path=self.dataset_path,
            exchange_code=exchange_code,
            domain_group=domain_group,
        )


        self.output_dir = (
            C.DATA_LAKE_ROOT
            / "validated"
            / domain_group
            / "fundamentals"
            / f"vendor={vendor}"
        )

        self.log = logging.getLogger(__name__)

    def _finalize_general_parquet(self):
        general_output_dir = (
                C.DATA_LAKE_VALIDATED /
                self.domain_group / "fundamentals" /
                f"vendor={self.vendor}" /
                f"exchange_code={self.exchange_code}"
        )
        jsonl_path = general_output_dir / f"fundamentals_general_{self.trd_dt}.jsonl"
        parquet_path = general_output_dir / f"fundamentals_general_{self.trd_dt}.parquet"
        latest_path = general_output_dir / "fundamentals_general_latest.parquet"

        if not jsonl_path.exists():
            self.log.warning(f"⚠️ No JSONL found for parquet conversion: {jsonl_path}")
            return

        df = pd.read_json(jsonl_path, lines=True)
        df.drop_duplicates(subset=["ticker"], keep="last", inplace=True)
        df.to_parquet(parquet_path, index=False)
        shutil.copyfile(parquet_path, latest_path)
        self.log.info(f"✅ Converted JSONL → Parquet: {parquet_path}")

    def _append_general_to_jsonl(self, general_dict: Dict[str, Any]) -> None:
        """
        ✅ 병렬 검증용 임시 append-safe JSONL 로깅
        - 위치: validated/fundamentals/.../fundamentals_general_{trd_dt}.jsonl
        """
        if not general_dict:
            return

        try:
            record = {
                "exchange_code": self.exchange_code,
                "ticker": general_dict.get("Code"),
                "Name": general_dict.get("Name"),
                "security_type": general_dict.get("Type"),
                "Sector": general_dict.get("Sector"),
                "Industry": general_dict.get("Industry"),
                "country_code": general_dict.get("CountryISO"),
                "CurrencyCode": general_dict.get("CurrencyCode"),
                "GicSector": general_dict.get("GicSector"),
                "GicGroup": general_dict.get("GicGroup"),
                "GicIndustry": general_dict.get("GicIndustry"),
                "GicSubIndustry": general_dict.get("GicSubIndustry"),
                "IPODate": general_dict.get("IPODate"),
                "IsDelisted": general_dict.get("IsDelisted"),
                "isin": general_dict.get("ISIN"),
                "cusip": general_dict.get("CUSIP"),
                "lei": general_dict.get("LEI"),
                "OpenFigi": general_dict.get("OpenFigi"),
                'fiscal_year_end': general_dict.get("FiscalYearEnd"),
                "PrimaryTicker": general_dict.get("PrimaryTicker"),
                "cik": general_dict.get("CIK"),
                "last_fundamental_update": general_dict.get("UpdatedAt"),
                "logo_url": general_dict.get("LogoURL"),
                "trd_dt": self.trd_dt,
                "validated_at": datetime.now(timezone.utc).isoformat(),
            }

            record = normalize_field_names(record)

            general_output_dir = (
                    C.DATA_LAKE_VALIDATED /
                    self.domain_group / "fundamentals" /
                    f"vendor={self.vendor}" /
                    f"exchange_code={self.exchange_code}"
            )
            general_output_dir.mkdir(parents=True, exist_ok=True)

            jsonl_path = general_output_dir / f"fundamentals_general_{self.trd_dt}.jsonl"

            with open(jsonl_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        except Exception as e:
            self.log.warning(f"⚠️ General JSONL append 실패: {e}")


    def _detect_fund_type(self, df: pd.DataFrame) -> str:
        """파일별 타입 자동 감지 (ETF or Stock)"""
        type_col = next(
            (c for c in ["General_Type", "Type", "General.Type", "General_Type.value"] if c in df.columns),
            None
        )
        fund_type = "etf"
        if type_col and df[type_col].astype(str).str.contains("Common Stock", case=False, na=False).any():
            fund_type = "stock"
        return fund_type

    def _define_checks(self, df: pd.DataFrame) -> Dict[str, Any]:
        """ETF/Stock별 Pandera schema 적용"""
        checks = {}
        fund_type = self._detect_fund_type(df)

        schema_path = self.schema_root / f"fundamentals_{fund_type}.json"
        self.log.info(f"🧩 schema_path: {schema_path}")

        if schema_path.exists():
            with open(schema_path, "r", encoding="utf-8") as f:
                schema_def = json.load(f)
            checks["pandera_schema"] = self._validate_with_pandera(df, schema_def)
        else:
            self.log.warning(f"⚠️ Pandera schema not found: {schema_path}")

        return checks


    def validate_file(self, file_path: Path) -> Dict[str, Any]:
        """단일 JSON 파일 검증"""
        with open(file_path, "r", encoding="utf-8") as infile:
            data = json.load(infile)

        general_data = data.get("General", {})
        if not general_data:
            return {"file": file_path.name, "status": "skipped", "reason": "No 'General' key"}

        df = pd.json_normalize(general_data, sep="_")
        df.columns = [c.replace(".", "_") for c in df.columns]
        df.columns = [f"General_{c}" if not c.startswith("General_") else c for c in df.columns]

        checks = self._define_checks(df)
        status = self._aggregate_status(checks)

        return {"file": file_path.name, "status": status, "checks": checks}

    # -------------------------------------------------------------------------
    # 1️⃣ 메인 검증 (Base 구조 유지)
    # -------------------------------------------------------------------------
    def validate(self, context: Optional[dict] = None) -> Dict[str, Any]:
        """
        ⚙️ fundamentals 전용 검증 로직 (Lake 레벨)
        - General 블록만 검증 (ETF/Stock 자동 구분)
        - 파일 단위 병렬 검증 (ThreadPoolExecutor)
        - 성공 시 원본 JSON validated로 복사 + General parquet append
        """
        files = [
            f for f in self.dataset_path.glob("*.json")
            if not f.name.startswith("_")
        ]

        total_files = len(files)

        if not files:
            if self.allow_empty:
                return self._skip_empty_result()
            raise FileNotFoundError(f"❌ No fundamental files found: {self.dataset_path}")

        self.log.info(f"📂 {len(files)} fundamentals 파일 병렬 검증 시작")

        validated_dir = (
                self.data_root
                / "validated"
                / self.domain_group
                / self.domain
                / f"vendor={self.vendor}"
                / f"exchange_code={self.exchange_code}"
                / f"trd_dt={self.trd_dt}"
        )
        validated_dir.mkdir(parents=True, exist_ok=True)

        jsonl_path = validated_dir / f"{self.domain}_validated.jsonl"

        # ✅ 실행 전 초기화 (파일 있으면 삭제)
        if jsonl_path.exists():
            jsonl_path.unlink()

        last_validated_path = validated_dir / "_last_validated.json"

        total_passed, total_failed = 0, 0
        failed_symbols = []
        records = []

        # ✅ 진행률 및 락 관리
        progress_lock = Lock()
        completed = 0
        # ---------------------------------------------------------------
        # ✅ 병렬 파일 검증 (ETF/Stock 자동 분기 포함)
        # ---------------------------------------------------------------
        def _validate_file(file_path):
            nonlocal completed
            try:
                with open(file_path, "r", encoding="utf-8") as infile:
                    full_data = json.load(infile)
                general_data = full_data.get("General", {})
                if not general_data:
                    return {"file": file_path.name, "status": "skipped", "reason": "No General key"}

                df = pd.json_normalize(general_data, sep="_")
                df.columns = [c.replace(".", "_") for c in df.columns]
                df.columns = [f"General_{c}" if not c.startswith("General_") else c for c in df.columns]

                checks = self._define_checks(df)
                status = self._aggregate_status(checks)

                # ✅ progress 카운터 증가 (thread-safe)
                with progress_lock:
                    completed += 1
                    progress_pct = (completed / total_files) * 100
                    self.log.info(
                        f"🔍 [{completed}/{total_files} | {progress_pct:.1f}%] "
                        f"검증 중: {file_path.name} (status={status})"
                    )

                if status == "success":
                    validated_json_path = validated_dir / f"{file_path.stem}.json"
                    with open(validated_json_path, "w", encoding="utf-8") as out_f:
                        json.dump(full_data, out_f, ensure_ascii=False, indent=2)
                    self._append_general_to_jsonl(full_data.get("General", {}))

                return {"file": file_path.name, "status": status, "checks": checks}

            except Exception as e:
                with progress_lock:
                    completed += 1
                    progress_pct = (completed / total_files) * 100
                    self.log.warning(
                        f"⚠️ [{completed}/{total_files} | {progress_pct:.1f}%] "
                        f"{file_path.name} 검증 실패: {e}"
                    )
                return {"file": file_path.name, "status": "failed", "error": str(e)}

        # ---------------------------------------------------------------
        # ✅ ThreadPoolExecutor로 병렬 처리
        # ---------------------------------------------------------------
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(_validate_file, f): f for f in files}
            for future in as_completed(futures):
                result = future.result()
                records.append(result)
                if result["status"] == "success":
                    total_passed += 1
                elif result["status"] == "failed":
                    total_failed += 1
                    failed_symbols.append(result["file"])

        # ✅ 최종 요약
        self.log.info(f"🎯 검증 완료 — 성공 {total_passed:,}, 실패 {total_failed:,}, 총 {total_files:,}")

        # ---------------------------------------------------------------
        # ✅ JSONL 로그 저장
        # ---------------------------------------------------------------
        with open(jsonl_path, "w", encoding="utf-8") as out_f:
            for rec in records:
                out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        # ---------------------------------------------------------------
        # ✅ 결과 요약 저장
        # ---------------------------------------------------------------
        summary = {
            "dataset": self.domain,
            "layer": self.layer,
            "vendor": self.vendor,
            "exchange_code": self.exchange_code,
            "trd_dt": self.trd_dt,
            "record_count": len(files),
            "passed_files": total_passed,
            "failed_files": total_failed,
            "failed_symbols": failed_symbols,
            "status": "success" if total_failed == 0 else "failed",
            "validated_source": str(self.dataset_path),
            "validated_file": str(jsonl_path),
            "validated_at": datetime.now(timezone.utc).isoformat(),
        }

        with open(last_validated_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        self._update_latest_snapshot_meta(
            domain=self.domain,
            trd_dt=self.trd_dt,
            meta_file=str(last_validated_path),
        )

        if total_failed > 0:
            self.log.error(f"❌ {total_failed:,}개 종목 검증 실패 — details: {last_validated_path}")
            raise ValueError(f"Fundamentals validation failed — {total_failed:,} files failed")

        self.log.info("fundamentals_general 파일 jsonl에서 parquet으로 변환")
        self._finalize_general_parquet()

        self.log.info(f"🎯 Fundamentals 검증 완료 — 성공 {total_passed:,}건, 결과 저장: {last_validated_path}")
        return summary

    # -------------------------------------------------------------------------
    # 4️⃣ empty 결과 스킵용 유틸 (Base와 일관성 유지)
    # -------------------------------------------------------------------------
    def _skip_empty_result(self) -> Dict[str, Any]:
        result = {
            "dataset": self.domain,
            "layer": self.layer,
            "vendor": self.vendor,
            "exchange_code": self.exchange_code,
            "trd_dt": self.trd_dt,
            "status": "skipped",
            "record_count": 0,
            "validated_source": str(self.dataset_path),
            "validated_at": datetime.now(timezone.utc).isoformat(),
            "message": "No data found (allow_empty=True)",
        }

        self.log.info(f"✅ No data found for {self.domain} → SKIPPED")
        return result