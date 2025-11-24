from plugins.validators.base_data_validator import BaseDataValidator
from plugins.utils.name_utils import normalize_field_names
from plugins.utils.path_manager import DataPathResolver

from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import pandas as pd
import json
import logging
import shutil


class FundamentalDataValidator(BaseDataValidator):
    """
    🧩 Fundamental 전용 Validator (Lake 레벨)
    - 파일 단위 검증 (OOM 방지)
    - Pandera/Soda 기반 검증
    - validated → latest(ticker) snapshot 생성
    - General JSONL → Parquet 변환
    """

    def __init__(
        self,
        trd_dt: str,
        domain: str,
        exchange_code: str,
        vendor: str = "eodhd",
        domain_group: str = "equity",
        allow_empty: bool = False,
    ):
        self.exchange_code = exchange_code
        self.vendor = vendor
        self.domain_group = domain_group
        self.trd_dt = trd_dt
        self.domain = domain

        # ============================
        #  RAW fundamentals 경로
        # ============================
        self.dataset_path = DataPathResolver.lake_raw_fundamentals(
            domain_group=domain_group,
            vendor=vendor,
            exchange_code=exchange_code,
            trd_dt=trd_dt,
        )

        # ============================
        #  validated root (exchange 단위)
        # ============================
        self.validated_root = DataPathResolver.lake_validated_fundamentals_root(
            domain_group=domain_group,
            vendor=vendor,
            exchange_code=exchange_code,
        )

        # ============================
        #  validated 작업 디렉토리 (날짜 포함)
        # ============================
        self.validated_trd_dir = DataPathResolver.lake_validated_fundamentals_trd_dt(
            domain_group=domain_group,
            vendor=vendor,
            exchange_code=exchange_code,
            trd_dt=trd_dt,
        )

        # ============================
        #  latest root
        # ============================
        self.latest_root = DataPathResolver.fundamentals_latest_root(
            domain_group=domain_group,
            vendor=vendor,
            exchange_code=exchange_code
        )

        self.validated_trd_dir.mkdir(parents=True, exist_ok=True)
        self.latest_root.mkdir(parents=True, exist_ok=True)

        super().__init__(
            domain=domain,
            layer="lake",
            trd_dt=trd_dt,
            vendor=vendor,
            dataset_path=self.dataset_path,
            exchange_code=exchange_code,
            domain_group=domain_group,
            allow_empty=allow_empty,
        )

        self.log = logging.getLogger(__name__)

    # ===============================================================
    # 🔹 latest (ticker=XXX.json) 생성
    # ===============================================================
    def _write_latest_ticker_snapshot(self, general_dict: Dict[str, Any]) -> None:
        ticker = general_dict.get("Code")
        if not ticker:
            self.log.warning("⚠️ General에 Code(티커) 없음 → latest 생성 스킵")
            return

        out_path = self.latest_root / f"{ticker}.json"

        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(general_dict, f, ensure_ascii=False, indent=2)

            self.log.info(f"🟩 latest snapshot 생성: {out_path}")

        except Exception as e:
            self.log.error(f"❌ latest snapshot 생성 실패: {e}")

    # ===============================================================
    # 🔹 General JSONL → Parquet 변환
    # ===============================================================
    def _finalize_general_parquet(self) -> None:
        """
        validated/trd_dt 에 생성된 JSONL을 Parquet으로 변환하고
        latest에도 최신 버전을 복사한다.
        """

        jsonl_path = self.validated_trd_dir / f"fundamentals_general_{self.trd_dt}.jsonl"
        parquet_path = self.validated_trd_dir / f"fundamentals_general_{self.trd_dt}.parquet"

        # latest 저장 위치
        latest_parquet_path = self.latest_root / "fundamentals_general_latest.parquet"

        if not jsonl_path.exists():
            self.log.warning(f"⚠️ JSONL 없음, parquet 변환 스킵: {jsonl_path}")
            return

        df = pd.read_json(jsonl_path, lines=True)
        df.drop_duplicates(subset=["ticker"], keep="last", inplace=True)

        df.to_parquet(parquet_path, index=False)
        shutil.copyfile(parquet_path, latest_parquet_path)

        self.log.info(f"📦 JSONL → Parquet 변환 완료: {parquet_path}")
        self.log.info(f"📦 latest parquet 갱신 완료: {latest_parquet_path}")

    # ===============================================================
    # 🔹 General JSONL append
    # ===============================================================
    def _append_general_to_jsonl(self, general_dict: Dict[str, Any]) -> None:
        if not general_dict:
            return

        try:
            record = {
                "exchange_code": self.exchange_code,
                "ticker": general_dict.get("Code"),
                "Name": general_dict.get("Name"),
                "Description": general_dict.get("Description"),
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
                "fiscal_year_end": general_dict.get("FiscalYearEnd"),
                "PrimaryTicker": general_dict.get("PrimaryTicker"),
                "cik": general_dict.get("CIK"),
                "last_fundamental_update": general_dict.get("UpdatedAt"),
                "logo_url": general_dict.get("LogoURL"),
                "trd_dt": self.trd_dt,
                "validated_at": datetime.now(timezone.utc).isoformat(),
            }

            record = normalize_field_names(record)

            jsonl_path = self.validated_trd_dir / f"fundamentals_general_{self.trd_dt}.jsonl"
            with open(jsonl_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        except Exception as e:
            self.log.warning(f"⚠️ General JSONL append 실패: {e}")

    # ===============================================================
    # 🔹 ETF/Stock 타입 감지 (Pandera schema 선택)
    # ===============================================================
    def _detect_fund_type(self, df: pd.DataFrame) -> str:
        type_col = next(
            (c for c in ["General_Type", "Type", "General.Type", "General_Type.value"] if c in df.columns),
            None,
        )

        if type_col and df[type_col].astype(str).str.contains("Common Stock", case=False, na=False).any():
            return "stock"

        return "etf"

    # ===============================================================
    # 🔹 Pandera + Soda 검증 적용
    # ===============================================================
    def _define_checks(self, df: pd.DataFrame) -> Dict[str, Any]:
        fund_type = self._detect_fund_type(df)
        schema_path = self.schema_root / f"fundamentals_{fund_type}.json"
        self.log.info(f"🧩 schema_path: {schema_path}")

        if not schema_path.exists():
            raise FileNotFoundError(f"Schema not found: {schema_path}")

        with open(schema_path, "r", encoding="utf-8") as f:
            schema_def = json.load(f)

        checks = {
            "pandera_schema": self._validate_with_pandera(df, schema_def)
        }

        return checks

    # ===============================================================
    # 🔹 본 검증 루틴
    # ===============================================================
    def validate(self, context: Optional[dict] = None) -> Dict[str, Any]:
        files = list(self.dataset_path.glob("*.json"))

        if not files:
            raise FileNotFoundError(f"❌ no fundamental files: {self.dataset_path}")

        total_files = len(files)
        self.log.info(f"📂 {total_files} fundamentals 파일 검증 시작")

        last_validated_path = self.validated_trd_dir / "_last_validated.json"
        results = []
        total_pass, total_fail = 0, 0
        failed_symbols = []

        progress_lock = Lock()
        completed = 0

        def _validate_file(file_path: Path):
            nonlocal completed, total_pass, total_fail

            try:
                raw = json.loads(file_path.read_text(encoding="utf-8"))
                general = raw.get("General", {})
                if not general:
                    return {"file": file_path.name, "status": "skipped"}

                df = pd.json_normalize(general, sep="_")
                df.columns = [c.replace(".", "_") for c in df.columns]
                df.columns = [f"General_{c}" if not c.startswith("General_") else c for c in df.columns]

                checks = self._define_checks(df)
                status = self._aggregate_status(checks)

                with progress_lock:
                    completed += 1
                    pct = completed / total_files * 100
                    self.log.info(f"🔍 [{completed}/{total_files} | {pct:.1f}%] {file_path.name}")

                # 성공 처리
                if status == "success":
                    out_path = self.validated_trd_dir / f"{file_path.stem}.json"
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(raw, f, ensure_ascii=False, indent=2)

                    self._append_general_to_jsonl(general)
                    self._write_latest_ticker_snapshot(general)
                    total_pass += 1
                else:
                    total_fail += 1
                    failed_symbols.append(file_path.name)

                return {"file": file_path.name, "status": status}

            except Exception as e:
                with progress_lock:
                    completed += 1
                total_fail += 1
                failed_symbols.append(file_path.name)
                return {"file": file_path.name, "status": "failed", "error": str(e)}

        # ThreadPoolExecutor 실행
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(_validate_file, f): f for f in files}
            for future in as_completed(futures):
                results.append(future.result())

        # 저장
        jsonl_log_path = self.validated_trd_dir / "validation_log.jsonl"
        with open(jsonl_log_path, "w", encoding="utf-8") as f:
            for r in results:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

        summary = {
            "domain": self.domain,
            "layer": self.layer,
            "vendor": self.vendor,
            "exchange_code": self.exchange_code,
            "trd_dt": self.trd_dt,
            "total_files": total_files,
            "passed": total_pass,
            "failed": total_fail,
            "failed_symbols": failed_symbols,
            "validated_at": datetime.now(timezone.utc).isoformat(),
        }

        with open(last_validated_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        # parquet 변환
        self._finalize_general_parquet()

        if total_fail > 0:
            raise ValueError(f"❌ Fundamentals validation failed: {total_fail} files")

        self.log.info(f"🎯 Fundamentals 검증 완료 — {total_pass} success")
        return summary
