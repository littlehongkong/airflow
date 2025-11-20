from plugins.validators.base_data_validator import BaseDataValidator
from plugins.utils.name_utils import normalize_field_names
from plugins.utils.path_manager import DataPathResolver

from datetime import datetime, timezone
from typing import Optional, Dict, Any

import pandas as pd
import json
import logging
import shutil

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

_parquet_lock = Lock()


class FundamentalDataValidator(BaseDataValidator):
    """
    🧩 Fundamental 전용 Validator
    - 파일 단위 검증 (OOM 방지)
    - Pandera/Soda 기반 검증 (기존 로직 유지)
    - General 블록 JSONL → Parquet 변환
    - validated → latest (ticker 레이어) 생성
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
        # RAW 경로: lake/raw/equity/fundamentals/vendor=.../exchange_code=.../trd_dt=...
        self.dataset_path = DataPathResolver.lake_raw_fundamentals(
            domain_group=domain_group,
            vendor=vendor,
            exchange_code=exchange_code,
            trd_dt=trd_dt,
        )

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

        # ✅ validated root (trd_dt 없이 상위 루트)
        #   /data_lake/validated/equity/fundamentals/vendor=eodhd/exchange_code=NASDAQ
        self.validated_root = DataPathResolver.lake_validated_fundamentals_root(
            domain_group=domain_group,
            vendor=vendor,
            exchange_code=exchange_code,
        )

        # ✅ latest root (ticker별 최신 snapshot - exchange_code 하위로 나뉨)
        #   /data_lake/validated/equity/fundamentals/vendor=eodhd/latest
        self.latest_root = DataPathResolver.fundamentals_latest_root(
            domain_group=domain_group,
            vendor=vendor,
            exchange_code=self.exchange_code
        )

        self.log = logging.getLogger(__name__)

    # ===============================================================
    # 🔹 latest/exchange_code=XXX/{ticker}.json 생성
    # ===============================================================
    def _write_latest_ticker_snapshot(self, general_dict: Dict[str, Any]) -> None:
        """
        validated된 General 정보를 기반으로 latest 파일 생성
        경로: .../fundamentals/vendor=.../latest/exchange_code=EX/{ticker}.json
        Lake 레이어에서는 ticker 기준 구조만 유지한다.
        """
        try:
            ticker = general_dict.get("Code")
            if not ticker:
                self.log.warning("⚠️ General 데이터에 Code(티커)가 없음 → latest 저장 스킵")
                return

            # latest/exchange_code=US/
            latest_ex_dir = self.latest_root
            latest_ex_dir.mkdir(parents=True, exist_ok=True)

            out_path = latest_ex_dir / f"{ticker}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(general_dict, f, ensure_ascii=False, indent=2)

            self.log.info(f"🟩 latest(ticker) 생성: {out_path}")

        except Exception as e:
            self.log.error(f"❌ latest ticker snapshot 실패: {e}")

    # ===============================================================
    # 🔹 General JSONL → Parquet + latest Parquet
    # ===============================================================
    def _finalize_general_parquet(self) -> None:
        """
        validated_root에 생성된 fundamentals_general_{trd_dt}.jsonl 을
        Parquet으로 변환하고,
        - 거래소 루트에 일별 parquet 생성
        - latest/exchange_code=EX/ 하위에 latest parquet 복사
        """
        validated_dir = self.validated_root

        jsonl_path = validated_dir / f"fundamentals_general_{self.trd_dt}.jsonl"
        parquet_path = validated_dir / f"fundamentals_general_{self.trd_dt}.parquet"

        # latest parquet 위치: latest/exchange_code=EX/fundamentals_general_latest.parquet
        latest_ex_dir = self.latest_root / f"exchange_code={self.exchange_code}"
        latest_ex_dir.mkdir(parents=True, exist_ok=True)
        latest_path = latest_ex_dir / "fundamentals_general_latest.parquet"

        if not jsonl_path.exists():
            self.log.warning(f"⚠️ No JSONL found for parquet conversion: {jsonl_path}")
            return

        df = pd.read_json(jsonl_path, lines=True)
        df.drop_duplicates(subset=["ticker"], keep="last", inplace=True)

        df.to_parquet(parquet_path, index=False)
        shutil.copyfile(parquet_path, latest_path)

        self.log.info(f"✅ Converted JSONL → Parquet: {parquet_path}")
        self.log.info(f"✅ Updated latest Parquet: {latest_path}")

    # ===============================================================
    # 🔹 General JSONL append
    # ===============================================================
    def _append_general_to_jsonl(self, general_dict: Dict[str, Any]) -> None:
        """
        ✅ 병렬 검증용 임시 append-safe JSONL 로깅
        위치: validated/.../fundamentals_general_{trd_dt}.jsonl
        """
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

            self.validated_root.mkdir(parents=True, exist_ok=True)
            jsonl_path = self.validated_root / f"fundamentals_general_{self.trd_dt}.jsonl"

            with open(jsonl_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        except Exception as e:
            self.log.warning(f"⚠️ General JSONL append 실패: {e}")

    # ===============================================================
    # 🔹 ETF/Stock 구분 (Pandera schema 선택용)
    # ===============================================================
    def _detect_fund_type(self, df: pd.DataFrame) -> str:
        """파일별 타입 자동 감지 (ETF or Stock)"""
        type_col = next(
            (c for c in ["General_Type", "Type", "General.Type", "General_Type.value"] if c in df.columns),
            None,
        )
        fund_type = "etf"
        if type_col and df[type_col].astype(str).str.contains("Common Stock", case=False, na=False).any():
            fund_type = "stock"
        return fund_type

    def _define_checks(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        ETF/Stock별 Pandera schema 적용
        (기존 Pandera 검증 로직 그대로 유지)
        """
        checks: Dict[str, Any] = {}

        fund_type = self._detect_fund_type(df)
        schema_path = self.schema_root / f"fundamentals_{fund_type}.json"
        self.log.info(f"🧩 schema_path: {schema_path}")

        if schema_path.exists():
            with open(schema_path, "r", encoding="utf-8") as f:
                schema_def = json.load(f)
            checks["pandera_schema"] = self._validate_with_pandera(df, schema_def)
        else:
            raise FileNotFoundError(f"Pandera schema not found: {schema_path}")

        # Soda Core 검증은 BaseDataValidator 쪽 _validate_with_soda 에서 layer/domain을 기준으로 실행됨
        # (여기서는 Pandera만 명시적으로 수행)
        return checks

    # ===============================================================
    # 🔹 단일 파일 검증 (옵션용, 현재 병렬 validate 내부에서와 유사)
    # ===============================================================
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

    # ===============================================================
    # 🔹 메인 validate (병렬 처리 + latest(ticker) 반영)
    # ===============================================================
    def validate(self, context: Optional[dict] = None) -> Dict[str, Any]:
        """
        ⚙️ fundamentals 전용 검증 로직 (Lake 레벨)
        - General 블록만 검증 (ETF/Stock 자동 구분)
        - 파일 단위 병렬 검증 (ThreadPoolExecutor)
        - 성공 시:
            - 원본 JSON validated로 복사
            - General JSONL append
            - latest/exchange_code=EX/{ticker}.json 업데이트
        - 마지막에 Parquet 변환 + latest Parquet 갱신
        """
        files = [f for f in self.dataset_path.glob("*.json") if not f.name.startswith("_")]
        total_files = len(files)

        if not files:
            raise FileNotFoundError(f"❌ No fundamental files found: {self.dataset_path}")

        self.log.info(f"📂 {total_files} fundamentals 파일 병렬 검증 시작")

        # validated 디렉터리 (trd_dt 포함)
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
        if jsonl_path.exists():
            jsonl_path.unlink()

        last_validated_path = validated_dir / "_last_validated.json"

        total_passed, total_failed = 0, 0
        failed_symbols = []
        records = []

        progress_lock = Lock()
        completed = 0

        # ---------------------------------------------------------------
        # 내부 파일 검증 함수 (병렬로 실행)
        # ---------------------------------------------------------------
        def _validate_file(file_path: Path) -> Dict[str, Any]:
            nonlocal completed

            try:
                full_data = json.loads(file_path.read_text(encoding="utf-8"))
                general_data = full_data.get("General", {})

                if not general_data:
                    with progress_lock:
                        completed += 1
                    return {"file": file_path.name, "status": "skipped", "reason": "No General key"}

                df = pd.json_normalize(general_data, sep="_")
                df.columns = [c.replace(".", "_") for c in df.columns]
                df.columns = [f"General_{c}" if not c.startswith("General_") else c for c in df.columns]

                checks = self._define_checks(df)
                status = self._aggregate_status(checks)

                with progress_lock:
                    completed += 1
                    progress_pct = (completed / total_files) * 100
                    self.log.info(
                        f"🔍 [{completed}/{total_files} | {progress_pct:.1f}%] "
                        f"{file_path.name} (status={status})"
                    )

                if status == "success":
                    # 1) validated JSON 복사
                    validated_json_path = validated_dir / f"{file_path.stem}.json"
                    with open(validated_json_path, "w", encoding="utf-8") as out_f:
                        json.dump(full_data, out_f, ensure_ascii=False, indent=2)

                    # 2) General JSONL append
                    self._append_general_to_jsonl(general_data)

                    # 3) latest(ticker) snapshot 업데이트
                    self._write_latest_ticker_snapshot(general_data)

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
        # ThreadPoolExecutor로 병렬 검증 실행
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

        # ---------------------------------------------------------------
        # 검증 결과 JSONL 로그 저장
        # ---------------------------------------------------------------
        with open(jsonl_path, "w", encoding="utf-8") as out_f:
            for rec in records:
                out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        # ---------------------------------------------------------------
        # 검증 요약 + 메타 저장
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

        # ---------------------------------------------------------------
        # Parquet 변환 + latest parquet 갱신
        # ---------------------------------------------------------------
        self.log.info("fundamentals_general 파일 jsonl에서 parquet으로 변환")
        self._finalize_general_parquet()

        if total_failed > 0:
            self.log.error(f"❌ {total_failed:,}개 종목 검증 실패 — details: {last_validated_path}")
            # 기존 로직을 그대로 유지: 실패가 있으면 예외 발생
            raise ValueError(f"Fundamentals validation failed — {total_failed:,} files failed")

        self.log.info(
            f"🎯 Fundamentals 검증 완료 — 성공 {total_passed:,}건, 결과 저장: {last_validated_path}"
        )
        return summary

    # ===============================================================
    # 🔹 빈 결과 스킵용 유틸 (Base와 일관성 유지)
    # ===============================================================
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
