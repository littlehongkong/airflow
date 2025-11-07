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


    def _append_general_to_parquet(self, general_dict: Dict[str, Any]) -> None:
        """
        ✅ fundamentals General-only 정보를 거래소 단위 parquet에 append
        - 파일명: fundamentals_general_{trd_dt}.parquet
        - 위치: validated/fundamentals/exchange_code={exchange_code}/
        """
        try:

            if not general_dict:
                return

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

            df = pd.DataFrame([record])

            general_output_dir = (
                C.DATA_LAKE_VALIDATED
                / self.domain_group
                / "fundamentals"
                / f"vendor={self.vendor}"
                / f"exchange_code={self.exchange_code}"
            )
            general_output_dir.mkdir(parents=True, exist_ok=True)

            parquet_path = general_output_dir / f"fundamentals_general_{self.trd_dt}.parquet"

            if parquet_path.exists():
                existing = pd.read_parquet(parquet_path)
                merged = pd.concat([existing, df], ignore_index=True)
                merged.drop_duplicates(subset=["ticker"], keep="last", inplace=True)
                merged.to_parquet(parquet_path, index=False)
            else:
                df.to_parquet(parquet_path, index=False)

            # 최신본 복사
            latest_path = general_output_dir / "fundamentals_general_latest.parquet"
            shutil.copyfile(parquet_path, latest_path)

            self.log.debug(f"📦 General parquet updated: {parquet_path.name}")

        except Exception as e:
            self.log.warning(f"⚠️ General parquet append 실패: {e}")


    def _define_checks(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        fundamentals 전용 Pandera + Soda Core 검증
        - ETF/Stock 자동 분기
        - flatten 컬럼 전처리
        """
        checks = {}

        # ✅ 1. 기본 전처리
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].apply(lambda x: None if isinstance(x, str) and x.strip() == "" else x)

        for c in ["General_UpdatedAt", "General_IPODate", "General_ReportDate"]:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce")

        # ✅ 2. ETF/Stock 분기
        type_col = next((c for c in ["General_Type", "Type", "General.Type", "General_Type.value"] if c in df.columns),
                        None)

        fund_type = "etf"
        if type_col and df[type_col].astype(str).str.contains("Common Stock", case=False, na=False).any():
            fund_type = "stock"

        # ✅ 3. Pandera Schema 적용
        schema_path = self.schema_root / f"fundamentals_{fund_type}.json"
        self.log.info(f"schema_path : {schema_path}")
        if schema_path.exists():
            with open(schema_path, "r", encoding="utf-8") as f:
                schema_def = json.load(f)
            checks["pandera_schema"] = self._validate_with_pandera(df, schema_def)
        else:
            self.log.warning(f"⚠️ Pandera schema not found: {schema_path}")

        # ✅ 4. Soda Core 적용
        soda_path = self.check_root / f"fundamentals_{fund_type}.yml"
        if soda_path.exists():
            checks.update(self._run_soda_duckdb_validation(df, soda_path))
        else:
            self.log.warning(f"⚠️ Soda check file not found: {soda_path}")

        return checks


    # -------------------------------------------------------------------------
    # 1️⃣ 메인 검증 (Base 구조 유지)
    # -------------------------------------------------------------------------
    def validate(self, context: Optional[dict] = None) -> Dict[str, Any]:
        """
        fundamentals 전용 검증 로직 (파일 단위)
        - General 블록만 검증하되, 나머지 블록(Financials 등)은 그대로 유지
        - 검증 성공 시 원본 전체 JSON을 validated로 복사
        """
        files = [
            f for f in self.dataset_path.glob("*.json")
            if not f.name.startswith("_")
        ]

        if not files:
            if self.allow_empty:
                return self._skip_empty_result()
            raise FileNotFoundError(f"❌ No fundamental files found: {self.dataset_path}")

        self.log.info(f"📁 {len(files)}개 fundamentals 파일 검증 시작")

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
        last_validated_path = validated_dir / "_last_validated.json"

        total_passed, total_failed = 0, 0
        failed_symbols = []

        for i, f in enumerate(files, 1):
            try:
                # ✅ 전체 JSON 로드 (General 외 key 포함)
                with open(f, "r", encoding="utf-8") as infile:
                    full_data = json.load(infile)

                # ✅ General 키만 검증용으로 추출
                general_data = full_data.get("General", {})
                if not general_data:
                    self.log.warning(f"⚠️ No 'General' key found in {f.name}")
                    continue

                df = pd.json_normalize(general_data, sep="_")
                df.columns = [c.replace(".", "_") for c in df.columns]
                df.columns = [f"General_{c}" if not c.startswith("General_") else c for c in df.columns]

                # ✅ 검증 수행
                checks = self._define_checks(df)
                status = self._aggregate_status(checks)

                record = {
                    "file": f.name,
                    "status": status,
                    "checks": checks,
                }
                with open(jsonl_path, "a", encoding="utf-8") as out_f:
                    out_f.write(json.dumps(record, ensure_ascii=False) + "\n")

                if status == "success":
                    total_passed += 1

                    # ✅ 원본 전체 JSON (Financials 포함)을 validated로 그대로 복사
                    validated_json_path = validated_dir / f"{f.stem}.json"
                    with open(validated_json_path, "w", encoding="utf-8") as out_f:
                        json.dump(full_data, out_f, ensure_ascii=False, indent=2)

                    # ✅ General-only parquet append
                    self._append_general_to_parquet(full_data.get("General", {}))

                else:
                    total_failed += 1
                    failed_symbols.append(f.stem)

            except Exception as e:
                total_failed += 1
                failed_symbols.append(f.stem)
                self.log.warning(f"⚠️ {f.name} 검증 실패: {e}")
                self.log.info(traceback.format_exc())
                continue

            finally:
                # ✅ 메모리 정리
                for var in ["df", "full_data", "general_data", "checks"]:
                    if var in locals():
                        del locals()[var]
                gc.collect()
                tracemalloc.clear_traces()

        # ---------------------------------------------------------------------
        # 2️⃣ 결과 요약
        # ---------------------------------------------------------------------
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

        # ✅ `_last_validated.json` 저장
        with open(last_validated_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        # ✅ snapshot 메타 업데이트
        self._update_latest_snapshot_meta(
            domain=self.domain,
            trd_dt=self.trd_dt,
            meta_file=str(last_validated_path)
        )

        if total_failed > 0:
            self.log.error(f"❌ {total_failed:,}개 종목 검증 실패 — details: {last_validated_path}")
            raise ValueError(f"Fundamentals validation failed — {total_failed:,} files failed")

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