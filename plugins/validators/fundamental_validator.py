import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from plugins.validators.base_validator import BaseDataValidator


class FundamentalValidator(BaseDataValidator):
    """
    🧾 펀더멘털 데이터 검증기 (ETF 전용룰 + 경고 상세 로그)
    --------------------------------------------------------------
    ✅ Common Stock ↔ ETF 분리 검증
    ✅ ETF: Soda YAML 기반 핵심 항목만 확인
    ✅ Common Stock: General + Financials 구조 검증
    ✅ 모든 경고 사유를 Airflow 로그에 직접 출력
    """

    def __init__(self, exchange_code: str, trd_dt: str, data_domain: str = "fundamentals", **kwargs):
        super().__init__(exchange_code, trd_dt, data_domain, **kwargs)
        self.required_general_fields = ["Code", "Type", "Name"]
        self.financial_sections = ["Balance_Sheet", "Income_Statement", "Cash_Flow"]

    # ----------------------------------------------------------------------
    def validate(self, **kwargs):
        """
        펀더멘털 배치 검증 실행
        - 배치별 JSONL 파일을 순회하면서 검증 수행
        - 각 배치별 검증 결과를 저장 후 최종 요약파일 생성
        - 항상 dict 반환 (Airflow 로그 처리 호환)
        """

        data_dir = self._get_lake_path("raw")
        validated_dir = self._get_lake_path("validated")
        os.makedirs(validated_dir, exist_ok=True)

        batch_files = sorted(
            f for f in os.listdir(data_dir)
            if f.startswith("batch_") and f.endswith(".jsonl")
        )

        if not batch_files:
            msg = f"⚠️ 검증할 배치 파일이 없습니다: {data_dir}"
            self.log.warning(msg)
            return {
                "status": "skipped",
                "exchange_code": self.exchange_code,
                "trd_dt": self.trd_dt,
                "data_domain": self.data_domain,
                "record_count": 0,
                "reason": "no_batch_files"
            }

        all_results = []
        total_passed, total_failed, total_records = 0, 0, 0
        skipped_batches = []

        for batch_file in batch_files:
            batch_path = os.path.join(data_dir, batch_file)
            self.log.info(f"🔍 배치 검증 시작: {batch_file}")

            with open(batch_path, "r", encoding="utf-8") as f:
                records = [json.loads(line) for line in f]

            # 🔹 빈 배치 스킵 처리
            if not records:
                self.log.warning(f"⚠️ {batch_file} — 데이터 없음, 스킵 처리")
                skipped_batches.append(batch_file)
                continue

            # 🔹 각 배치 검증 수행
            batch_result = self._validate_records(records, batch_file)
            all_results.append(batch_result)
            total_records += batch_result["record_count"]

            # 🔹 배치별 검증 결과 저장 (validated 경로)
            batch_result_path = os.path.join(validated_dir, batch_file.replace(".jsonl", "_validation.json"))
            with open(batch_result_path, "w", encoding="utf-8") as vf:
                json.dump(batch_result, vf, indent=2, ensure_ascii=False)
            self.log.info(f"📄 배치 검증 결과 저장: {batch_result_path}")

            if batch_result["status"] == "success":
                total_passed += 1
            else:
                total_failed += 1

        # ------------------------------------------------------------------
        # 🔹 요약 파일 저장 (validated 경로)
        summary_path = os.path.join(validated_dir, "_validation_summary.json")
        summary_data = {
            "timestamp": datetime.now().isoformat(),
            "exchange_code": self.exchange_code,
            "trd_dt": self.trd_dt,
            "status": "failed" if total_failed else "success",
            "total_batches": len(batch_files),
            "validated_batches": len(all_results),
            "skipped_batches": skipped_batches,
            "total_records": total_records,
            "summary": {"passed": total_passed, "failed": total_failed, "skipped": len(skipped_batches)},
            "batches": [
                {"batch": r["batch"], "status": r["status"], "record_count": r["record_count"]}
                for r in all_results
            ],
        }

        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        self.log.info(f"📋 검증 요약 저장: {summary_path}")

        # ------------------------------------------------------------------
        # 🔹 검증 완료된 데이터 Parquet 저장 (validated 계층)
        try:
            self._save_validated_output(all_results)
        except Exception as e:
            self.log.warning(f"⚠️ 검증 결과 저장 중 예외 발생: {e}")

        # ------------------------------------------------------------------
        # 🔹 리턴 값 보장 (Airflow 후속 처리용)
        final_status = "failed" if total_failed > 0 else "success"
        result = {
            "status": final_status,
            "exchange_code": self.exchange_code,
            "trd_dt": self.trd_dt,
            "data_domain": self.data_domain,
            "record_count": total_records,
            "total_batches": len(batch_files),
            "failed_batches": total_failed,
            "skipped_batches": skipped_batches,
        }

        if total_failed > 0:
            self.log.error(f"❌ 펀더멘털 검증 실패 ({total_failed}/{len(batch_files)}개 배치)")
            return result

        self.log.info(f"✅ 펀더멘털 검증 성공 — {total_passed}/{len(batch_files)}개 배치 통과, {len(skipped_batches)}개 스킵")
        return result

    # ----------------------------------------------------------------------
    def _validate_records(self, records: list[dict], batch_name: str) -> dict:
        errors = []
        warnings = []
        record_results = []

        for rec in records:
            code = rec.get("General", {}).get("Code", "UNKNOWN")
            general = rec.get("General", {})
            sec_type = (general.get("Type") or "").lower()
            rec_errors, rec_warnings = [], []

            try:
                # ✅ 공통 General 필수 필드
                for field in self.required_general_fields:
                    if not general.get(field):
                        rec_warnings.append(f"General.{field} 누락")

                if sec_type in ["etf"]:
                    etf_result = self._validate_etf_fields(rec)
                    rec_errors.extend(etf_result["errors"])
                    rec_warnings.extend(etf_result["warnings"])

                elif sec_type in ["common stock"]:
                    fin = rec.get("Financials", {})
                    if not fin:
                        rec_errors.append("Financials 필드 없음")
                    else:
                        for section in self.financial_sections:
                            sec_data = fin.get(section, {})
                            if not sec_data:
                                rec_warnings.append(f"{section} 데이터 없음")
                                continue
                            latest_period, latest_data = self._get_latest_period(sec_data)
                            if not latest_data:
                                rec_warnings.append(f"{section} 최신 데이터 없음")
                                continue
                            checks = self._check_financial_section(section, latest_data, code, latest_period)
                            rec_errors.extend(checks["errors"])
                            rec_warnings.extend(checks["warnings"])
                else:
                    rec_warnings.append(f"알 수 없는 종목 타입: {sec_type}")

            except Exception as e:
                rec_errors.append(f"레코드 처리 중 예외: {str(e)}")

            record_results.append({
                "code": code,
                "type": sec_type,
                "status": "failed" if rec_errors else ("warning" if rec_warnings else "success"),
                "errors": rec_errors,
                "warnings": rec_warnings,
            })

            errors.extend([f"{code}: {msg}" for msg in rec_errors])
            warnings.extend([f"{code}: {msg}" for msg in rec_warnings])

        status = "failed" if errors else ("warning" if warnings else "success")
        return {
            "batch": batch_name,
            "status": status,
            "record_count": len(records),
            "errors": errors,
            "warnings": warnings,
            "record_results": record_results,
        }

    # ----------------------------------------------------------------------
    def _validate_etf_fields(self, rec: dict) -> dict:
        errors = []
        warnings = []
        general = rec.get("General", {})
        data = rec.get("ETF_Data", {})

        required_fields = ["TotalAssets", "Holdings_Count", "NetExpenseRatio", "Yield"]
        for f in required_fields:
            val = data.get(f)
            if val is None:
                warnings.append(f"{f} 누락")
            else:
                try:
                    v = float(val)
                    if v < 0:
                        errors.append(f"{f} 음수값 ({v})")
                    elif f in ["TotalAssets", "Holdings_Count"] and v == 0:
                        errors.append(f"{f} 0값 ({v})")
                except Exception:
                    errors.append(f"{f} 수치형 변환 불가 ({val})")

        return {"errors": errors, "warnings": warnings}

    # ----------------------------------------------------------------------
    def _get_latest_period(self, sec_data: dict):
        for key in ["yearly", "Yearly", "quarterly", "Quarterly"]:
            if key in sec_data and isinstance(sec_data[key], dict):
                values = sec_data[key]
                if values:
                    try:
                        latest_period = sorted(values.keys())[-1]
                        return latest_period, values[latest_period]
                    except Exception:
                        pass
        return None, None

    # ----------------------------------------------------------------------
    def _check_financial_section(self, section: str, data: dict, code: str, period: str) -> dict:
        errors, warnings = [], []

        def invalid_num(val):
            try:
                v = float(val)
                return pd.isna(v) or v == 0
            except Exception:
                return True

        if section == "Balance_Sheet":
            keys = ["totalAssets", "totalLiab"]
        elif section == "Income_Statement":
            keys = ["netIncome", "totalRevenue"]
        else:
            keys = []

        for k in keys:
            v = data.get(k)
            if v is None:
                warnings.append(f"{section}.{period}.{k} 누락")
            elif invalid_num(v):
                errors.append(f"{section}.{period}.{k} 비정상값 ({v})")

        return {"errors": errors, "warnings": warnings}

    # ----------------------------------------------------------------------
    def _save_validated_output(self, batch_results: list):
        validated_dir = self._get_lake_path("validated")
        os.makedirs(validated_dir, exist_ok=True)
        validated_files = []

        # ✅ 상태 집계용 변수
        passed_batches = 0
        warned_batches = 0
        failed_batches = 0

        for br in batch_results:
            batch_name = os.path.splitext(br["batch"])[0]
            batch_parquet = os.path.join(validated_dir, f"{batch_name}.parquet")

            # 상태별 카운트
            if br["status"] == "success":
                passed_batches += 1
            elif br["status"] == "warning":
                warned_batches += 1
            else:
                failed_batches += 1

            recs = []
            for rr in br.get("record_results", []):
                recs.append({
                    "code": rr["code"],
                    "type": rr.get("type"),
                    "status": rr.get("status"),
                    "error_count": len(rr.get("errors", [])),
                    "warning_count": len(rr.get("warnings", [])),
                })
            if not recs:
                self.log.warning(f"⚠️ {batch_name} 검증 데이터 없음, 저장 건너뜀.")
                continue

            df = pd.DataFrame(recs)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, batch_parquet)
            validated_files.append(batch_parquet)
            self.log.info(f"📦 {batch_name}.parquet 저장 완료 ({len(df)}건)")

        # ✅ 메타데이터 기록
        meta = {
            "dataset": self.data_domain,
            "exchange_code": self.exchange_code,
            "trd_dt": self.trd_dt,
            "last_validated_timestamp": datetime.now().isoformat() + "+00:00",
            "status": (
                "failed" if failed_batches > 0
                else "warning" if warned_batches > 0
                else "success"
            ),
            "checks_summary": {
                "total_batches": len(batch_results),
                "passed": passed_batches,
                "failed": failed_batches,
                "warned": warned_batches
            },
            "validated_batches": [os.path.basename(f) for f in validated_files],
            "record_total": sum([len(pd.read_parquet(f)) for f in validated_files]),
            "validation_log_file": f"{self.data_domain}_{datetime.now():%Y%m%d_%H%M}_scheduled__{self.trd_dt}T00-00-00.json",
            "source_file": str(self._get_lake_path('raw')),
            "validated_dir": validated_dir,
            "source_meta": {
                "vendor": "EODHD",
                "endpoint": "api/fundamentals",
                "params": {"api_token": "***", "fmt": "json"},
            }
        }

        meta_path = os.path.join(validated_dir, "_last_validated.json")
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)
        self.log.info(f"📋 메타 저장 완료: {meta_path}")
