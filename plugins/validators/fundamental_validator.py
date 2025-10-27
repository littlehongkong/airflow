import os
import json
import pandas as pd
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
        data_dir = self._get_lake_path("raw")
        batch_files = sorted(
            f for f in os.listdir(data_dir)
            if f.startswith("batch_") and f.endswith(".jsonl")
        )

        if not batch_files:
            msg = f"⚠️ 검증할 배치 파일이 없습니다: {data_dir}"
            self.log.warning(msg)
            raise Exception(msg)

        results = []
        total_failed = 0
        total_passed = 0
        all_records = 0

        try:
            for batch_file in batch_files:
                batch_path = os.path.join(data_dir, batch_file)
                self.log.info(f"🔍 배치 검증 시작: {batch_file}")

                with open(batch_path, "r", encoding="utf-8") as f:
                    records = [json.loads(line) for line in f]

                batch_result = self._validate_records(records, batch_file)
                results.append(batch_result)
                all_records += batch_result["record_count"]

                if batch_result["status"] == "success":
                    total_passed += 1
                else:
                    total_failed += 1

                # 배치별 요약 로그
                self.log.info(
                    f"📊 {batch_file} 결과: {batch_result['status'].upper()} | "
                    f"총 {batch_result['record_count']}건 | "
                    f"에러 {len(batch_result['errors'])} | 경고 {len(batch_result['warnings'])}"
                )
                for rr in batch_result["record_results"]:
                    self.log.info(
                        f"   - {rr['code']} ▶ {rr['status'].upper()} "
                        f"({len(rr.get('errors', []))} errors, {len(rr.get('warnings', []))} warnings)"
                    )

                    # 개별 경고 상세 로그
                    for w in rr.get("warnings", []):
                        self.log.warning(f"      ⚠️ {rr['code']} 경고 사유: {w}")

                    # 개별 오류 상세 로그
                    for e in rr.get("errors", []):
                        self.log.error(f"      ❌ {rr['code']} 오류 사유: {e}")

            final_status = "failed" if total_failed > 0 else "success"
            self.log.info(
                f"✅ 펀더멘털 전체 검증 완료 — 총 {len(batch_files)}개 배치, {all_records}건 "
                f"(성공 {total_passed}, 실패 {total_failed})"
            )

            summary_path = os.path.join(data_dir, "_validation_summary.json")
            with open(summary_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "timestamp": datetime.now().isoformat(),
                        "exchange_code": self.exchange_code,
                        "trd_dt": self.trd_dt,
                        "status": final_status,
                        "total_batches": len(batch_files),
                        "total_records": all_records,
                        "results": results,
                    },
                    f,
                    indent=2,
                    ensure_ascii=False,
                )
            self.log.info(f"📋 검증 요약 저장: {summary_path}")

            if final_status == "failed":
                raise Exception(f"❌ 펀더멘털 검증 실패 ({total_failed}/{len(batch_files)}개 배치)")

            return {"status": final_status, "batches": results}

        finally:
            deleted = 0
            for f in os.listdir(data_dir):
                if any(x in f for x in [".sample", ".parquet", "tmp_"]):
                    try:
                        os.remove(os.path.join(data_dir, f))
                        deleted += 1
                    except Exception:
                        pass
            if deleted:
                self.log.info(f"🧹 임시파일 {deleted}개 정리 완료 ({data_dir})")

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
                    # ✅ ETF 전용 검증 로직
                    etf_result = self._validate_etf_fields(rec)
                    rec_errors.extend(etf_result["errors"])
                    rec_warnings.extend(etf_result["warnings"])

                elif sec_type in ["common stock"]:
                    # ✅ 기존 Common Stock 검증 로직
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
    # ✅ ETF 전용 검증 로직 (Soda YAML 대응)
    # ----------------------------------------------------------------------
    def _validate_etf_fields(self, rec: dict) -> dict:
        """
        Soda checks 대응:
        - row_count > 0 → 자동 충족
        - duplicate_count(code) = 0 → DAG 레벨에서 검증
        - missing_percent(field) < 5 → 해당 필드 존재 여부만 확인
        - avg(field) > 0 → 음수/0 여부 검증
        """
        errors = []
        warnings = []
        general = rec.get("General", {})
        code = general.get("Code", "UNKNOWN")
        data = rec.get("ETF_Data", {})

        assert data != {}, 'data가 비어있습니다.'

        required_fields = [
            "TotalAssets",
            "Holdings_Count",
            "NetExpenseRatio",
            "Yield",
        ]

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
        # elif section == "Cash_Flow":
        #     keys = ["OperatingCashFlow", "FreeCashFlow"]
        else:
            keys = []

        for k in keys:
            v = data.get(k)
            if v is None:
                warnings.append(f"{section}.{period}.{k} 누락")
            elif invalid_num(v):
                errors.append(f"{section}.{period}.{k} 비정상값 ({v})")

        return {"errors": errors, "warnings": warnings}
