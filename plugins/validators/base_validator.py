# plugins/validators/base_validator.py
import os
import json
import pandas as pd
import pandera.pandas as pa
from soda.scan import Scan
import tempfile
import yaml
import duckdb
import logging
from typing import Optional, Tuple, Any, Dict
from pandera import DataFrameSchema
from plugins.config.constants import DATA_LAKE_ROOT
from datetime import datetime, timezone

class BaseDataValidator:
    """
    모든 데이터셋 검증의 베이스 클래스 (재사용성 극대화 버전)
    ------------------------------------------------------------
    ✅ Pandera + Soda Core 지원
    ✅ Hive 파티션 경로 자동 탐색
    ✅ layer 전환(raw → validated 등)
    ✅ Exchange / Date 기반 경로 자동 결정
    """

    # 각 validator별로 override 해야 함
    schema: Optional[DataFrameSchema] = None
    soda_check_file: Optional[str] = None  # 예: "/opt/airflow/plugins/soda/checks/equity_price_checks.yml"

    def __init__(self, exchange_code: str, trd_dt: str, data_domain: str, layer: str = "raw", vendor: str = None, **kwargs):
        self.exchange_code = exchange_code
        self.trd_dt = trd_dt
        self.layer = layer
        self.data_domain=data_domain
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # ✅ DAG에서 넘겨주는 추가 인자 처리
        self.allow_empty = kwargs.get("allow_empty", False)

        self.vendor = (vendor or "").lower()
        if not self.vendor:
            raise ValueError(
                f"❌ vendor 값이 지정되지 않았습니다. "
                f"Validator 초기화 시 vendor 인자가 필수입니다 "
                f"(data_domain={data_domain}, exchange_code={exchange_code})."
            )

    # ----------------------------------------------------------------------
    # ✅ 1️⃣ Data Lake 경로 헬퍼
    # ----------------------------------------------------------------------
    def _get_lake_path(self, layer: Optional[str] = None) -> str:
        """Hive-style 경로 반환 (/data_lake/{layer}/{domain}/exchange_code=..../trd_dt=...)"""
        layer = layer or self.layer
        base_path = os.path.join(
            DATA_LAKE_ROOT,
            layer,
            self.data_domain,
            f"vendor={self.vendor}",
            f"exchange_code={self.exchange_code}",
            f"trd_dt={self.trd_dt}"
        )
        os.makedirs(base_path, exist_ok=True)
        return base_path

    # ----------------------------------------------------------------------
    # ✅ 3️⃣ Pandera 검증
    # ----------------------------------------------------------------------
    def _run_pandera(self, df: pd.DataFrame) -> Tuple[bool, Any]:
        if not self.schema:
            raise NotImplementedError("하위 클래스에서 schema를 정의해야 합니다.")

        try:
            self.schema.validate(df, lazy=True)
            return True, None
        except pa.errors.SchemaErrors as err:
            return False, err.failure_cases

    # ----------------------------------------------------------------------
    # ✅ 4️⃣ Soda 검증
    # ----------------------------------------------------------------------
    def _run_soda(self, layer: str = "raw", dag_run_id: str = None, task_id: str = None, allow_empty: bool = False) -> Dict:
        """Soda Core 검증만 수행하고 결과 반환"""

        self.soda_check_file = self._get_soda_check_path()
        if not self.soda_check_file or not os.path.exists(self.soda_check_file):
            print("⚠️ Soda 검증을 건너뜁니다 (체크파일 없음).")
            return {"status": "skipped", "reason": "no_check_file"}

        # ✅ _get_lake_path()가 이미 파티션 경로까지 포함하므로 파일명만 추가
        raw_dataset_path = os.path.join(
            self._get_lake_path(layer),
            f"{self.data_domain}.jsonl"
        )

        # ✅ 데이터 파일 존재 확인
        if not os.path.exists(raw_dataset_path):
            msg = f"⚠️ 검증 대상 파일이 없습니다: {raw_dataset_path}"
            if allow_empty:
                print(msg + " → allow_empty=True, skip 처리")
                return {"status": "skipped", "reason": "no_data_file"}
            raise FileNotFoundError(msg)


        # ✅ 파일 크기 0인 경우
        if os.path.getsize(raw_dataset_path) == 0:
            msg = f"⚠️ 파일 비어 있음: {raw_dataset_path}"
            if allow_empty:
                print(msg + " → allow_empty=True, skip 처리")
                return {"status": "skipped", "reason": "empty_file"}
            raise ValueError(msg)


        # ✅ JSONL 첫줄 검사
        try:
            with open(raw_dataset_path, "r") as f:
                first_line = next(f, None)
                if not first_line:
                    msg = f"⚠️ 데이터 내용 없음: {raw_dataset_path}"
                    if allow_empty:
                        print(msg + " → allow_empty=True, skip 처리")
                        return {"status": "skipped", "reason": "empty_content"}
                    raise ValueError(msg)
        except Exception as e:
            if allow_empty:
                print(f"⚠️ 파일 읽기 오류 ({e}) → skip 처리")
                return {"status": "skipped", "reason": "read_error"}
            raise

        return self._execute_soda(raw_dataset_path, layer, dag_run_id, task_id)

        # ----------------------------------------------------------------------
        # ✅ Soda 검증 실행 (기존 로직 그대로 별도 함수로 분리)
        # ----------------------------------------------------------------------
    def _execute_soda(self, raw_dataset_path: str, layer: str, dag_run_id: str, task_id: str):

        tmp_config = {
            "data_source my_duckdb": {
                "type": "duckdb",
                "path": ":memory:",
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as tmp_file:
            yaml.dump(tmp_config, tmp_file)
            tmp_config_path = tmp_file.name

        try:
            scan = Scan()
            scan.set_data_source_name("my_duckdb")
            scan.add_configuration_yaml_file(tmp_config_path)
            scan.add_sodacl_yaml_files(self.soda_check_file)

            data_source = scan._data_source_manager.get_data_source("my_duckdb")
            data_source.connection.execute(
                f"CREATE TABLE {self.data_domain} AS SELECT * FROM read_json_auto('{raw_dataset_path}')"
            )

            scan_start_time = datetime.now(timezone.utc)
            exit_code = scan.execute()
            scan_end_time = datetime.now(timezone.utc)

            # 검증 결과 수집
            validation_result = {
                "scan_metadata": {
                    "dataset": self.data_domain,
                    "layer": layer,
                    "source_path": raw_dataset_path,
                    "scan_timestamp": scan_start_time.isoformat(),
                    "scan_duration_seconds": (scan_end_time - scan_start_time).total_seconds(),
                    "exit_code": exit_code,
                    "dag_run_id": dag_run_id,
                    "task_id": task_id,
                },
                "checks": [],
                "errors": [],
                "summary": {
                    "total_checks": 0,
                    "passed": 0,
                    "failed": 0,
                    "warned": 0,
                    "errored": 0,
                },
                "final_status": "pending",
                "log_file": None,
            }

            has_failures = False
            has_errors = False

            if hasattr(scan, '_checks'):
                for check in scan._checks:
                    validation_result["summary"]["total_checks"] += 1

                    # ✅ CheckOutcome enum을 문자열로 변환
                    outcome_value = getattr(check, 'outcome', 'unknown')
                    if hasattr(outcome_value, 'value'):
                        # Enum 타입인 경우 .value로 문자열 추출
                        outcome_str = outcome_value.value
                    else:
                        # 이미 문자열인 경우
                        outcome_str = str(outcome_value)

                    check_info = {
                        "name": getattr(check, 'name', 'Unknown check'),
                        "outcome": outcome_str,  # ✅ 문자열로 저장
                    }

                    if outcome_str == 'pass':
                        validation_result["summary"]["passed"] += 1
                    elif outcome_str == 'fail':
                        validation_result["summary"]["failed"] += 1
                        has_failures = True
                        print(f"  ❌ FAILED: {check_info['name']}")
                    elif outcome_str == 'warn':
                        validation_result["summary"]["warned"] += 1

                    if hasattr(check, 'check_value'):
                        check_value = getattr(check, 'check_value', None)
                        # ✅ check_value도 JSON 직렬화 가능하도록 변환
                        if check_value is not None:
                            try:
                                json.dumps(check_value)  # 직렬화 가능 여부 테스트
                                check_info["check_value"] = check_value
                            except (TypeError, ValueError):
                                check_info["check_value"] = str(check_value)

                    validation_result["checks"].append(check_info)

            if hasattr(scan, '_logs') and hasattr(scan._logs, 'logs'):
                for log in scan._logs.logs:
                    if log.level == 'error':
                        has_errors = True
                        validation_result["summary"]["errored"] += 1
                        error_info = {
                            "level": log.level,
                            "message": log.message,
                        }
                        validation_result["errors"].append(error_info)
                        print(f"  ⚠️ ERROR: {log.message}")

            # ✅ validated 메타데이터 디렉토리 (파티션 없이 최상위에 저장)
            validated_base = os.path.join(DATA_LAKE_ROOT, "validated")
            validated_metadata_dir = os.path.join(
                validated_base,
                "_metadata",
                "validation_logs"
            )
            os.makedirs(validated_metadata_dir, exist_ok=True)

            # 로그 파일명 생성
            timestamp_str = scan_start_time.strftime('%Y%m%d_%H%M%S')
            if dag_run_id:
                safe_dag_run_id = dag_run_id.replace(':', '-').replace('+', '_')
                log_filename = f"{self.data_domain}_{timestamp_str}_{safe_dag_run_id}.json"
            else:
                log_filename = f"{self.data_domain}_{timestamp_str}.json"

            validation_log_file = os.path.join(validated_metadata_dir, log_filename)

            # 실패/에러 판단
            validation_result["final_status"] = "success"
            validation_result["log_file"] = validation_log_file

            if exit_code >= 2 or has_failures:
                validation_result["final_status"] = "failed"
                with open(validation_log_file, 'w', encoding='utf-8') as f:
                    json.dump(validation_result, f, indent=2, ensure_ascii=False)
                print(f"📄 검증 실패 로그 저장: {validation_log_file}")
                raise AssertionError(f"❌ Soda 검증 실패 (exit_code: {exit_code})")

            if exit_code == 3 or has_errors:
                validation_result["final_status"] = "error"
                with open(validation_log_file, 'w', encoding='utf-8') as f:
                    json.dump(validation_result, f, indent=2, ensure_ascii=False)
                print(f"📄 검증 에러 로그 저장: {validation_log_file}")
                raise AssertionError(f"❌ Soda 검증 중 에러 발생 (exit_code: {exit_code})")

            if exit_code == 1:
                validation_result["final_status"] = "warning"
                print(f"⚠️ Soda 검증 경고 있음 (exit_code: {exit_code}) - 계속 진행")

            # 검증 로그 저장
            with open(validation_log_file, 'w', encoding='utf-8') as f:
                json.dump(validation_result, f, indent=2, ensure_ascii=False)
            print(f"📄 검증 로그 저장: {validation_log_file}")
            print(f"✅ Soda 검증 통과 (exit_code: {exit_code})")

            return validation_result

        finally:
            if os.path.exists(tmp_config_path):
                os.unlink(tmp_config_path)

    # ----------------------------------------------------------------------
    # ✅ 5️⃣ 전체 실행
    # ----------------------------------------------------------------------
    def run(self, context: Dict = None, allow_empty: bool = False) -> Dict:
        """Airflow DAG 내에서 실행되는 데이터 검증 실행"""

        airflow_ctx = context.get("context")
        dag_run_id = airflow_ctx.get("run_id")
        task_id = getattr(airflow_ctx.get("task_instance"), "task_id", None)

        print(f"📘 [Validator Context] dag_run_id={dag_run_id}, task_id={task_id}")

        validation_result = self._run_soda(
            layer="raw",
            dag_run_id=dag_run_id,
            task_id=task_id,
            allow_empty=allow_empty,
        )

        # ✅ allow_empty=True 시에도 _last_validated.json은 항상 남긴다.
        if not validation_result or validation_result.get("status") == "skipped":
            print("⚠️ 데이터 없음 또는 skip 처리됨 — 빈 메타데이터 기록 중...")

            # 빈 Parquet 파일도 함께 생성 (schema 유지)
            validated_dir = self._get_lake_path("validated")
            validated_parquet_path = os.path.join(
                self._get_lake_path("validated"),  # vendor-aware 경로 자동 생성
                f"{self.data_domain}.parquet"
            )

            import pyarrow as pa
            import pyarrow.parquet as pq
            empty_table = pa.Table.from_pandas(pd.DataFrame())
            pq.write_table(empty_table, validated_parquet_path)

            print(f"📦 Empty Validated 데이터 저장: {validated_parquet_path}")

            # _last_validated.json 작성
            last_validated_meta = {
                "dataset": self.data_domain,
                "vendor": self.vendor,
                "last_validated_timestamp": datetime.now(timezone.utc).isoformat(),
                "validation_log_file": None,
                "source_file": None,
                "validated_file": validated_parquet_path,
                "dag_run_id": dag_run_id,
                "task_id": task_id,
                "status": "skipped",
                "checks_summary": {
                    "total_checks": 0,
                    "passed": 0,
                    "failed": 0,
                    "warned": 0,
                    "errored": 0
                },
                "record_count": 0,
                "reason": validation_result.get("reason", "empty_file" if validation_result else "unknown")
            }

            last_validated_path = os.path.join(validated_dir, "_last_validated.json")
            with open(last_validated_path, 'w', encoding='utf-8') as f:
                json.dump(last_validated_meta, f, indent=2, ensure_ascii=False)

            print(f"📋 Empty 메타데이터 저장: {last_validated_path}")
            return last_validated_meta

            # --- 검증 통과 시 validated 저장 ---
        self._save_to_validated(validation_result, dag_run_id, task_id)

        # --- row_count 계산 ---
        record_count = 0
        try:
            conn = duckdb.connect()
            parquet_path = os.path.join(
                self._get_lake_path("validated"),
                f"{self.data_domain}.parquet"
            )
            result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()
            record_count = result[0] if result else 0
            conn.close()
        except Exception as e:
            print(f"⚠️ Row count 계산 중 오류 발생: {e}")

        # --- 공통 반환 구조 ---
        return {
            "data_domain": self.data_domain,
            "vendor": self.vendor,
            "exchange_code": self.exchange_code,
            "trd_dt": self.trd_dt,
            "status": validation_result.get("final_status", "success"),
            "record_count": record_count,
            "log_file": validation_result.get("log_file"),
        }

    def _save_to_validated(self, validation_result: Dict, dag_run_id: str = None, task_id: str = None) -> None:
        """검증 통과 데이터를 validated 계층에 Parquet로 저장"""

        raw_dataset_path = validation_result["scan_metadata"]["source_path"]

        # ✅ validated 계층 경로 (파티션 포함)
        validated_parquet_path = os.path.join(
            self._get_lake_path("validated"),
            f"{self.data_domain}.parquet"
        )

        # ✅ 1️⃣ Raw 단계의 원천 메타파일 경로 탐색
        raw_dir = os.path.dirname(raw_dataset_path)
        source_meta_path = os.path.join(raw_dir, "_source_meta.json")
        source_meta_data = None

        if os.path.exists(source_meta_path):
            try:
                with open(source_meta_path, "r", encoding="utf-8") as f:
                    source_meta_data = json.load(f)
            except Exception as e:
                print(f"⚠️ _source_meta.json 로드 실패: {e}")

        # ✅ JSONL → Parquet 변환 (DuckDB 사용)
        conn = duckdb.connect(':memory:')
        conn.execute(f"""
            COPY (SELECT * FROM read_json_auto('{raw_dataset_path}'))
            TO '{validated_parquet_path}' (FORMAT PARQUET)
        """)
        conn.close()

        print(f"📦 Validated 데이터 저장: {validated_parquet_path}")

        # _last_validated.json 메타데이터 저장
        last_validated_meta = {
            "dataset": self.data_domain,
            "last_validated_timestamp": validation_result["scan_metadata"]["scan_timestamp"],
            "validation_log_file": os.path.basename(validation_result.get("log_file", "")),
            "source_file": raw_dataset_path,
            "validated_file": validated_parquet_path,
            "dag_run_id": dag_run_id,
            "task_id": task_id,
            "status": validation_result["final_status"],
            "checks_summary": validation_result["summary"]
        }

        # ✅ 4️⃣ source_meta 병합
        if source_meta_data:
            last_validated_meta["source_meta"] = source_meta_data
            print("🔗 원천 메타데이터 병합 완료 (_source_meta.json → _last_validated.json)")
        else:
            print("⚠️ 원천 메타데이터 파일(_source_meta.json)이 없어 병합을 건너뜀")

        validated_dir = self._get_lake_path("validated")
        last_validated_path = os.path.join(validated_dir, "_last_validated.json")
        with open(last_validated_path, 'w', encoding='utf-8') as f:
            json.dump(last_validated_meta, f, indent=2, ensure_ascii=False)

        print(f"📋 메타데이터 저장: {last_validated_path}")

    # ✅  Soda 체크파일 경로 자동 추론
    def _get_soda_check_path(self) -> Optional[str]:
        """
        data_domain 기반으로 Soda 체크파일 경로를 자동 탐색합니다.
        예: equity_prices → /opt/airflow/plugins/soda/checks/prices_checks.yml
        """
        base_dir = "/opt/airflow/plugins/soda/checks"
        file_name = f"{self.data_domain}_checks.yml"
        file_path = os.path.join(base_dir, file_name)

        assert os.path.exists(file_path), f"⚠️ Soda 체크파일이 존재하지 않습니다: {file_path}"

        return file_path



    def _format_soda_result(
        self,
        scan,
        layer: str,
        dag_run_id: str,
        task_id: str,
        scan_start,
        scan_end,
        exit_code: int,
    ):
        """Soda Scan 결과를 표준 포맷(dict)으로 정리"""
        has_failures = False
        has_errors = False

        summary = {
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "warned": 0,
            "errored": 0,
        }
        checks = []
        errors = []

        # ✅ 체크 결과 집계
        if hasattr(scan, "_checks"):
            for c in scan._checks:
                summary["total_checks"] += 1
                outcome_val = getattr(c, "outcome", "unknown")
                outcome = outcome_val.value if hasattr(outcome_val, "value") else str(outcome_val)

                if outcome == "pass":
                    summary["passed"] += 1
                elif outcome == "fail":
                    summary["failed"] += 1
                    has_failures = True
                elif outcome == "warn":
                    summary["warned"] += 1

                checks.append({
                    "name": getattr(c, "name", "unknown"),
                    "outcome": outcome,
                })

        # ✅ 로그 오류 집계
        if hasattr(scan, "_logs") and hasattr(scan._logs, "logs"):
            for lg in scan._logs.logs:
                if lg.level == "error":
                    has_errors = True
                    summary["errored"] += 1
                    errors.append({"level": lg.level, "message": lg.message})

        # ✅ 최종 상태 결정
        final_status = "success"
        if exit_code >= 2 or has_failures:
            final_status = "failed"
        elif exit_code == 3 or has_errors:
            final_status = "error"
        elif exit_code == 1:
            final_status = "warning"

        # ✅ 로그 파일 경로 생성
        validated_base = os.path.join(DATA_LAKE_ROOT, "data_lake", "validated")
        log_dir = os.path.join(validated_base, "_metadata", "validation_logs")
        os.makedirs(log_dir, exist_ok=True)

        ts = scan_start.strftime("%Y%m%d_%H%M%S")
        safe_run_id = (dag_run_id or "").replace(":", "-").replace("+", "_")
        log_name = f"{self.data_domain}_{ts}_{safe_run_id}.json" if safe_run_id else f"{self.data_domain}_{ts}.json"
        log_path = os.path.join(log_dir, log_name)

        # ✅ 표준 반환 dict
        result = {
            "scan_metadata": {
                "dataset": self.data_domain,
                "layer": layer,
                "scan_timestamp": scan_start.isoformat(),
                "scan_duration_seconds": (scan_end - scan_start).total_seconds(),
                "exit_code": exit_code,
                "dag_run_id": dag_run_id,
                "task_id": task_id,
            },
            "checks": checks,
            "errors": errors,
            "summary": summary,
            "final_status": final_status,
            "log_file": log_path,
        }

        # ✅ 로그 파일 저장
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        print(f"📋 Soda 검증 로그 저장: {log_path}")

        return result