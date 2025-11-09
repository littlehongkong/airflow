"""
Base Data Validator (Layer Unified with Vendor Support)
--------------------------------------------------------
✅ 기능 요약
1️⃣ Lake / Warehouse / Mart 공통 Pandera + Soda Core(DuckDB) 검증 엔진
2️⃣ 모든 경로는 constants.py 기반 (vendor 포함)
3️⃣ 검증 실패 시 Airflow Task 실패 + validated 경로에 메타파일 저장
"""

import os, yaml, tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import pandas as pd
import pandera.pandas as pa
from pandera import DataFrameSchema, Column
from soda.scan import Scan
from filelock import FileLock
import logging
import gc
import json
import psutil
import duckdb
import re
# ✅ import 경로를 현재 구조에 맞게 수정
from plugins.config import constants as C



class BaseDataValidator:
    def __init__(
        self,
        domain: str,
        layer: str,
        trd_dt: Optional[str] = None,
        dataset_path: Optional[str] = None,
        vendor: Optional[str] = "eodhd",
        exchange_code: Optional[str] = "ALL",
        allow_empty: bool = False,
        domain_group: Optional[str] = None,
        **kwargs,
    ):
        self.domain = domain
        self.domain_group = domain_group
        self.layer = layer.lower()
        self.trd_dt = trd_dt
        self.vendor = vendor
        self.exchange_code = exchange_code
        self.allow_empty = allow_empty
        self.dataset_path = Path(dataset_path)
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # ✅ constants 기반 경로 설정
        if self.layer == "lake":
            self.schema_root = C.VALIDATOR_SCHEMA_LAKE / domain_group / vendor.lower()
            self.check_root = C.VALIDATOR_CHECKS_LAKE / domain_group / vendor.lower()
            self.data_root = C.DATA_LAKE_ROOT

        elif self.layer == "warehouse":
            # ✅ equity 도메인 폴더를 포함하도록 수정
            self.schema_root = C.VALIDATOR_SCHEMA_WAREHOUSE / domain_group
            self.check_root = C.VALIDATOR_CHECKS_WAREHOUSE / domain_group
            self.data_root = C.DATA_WAREHOUSE_ROOT
        #
        # else:
        #     # ✅ mart 도 동일하게 도메인 단위 폴더 추가
        #     self.schema_root = VALIDATOR_SCHEMA_MART / domain_group
        #     self.check_root = VALIDATOR_CHECKS_MART / domain_group
        #     self.data_root = DATA_MART_ROOT
    # -------------------------------------------------------------------------
    # 1️⃣ Main Validation
    # -------------------------------------------------------------------------
    def validate(self, context: Optional[dict] = None) -> Dict[str, Any]:
        df = self._load_dataset()

        if df.empty:
            if self.allow_empty:
                print(f"✅ No data found for {self.domain} (allow_empty=True) → SKIP validation.")

                result = {
                    "dataset": self.domain,
                    "layer": self.layer,
                    "vendor": self.vendor,
                    "exchange_code": getattr(self, "exchange_code", None),
                    "trd_dt": self.trd_dt,
                    "status": "skipped",
                    "record_count": 0,
                    "checks": {},
                    "validated_source": str(self.dataset_path),
                    "validated_at": datetime.now(timezone.utc).isoformat(),
                    "message": "No data found (allow_empty=True)",
                }

                validated_dir = self._save_result(result, df)
                print(f"🧾 Skipped validation result saved: {validated_dir}")
                return result

            else:
                raise ValueError(f"❌ No data found for {self.domain} (allow_empty=False)")

        checks = self._define_checks(df)
        status = self._aggregate_status(checks)

        result = {
            "dataset": self.domain,
            "layer": self.layer,
            "vendor": self.vendor,
            "trd_dt": self.trd_dt,
            "status": status,
            "record_count": len(df),
            "checks": checks,
            "validated_source": str(self.dataset_path),
            "validated_at": datetime.now(timezone.utc).isoformat(),
        }

        validated_dir = self._save_result(result, df)
        if status != "success":
            raise ValueError(f"❌ Validation failed — see {validated_dir}/_last_validated.json")

        meta_file_path = str(self.dataset_path)
        self._update_latest_snapshot_meta(self.domain, self.trd_dt, meta_file_path)

        print(f"✅ Validation SUCCESS — saved to {validated_dir}")
        return result

    # -------------------------------------------------------------------------
    # 2️⃣ Load Dataset
    # -------------------------------------------------------------------------
    def _load_dataset(self) -> pd.DataFrame:
        """
        ✅ 데이터셋 로더 (대용량 안전 버전)
        - fundamentals: 파일별 스트리밍 검증 지원
        - 기타: parquet/jsonl 단일 파일 로드
        """


        if self.dataset_path.is_dir():
            files = list(self.dataset_path.glob("*.json"))
            if not files:
                raise FileNotFoundError(f"❌ {self.dataset_path} 내에 JSON 파일이 없습니다.")

            dfs = []
            for f in files:
                try:
                    # ✅ JSON 읽기
                    with open(f, "r", encoding="utf-8") as fp:
                        data = json.load(fp)

                    # ✅ flatten 처리 - sep='_' 사용!
                    df_flat = pd.json_normalize(data, sep='_')

                    dfs.append(df_flat)
                except Exception as e:
                    self.log.warning(f"⚠️ {f.name} 로드 실패: {e}")
                    continue

            combined = pd.concat(dfs, ignore_index=True)

            # ✅ 혹시 모를 . 제거 (2차 안전장치)
            combined.columns = [c.replace(".", "_") for c in combined.columns]

            # ✅ 검증
            assert all("." not in col for col in combined.columns), \
                f"❌ 컬럼명에 여전히 . 존재: {[c for c in combined.columns if '.' in c][:5]}"

            self.log.info(f"🔍 변환된 컬럼 샘플: {list(combined.columns[:10])}")
            self.log.info(f"✅ Flatten 완료: {len(combined):,}행, {len(combined.columns)}열")

            return combined


        # ✅ 단일 파일 처리
        if self.dataset_path is None or not self.dataset_path.exists():
            return pd.DataFrame()

        ext = self.dataset_path.suffix.lower()
        if ext == ".parquet":
            return pd.read_parquet(self.dataset_path)
        elif ext in [".json", ".jsonl"]:
            return duckdb.query(f"SELECT * FROM read_json_auto('{self.dataset_path}')").to_df()
        else:
            raise ValueError(f"❌ Unsupported file format: {ext}")

    # -------------------------------------------------------------------------
    # 3️⃣ Pandera + Soda Core Validation
    # -------------------------------------------------------------------------
    def _define_checks(self, df: pd.DataFrame) -> Dict[str, Any]:
        checks = {}

        # ✅ numeric 컬럼 타입 보정 (Pandera Float 대응)
        for col in ["active_tickers", "previous_day_updated_tickers", "updated_tickers"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # ✅ Pandera 검증
        self.log.info("Pandera 검증 시작")
        schema_path = self.schema_root / f"{self.domain}.json"
        if schema_path.exists():
            with open(schema_path, "r", encoding="utf-8") as f:
                schema_def = json.load(f)
            checks["pandera_schema"] = self._validate_with_pandera(df, schema_def)
            self.log.info(f"Pandera 검증 완료 : {checks['pandera_schema']}")
        else:
            print(f"⚠️ Pandera schema not found: {schema_path}")

        # ✅ Soda Core 검증 (DuckDB)
        if self.domain == "fundamentals":
            # ✅ Type 필드 탐색 (General.Type or General.Type.value)
            type_col = None
            for cand in ["General.Type", "Type", "General.Type.value"]:
                if cand in df.columns:
                    type_col = cand
                    break

            # 기본값 Common Stock
            fund_type = "stock"
            if type_col and "ETF" in df[type_col].astype(str).unique():
                fund_type = "etf"

            soda_filename = f"{self.domain}_{fund_type}.yml"
            soda_path = self.check_root / soda_filename

            if soda_path.exists():
                print(f"✅ Using Soda checks: {soda_path.name}")

                # print(df.columns)
                # print(df.head())

                checks.update(self._run_soda_duckdb_validation(df, soda_path))
            else:
                print(f"⚠️ Soda check file not found: {soda_path}")

        else:
            # ✅ 일반 도메인 처리
            soda_path = self.check_root / f"{self.domain}.yml"
            if soda_path.exists():
                checks.update(self._run_soda_duckdb_validation(df, soda_path))
            else:
                print(f"⚠️ Soda check file not found: {soda_path}")

        return checks

    # -------------------------------------------------------------------------
    # 4️⃣ Pandera Validation
    # -------------------------------------------------------------------------
    def _validate_with_pandera(self, df: pd.DataFrame, schema_def: dict) -> Dict[str, Any]:
        # 1) 스키마 → pandera dtype 매핑
        type_map = {
            "String": pa.String,
            "Int": pa.Int,
            "Float": pa.Float,
            "DateTime": pa.DateTime,
            "Datetime": pa.DateTime,
            "Bool": pa.Bool,
        }

        try:
            # 2) 필수 컬럼 확인
            defined_cols = [c["name"] for c in schema_def.get("columns", [])]
            missing_cols = [c for c in defined_cols if c not in df.columns]
            if missing_cols:
                raise ValueError(f"❌ Missing required columns in dataset: {missing_cols}")

            # 3) 사전 정규화: 공백→NaN, 문자열 트림
            for c in df.columns:
                if df[c].dtype == object:
                    df[c] = df[c].apply(lambda x: x.strip() if isinstance(x, str) else x)
                    df[c] = df[c].replace({"": None})

            # 4) 스키마 기반 타입 강제(coerce)
            #    - 숫자/날짜가 문자열이어도 올바른 dtype으로 변환
            for col_def in schema_def.get("columns", []):
                name = col_def["name"]
                typ = (col_def.get("type") or "String").lower()
                if name not in df.columns:
                    continue
                try:
                    if typ in ("float",):
                        df[name] = pd.to_numeric(df[name], errors="coerce")
                    elif typ in ("int",):
                        df[name] = pd.to_numeric(df[name], errors="coerce").astype("Int64")
                    elif typ in ("datetime", "datetime"):
                        # 날짜는 표준 YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS 모두 허용
                        df[name] = pd.to_datetime(df[name], errors="coerce", utc=False)
                    elif typ in ("bool",):
                        # "true"/"false"/1/0 등 문자열도 처리
                        df[name] = df[name].astype(str).str.lower().map(
                            {"true": True, "false": False, "1": True, "0": False}
                        ).astype("boolean")
                    else:
                        df[name] = df[name].astype("string")
                except Exception as e:
                    self.log.warning(f"⚠️ Coercion failed for {name}({typ}): {e}")

            # 5) pandera Column 정의 (nullable 반영)
            columns = {
                c["name"]: Column(
                    type_map.get(c["type"], pa.String),
                    nullable=c.get("nullable", True),
                    coerce=True,  # 컬럼 단위 coerce
                )
                for c in schema_def.get("columns", [])
            }

            # 6) 스키마 검증 (스키마 레벨 coerce 추가)
            schema = DataFrameSchema(columns, coerce=True)
            schema.validate(df, lazy=True)

            # 7) 추가 constraints (패턴/NOT NULL 등)
            cons = schema_def.get("constraints", {})
            if "patterns" in cons:
                for col, pattern in cons["patterns"].items():
                    if col in df.columns:
                        invalid_mask = df[col].dropna().astype(str).str.match(pattern) == False
                        if invalid_mask.any():
                            bad = df.loc[invalid_mask, col].head(5).tolist()
                            raise ValueError(f"❌ Pattern mismatch in '{col}': samples={bad}")

            if "non_nullable" in cons:
                for col in cons["non_nullable"]:
                    if col in df.columns and df[col].isna().any():
                        cnt = int(df[col].isna().sum())
                        raise ValueError(f"❌ Null values found in non-nullable column '{col}' (rows={cnt})")

            return {"passed": True, "message": "Pandera schema validation passed"}

        except Exception as e:
            # 더 풍부한 디버깅 정보를 로그에 남김
            self.log.error(f"[Pandera] {e}")
            # 실패 컬럼 위주로 간단 프로파일
            try:
                debug_cols = [c["name"] for c in schema_def.get("columns", [])]
                preview = df[debug_cols].head(3).to_dict(orient="records")
                self.log.error(f"[Pandera] Sample rows: {preview}")
            except Exception:
                pass
            return {"passed": False, "message": str(e)}

    # -------------------------------------------------------------------------
    # 5️⃣ Soda Core Validation (DuckDB)
    # -------------------------------------------------------------------------
    def _run_soda_duckdb_validation(self, df: pd.DataFrame, soda_path: Path) -> Dict[str, Any]:

        checks = {}
        db_path = None
        tmp_config_path = None
        scan = Scan()
        try:

            # 🔍 디버깅: DuckDB 등록 전 컬럼명 확인
            # self.log.info("=" * 80)
            # self.log.info(f"🔍 DuckDB 등록 직전 컬럼명 (처음 10개):")
            # self.log.info(df.columns.tolist()[:10])
            # self.log.info(f"🔍 General 관련 컬럼:")
            # self.log.info([c for c in df.columns if 'General' in c][:10])
            # self.log.info("=" * 80)

            with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as db_file:
                db_path = db_file.name
            if os.path.exists(db_path):
                os.unlink(db_path)

            con = duckdb.connect(database=db_path)
            con.execute(f"CREATE TABLE {self.domain} AS SELECT * FROM df")
            con.close()

            del con

            tmp_config = {
                "data_source my_duckdb": {"type": "duckdb", "path": db_path}
            }

            with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as tmp_file:
                yaml.dump(tmp_config, tmp_file)
                tmp_config_path = tmp_file.name

            scan.set_data_source_name("my_duckdb")
            scan.add_configuration_yaml_file(tmp_config_path)
            scan.add_sodacl_yaml_files(str(soda_path))

            scan_start = datetime.now(timezone.utc)
            exit_code = scan.execute()
            scan_end = datetime.now(timezone.utc)

            checks["soda_core"] = {
                "passed": exit_code == 0,
                "message": "All Soda rules passed" if exit_code == 0 else "Some Soda rules failed",
                "execution_time": (scan_end - scan_start).total_seconds(),
            }

        except Exception as e:
            msg = f"Soda Core validation error: {e}"
            checks["soda_core"] = {"passed": False, "message": msg}
            raise RuntimeError(msg)


        finally:
            for path in [tmp_config_path, db_path, f"{db_path}.wal"]:
                if path and os.path.exists(path):
                    os.unlink(path)
            del scan
            gc.collect()

        return checks

    def _flatten_json_column(self, df: pd.DataFrame) -> pd.DataFrame:
        dict_cols = [c for c in df.columns if df[c].apply(lambda x: isinstance(x, dict)).any()]
        for col in dict_cols:
            expanded = pd.json_normalize(df[col])
            expanded.columns = [f"{col}.{sub}" for sub in expanded.columns]
            df = pd.concat([df.drop(columns=[col]), expanded], axis=1)
        # 🔁 recursive flatten
        if any(df[c].apply(lambda x: isinstance(x, dict)).any() for c in df.columns):
            return self._flatten_json_column(df)
        return df

    # -------------------------------------------------------------------------
    # 6️⃣ Save Validation Result
    # -------------------------------------------------------------------------
    def _save_result(self, result: dict, df: pd.DataFrame) -> Path:
        """
        ✅ 모든 도메인 공통 저장 로직
        - 기본적으로 parquet 저장
        - validated 디렉토리 구조: data_root/validated/domain_group/domain/vendor=.../exchange_code=.../trd_dt=...
        """

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

        # ✅ parquet 저장
        parquet_path = validated_dir / f"{self.domain}.parquet"
        df.to_parquet(parquet_path, index=False)
        self.log.info(f"✅ Parquet 저장 완료: {parquet_path} ({len(df):,}행)")

        # ✅ _last_validated.json 생성
        meta_path = validated_dir / "_last_validated.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        return validated_dir

    # -------------------------------------------------------------------------
    # 7️⃣ Utility
    # -------------------------------------------------------------------------
    def _aggregate_status(self, checks: Dict[str, Any]) -> str:
        if not checks:
            return "skipped"
        if any(not c.get("passed", False) for c in checks.values()):
            return "failed"
        return "success"


    # -------------------------------------------------------------------------
    # ✅ 최신 snapshot 메타 업데이트
    # -------------------------------------------------------------------------
    def _update_latest_snapshot_meta(self, domain: str, trd_dt: str, meta_file: str):
        """
        ✅ 최신 메타파일 업데이트 (Lake / Warehouse 모두 지원)
        - backfill 고려: 기존 날짜보다 최신일 경우만 갱신
        - 여러 프로세스 접근 시 file lock으로 동시성 제어
        """

        # ✅ layer 구분: lake → LATEST_VALIDATED_META / warehouse → LATEST_SNAPSHOT_META
        if getattr(self, "layer", "lake") == "lake":
            meta_path = C.LATEST_VALIDATED_META_PATH
            lock_path = C.LATEST_VALIDATED_META_LOCK
        else:
            meta_path = C.LATEST_SNAPSHOT_META_PATH
            lock_path = C.LATEST_SNAPSHOT_META_LOCK

        meta_path.parent.mkdir(parents=True, exist_ok=True)

        with FileLock(str(lock_path)):
            if meta_path.exists():
                try:
                    with open(meta_path, "r", encoding="utf-8") as f:
                        latest_meta = json.load(f)
                except json.JSONDecodeError:
                    latest_meta = {}
            else:
                latest_meta = {}

            prev_info = latest_meta.get(domain)
            prev_dt = prev_info.get("latest_trd_dt") if prev_info else None

            if (not prev_dt) or (trd_dt > prev_dt):
                latest_meta[domain] = {
                    "latest_trd_dt": trd_dt,
                    "meta_file": meta_file
                }
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(latest_meta, f, indent=2, ensure_ascii=False)
                print(f"🧭 [UPDATED] latest_meta ({self.layer}): {domain} → {trd_dt}")
            else:
                print(f"ℹ️ Skipped meta update for {domain} ({self.layer}) (existing={prev_dt}, new={trd_dt})")
