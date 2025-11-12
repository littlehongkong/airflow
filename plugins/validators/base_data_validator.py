"""
Base Data Validator (Layer Unified with Vendor Support)
--------------------------------------------------------
✅ 기능 요약
1️⃣ Lake / Warehouse / Mart 공통 Pandera + Soda Core(DuckDB) 검증 엔진
2️⃣ 모든 경로는 DataPathResolver 기반 (constants.py 단순화)
3️⃣ 검증 실패 시 Airflow Task 실패 + validated 경로에 메타파일 저장
"""

import os, yaml, tempfile, gc, json, logging, duckdb
import pandas as pd
import pandera.pandas as pa
from pandera import DataFrameSchema, Column
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from soda.scan import Scan
from filelock import FileLock
from plugins.config import constants as C
from plugins.config.constants import WAREHOUSE_DOMAINS
from plugins.utils.path_manager import DataPathResolver  # ✅ 새 경로 관리 유틸

class BaseDataValidator:
    def __init__(
        self,
        domain: str,
        layer: str,
        trd_dt: Optional[str] = None,
        vendor: Optional[str] = "eodhd",
        exchange_code: Optional[str] = None,
        country_code: Optional[str] = None,
        allow_empty: bool = False,
        domain_group: Optional[str] = None,
        **kwargs,
    ):
        self.domain = domain
        self.domain_group = domain_group or "equity"
        self.layer = layer.lower()
        self.trd_dt = trd_dt
        self.vendor = vendor
        self.exchange_code = exchange_code or "ALL"
        self.country_code = country_code
        self.allow_empty = allow_empty
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # ---------------------------------------------------------------------
        # ✅ 경로 자동 구성 (DataPathResolver 사용)
        # ---------------------------------------------------------------------
        if self.layer == "lake":
            self.data_root = C.DATA_LAKE_ROOT
            self.schema_root = C.VALIDATOR_SCHEMA_LAKE / self.domain_group / vendor.lower()
            self.check_root = C.VALIDATOR_CHECKS_LAKE / self.domain_group / vendor.lower()

            # 원천(raw) 데이터 읽기용
            self.dataset_path = DataPathResolver.lake_raw(
                self.domain_group, self.domain, self.vendor, self.exchange_code, self.trd_dt
            )

            # 검증 결과(validated) 저장용
            self.validated_dir = DataPathResolver.lake_validated(
                self.domain_group, self.domain, self.vendor, self.exchange_code, self.trd_dt
            )

        elif self.layer == "warehouse":
            self.data_root = C.DATA_WAREHOUSE_ROOT
            self.schema_root = C.VALIDATOR_SCHEMA_WAREHOUSE / self.domain_group
            self.check_root = C.VALIDATOR_CHECKS_WAREHOUSE / self.domain_group

            # Warehouse 스냅샷 읽기용
            self.dataset_path = DataPathResolver.warehouse_snapshot(
                self.domain_group, self.domain, self.country_code, self.trd_dt
            )

            # 검증 결과(validated) 저장용
            self.validated_dir = DataPathResolver.warehouse_validated(
                self.domain_group, self.domain, self.country_code, self.trd_dt
            )

        else:
            raise ValueError(f"Unsupported layer type: {self.layer}")

        self.log.info(f"📦 Validator dataset path: {self.dataset_path}")
        self.log.info(f"📁 Validation results will be saved in: {self.validated_dir}")


    def _aggregate_status(self, checks: Dict[str, Any]) -> str:
        if not checks:
            return "skipped"
        if any(not c.get("passed", False) for c in checks.values()):
            return "failed"
        return "success"

    # -------------------------------------------------------------------------
    # 1️⃣ Main Validation
    # -------------------------------------------------------------------------
    def validate(self, context: Optional[dict] = None) -> Dict[str, Any]:
        df = self._load_dataset()

        # ✅ 데이터 없을 경우 처리
        if df.empty:
            if self.allow_empty:
                result = {
                    "dataset": self.domain,
                    "layer": self.layer,
                    "vendor": self.vendor,
                    "exchange_code": self.exchange_code,
                    "country_code": self.country_code,
                    "trd_dt": self.trd_dt,
                    "status": "skipped",
                    "record_count": 0,
                    "checks": {},
                    "validated_source": str(self.dataset_path),
                    "validated_at": datetime.now(timezone.utc).isoformat(),
                    "message": "No data found (allow_empty=True)",
                }
                validated_dir = self._save_result(result, df)
                self.log.info(f"🧾 Skipped validation saved: {validated_dir}")
                return result
            else:
                raise ValueError(f"❌ No data found for {self.domain} (allow_empty=False)")

        # ✅ Pandera + Soda 검증 수행
        checks = self._define_checks(df)
        status = self._aggregate_status(checks)

        result = {
            "dataset": self.domain,
            "layer": self.layer,
            "vendor": self.vendor,
            "country_code": self.country_code,
            "exchange_code": self.exchange_code,
            "trd_dt": self.trd_dt,
            "status": status,
            "record_count": len(df),
            "checks": checks,
            "validated_source": str(self.dataset_path),
            "validated_at": datetime.now(timezone.utc).isoformat(),
        }

        # ✅ 결과 저장
        validated_dir = self._save_result(result, df)
        if status != "success":
            raise ValueError(f"❌ Validation failed — see {validated_dir}/_last_validated.json")

        # ✅ 메타 갱신
        self._update_latest_snapshot_meta(self.domain, self.trd_dt, str(self.dataset_path))
        self.log.info(f"✅ Validation SUCCESS — saved to {validated_dir}")
        return result

    # -------------------------------------------------------------------------
    # 2️⃣ Dataset Load
    # -------------------------------------------------------------------------
    def _load_dataset(self) -> pd.DataFrame:
        if not self.dataset_path.exists():
            self.log.warning(f"⚠️ Dataset path not found: {self.dataset_path}")
            return pd.DataFrame()

        if self.dataset_path.is_dir():
            json_files = [
                f
                for f in list(self.dataset_path.glob("*.json")) + list(self.dataset_path.glob("*.jsonl"))
                if not f.name.startswith("_")  # ✅ 메타 파일 제외
            ]

            if not json_files:
                self.log.warning(f"⚠️ No JSON files in {self.dataset_path}")
                raise

            dfs = []
            for f in json_files:
                try:
                    if f.suffix == ".jsonl":
                        df_flat = pd.read_json(f, lines=True, dtype=False)
                    else:
                        data = json.load(open(f, "r", encoding="utf-8"))
                        if isinstance(data, list):
                            df_flat = pd.json_normalize(data, sep="_")
                        else:
                            df_flat = pd.json_normalize([data], sep="_")

                    # ✅ DataFrame 존재 여부는 empty로 체크해야 함
                    if df_flat is not None and not df_flat.empty:
                        df_flat = df_flat.dropna(how="all").reset_index(drop=True)
                        dfs.append(df_flat)
                    else:
                        self.log.warning(f"⚠️ {f.name} has no valid rows")

                except Exception as e:
                    self.log.warning(f"⚠️ Failed to load {f.name}: {e}")

            if not dfs:
                self.log.warning(f"⚠️ No valid dataframes loaded from {self.dataset_path}")
                return pd.DataFrame()

            combined = pd.concat(dfs, ignore_index=True)
            self.log.info(f"✅ Loaded {len(combined)} rows after filtering invalid JSON files")
            return combined

        # ✅ 단일 parquet 파일 처리
        if self.dataset_path.suffix.lower() == ".parquet":
            df = pd.read_parquet(self.dataset_path)
            df = df.dropna(how="all").reset_index(drop=True)
            return df

        raise ValueError(f"❌ Unsupported file type: {self.dataset_path.suffix}")

    # -------------------------------------------------------------------------
    # 3️⃣ Validation Logic (Pandera + Soda)
    # -------------------------------------------------------------------------
    def _define_checks(self, df: pd.DataFrame) -> Dict[str, Any]:
        checks = {}

        schema_path = self.schema_root / f"{self.domain}.json"
        soda_path = self.check_root / f"{self.domain}.yml"

        if self.layer == 'warehouse':
            schema_path = self.schema_root / f"{WAREHOUSE_DOMAINS[self.domain]}.json"
            soda_path = self.check_root / f"{WAREHOUSE_DOMAINS[self.domain]}.yml"

        # Pandera
        if schema_path.exists():
            with open(schema_path, "r", encoding="utf-8") as f:
                schema_def = json.load(f)
            checks["pandera"] = self._validate_with_pandera(df, schema_def)
        else:
            self.log.warning(f"⚠️ Pandera schema not found: {schema_path}")

        # Soda
        if soda_path.exists():
            checks["soda_core"] = self._run_soda_duckdb_validation(df, soda_path)
        else:
            self.log.warning(f"⚠️ Soda YAML not found: {soda_path}")

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
    # 5️⃣ Soda Validation
    # -------------------------------------------------------------------------
    def _run_soda_duckdb_validation(self, df: pd.DataFrame, soda_path: Path) -> Dict[str, Any]:
        checks = {}
        db_path = None
        tmp_config_path = None
        scan = Scan()

        try:
            # ✅ 데이터가 비어있으면 Soda 스킵
            if df is None or df.empty:
                self.log.warning("⚠️ Skip Soda validation — dataframe is empty.")
                return {"passed": True, "message": "No data to validate (skipped)"}

            # ✅ 임시 DuckDB 파일 생성
            with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as db_file:
                db_path = db_file.name
            if os.path.exists(db_path):
                os.unlink(db_path)

            con = duckdb.connect(database=db_path)

            # ✅ Pandas DataFrame을 DuckDB 테이블로 안전하게 등록
            con.register("df_view", df)
            con.execute(f"CREATE TABLE {self.domain} AS SELECT * FROM df_view")
            con.unregister("df_view")
            con.close()

            # ✅ Soda 설정 파일 임시 생성
            tmp_config = {"data_source my_duckdb": {"type": "duckdb", "path": db_path}}
            with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as tmp_file:
                yaml.dump(tmp_config, tmp_file)
                tmp_config_path = tmp_file.name

            # ✅ Soda 실행
            scan.set_data_source_name("my_duckdb")
            scan.add_configuration_yaml_file(tmp_config_path)
            scan.add_sodacl_yaml_files(str(soda_path))

            start = datetime.now(timezone.utc)
            exit_code = scan.execute()
            end = datetime.now(timezone.utc)

            checks["passed"] = exit_code == 0
            checks["message"] = "All Soda rules passed" if exit_code == 0 else "Some Soda rules failed"
            checks["execution_time"] = (end - start).total_seconds()

        except Exception as e:
            msg = f"Soda validation error: {e}"
            self.log.error(msg)
            checks = {"passed": False, "message": msg}

        finally:
            for p in [tmp_config_path, db_path, f"{db_path}.wal"]:
                if p and os.path.exists(p):
                    os.unlink(p)
            del scan
            gc.collect()

        return checks

    # -------------------------------------------------------------------------
    # 6️⃣ 결과 저장
    # -------------------------------------------------------------------------
    def _save_result(self, result: dict, df: pd.DataFrame) -> Path:
        self.validated_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(self.validated_dir / f"{self.domain}.parquet", index=False)
        with open(self.validated_dir / "_last_validated.json", "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        self.log.info(f"✅ Validation results saved → {self.validated_dir}")
        return self.validated_dir

    # -------------------------------------------------------------------------
    # 7️⃣ Meta update
    # -------------------------------------------------------------------------
    def _update_latest_snapshot_meta(self, domain: str, trd_dt: str, meta_file: str):
        meta_path = C.LATEST_VALIDATED_META_PATH if self.layer == "lake" else C.LATEST_SNAPSHOT_META_PATH
        lock_path = C.LATEST_VALIDATED_META_LOCK if self.layer == "lake" else C.LATEST_SNAPSHOT_META_LOCK
        meta_path.parent.mkdir(parents=True, exist_ok=True)

        with FileLock(str(lock_path)):
            latest_meta = json.load(open(meta_path, "r")) if meta_path.exists() else {}
            prev_info = latest_meta.get(domain)
            prev_dt = prev_info.get("latest_trd_dt") if prev_info else None
            if (not prev_dt) or (trd_dt > prev_dt):
                latest_meta[domain] = {"latest_trd_dt": trd_dt, "meta_file": meta_file}
                json.dump(latest_meta, open(meta_path, "w"), indent=2, ensure_ascii=False)
                self.log.info(f"🧭 Updated latest meta for {domain}: {trd_dt}")
            else:
                self.log.info(f"ℹ️ Skipped meta update for {domain} (existing={prev_dt}, new={trd_dt})")
