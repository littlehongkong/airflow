# plugins/utils/loaders/base_loader.py
from pathlib import Path
import duckdb
import pandas as pd
import logging
import json
from plugins.config import constants as C

log = logging.getLogger(__name__)

def read_parquet_dir(base_path: Path, pattern: str = "*.parquet", union_by_name: bool = True) -> pd.DataFrame:
    """📦 지정 경로의 parquet 데이터를 DuckDB로 읽어 DataFrame으로 반환"""
    if not base_path.exists():
        raise FileNotFoundError(f"❌ Directory not found: {base_path}")
    files = list(base_path.rglob(pattern))
    if not files:
        raise FileNotFoundError(f"❌ No {pattern} files found in {base_path}")

    conn = duckdb.connect(database=":memory:")
    query = f"SELECT * FROM read_parquet('{base_path}/{pattern}', union_by_name={str(union_by_name).lower()})"
    df = conn.execute(query).df()
    conn.close()

    log.info(f"📊 Loaded {len(df):,} rows from {base_path}")
    return df


def read_json_dir(base_path: Path, pattern: str = "*.json") -> pd.DataFrame:
    """📦 지정 경로의 JSON 데이터를 자동 스키마로 읽어 DataFrame으로 반환"""
    if not base_path.exists():
        raise FileNotFoundError(f"❌ Directory not found: {base_path}")
    files = list(base_path.rglob(pattern))
    if not files:
        raise FileNotFoundError(f"❌ No JSON files found in {base_path}")

    conn = duckdb.connect(database=":memory:")
    query = f"SELECT * FROM read_json_auto('{base_path}/{pattern}')"
    df = conn.execute(query).df()
    conn.close()

    log.info(f"📊 Loaded {len(df):,} JSON rows from {base_path}")
    return df


def latest_partition(base_dir: Path) -> Path:
    print("🔍 DEBUG base_dir:", base_dir, type(base_dir))
    candidates = sorted(base_dir.glob("trd_dt=*"), reverse=True)
    print("🔍 DEBUG candidates:", candidates)
    if not candidates:
        raise FileNotFoundError(f"❌ No partitions under {base_dir}")
    latest = candidates[0]
    log.warning(f"⚠️ Using latest snapshot: {latest.name}")
    return latest



def resolve_snapshot_date(domain: str, fallback_dir: Path | None = None) -> str:
    """
    ✅ domain 기준으로 latest_snapshot_meta.json에서 최신 snapshot 일자를 반환
    - 파일이 없거나 domain이 없으면 fallback_dir에서 최신 파티션 탐색
    """
    meta_path = getattr(C, "LATEST_SNAPSHOT_META_PATH", None)
    if meta_path and meta_path.exists():
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            if domain in meta:
                return meta[domain]["latest_trd_dt"]
        except Exception as e:
            log.warning(f"⚠️ Failed to read latest_snapshot_meta.json: {e}")

    # fallback: 폴더 내 최신 trd_dt 파티션 검색
    if fallback_dir and fallback_dir.exists():
        latest_dir = latest_partition(fallback_dir)
        return latest_dir.name.split("=")[-1]

    raise FileNotFoundError(f"❌ Cannot resolve snapshot date for domain={domain}")