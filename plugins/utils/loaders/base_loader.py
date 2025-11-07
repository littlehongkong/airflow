# plugins/utils/loaders/base_loader.py
from pathlib import Path
import duckdb
import pandas as pd
import logging

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
    """📂 가장 최신 trd_dt 파티션을 반환"""
    candidates = sorted(base_dir.glob("trd_dt=*"), reverse=True)
    if not candidates:
        raise FileNotFoundError(f"❌ No partitions under {base_dir}")
    latest = candidates[0]
    log.warning(f"⚠️ Using latest snapshot: {latest.name}")
    return latest
