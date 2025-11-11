# plugins/utils/loaders/equity/fundamentals_loader.py
import pandas as pd
from plugins.config.constants import DATA_LAKE_VALIDATED
from plugins.utils.loaders.base_loader import read_parquet_dir, read_json_dir


import logging as log


def load_fundamentals_raw(domain_group: str, vendor: str, exchange_code: str, trd_dt: str) -> pd.DataFrame:
    """
    ✅ Fundamentals Raw Loader
    - 거래소별 종목단위 JSON 데이터 로드
    - validator 또는 전처리용
    경로 예시:
    /data_lake/validated/equity/fundamentals/vendor=eodhd/exchange_code=KO/trd_dt=2025-11-05/000500.json
    """
    base_path = (
        DATA_LAKE_VALIDATED
        / domain_group
        / "fundamentals"
        / f"vendor={vendor}"
        / f"exchange_code={exchange_code}"
        / f"trd_dt={trd_dt}"
    )

    if not base_path.exists():
        raise FileNotFoundError(f"❌ Directory not found: {base_path}")

    # JSONL or multi-JSON structure → DuckDB auto detection
    try:
        df = read_json_dir(base_path, "*.json")
    except Exception as e:
        raise RuntimeError(f"⚠️ Failed to read fundamentals raw JSON: {e}")

    df["exchange_code"] = exchange_code
    df["trd_dt"] = trd_dt

    log.info(f"📦 Loaded {len(df):,} raw fundamentals rows from {base_path}")
    return df


def load_fundamentals_latest(domain_group: str, vendor: str, exchange_codes: list[str]) -> pd.DataFrame:
    """
    ✅ Fundamentals Latest Loader
    - fundamentals_general_latest.parquet 기반 (요약본)
    - AssetMaster, Snapshot 생성 시 사용
    """
    dfs = []
    for ex in exchange_codes:
        path = (
            DATA_LAKE_VALIDATED
            / domain_group
            / "fundamentals"
            / f"vendor={vendor}"
            / f"exchange_code={ex}"
            / "fundamentals_general_latest.parquet"
        )

        if not path.exists():
            log.warning(f"⚠️ Missing fundamentals_general_latest.parquet for {ex}")
            continue

        try:
            df = read_parquet_dir(path.parent, "fundamentals_general_latest.parquet")
            df["exchange_code"] = ex
            dfs.append(df)
        except Exception as e:
            log.warning(f"⚠️ Failed to load fundamentals for {ex}: {e}")
            continue

    if not dfs:
        log.warning("⚠️ No fundamentals_general_latest files found")
        raise FileNotFoundError("No valid fundamentals_general_latest data for exchanges={exchange_codes}")

    final_df = pd.concat(dfs, ignore_index=True)
    log.info(f"📊 Loaded {len(final_df):,} rows from fundamentals_general_latest")
    return final_df
