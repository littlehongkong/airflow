# plugins/utils/loaders/lake/load_symbol_change_list.py
import pandas as pd
from pathlib import Path
import plugins.config.constants as C
import logging

log = logging.getLogger(__name__)

def load_symbol_change_list(domain_group: str, vendor: str, exchange_code: str, trd_dt: str) -> pd.DataFrame:
    """symbol_changes validated parquet/jsonl 로드"""
    try:
        symbol_path = (
            Path(C.DATA_LAKE_VALIDATED)
            / domain_group
            / "symbol_changes"
            / f"vendor={vendor}"
            / f"exchange_code={exchange_code}"
            / f"trd_dt={trd_dt}"
        )
        df = pd.read_parquet(symbol_path / "symbol_changes.parquet")

        log.info(f"📊 Loaded {len(df):,} rows from symbol_change")

        return df
    except Exception:
        log.warning("⚠️ symbol_changes 파일이 존재하지 않아 티커 매핑을 건너뜁니다.")
        return pd.DataFrame()

