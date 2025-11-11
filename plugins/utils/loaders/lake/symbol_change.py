# plugins/utils/loaders/lake/load_symbol_change_list.py
import pandas as pd
from pathlib import Path
import plugins.config.constants as C

def _load_symbol_change_list(self) -> pd.DataFrame:
    """symbol_changes validated parquet/jsonl 로드"""
    try:
        symbol_path = (
            Path(C.DATA_LAKE_VALIDATED)
            / self.domain_group
            / "symbol_changes"
            / f"vendor={self.vendor}"
            / f"exchange_code={self.exchanges[0]}"
            / f"trd_dt={self.trd_dt}"
        )
        df = pd.read_parquet(symbol_path / "symbol_changes.parquet")

        return df
    except Exception:
        self.log.warning("⚠️ symbol_changes 파일이 존재하지 않아 티커 매핑을 건너뜁니다.")
        return pd.DataFrame()

