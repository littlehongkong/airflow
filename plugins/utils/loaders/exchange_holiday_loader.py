# plugins/utils/loaders/equity/exchange_holiday_loader.py
import pandas as pd
from plugins.config.constants import DATA_LAKE_VALIDATED
from plugins.utils.loaders.base_loader import read_parquet_dir, latest_partition

def load_exchange_holiday_list(domain_group: str, vendor: str, trd_dt: str, exchange_code: str) -> pd.DataFrame:
    """
    ✅ Exchange List 로더 (월 1회 수집 기준)
    - 해당 trd_dt 폴더 없으면 최신 partition 자동 선택
    """
    base_dir = (
        DATA_LAKE_VALIDATED
        / domain_group
        / "exchange_holiday"
        / f"vendor={vendor}"
        / f"exchange_code={exchange_code}"
    )

    target_dir = base_dir / f"trd_dt={trd_dt}"
    if not target_dir.exists():
        target_dir = latest_partition(base_dir)

    df = read_parquet_dir(target_dir)
    return df
