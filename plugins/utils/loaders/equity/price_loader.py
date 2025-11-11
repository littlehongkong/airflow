# plugins/utils/loaders/equity/price_loader.py
import pandas as pd
from plugins.config.constants import DATA_LAKE_VALIDATED
from plugins.utils.loaders.base_loader import read_parquet_dir

def load_prices(domain_group: str, vendor: str, exchange_codes: list[str], trd_dt: str) -> pd.DataFrame:
    """
    ✅ Prices 로더
    - 거래소별 parquet 읽기
    """
    dfs = []
    for ex in exchange_codes:
        base_path = (
            DATA_LAKE_VALIDATED
            / domain_group
            / "prices"
            / f"vendor={vendor}"
            / f"exchange_code={ex}"
            / f"trd_dt={trd_dt}"
        )

        try:
            df = read_parquet_dir(base_path)
            df["exchange_code"] = ex

            dfs.append(df)
        except FileNotFoundError:
            continue

    if not dfs:
        raise FileNotFoundError(f"❌ No valid price data found for {exchange_codes}")
    return pd.concat(dfs, ignore_index=True)
