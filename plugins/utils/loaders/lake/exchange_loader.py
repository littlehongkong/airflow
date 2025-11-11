# plugins/utils/loaders/equity/exchange_loader.py
import pandas as pd
from plugins.config.constants import DATA_LAKE_VALIDATED
from plugins.utils.loaders.base_loader import read_parquet_dir, latest_partition

def load_exchange_list(
        domain_group: str, vendor: str, trd_dt: str,
        include_field: str | None = None,
        include_values: list[str] | None = None,
    ) -> pd.DataFrame:
    """
    ✅ Exchange List 로더 (월 1회 수집 기준)
    - 해당 trd_dt 폴더 없으면 최신 partition 자동 선택
    """
    base_dir = (
        DATA_LAKE_VALIDATED
        / domain_group
        / "exchange_list"
        / f"vendor={vendor}"
        / "exchange_code=ALL"
    )

    target_dir = base_dir / f"trd_dt={trd_dt}"
    if not target_dir.exists():
        target_dir = latest_partition(base_dir)

    df = read_parquet_dir(target_dir)


    if include_field and include_values:
        if include_field in df.columns:
            before_rows = len(df)
            df = df[df[include_field].isin(include_values)]
            after_rows = len(df)
            print(
                f"🚫 Excluded rows where {include_field} in {include_values} "
                f"({before_rows - after_rows:,} remaining={after_rows:,})"
            )
        else:
            print(f"⚠️ Include field '{include_field}' not found in DataFrame columns.")


    return df
