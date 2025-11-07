# plugins/utils/loaders/equity/symbol_loader.py
import pandas as pd
from plugins.config.constants import DATA_LAKE_VALIDATED
from plugins.utils.loaders.base_loader import read_parquet_dir
import logging

log = logging.getLogger(__name__)

def load_symbol_list(
    domain_group: str,
    vendor: str,
    exchange_codes: list[str],
    trd_dt: str,
    include_types: list[str] | None = None,
    exclude_field: str | None = None,
    exclude_values: list[str] | None = None
) -> pd.DataFrame:
    """
    ✅ Symbol List 로더 (ETF 포함)
    - 거래소별 symbol_list parquet 병합
    - type 필터링 지원 (예: ["ETF"], ["Common Stock"])
    """
    dfs = []
    for ex in exchange_codes:
        base_path = (
            DATA_LAKE_VALIDATED
            / domain_group
            / "symbol_list"
            / f"vendor={vendor}"
            / f"exchange_code={ex}"
            / f"trd_dt={trd_dt}"
        )

        try:
            df = read_parquet_dir(base_path)
            df["exchange_code"] = ex
            dfs.append(df)
        except FileNotFoundError:
            log.warning(f"⚠️ symbol_list not found for exchange_code={ex}")
            continue

    if not dfs:
        raise FileNotFoundError(f"❌ No valid symbol_list data for exchanges={exchange_codes}")

    final_df = pd.concat(dfs, ignore_index=True)

    # ✅ type 필터링
    if include_types:
        final_df = final_df[final_df["type"].isin(include_types)]
        log.info(f"📊 Filtered symbol_list by type={include_types} | {len(final_df):,} rows")


    # ✅ 특정 필드 값 제외
    if exclude_field and exclude_values:
        if exclude_field in final_df.columns:
            before_rows = len(final_df)
            final_df = final_df[~final_df[exclude_field].isin(exclude_values)]
            after_rows = len(final_df)
            log.info(
                f"🚫 Excluded rows where {exclude_field} in {exclude_values} "
                f"({before_rows - after_rows:,} removed, remaining={after_rows:,})"
            )
        else:
            log.warning(f"⚠️ Exclude field '{exclude_field}' not found in DataFrame columns.")


    return final_df
