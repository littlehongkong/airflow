# plugins/utils/loaders/equity/symbol_loader.py
import pandas as pd
import logging
import re
from datetime import datetime
from pathlib import Path

from plugins.config.constants import DATA_LAKE_VALIDATED
from plugins.utils.loaders.base_loader import read_parquet_dir

log = logging.getLogger(__name__)


def find_latest_partition_before(base_dir: Path, target_date: str) -> Path:
    """
    ✅ 주어진 trd_dt 이하 중 가장 최신 파티션 반환
    - base_dir/trd_dt=YYYY-MM-DD 구조 가정
    - target_date 이후의 폴더는 무시하고, 그 이전 중 가장 최근 날짜 반환
    """
    target = datetime.fromisoformat(target_date)
    candidates = []

    if not base_dir.exists():
        raise FileNotFoundError(f"❌ Directory not found: {base_dir}")

    for d in base_dir.glob("trd_dt=*"):
        match = re.search(r"trd_dt=(\d{4}-\d{2}-\d{2})", d.name)
        if not match:
            continue
        part_date = datetime.fromisoformat(match.group(1))
        if part_date <= target:
            candidates.append((part_date, d))

    if not candidates:
        raise FileNotFoundError(f"❌ No partitions before {target_date} in {base_dir}")

    # 날짜 기준 내림차순 정렬 후 첫 번째 선택
    latest_path = sorted(candidates, key=lambda x: x[0], reverse=True)[0][1]
    log.warning(f"⚠️ Using fallback symbol_list partition: {latest_path.name}")
    return latest_path


def load_symbol_list(
    domain_group: str,
    vendor: str,
    exchange_codes: list[str],
    trd_dt: str,
    include_types: list[str] | None = None,
    exclude_field: str | None = None,
    exclude_values: list[str] | None = None,
) -> pd.DataFrame:
    """
    ✅ Symbol List 로더 (ETF 포함)
    - 거래소별 symbol_list parquet 병합
    - type 필터링 지원 (예: ["ETF"], ["Common Stock"])
    - ⚙️ trd_dt 데이터가 없을 경우 가장 최근 파티션 fallback
    """
    dfs = []

    for ex in exchange_codes:
        base_dir = (
            DATA_LAKE_VALIDATED
            / domain_group
            / "symbol_list"
            / f"vendor={vendor}"
            / f"exchange_code={ex}"
        )
        base_path = base_dir / f"trd_dt={trd_dt}"

        try:
            if not base_path.exists():
                log.warning(f"⚠️ symbol_list for {trd_dt} not found at {base_path}, finding latest available...")
                base_path = find_latest_partition_before(base_dir, trd_dt)

            log.info(f"📦 Loading symbol_list from: {base_path}")
            df = read_parquet_dir(base_path)
            df["exchange_code"] = ex
            dfs.append(df)

        except FileNotFoundError as e:
            log.warning(f"⚠️ No symbol_list found for exchange_code={ex}: {e}")
            continue

    if not dfs:
        raise FileNotFoundError(f"❌ No valid symbol_list data for exchanges={exchange_codes}")

    final_df = pd.concat(dfs, ignore_index=True)

    # ✅ type 필터링
    if include_types:
        before = len(final_df)
        final_df = final_df[final_df["type"].isin(include_types)]
        log.info(f"📊 Filtered symbol_list by type={include_types} ({before:,}→{len(final_df):,})")

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
