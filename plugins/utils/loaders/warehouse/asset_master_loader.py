# plugins/utils/loaders/warehouse/asset_master_loader.py
import pandas as pd
from pathlib import Path
from plugins.config.constants import DATA_WAREHOUSE_ROOT
import logging

log = logging.getLogger(__name__)

def load_asset_master_latest(domain_group: str = "equity", country_code: str = None) -> pd.DataFrame:
    """
    ✅ 최신 asset_master 스냅샷 로드
    - 최신 trd_dt 파티션을 자동 탐색
    - security_id, ticker, exchange_code 중심의 매핑 반환
    - downstream 파이프라인(예: prices, fundamentals 등)에서 재사용 가능
    """

    return load_asset_master(domain_group=domain_group, country_code=country_code, trd_dt=None)


def load_asset_master(
    domain_group: str = "equity",
    country_code: str | None = None,
    trd_dt: str | None = None,
) -> pd.DataFrame:
    """
    ✅ 특정 날짜(or 최신)의 asset_master 스냅샷 로드
    - trd_dt 가 있으면 해당 날짜만
    - trd_dt 가 없으면 최신 스냅샷
    """
    assert country_code is not None, "🔴 country_code is required"

    base_dir = (
        Path(DATA_WAREHOUSE_ROOT)
        / "snapshot"
        / domain_group
        / "asset_master"
        / f"country_code={country_code}"
    )

    if not base_dir.exists():
        log.warning(f"⚠️ asset_master snapshot base directory not found: {base_dir}")
        return pd.DataFrame()

    # 1) 특정 날짜 스냅샷
    if trd_dt is not None:
        snapshot_dir = base_dir / f"trd_dt={trd_dt}"
        if not snapshot_dir.exists():
            log.warning(f"⚠️ No snapshot folder for trd_dt={trd_dt}: {snapshot_dir}")
            return pd.DataFrame()

        parquet_files = list(snapshot_dir.rglob("asset_master.parquet"))
        if not parquet_files:
            log.warning(f"⚠️ No asset_master.parquet under {snapshot_dir}")
            return pd.DataFrame()

        target_file = parquet_files[0]
        log.info(f"📦 Loading asset_master snapshot {trd_dt}: {target_file}")
    else:
        # 2) 최신 스냅샷
        snapshots = sorted(base_dir.glob("trd_dt=*"), reverse=True)
        if not snapshots:
            log.warning(f"⚠️ No snapshot folders found under {base_dir}")
            return pd.DataFrame()

        latest_snapshot = snapshots[0]
        parquet_files = list(latest_snapshot.rglob("asset_master.parquet"))
        if not parquet_files:
            log.warning(f"⚠️ No asset_master.parquet found in {latest_snapshot}")
            return pd.DataFrame()

        target_file = parquet_files[0]
        log.info(f"📦 Loading latest asset_master: {target_file}")

    try:
        df = pd.read_parquet(target_file)
        # 매핑에 필요한 최소 컬럼만 사용
        cols = [c for c in ["security_id", "ticker", "exchange_code", "country_code"] if c in df.columns]
        df = df[cols].drop_duplicates()
        log.info(f"✅ Loaded {len(df):,} asset_master records from {target_file.name}")
        return df
    except Exception as e:
        log.error(f"❌ Failed to read asset_master from {target_file}: {e}")
        return pd.DataFrame()