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

    assert country_code is not None, "🔴 country_code is required"

    snapshot_dir = Path(DATA_WAREHOUSE_ROOT) / "snapshot" / domain_group / "asset_master" / f"country_code={country_code}"

    if not snapshot_dir.exists():
        log.warning(f"⚠️ asset_master snapshot directory not found: {snapshot_dir}")
        return pd.DataFrame()

    # 최신 스냅샷 탐색
    snapshots = sorted(snapshot_dir.glob("trd_dt=*"), reverse=True)
    if not snapshots:
        log.warning(f"⚠️ No snapshot folders found under {snapshot_dir}")
        return pd.DataFrame()

    latest_snapshot = snapshots[0]
    parquet_files = list(latest_snapshot.rglob("asset_master.parquet"))
    if not parquet_files:
        log.warning(f"⚠️ No asset_master.parquet found in {latest_snapshot}")
        return pd.DataFrame()

    latest_file = parquet_files[0]
    log.info(f"📦 Loading latest asset_master: {latest_file}")

    try:
        df = pd.read_parquet(latest_file)
        df = df[["security_id", "ticker", "exchange_code", "country_code"]].drop_duplicates()
        log.info(f"✅ Loaded {len(df):,} asset_master records from {latest_file.name}")
        return df
    except Exception as e:
        log.error(f"❌ Failed to read asset_master: {e}")
        return pd.DataFrame()
