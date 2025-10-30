from pathlib import Path
from typing import Optional
from plugins.config.constants import DATA_LAKE_ROOT, WAREHOUSE_SOURCE_MAP

def _normalize_domain(domain: str) -> str:
    """Lake 도메인을 Warehouse 호환형으로 정규화"""
    for wh_domain, sources in WAREHOUSE_SOURCE_MAP.items():
        if domain in sources:
            return wh_domain
    return domain

def latest_trd_dt_path(domain: str, exchange_code: str, vendor: str, layer: str = "validated") -> Optional[Path]:
    """가장 최근 거래일 디렉토리 경로 탐색"""
    normalized = _normalize_domain(domain)
    base_path = DATA_LAKE_ROOT / layer / normalized / f"vendor={vendor}" / f"exchange_code={exchange_code}"
    if not base_path.exists():
        return None

    trd_dirs = sorted([p for p in base_path.iterdir() if p.is_dir() and p.name.startswith("trd_dt=")], reverse=True)
    return trd_dirs[0] if trd_dirs else None

def parquet_file_in(partition_dir: Path, domain: str) -> Optional[Path]:
    """파티션 내 parquet 파일 탐색"""
    normalized = _normalize_domain(domain)
    candidate = partition_dir / f"{normalized}.parquet"
    if candidate.exists():
        return candidate
    for f in partition_dir.glob("*.parquet"):
        return f
    return None
