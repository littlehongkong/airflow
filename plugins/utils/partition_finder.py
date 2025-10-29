# plugins/utils/partition_finder.py
from pathlib import Path
from typing import Optional, List, Tuple
from plugins.config.constants import DATA_LAKE_ROOT

def _list_dirs(p: Path) -> List[Path]:
    return [d for d in p.iterdir() if d.is_dir()]

def latest_trd_dt_path(domain: str, exchange_code: str, vendor: str, layer: str = "validated") -> Optional[Path]:
    """
    /data_lake/{layer}/{domain}/vendor={vendor}/exchange_code={ex}/trd_dt=YYYY-MM-DD/ 를 스캔해서
    가장 최신 날짜 경로를 반환. 없으면 None.
    """
    base = DATA_LAKE_ROOT / layer / domain / f"vendor={vendor}" / f"exchange_code={exchange_code}"
    if not base.exists():
        return None
    trd_dirs = [d for d in _list_dirs(base) if d.name.startswith("trd_dt=")]
    if not trd_dirs:
        return None
    # 정렬: trd_dt=YYYY-MM-DD 문자열 사전순 == 날짜 오름차순
    trd_dirs.sort(key=lambda p: p.name)
    return trd_dirs[-1]  # 최신

def parquet_file_in(partition_dir: Path, domain: str) -> Optional[Path]:
    """
    파티션 디렉터리 내 파일명 {domain}.parquet를 반환
    """
    if not partition_dir:
        return None
    f = partition_dir / f"{domain}.parquet"
    return f if f.exists() else None
