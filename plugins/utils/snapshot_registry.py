import json
from pathlib import Path
from typing import Optional, Dict

WAREHOUSE_ROOT = Path("/opt/airflow/data/data_warehouse")
REGISTRY_FILE = WAREHOUSE_ROOT / "latest_snapshot_meta.json"


def _load_registry() -> Dict:
    if REGISTRY_FILE.exists():
        try:
            return json.loads(REGISTRY_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            print(f"⚠️ Invalid JSON in {REGISTRY_FILE}, resetting...")
            return {}
    return {}


def _save_registry(registry: Dict):
    REGISTRY_FILE.write_text(json.dumps(registry, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"📦 Global registry updated → {REGISTRY_FILE}")


def update_global_snapshot_registry(domain: str, snapshot_dt: str, meta_file: Path, country_code: Optional[str] = None):
    """
    ✅ 전역 메타파일(latest_snapshot_meta.json)에 최신 snapshot 정보 기록
    - domain: 예) 'exchange_master', 'asset_master', 'fundamentals'
    - country_code: asset_master처럼 국가별 세분화 필요할 때만 지정
    """
    registry = _load_registry()

    if domain == "asset_master" and country_code:
        registry.setdefault(domain, {})
        registry[domain][country_code.upper()] = {
            "latest_snapshot_dt": snapshot_dt,
            "meta_file": str(meta_file)
        }
    else:
        registry[domain] = {
            "latest_snapshot_dt": snapshot_dt,
            "meta_file": str(meta_file)
        }

    _save_registry(registry)
    print(f"🆕 Updated registry → {domain}{'[' + country_code + ']' if country_code else ''}: {snapshot_dt}")


def get_latest_snapshot_meta(domain: str, country_code: Optional[str] = None) -> Optional[dict]:
    """
    ✅ 특정 도메인(및 국가)의 최신 snapshot 정보를 반환
    """
    if not REGISTRY_FILE.exists():
        print(f"⚠️ No snapshot registry found at {REGISTRY_FILE}")
        return None

    try:
        data = json.loads(REGISTRY_FILE.read_text(encoding="utf-8"))
        if domain not in data:
            return None
        if country_code:
            return data.get(domain, {}).get(country_code.upper())
        return data[domain]
    except json.JSONDecodeError:
        print(f"⚠️ Invalid registry JSON format at {REGISTRY_FILE}")
        return None
