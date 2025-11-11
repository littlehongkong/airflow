# plugins/utils/id_generator.py

import hashlib
import base64
import json
from typing import Dict
from plugins.config.constants import SECURITY_ID_MAP_FILE, SECURITY_ID_MAP_LOCK
from filelock import FileLock  # 동시 접근 안전용

def _b32(n: bytes, length: int = 16) -> str:
    """base32로 인코딩 후 길이 제한 (기본 16자리, 유니크성 강화)"""
    return base64.b32encode(n).decode("ascii").rstrip("=").lower()[:length]

def _load_id_map() -> Dict[str, str]:
    """보조 ID 매핑 파일 로드"""
    if SECURITY_ID_MAP_FILE.exists():
        try:
            with open(SECURITY_ID_MAP_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_id_map(id_map: Dict[str, str]):
    """보조 ID 매핑 파일 저장 (락파일 사용)"""
    SECURITY_ID_MAP_FILE.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(str(SECURITY_ID_MAP_LOCK)):
        with open(SECURITY_ID_MAP_FILE, "w", encoding="utf-8") as f:
            json.dump(id_map, f, indent=2, ensure_ascii=False)

def generate_or_reuse_entity_id(prefix: str, *, country: str, exchange: str, ticker: str) -> str:
    """
    ✅ 전역 Entity ID 발급/재사용 유틸
    - 기존 ID 있으면 재사용
    - 없으면 신규 생성 및 저장
    """
    id_map = _load_id_map()
    key = f"{ticker.upper()}|{exchange.upper()}|{country.upper()}"

    if key in id_map:
        return id_map[key]

    # 신규 해시 생성 (sha1 → base32 → 16자리)
    seed = f"{country.strip().upper()}|{exchange.strip().upper()}|{ticker.strip().upper()}"
    digest = hashlib.sha1(seed.encode("utf-8")).digest()
    entity_id = f"{prefix}_{_b32(digest, length=16)}"

    # 중복 감지 (이론상 거의 불가능)
    if entity_id in id_map.values():
        print(f"⚠️ Duplicate hash detected for {key} → {entity_id}")

    id_map[key] = entity_id
    _save_id_map(id_map)
    return entity_id
