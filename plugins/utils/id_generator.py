# plugins/utils/id_generator.py
import hashlib
import base64

def _b32(n: bytes, length: int = 10) -> str:
    """base32로 인코딩 후 길이 제한."""
    return base64.b32encode(n).decode("ascii").rstrip("=").lower()[:length]

def generate_entity_id(prefix: str, *, country: str, exchange: str, ticker: str) -> str:
    """
    전역 Entity ID 생성 (결정적)
    - prefix: 예) 'AST' (EQUITY 공통)
    - country/exchange/ticker는 대소문자/공백 정규화
    """
    seed = f"{country.strip().upper()}|{exchange.strip().upper()}|{ticker.strip().upper()}"
    digest = hashlib.sha1(seed.encode("utf-8")).digest()
    return f"{prefix}_{_b32(digest, length=12)}"
