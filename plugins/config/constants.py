# plugins/config/constants.py
"""
📘 Data Pipeline Constants
- 모든 vendor / domain / exchange 관련 식별자는 여기서만 정의합니다.
- 다른 코드에서는 절대 하드코드하지 말고, 여기의 상수를 import해서 사용합니다.
"""

from pathlib import Path
import os

# ==============================================
# 📂 기본 경로 설정
# ==============================================
DEFAULT_LOCAL_ROOT = Path(os.getenv("DATA_ROOT_PATH", "/opt/airflow/data"))

# Lake / Warehouse 계층
DATA_LAKE_ROOT = DEFAULT_LOCAL_ROOT / "data_lake"
DATA_WAREHOUSE_ROOT = DEFAULT_LOCAL_ROOT / "data_warehouse"

# ==============================================
# 🏷️ Vendors (데이터 소스)
# ==============================================
VENDORS = {
    "EODHD": "eodhd",
    # "KRX": "krx",
    # "FRED": "fred",
    # "IMF": "imf",
    # "UPBIT": "upbit",  # ✅ 향후 암호화폐 거래소 확장 대비
    # "BINANCE": "binance",
}

# ==============================================
# 📘 Data Domains (Lake 기준)
# ==============================================
DATA_DOMAINS = {
    "SYMBOL_LIST": "symbol_list",
    "SYMBOL_CHANGES": "symbol_changes",
    "FUNDAMENTALS": "fundamentals",
    "PRICES": "prices",
    "DIVIDENDS": "dividends",
    "SPLITS": "splits",
    "EXCHANGE_HOLIDAY": "exchange_holiday",
    "CORPORATE_ACTIONS": "corporate_actions",
    "EXCHANGE_LIST": "exchange_list",
}

# ==============================================
# 📊 Layer (데이터 계층)
# ==============================================
LAYERS = {
    "RAW": "raw",
    "VALIDATED": "validated",
    "STAGING": "staging",
    "CURATED": "curated",
    "WAREHOUSE": "warehouse",
}

# ==============================================
# 🌍 Exchange / Market Codes
# ==============================================
EXCHANGES = {
    "US": {"country": "USA", "vendor": VENDORS["EODHD"]},
    "KO": {"country": "KOR", "vendor": VENDORS["KRX"]},
    "UPBIT": {"country": "KOR", "vendor": VENDORS["UPBIT"]},
}

# ==============================================
# 🏗️ Warehouse 도메인 (공통 도메인명 통일)
# ==============================================
WAREHOUSE_DOMAINS = {
    # ✅ 도메인명은 Lake와 동일하게 유지
    "EXCHANGE": "exchange",
    "ASSET": "asset",
    "PRICE": "price",
    "FUNDAMENTAL": "fundamental",
    "HOLIDAY": "holiday",
}

# ==============================================
# 🧩 Warehouse Source Mapping (Lake → Warehouse)
# ==============================================
WAREHOUSE_SOURCE_MAP = {
    # 거래소 마스터 = exchange_list + exchange_holiday
    "exchange": ["exchange_list", "exchange_holiday"],

    # 종목 마스터 = symbol_list + symbol_changes
    "asset": ["symbol_list", "symbol_changes"],

    # 가격 마스터 = prices + splits + dividends
    "price": ["prices", "splits", "dividends"],

    # 재무 마스터 = fundamentals + corporate_actions
    "fundamental": ["fundamentals", "corporate_actions"],
}

# ==============================================
# 🪪 Entity Prefix
# ==============================================
ENTITY_PREFIX = {
    "EQUITY": "AST",  # 나중에 CRYPTO, FX, MACRO 등 확장 가능
}

# ==============================================
# 🧱 Deprecated / Legacy alias (호환성 유지용)
# ==============================================
MASTER_DOMAIN = "asset"  # ✅ 통일된 도메인명으로 변경
