from pathlib import Path
import os

# ==============================================
# 📂 기본 경로 설정
# ==============================================
DEFAULT_LOCAL_ROOT = Path(os.getenv("DATA_ROOT_PATH", "/opt/airflow/data"))
DATA_LAKE_ROOT = DEFAULT_LOCAL_ROOT / "data_lake"
DATA_WAREHOUSE_ROOT = DEFAULT_LOCAL_ROOT / "data_warehouse"

# ==============================================
# 🏷️ Vendors
# ==============================================
VENDORS = {
    "EODHD": "eodhd",
}

# ==============================================
# 📘 Data Domains (Lake)
# ==============================================
DATA_DOMAINS = {
    "symbol_list": "symbol_list",
    "symbol_changes": "symbol_changes",
    "fundamentals": "fundamentals",
    "prices": "prices",
    "dividends": "dividends",
    "splits": "splits",
    "exchange_holiday": "exchange_holiday",
    "corporate_actions": "corporate_actions",
    "exchange_list": "exchange_list",
}

# ==============================================
# 📊 Layers
# ==============================================
LAYERS = {
    "raw": "raw",
    "validated": "validated",
    "staging": "staging",
    "curated": "curated",
    "warehouse": "warehouse",
}

# ==============================================
# 🌍 Exchange Codes
# ==============================================
EXCHANGES = {
    "US": {"country": "USA", "vendor": VENDORS["EODHD"]},
}

# ==============================================
# 🏗️ Warehouse Domains
# ==============================================
WAREHOUSE_DOMAINS = {
    "exchange": "exchange",
    "asset": "asset",
    "price": "price",
    "fundamental": "fundamental",
    "holiday": "holiday",
}

# ==============================================
# 🧩 Warehouse Source Mapping (Lake → Warehouse)
# ==============================================
WAREHOUSE_SOURCE_MAP = {
    "exchange": ["exchange_list", "exchange_holiday"],
    "asset": ["symbol_list", "symbol_changes"],
    "price": ["prices", "splits", "dividends"],
    "fundamental": ["fundamentals", "corporate_actions"],
}

# ✅ 역방향 매핑 (Lake → Warehouse)
WAREHOUSE_REVERSE_MAP = {
    lake: wh for wh, lakes in WAREHOUSE_SOURCE_MAP.items() for lake in lakes
}

# ==============================================
# 🪪 Entity Prefix
# ==============================================
ENTITY_PREFIX = {"EQUITY": "AST"}

# ==============================================
# 🧰 Path Helper
# ==============================================
def get_layer_path(layer: str, domain: str) -> Path:
    base = {
        "raw": DATA_LAKE_ROOT / "raw",
        "validated": DATA_LAKE_ROOT / "validated",
        "warehouse": DATA_WAREHOUSE_ROOT,
    }.get(layer.lower())
    if not base:
        raise ValueError(f"❌ Unknown layer: {layer}")
    return base / domain

# ==============================================
# 🧱 Deprecated (호환성 유지)
# ==============================================
MASTER_DOMAIN = "asset"
