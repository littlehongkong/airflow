from pathlib import Path
import os

# ===========================================================
# 📂 DATA ROOTS (기본 로컬 데이터 루트)
# ===========================================================
DEFAULT_LOCAL_ROOT = Path(os.getenv("DATA_ROOT_PATH", "/opt/airflow/data"))

# ✅ 레이어별 데이터 루트
DATA_LAKE_ROOT       = DEFAULT_LOCAL_ROOT / "data_lake"
DATA_WAREHOUSE_ROOT  = DEFAULT_LOCAL_ROOT / "data_warehouse"
DATA_MONITORING_ROOT = DEFAULT_LOCAL_ROOT / "data_monitoring"

# DATA_MART_ROOT       = DEFAULT_LOCAL_ROOT / "data_mart"

# ===========================================================
# 🧭 Warehouse Global Metadata
# ===========================================================

# 📦 Warehouse Latest Snapshot Meta

LATEST_SNAPSHOT_META_PATH = DATA_WAREHOUSE_ROOT / "latest_snapshot_meta.json"
LATEST_SNAPSHOT_META_LOCK = DATA_WAREHOUSE_ROOT / "latest_snapshot_meta.lock"

# 💧 Data Lake Latest Validated Meta
LATEST_VALIDATED_META_PATH = DATA_LAKE_ROOT / "latest_validated_meta.json"
LATEST_VALIDATED_META_LOCK = DATA_LAKE_ROOT / "latest_validated_meta.lock"

# ✅ 서브디렉터리 구조 예시
# /data_lake/raw/equity/eodhd/
# /data_lake/validated/equity/krx/
# /data_warehouse/snapshot/equity/
# /data_warehouse/validated/equity/merged/

DATA_LAKE_RAW        = DATA_LAKE_ROOT / "raw"
DATA_LAKE_VALIDATED  = DATA_LAKE_ROOT / "validated"
DATA_WAREHOUSE_SNAPSHOT  = DATA_WAREHOUSE_ROOT / "snapshot"
DATA_WAREHOUSE_VALIDATED = DATA_WAREHOUSE_ROOT / "validated"

# ===========================================================
# 🧩 VALIDATOR ROOT PATHS (PLUGINS 내 스키마/체크 정의)
# ===========================================================
# 예: /opt/airflow/plugins/validators/schemas/lake/equity/eodhd/prices.json
#     /opt/airflow/plugins/validators/checks/lake/equity/eodhd/equity_price.yml

VALIDATOR_ROOT          = Path("/opt/airflow/plugins/validators")

# ✅ 공통 루트
VALIDATOR_SCHEMA_ROOT   = VALIDATOR_ROOT / "schemas"
VALIDATOR_CHECKS_ROOT   = VALIDATOR_ROOT / "soda" /"checks"

# ✅ 레이어 구분 (lake / warehouse / mart)
VALIDATOR_SCHEMA_LAKE        = VALIDATOR_SCHEMA_ROOT / "lake"
VALIDATOR_SCHEMA_WAREHOUSE   = VALIDATOR_SCHEMA_ROOT / "warehouse"
# VALIDATOR_SCHEMA_MART        = VALIDATOR_SCHEMA_ROOT / "mart"

VALIDATOR_CHECKS_LAKE        = VALIDATOR_CHECKS_ROOT / "lake"
VALIDATOR_CHECKS_WAREHOUSE   = VALIDATOR_CHECKS_ROOT / "warehouse"
# VALIDATOR_CHECKS_MART        = VALIDATOR_CHECKS_ROOT / "mart"


# ------------------------------------------------------------------------
# 🧩 Entity ID 매핑 파일 (보조 캐시)
# ------------------------------------------------------------------------
DATA_META_ROOT = DEFAULT_LOCAL_ROOT / "meta"
SECURITY_ID_MAP_FILE = DATA_META_ROOT / "security_id_map.json"
SECURITY_ID_MAP_LOCK = DATA_META_ROOT / "security_id_map.lock"

# ===========================================================
# 🌍 DOMAIN-BASED SUBDIRECTORY EXAMPLES (확장형)
# ===========================================================
# 예: lake/equity/eodhd, lake/equity/krx, lake/macro/fred ...


EXCLUDED_EXCHANGES_BY_COUNTRY = {
    "USA": ['OTCQB', 'PINK', 'OTCQX', 'OTCMKTS', 'NMFQS', 'NYSE MKT','OTCBB', 'OTCGREY', 'BATS', 'OTC',  'OTCMTKS','OTCCE' ],
    "KOR": ["KONEX"]
}

# ✅ 상위 자산군 (데이터 그룹)
DOMAIN_GROUPS = {
    "equity": "equity",
    "crypto": "crypto",
    "fx": "fx",
    "macro": "macro",
    "news": "news",
}

EVENT_CATEGORIES = {
    "new_listing": "new_listing",
    "symbol_change": "symbol_change",
    "split": "split",
    "delisting": "delisting",
}

# ===========================================================
# 🧭 PATH HELPER FUNCTIONS
# ===========================================================

def get_schema_path(layer: str, domain: str, vendor: str, dataset_name: str) -> Path:
    """
    layer/domain/vendor/dataset_name.json 경로를 반환
    예시: lake/equity/eodhd/prices.json
    """
    return VALIDATOR_SCHEMA_ROOT / layer / domain / vendor / f"{dataset_name}.json"

def get_check_path(layer: str, domain: str, vendor: str, dataset_name: str) -> Path:
    """
    layer/domain/vendor/dataset_name.yml 경로를 반환
    예시: lake/equity/eodhd/equity_price.yml
    """
    return VALIDATOR_CHECKS_ROOT / layer / domain / vendor / f"{dataset_name}.yml"

# ==============================================
# 🏷️ Vendors
# ==============================================
VENDORS = {
    "eodhd": "eodhd",
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
    "exchange_detail": "exchange_detail",
    "corporate_actions": "corporate_actions",
    "exchange_list": "exchange_list",
}

# ==============================================
# 📊 Stages
# ==============================================
Stages = {
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
    "US": {"country": "USA", "vendor": VENDORS["eodhd"]},
}

# ==============================================
# 🏗️ Warehouse Domains
# ==============================================
WAREHOUSE_DOMAINS = {
    "exchange": "exchange_master",
    "asset": "asset_master",
    "price": "price_master",
    "fundamental": "fundamental_master",
    "holiday": "holiday_master",
}


# ==============================================
# 🧩 Warehouse Source Mapping (Lake → Warehouse)
# ==============================================
WAREHOUSE_SOURCE_MAP = {
    "exchange": ["exchange_list", "exchange_detail"],
    "asset_master": ["symbol_list", "exchange_list"],
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
MASTER_DOMAIN = "asset_master"
