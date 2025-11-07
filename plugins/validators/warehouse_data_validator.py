"""
WarehouseDataValidator
---------------------------------------
🏭 Data Warehouse 전용 Validator
- constants.py 기반 dataset_path 자동 세팅
- BaseDataValidator 상속
"""

from pathlib import Path
from plugins.validators.base_data_validator import BaseDataValidator
from plugins.config.constants import DATA_WAREHOUSE_ROOT


class WarehouseDataValidator(BaseDataValidator):
    """
    ✅ Data Warehouse 계층 유효성 검증 Validator

    예시 경로 구조:
    /opt/airflow/data/data_warehouse/equity/{domain}/snapshot/trd_dt={YYYY-MM-DD}/{domain}.parquet
    """

    def __init__(self, domain: str, trd_dt: str, dataset_path: str, vendor: str = "eodhd", exchange_code: str = "ALL", domain_group: str = "equity", **kwargs):
        """
        - domain: 검증 대상 도메인 (예: exchange, fundamentals, price 등)
        - trd_dt: 검증 날짜
        - vendor: 데이터 벤더 (기본값: eodhd)
        """

        super().__init__(
            domain=domain,
            domain_group=domain_group,
            layer="warehouse",
            trd_dt=trd_dt,
            dataset_path=dataset_path,
            vendor=vendor,
            exchange_code=exchange_code,
            **kwargs,
        )
