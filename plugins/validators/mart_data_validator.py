"""
MartDataValidator
---------------------------------------
📊 Data Mart 전용 Validator
- constants.py 기반 dataset_path 자동 세팅
- BaseDataValidator 상속
"""

from pathlib import Path
from plugins.validators.base_data_validator import BaseDataValidator
from plugins.config import constants as C


class MartDataValidator(BaseDataValidator):
    """Data Mart 유효성 검증"""

    def __init__(self, domain: str, trd_dt: str, **kwargs):
        """
        Data Mart 구조:
        /opt/airflow/data/data_mart/{domain}/trd_dt={YYYY-MM-DD}/{domain}.parquet
        """

        dataset_path = (
            C.DATA_MART_ROOT
            / domain
            / f"trd_dt={trd_dt}"
            / f"{domain}.parquet"
        )

        super().__init__(
            domain=domain,
            layer="mart",
            trd_dt=trd_dt,
            dataset_path=dataset_path,
            **kwargs,
        )
