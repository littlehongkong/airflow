"""
Asset Master Warehouse Validator
-----------------------------------------
- BaseWarehouseValidator 상속
- 주요 Entity 무결성 검증
"""

from pathlib import Path
from typing import Dict, Any
import pandas as pd
from plugins.validators.warehouse.base_warehouse_validator import BaseWarehouseValidator


class AssetMasterValidator(BaseWarehouseValidator):
    def __init__(self, country_code: str, snapshot_dt: str):
        super().__init__(
            domain="asset_master",
            snapshot_dt=snapshot_dt,
            partition_key="country_code",
            partition_value=country_code,
        )

    def _get_parquet_path(self) -> Path:
        return (
            self.warehouse_root
            / self.domain
            / f"country_code={self.partition_value}"
            / f"snapshot_dt={self.snapshot_dt}"
            / f"{self.domain}.parquet"
        )

    def _define_checks(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {
            "row_count_positive": self._check_row_count(df, min_rows=1),
            "no_null_entity_id": self._check_no_nulls(df, "entity_id"),
            "no_duplicate_entity_id": self._check_no_duplicates(df, "entity_id"),
            "no_null_ticker": self._check_no_nulls(df, "ticker"),
        }
