# plugins/validators/warehouse/asset_master_validator.py
from pathlib import Path
from typing import Dict, Any
import pandas as pd

from plugins.validators.warehouse.base_warehouse_validator import BaseWarehouseValidator


class AssetMasterValidator(BaseWarehouseValidator):
    """
    ✅ 종목 마스터 유효성 검증
    """

    def __init__(self, snapshot_dt: str):
        super().__init__(
            domain="asset",
            snapshot_dt=snapshot_dt,
            partition_key="snapshot_dt",
            partition_value=snapshot_dt,
        )

    def _get_parquet_path(self) -> Path:
        return (
            self.warehouse_root
            / self.domain
            / f"snapshot_dt={self.snapshot_dt}"
            / f"{self.domain}.parquet"
        )

    def _define_checks(self, df: pd.DataFrame) -> Dict[str, Any]:
        checks = {
            "row_count_positive": self._check_row_count(df, min_rows=1),
            "no_null_symbol": self._check_no_nulls(df, "symbol"),
            "no_duplicate_symbol": self._check_no_duplicates(df, "symbol"),
            "no_null_exchange_code": self._check_no_nulls(df, "exchange_code"),
        }
        return checks
