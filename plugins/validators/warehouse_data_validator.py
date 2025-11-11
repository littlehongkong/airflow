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
import pandas as pd
import json


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
        self.country_code = kwargs.get("country_code")


    def _save_result(self, result: dict, df: pd.DataFrame) -> Path:
        """
        ✅ Warehouse 전용 parquet 저장 경로
        - data_warehouse/snapshot/equity/{domain}/trd_dt=YYYY-MM-DD/country_code=XXX/{domain}.parquet
        """
        snapshot_dir = (
                Path(DATA_WAREHOUSE_ROOT)
                / "snapshot"
                / self.domain_group
                / self.domain
        )

        if self.country_code:
            snapshot_dir = snapshot_dir / f"country_code={self.country_code}" / f"trd_dt={self.trd_dt}"
        else:
            snapshot_dir = snapshot_dir / f"trd_dt={self.trd_dt}"

        snapshot_dir.mkdir(parents=True, exist_ok=True)

        parquet_path = snapshot_dir / f"{self.domain}.parquet"
        df.to_parquet(parquet_path, index=False)

        self.log.info(f"✅ Parquet 저장 완료: {parquet_path} ({len(df):,}행)")

        # 메타파일(_build_meta.json)도 같이 기록 (선택)
        meta_info = {
            "domain": self.domain,
            "domain_group": self.domain_group,
            "snapshot_dt": self.trd_dt,
            "row_count": len(df),
            "country_code": self.country_code,
        }
        meta_path = snapshot_dir / "_build_meta.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta_info, f, indent=2, ensure_ascii=False)

        return parquet_path
