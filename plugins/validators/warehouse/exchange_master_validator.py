"""
Exchange Master Warehouse Validator
-----------------------------------------
- BaseWarehouseValidator 상속
- 거래소 코드, 국가 코드 정합성 검증
"""

from pathlib import Path
from typing import Dict, Any
import pandas as pd
from plugins.validators.warehouse.base_warehouse_validator import BaseWarehouseValidator



class ExchangeMasterValidator(BaseWarehouseValidator):
    def __init__(self, snapshot_dt: str):
        from plugins.config.constants import WAREHOUSE_DOMAIN

        super().__init__(
            domain=WAREHOUSE_DOMAIN.get("EXCHANGE_MASTER"),
            snapshot_dt=snapshot_dt,
            partition_key="snapshot_dt",
            partition_value=snapshot_dt,
        )

    # ✅ warehouse parquet 경로 지정
    def _get_parquet_path(self) -> Path:
        return (
            self.warehouse_root
            / self.domain
            / f"snapshot_dt={self.snapshot_dt}"
            / f"{self.domain}.parquet"
        )

    # ✅ 실제 검증 로직 (_validate 필수 구현)
    def _validate(self, **kwargs) -> Dict[str, Any]:
        """
        Warehouse 레벨의 데이터 유효성 검증
        1. Parquet 로드
        2. 정의된 체크 수행
        3. 결과 반환
        """
        parquet_path = self._get_parquet_path()

        if not parquet_path.exists():
            raise FileNotFoundError(f"❌ Parquet file not found: {parquet_path}")

        df = pd.read_parquet(parquet_path)
        checks = self._define_checks(df)

        # 결과 요약
        passed = all(v.get("passed", False) for v in checks.values())
        failed = [k for k, v in checks.items() if not v.get("passed", False)]

        return {
            "status": "success" if passed else "failed",
            "record_count": len(df),
            "output_file": parquet_path.as_posix(),
            "checks": checks,
            "failed_checks": failed,
        }

    # ✅ 각 컬럼 단위 체크 정의
    def _define_checks(self, df: pd.DataFrame) -> Dict[str, Any]:
        checks = {
            "row_count_positive": self._check_row_count(df, min_rows=1),
            "no_null_exchange_code": self._check_no_nulls(df, "exchange_code"),
            "no_duplicate_exchange_code": self._check_no_duplicates(df, "exchange_code"),
            "no_null_country_code": self._check_no_nulls(df, "country_code"),
        }

        # Country ISO2 형식 검증
        invalid_iso = df[~df["country_code"].str.match(r"^[A-Z]{2}$", na=False)]
        checks["valid_country_iso2"] = {
            "passed": invalid_iso.empty,
            "value": len(invalid_iso),
            "expected": "2-letter uppercase (e.g., US, KR)",
            "message": f"Invalid ISO2 codes: {len(invalid_iso)}",
        }

        return checks
