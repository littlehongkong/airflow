"""
Exchange Master Warehouse Validator (Refactored)
-----------------------------------------
✅ 목적:
- Warehouse(exchange) 적재 데이터의 품질 및 형식 검증
- BaseWarehouseValidator 기반으로 단순화된 구조 유지
"""

from typing import Dict, Any
import pandas as pd
from plugins.validators.warehouse.base_warehouse_validator import BaseWarehouseValidator


class ExchangeMasterValidator(BaseWarehouseValidator):
    """거래소 마스터 유효성 검증"""

    def __init__(self, snapshot_dt: str):
        from plugins.config.constants import WAREHOUSE_DOMAINS

        super().__init__(
            domain=WAREHOUSE_DOMAINS["exchange"],
            snapshot_dt=snapshot_dt,
        )

    # -------------------------------------------------------------------------
    # ✅ 핵심 검증 로직
    # -------------------------------------------------------------------------
    def _define_checks(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Warehouse(exchange) 데이터 검증 항목 정의
        - 거래소 코드 중복/누락
        - 국가 코드 ISO 형식
        - 행 개수 유효성
        """
        checks = {}

        # 1️⃣ 기본 무결성 검사
        checks["row_count_positive"] = self._check_row_count(df, min_rows=1)
        checks["no_null_exchange_code"] = self._check_no_nulls(df, "exchange_code")
        checks["no_duplicate_exchange_code"] = self._check_no_duplicates(df, "exchange_code")
        checks["no_null_country_code"] = self._check_no_nulls(df, "country_code")

        # 2️⃣ 국가 코드 형식 검증 (ISO3)
        if "country_code" in df.columns:
            invalid_iso = df[~df["country_code"].str.match(r"^[A-Z]{2}$", na=False)]
            checks["valid_country_iso2"] = {
                "passed": bool(invalid_iso.empty),
                "value": int(len(invalid_iso)),
                "expected": "2-letter uppercase (e.g., US, KR)",
                "message": f"Invalid ISO2 codes: {len(invalid_iso)}",
            }

        # 3️⃣ 통화 코드 검증 (3자리 알파벳)
        if "currency" in df.columns:
            invalid_currency = df[~df["currency"].astype(str).str.match(r"^[A-Z]{3}$", na=False)]
            checks["valid_currency_format"] = {
                "passed": invalid_currency.empty,
                "value": len(invalid_currency),
                "expected": "3-letter uppercase (e.g., USD, KRW)",
                "message": f"Invalid currency codes: {len(invalid_currency)}",
            }

        # 4️⃣ 운영 MIC 코드 유효성 (선택적)
        if "operating_mic" in df.columns:
            missing_mic = df["operating_mic"].isnull().sum()
            checks["valid_operating_mic"] = {
                "passed": missing_mic == 0,
                "value": missing_mic,
                "expected": 0,
                "message": f"Missing MIC values: {missing_mic}",
            }

        # 5️⃣ 중복된 국가+거래소코드 조합 검사
        if {"country_code", "exchange_code"}.issubset(df.columns):
            combo_dups = df.duplicated(subset=["country_code", "exchange_code"]).sum()
            checks["unique_country_exchange_pair"] = {
                "passed": combo_dups == 0,
                "value": combo_dups,
                "expected": 0,
                "message": f"Duplicated country+exchange pairs: {combo_dups}",
            }

        return checks
