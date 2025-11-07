"""
LakeDataValidator
---------------------------------------
🌊 Data Lake 전용 Validator
- constants.py 기반 dataset_path 자동 세팅
- BaseDataValidator 상속
"""

from plugins.validators.base_data_validator import BaseDataValidator
from plugins.config import constants as C


class LakeDataValidator(BaseDataValidator):
    """Data Lake 유효성 검증"""

    def __init__(
            self,
            domain: str,
            trd_dt: str,
            vendor: str = None,
            exchange_code: str = None,
            domain_group: str | None = None,
            allow_empty: bool = False,
            **kwargs,
    ):
        """
        ✅ Data Lake 구조:
          일반: /raw/{group}/{domain}/vendor=.../exchange_code=.../trd_dt=.../{domain}.jsonl
          fundamentals: /raw/{group}/{domain}/vendor=.../exchange_code=.../snapshot_dt=.../{symbol}.json
        """
        from plugins.config import constants as C

        vendor = (vendor or "unknown").lower()
        exchange_code = exchange_code or "ALL"
        if not domain_group:
            domain_group = C.DOMAIN_GROUPS.get(domain.lower(), "equity")

        if domain.lower() == "fundamentals":
            dataset_path = (
                    C.DATA_LAKE_ROOT
                    / "raw"
                    / domain_group
                    / domain
                    / f"vendor={vendor}"
                    / f"exchange_code={exchange_code}"
                    / f"trd_dt={trd_dt}"
            )
        else:
            dataset_path = (
                    C.DATA_LAKE_ROOT
                    / "raw"
                    / domain_group
                    / domain
                    / f"vendor={vendor}"
                    / f"exchange_code={exchange_code}"
                    / f"trd_dt={trd_dt}"
                    / f"{domain}.jsonl"
            )


        super().__init__(
            domain=domain,
            domain_group=domain_group,
            layer="lake",
            trd_dt=trd_dt,
            dataset_path=dataset_path,
            exchange_code=exchange_code,
            vendor=vendor,
            allow_empty=allow_empty,
            **kwargs,
        )

        self.domain_group = domain_group
