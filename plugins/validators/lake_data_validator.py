"""
LakeDataValidator
---------------------------------------
🌊 Data Lake 전용 Validator
- BaseDataValidator 기반, DataPathResolver를 통해 경로 자동 세팅
"""

from plugins.validators.base_data_validator import BaseDataValidator
from plugins.utils.path_manager import DataPathResolver


class LakeDataValidator(BaseDataValidator):
    """🌊 Data Lake 유효성 검증"""

    def __init__(
        self,
        domain: str,
        trd_dt: str,
        vendor: str | None = None,
        exchange_code: str | None = None,
        domain_group: str | None = None,
        allow_empty: bool = False,
        **kwargs,
    ):
        """
        ✅ Data Lake 구조 (자동 처리됨)
          ├─ raw/{group}/{domain}/vendor=.../exchange_code=.../trd_dt=...
          └─ validated/{group}/{domain}/vendor=.../exchange_code=.../trd_dt=...
        """
        vendor = (vendor or "unknown").lower()
        exchange_code = exchange_code or "ALL"
        domain_group = domain_group or "equity"

        assert vendor != 'unknwon', 'vendor 변수에 값을 할당해주세요'
        if domain not in ['exchange_list']:
            assert exchange_code != 'ALL', 'exchange_code 변수에 값을 할당해주세요'

        # ✅ BaseDataValidator는 DataPathResolver를 통해 dataset_path 자동 설정
        super().__init__(
            domain=domain,
            domain_group=domain_group,
            layer="lake",
            trd_dt=trd_dt,
            vendor=vendor,
            exchange_code=exchange_code,
            allow_empty=allow_empty,
            **kwargs,
        )
