from plugins.validators.base_data_validator import BaseDataValidator
from plugins.config import constants as C
import pandas as pd
import json
from pathlib import Path


class ExchangeDetailValidator(BaseDataValidator):
    """
    📅 Exchange Detail Validator
    - DataPathResolver 기반 in/out 경로 자동화
    - nested dict 필드(JSON 직렬화) 처리
    """

    def __init__(
        self,
        domain: str,
        domain_group: str,
        trd_dt: str,
        exchange_code: str,
        vendor: str,
        allow_empty: bool = False,
        **kwargs,
    ):
        """
        Data Lake 구조 예시:
        /opt/airflow/data/data_lake/raw/equity/exchange_detail/
            vendor=eodhd/exchange_code=US/trd_dt=2025-11-11/exchange_detail.jsonl
        """
        self.allow_empty = allow_empty
        self.vendor = vendor.lower()
        self.exchange_code = exchange_code
        self.domain_group = domain_group
        self.domain = domain

        # ✅ 원천(raw) 경로
        dataset_path = (
            C.DATA_LAKE_ROOT
            / "raw"
            / domain_group
            / domain
            / f"vendor={self.vendor}"
            / f"exchange_code={exchange_code}"
            / f"trd_dt={trd_dt}"
            / f"{domain}.jsonl"
        )

        # ✅ 검증결과(validated) 경로는 BaseDataValidator에서 자동 생성
        super().__init__(
            domain=domain,
            domain_group=domain_group,
            layer="lake",
            trd_dt=trd_dt,
            vendor=self.vendor,
            exchange_code=exchange_code,
            allow_empty=allow_empty,
            **kwargs,
        )

        self.dataset_path = dataset_path  # 원천 JSONL 파일 경로
        self.log.info(f"📦 ExchangeHolidayValidator dataset_path: {self.dataset_path}")

    # -------------------------------------------------------------------------
    # 1️⃣ JSONL 로더
    # -------------------------------------------------------------------------
    def _load_dataset(self) -> pd.DataFrame:
        """
        ✅ JSONL → DataFrame 로드
        - 빈 파일/디렉터리 예외처리
        - nested dict → 문자열 변환
        """
        try:
            # 📁 dataset_path가 디렉토리면 실제 파일 찾기
            if Path(self.dataset_path).is_dir():
                candidate = list(Path(self.dataset_path).glob("*.jsonl"))
                if not candidate:
                    self.log.error(f"🚫 No JSONL found under {self.dataset_path}")
                    return pd.DataFrame()
                self.dataset_path = candidate[0]  # 첫 번째 파일 선택

            if not Path(self.dataset_path).exists():
                self.log.error(f"🚫 Missing source file: {self.dataset_path}")
                return pd.DataFrame()

            self.log.info(f"📂 Loading JSONL file → {self.dataset_path}")
            df = pd.read_json(self.dataset_path, lines=True)

        except Exception as e:
            self.log.error(f"❌ Failed to read JSONL: {e}")
            return pd.DataFrame()

        if df.empty:
            self.log.warning(f"⚠️ Empty data for {self.exchange_code}")
            return df

        df = self._normalize_nested_fields(df)
        return df

    # -------------------------------------------------------------------------
    # 2️⃣ Nested dict → 문자열 직렬화
    # -------------------------------------------------------------------------
    def _normalize_nested_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PyArrow Parquet 저장 시 struct 타입 충돌 방지
        → nested dict를 JSON 문자열로 직렬화
        """
        nested_cols = ["ExchangeHolidays", "ExchangeEarlyCloseDays", "TradingHours"]
        for col in nested_cols:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: (
                        json.dumps(x if x else {}, ensure_ascii=False)
                        if isinstance(x, (dict, list))
                        else x
                    )
                )
        return df
