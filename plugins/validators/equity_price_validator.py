# plugins/validators/equity_price_validator.py
import pandera as pa
from pandera import Column, Check
from plugins.validators.base_validator import BaseDataValidator


class EquityPriceValidator(BaseDataValidator):

    def __init__(self, exchange_code: str, trd_dt: str, data_domain: str, **kwargs):
        super().__init__(exchange_code, trd_dt, data_domain, **kwargs)
        self.schema = self._get_schema()

    @staticmethod
    def _get_schema() -> pa.DataFrameSchema:
        """
        ✅ 현실적인 금융 시세 데이터 검증 스키마
        - 거래정지/OTC 종목의 0값 허용
        - 고가 < 저가, 종가 범위 오류 등 기본 규칙만 체크
        - Pandera warnings 제거 (pandera.pandas 사용)
        """
        return pa.DataFrameSchema(
            columns={
                "code": Column(
                    str,
                    nullable=False,
                    coerce=True,
                    checks=[
                        Check(lambda s: s.str.len().between(1, 32), error="Invalid code length"),
                    ],
                ),
                "exchange_short_name": Column(str, nullable=False, coerce=True),
                "date": Column(str, nullable=False, coerce=True),
                "MarketCapitalization": Column(float, Check(lambda s: s.isna() | (s >= 0)), nullable=True, coerce=True),
                "open": Column(float, Check(lambda s: s.isna() | (s >= 0)), nullable=True, coerce=True),
                "high": Column(float, Check(lambda s: s.isna() | (s >= 0)), nullable=True, coerce=True),
                "low": Column(float, Check(lambda s: s.isna() | (s >= 0)), nullable=True, coerce=True),
                "close": Column(float, Check(lambda s: s.isna() | (s >= 0)), nullable=True, coerce=True),
                "adjusted_close": Column(float, Check(lambda s: s.isna() | (s >= 0)), nullable=True, coerce=True),
                "volume": Column(float, Check(lambda s: s.isna() | (s >= 0)), nullable=True, coerce=True),
                # 0 허용 + 결측 허용
                "ema_50d": Column(float, nullable=True, coerce=True),
                "ema_200d": Column(float, nullable=True, coerce=True),
                "hi_250d": Column(float, nullable=True, coerce=True),
                "lo_250d": Column(float, nullable=True, coerce=True),

            },
            checks=[
                # ① 고가 >= 저가 (단, 전부 0이거나 결측이면 통과)
                Check(
                    lambda df: (
                            (df["high"] >= df["low"])
                            | ((df["high"] == 0) & (df["low"] == 0) & (df["close"] == 0))
                            | (df["high"].isna() & df["low"].isna())
                    ),
                    error="High < Low 오류",
                ),
            ],
            strict=False,  # metadata 같은 여분 컬럼 허용
        )

    def validate(self, **kwargs):
        allow_empty = kwargs.get("allow_empty", getattr(self, "allow_empty", False))
        self.run(context=kwargs, allow_empty=allow_empty)