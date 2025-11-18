from plugins.pipelines.lake.equity.fundamental_pipeline import FundamentalPipeline
from plugins.config import constants as C
import json

class NewListingFundamentalCollector:

    def __init__(self, exchange_code: str, trd_dt: str):
        self.exchange_code = exchange_code
        self.trd_dt = trd_dt

    def read_new_tickers(self) -> list:
        path = (
            C.DATA_MONITORING_ROOT
            / "new_listing"
            / "candidates"
            / f"trd_dt={self.trd_dt}"
            / "new_tickers.json"
        )
        if not path.exists():
            raise FileNotFoundError(f"new_tickers.json not found: {path}")

        with open(path, "r") as f:
            return json.load(f)

    def run(self):
        tickers = self.read_new_tickers()
        print(f"📦 신규상장 후보: {tickers}")

        pipeline = FundamentalPipeline(
            domain="fundamentals",
            exchange_code=self.exchange_code,
            trd_dt=self.trd_dt,
            domain_group=C.DOMAIN_GROUPS["equity"]
        )

        # 핵심: batch_symbols 로 전달하기만 하면 끝
        result = pipeline.fetch_and_load(batch_symbols=tickers)

        print(f"🎯 fundamentals 수집 완료: {result}")
        return result
