from plugins.pipelines.lake.equity.fundamental_pipeline import FundamentalPipeline
from plugins.utils.path_manager import DataPathResolver
from plugins.config import constants as C
import json


class NewListingFundamentalCollector:

    def __init__(self, trd_dt: str, country_code: str, domain_group: str):
        self.trd_dt = trd_dt
        self.country_code = country_code
        self.domain_group = domain_group

    # -----------------------------------------------------------
    # 1) 신규상장 후보 ticker 목록 읽기
    # -----------------------------------------------------------
    def read_new_tickers(self) -> list:
        path = DataPathResolver.warehouse_monitoring(
            country_code=self.country_code,
            trd_dt=self.trd_dt,
            category=C.EVENT_CATEGORIES["new_listing"],
            domain_group=self.domain_group
        )

        full_path = path / "candidates.jsonl"

        if not full_path.exists():
            raise FileNotFoundError(f"candidates.jsonl not found: {full_path}")

        records = []
        with open(full_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                records.append(json.loads(line))

        return records

    # -----------------------------------------------------------
    # 2) 신규상장 fundamentals 수집 실행
    # -----------------------------------------------------------
    def run(self, context=None, **kwargs):
        candidates = self.read_new_tickers()

        grouped = {}
        for item in candidates:
            tkr = item["ticker"]
            exch = item["exchange_code"]
            country = item["country_code"]

            if country == "USA":
                exch = "US"

            grouped.setdefault(exch, []).append(tkr)

        return grouped