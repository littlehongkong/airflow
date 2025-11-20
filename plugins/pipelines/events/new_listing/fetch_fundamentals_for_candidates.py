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
    # 🔹 신규상장 후보 ticker 목록 읽기 (PathResolver 사용)
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
    # 🔹 fundamentals 수집 실행
    # -----------------------------------------------------------
    def run(self, context=None, **kwargs):
        candidates = self.read_new_tickers()
        print(f"📦 신규상장 후보({self.country_code}): {candidates}")

        # ----------------------------------------------------
        # 🔥 신규상장 파라미터 → EODHD 서비스 호출용 symbol 변환
        #   - 미국(USA) → exchange_code = "US" 강제
        #   - 비미국 → 원천 exchange_code 그대로
        # ----------------------------------------------------
        batch_symbols = []
        grouped = {}

        for item in candidates:
            ticker = item["ticker"]
            country = item["country_code"]
            exchange = item["exchange_code"]

            # 미국 예외 처리
            if country == "USA":
                exchange = "US"

            batch_symbols.append(ticker)
            grouped.setdefault(exchange, []).append(ticker)

        print(f"📡 fundamentals 수집 대상 심볼: {batch_symbols}")

        # ----------------------------------------------------
        # 🔥 FundamentalPipeline 실행
        #   exchange_code는 개별 symbol 안에 들어있으므로 제거
        # ----------------------------------------------------
        all_results = {}

        for ex_code, tickers in grouped.items():
            pipeline = FundamentalPipeline(
                domain="fundamentals",
                exchange_code=ex_code,  # 🔥 핵심!
                trd_dt=self.trd_dt,
                domain_group=C.DOMAIN_GROUPS["equity"],
                allow_empty=False
            )

            # symbol 형태로 변환
            batch_symbols = [f"{t}" for t in tickers]

            print(f"📡 호출 → exchange={ex_code}, symbols={batch_symbols}")

            result = pipeline.fetch_and_load(batch_symbols=batch_symbols)
            all_results[ex_code] = result

        print(f"🎯 fundamentals 수집 완료: {all_results}")
        return all_results