from typing import List, Dict
import gc
from plugins.hooks.eodhd_hook import EODHDHook
from plugins.pipelines.base_equity_pipeline import BaseEquityPipeline
from plugins.utils.symbol_loader import load_symbols_from_datalake_pd

class FundamentalPipeline(BaseEquityPipeline):
    """
    📊 EODHD Fundamentals 데이터 파이프라인
    - 거래소별 종목 목록 기반 개별 종목 펀더멘털 수집
    - 메모리 사용 최소화를 위해 Chunk 단위로 수집 및 파일 저장
    """

    def __init__(self, data_domain: str, exchange_code: str, trd_dt: str):
        super().__init__(data_domain=data_domain, exchange_code=exchange_code, trd_dt=trd_dt)
        self.hook = EODHDHook()


    # ---------------------------------
    # 2️⃣ 단일 종목 수집
    # ---------------------------------
    def _fetch_single_fundamental(self, symbol: str) -> Dict:
        """
        개별 종목 Fundamentals 호출
        """
        try:
            data = self.hook.get_fundamentals(symbol)
            if data:
                return data
        except Exception as e:
            self.log.warning(f"⚠️ {symbol} 수집 실패: {e}")
        return {}

    # ---------------------------------
    # 3️⃣ Load (티커별 JSON 저장)
    # ---------------------------------
    def load(self, records: List[Dict], **kwargs):
        """
        펀더멘털 데이터를 티커 단위 JSON으로 저장
        """
        kwargs['partition_key_name'] = 'trd_dt'
        kwargs['geo_partition_key'] = kwargs.get('geo_partition_key', 'exchange_code')

        target_dir, base_metadata = self._get_lake_path_and_metadata(**kwargs)

        return self._write_records_to_lake(
            records=records,
            target_dir=target_dir,
            base_metadata=base_metadata,
            file_name=lambda r: r.get("General", {}).get("Code", "unknown") + ".json",
            is_multi_file=True,
        )

    # ---------------------------------
    # 4️⃣ Chunk 기반 Fetch + Load
    # ---------------------------------
    def fetch_and_load(self, **kwargs):
        exchange_code = kwargs.get("exchange_code")
        trd_dt = kwargs.get("trd_dt")

        # todo 추후 제거
        exchange_sample_symbols = {
            "US": ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "LABU", 'TMF'],
            "KO": ["005930", "000660", "035420", "068270", '069500'],
            "KQ": ["196170", "247540", "277810", "091990"],
        }

        df = load_symbols_from_datalake_pd(
            exchange_code=exchange_code,
            trd_dt=trd_dt,
            filter_dict={"Code": exchange_sample_symbols[exchange_code]}
        )

        CHUNK_SIZE = 100
        total_saved = 0

        self.log.info(f"📦 {exchange_code} 거래소 종목 {len(df):,}건 수집 시작 (chunk={CHUNK_SIZE})")

        symbols = df['Code'].tolist()

        for i in range(0, len(symbols), CHUNK_SIZE):
            batch = symbols[i:i + CHUNK_SIZE]
            results = []

            for sym in batch:
                symbol = f"{sym}.{exchange_code}"
                data = self._fetch_single_fundamental(symbol)
                if data:
                    results.append(data)

            if results:
                load_result = self.load(results, exchange_code=exchange_code, trd_dt=trd_dt)
                saved_count = load_result.get("count", len(results))
                total_saved += saved_count
                self.log.info(f"💾 Chunk {i//CHUNK_SIZE + 1} 저장 완료 ({saved_count}건)")

            del results
            gc.collect()

        self.log.info(f"✅ {exchange_code} 거래소 총 {total_saved:,}건 펀더멘털 수집 및 저장 완료")
        return {"record_count": total_saved}

    # ---------------------------------
    # 5️⃣ Validate (필수 섹션 검증)
    # ---------------------------------
    def validate(self, **kwargs) -> bool:
        """
        저장된 펀더멘털 파일의 필수 구조 검증
        """
        records = self._read_records_from_lake(**kwargs)

        if not records:
            raise ValueError(f"No fundamental files found for {kwargs.get('exchange_code')}")

        required_sections = ['General', 'Highlights', 'Financials']
        for i, rec in enumerate(records):
            for section in required_sections:
                if section not in rec or not isinstance(rec.get(section), (dict, list)):
                    raise ValueError(f"Missing or invalid section '{section}' in record {i}")

            code = rec.get("General", {}).get("Code")
            if not code:
                raise ValueError(f"Invalid ticker code in record {i}")

        self.log.info(f"✅ Fundamental validation passed for {len(records)} tickers.")
        return True
