# plugins/pipelines/fundamental_pipeline.py

import json
from pathlib import Path
from plugins.hooks.eodhd_hook import EODHDHook
from plugins.pipelines.base_equity_pipeline import BaseEquityPipeline
from plugins.utils.loaders.lake.exchange_loader import load_exchange_list
from plugins.utils.loaders.lake.symbol_loader import load_symbol_list
from plugins.config import constants as C
from plugins.utils.path_manager import DataPathResolver


class FundamentalPipeline(BaseEquityPipeline):
    """
    펀더멘털(Fundamentals) 데이터 수집 및 적재 파이프라인
    --------------------------------------------------------
    schedule 모드 → exchange 전체 심볼 수집
    event 모드 → candidates.jsonl 기반 신규상장 심볼만 수집
    """

    def __init__(self, domain: str, exchange_code: str, trd_dt: str,
                 domain_group: str = None, allow_empty: bool = False, mode: str="schedule"):
        super().__init__(domain, exchange_code, trd_dt,
                         domain_group=domain_group, allow_empty=allow_empty)
        self.mode = mode
        self.hook = EODHDHook()

    # -----------------------------------------------------------
    # 단일 종목 fundamentals fetch
    # -----------------------------------------------------------
    def fetch(self, **kwargs):
        symbol = kwargs.get('symbol')
        data = self.hook.get_fundamentals(symbol=symbol)
        return data

    # -----------------------------------------------------------
    # Fetch + Load 통합 로직
    # -----------------------------------------------------------
    def fetch_and_load(self, **kwargs):
        self.log.info(f"🚀 Fundamentals 파이프라인 시작 ({self.exchange_code}, mode={self.mode}, {self.trd_dt})")

        exchange_code = kwargs.get("exchange_code", self.exchange_code)
        trd_dt = kwargs.get("trd_dt", self.trd_dt)
        self.exchange_code = exchange_code

        # -----------------------------------------------------------
        # 거래소 → 국가 매핑
        # -----------------------------------------------------------
        exchange_df = load_exchange_list(
            domain_group=self.domain_group,
            vendor=C.VENDORS['eodhd'],
            trd_dt=self.trd_dt
        )

        filter_df = exchange_df[exchange_df['Code'] == self.exchange_code]
        assert not filter_df.empty, f"❌ 국가코드를 찾지 못했습니다. exchange_code={self.exchange_code}"

        country_code = filter_df['CountryISO3'].values[0]

        # -----------------------------------------------------------
        # 1) 대상 종목 결정 (event vs schedule)
        # -----------------------------------------------------------
        batch_symbols = kwargs.get("batch_symbols")

        # 🎯 우선순위: 수동으로 batch_symbols 전달된 경우
        if batch_symbols:
            self.log.info(f"📦 batch_symbols 전달됨: {len(batch_symbols)}개")
            symbols_to_process = [s.strip().upper() for s in batch_symbols if s]

        # 🎯 event 모드 → candidates.jsonl 기반 신규상장 ticker만 수집
        elif self.mode == "event":
            self.log.info(f"[event mode] 신규상장 ticker만 수집 (exchange={self.exchange_code})")

            candidates_path = DataPathResolver.warehouse_monitoring(
                country_code=country_code,
                trd_dt=trd_dt,
                category=C.EVENT_CATEGORIES["new_listing"],
                domain_group=self.domain_group,
            ) / "candidates.jsonl"

            if not candidates_path.exists():
                raise FileNotFoundError(f"[event mode] candidates.jsonl 없음: {candidates_path}")

            tickers = []
            with open(candidates_path, "r") as f:
                for line in f:
                    row = json.loads(line)

                    # 미국인 경우 exchange 단일화 처리
                    exch = row["exchange_code"]
                    if row["country_code"] == "USA":
                        exch = "US"

                    if exch == self.exchange_code:
                        tickers.append(row["ticker"])

            if not tickers:
                self.log.info(f"🟦 event 모드: 신규상장 종목 없음 → skip")
                return {"status": "skipped", "records": 0}

            symbols_to_process = [t.upper() for t in tickers]

        # 🎯 schedule 모드 → symbol_list 전체 로드
        else:
            self.log.info("📦 schedule 모드: symbol_list.parquet 자동 로드")

            df = load_symbol_list(
                exchange_codes=[exchange_code],
                trd_dt=trd_dt,
                vendor=C.VENDORS["eodhd"],
                domain_group=C.DOMAIN_GROUPS["equity"],
                exclude_field="Exchange",
                exclude_values=C.EXCLUDED_EXCHANGES_BY_COUNTRY[country_code],
            )

            symbols_to_process = (
                df["Code"].dropna().astype(str).str.upper().unique().tolist()
            )

            self.log.info(f"📊 {exchange_code} 전체 {len(symbols_to_process):,}개 심볼 로드 완료")

        # -----------------------------------------------------------
        # 2) 이미 수집된 파일 제외
        # -----------------------------------------------------------
        kwargs["partition_key_name"] = kwargs.get("partition_key_name", "trd_dt")
        kwargs["geo_key_name"] = kwargs.get("geo_key_name", "exchange_code")

        target_dir, base_metadata = self._get_lake_path_and_metadata(**kwargs)

        existing_symbols = self._get_existing_symbols(target_dir)
        symbols_to_fetch = [s for s in symbols_to_process if s not in existing_symbols]

        if not symbols_to_fetch:
            self.log.info("🟦 모든 종목이 이미 수집됨 → skip")
            return {"status": "skipped", "records": 0}

        # -----------------------------------------------------------
        # 3) 종목별 EODHD 호출 & 저장
        # -----------------------------------------------------------
        total_saved = 0

        for sym in symbols_to_fetch:
            try:
                data = self.fetch(symbol=f"{sym}.{exchange_code}")

                if not data:
                    self.log.warning(f"⚠️ {sym} 데이터 없음 → 건너뜀")
                    continue

                # 표준화
                records, meta = self._standardize_fetch_output(data)
                if not records:
                    continue

                # 저장
                file_path = target_dir / f"{sym}.json"
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(records[0], f, ensure_ascii=False)

                total_saved += 1

            except Exception as e:
                self.log.error(f"❌ {sym} 수집 실패: {e}")
                continue

        # -----------------------------------------------------------
        # 4) 메타 저장
        # -----------------------------------------------------------
        self._save_source_meta(
            target_dir=target_dir,
            record_count=total_saved,
            source_meta={
                "vendor": "EODHD",
                "endpoint": "api/fundamentals",
                "exchange_code": exchange_code,
                "symbols_collected": total_saved,
                "symbols_total": len(symbols_to_process),
                "existing_symbols": len(existing_symbols),
            },
        )

        self.log.info(
            f"🎯 Fundamentals 완료 ({exchange_code}, {self.trd_dt}) - "
            f"{total_saved:,}/{len(symbols_to_process):,} symbols"
        )

        return {"status": "success", "records": total_saved}

    # -----------------------------------------------------------
    # 이미 저장된 파일 체크
    # -----------------------------------------------------------
    def _get_existing_symbols(self, target_dir: Path) -> set[str]:
        symbols = set()
        if not target_dir.exists():
            return symbols

        for f in target_dir.glob("*.json"):
            symbols.add(f.stem.upper())

        self.log.info(f"🧭 기존 수집 종목 {len(symbols):,}건 확인됨")
        return symbols
