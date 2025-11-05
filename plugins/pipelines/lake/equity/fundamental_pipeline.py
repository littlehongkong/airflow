# plugins/pipelines/fundamental_pipeline.py

from typing import Dict, List
import json
from pathlib import Path
from plugins.hooks.eodhd_hook import EODHDHook
from plugins.pipelines.base_equity_pipeline import BaseEquityPipeline
from plugins.config.constants import DOMAIN_GROUPS


class FundamentalPipeline(BaseEquityPipeline):
    """
    펀더멘털(Fundamentals) 데이터 수집 및 적재 파이프라인
    --------------------------------------------------------
    ✅ DAG에서 전달받은 종목(batch_symbols)만 수집
    ✅ 구조: /exchange_code=KR/snapshot_dt=YYYY-MM-DD/{symbol}.json
    """

    def __init__(self, domain: str, exchange_code: str, trd_dt: str, domain_group: str = None):
        super().__init__(domain, exchange_code, trd_dt, domain_group=domain_group or DOMAIN_GROUPS.get(domain, "equity"))
        self.hook = EODHDHook()


    def fetch(self, **kwargs):
        symbol = kwargs.get('symbol')
        data = self.hook.get_fundamentals(symbol=symbol)
        return data


    # ------------------------------------------------------------------
    # ✅ Fetch + Load 통합 실행 (DAG에서 종목 전달)
    # ------------------------------------------------------------------
    def fetch_and_load(self, **kwargs):
        """
        Fundamentals 수집 및 저장
        - DAG에서 batch_symbols가 전달되면 해당 종목만 수집
        - 전달되지 않으면 symbol_list.parquet을 자동 로드하여 수집
        """
        import pandas as pd
        from plugins.utils.symbol_loader import load_symbols_from_datalake_pd
        from plugins.config import constants as C

        self.log.info(f"🚀 Fundamentals 파이프라인 시작 ({self.exchange_code}, {self.trd_dt})")

        exchange_code = kwargs.get("exchange_code", self.exchange_code)
        self.exchange_code = exchange_code
        trd_dt = kwargs.get("trd_dt", self.trd_dt)

        # ----------------------------------------------------------------------
        # ✅ 1️⃣ 수집 대상 심볼 결정
        # ----------------------------------------------------------------------
        batch_symbols = kwargs.get("batch_symbols")

        if batch_symbols:
            self.log.info(f"📦 DAG에서 전달된 batch_symbols: {len(batch_symbols)}개")
            symbols_to_process = [s.strip().upper() for s in batch_symbols if s]
        else:
            self.log.info(f"📦 batch_symbols 미전달 → symbol_list.parquet 자동 로드")
            try:
                df = load_symbols_from_datalake_pd(
                    exchange_code=exchange_code,
                    trd_dt=trd_dt,
                    vendor=C.VENDORS["eodhd"],
                    domain_group=C.DOMAIN_GROUPS["equity"],
                )
                symbols_to_process = df["Code"].dropna().astype(str).str.upper().unique().tolist()
                self.log.info(f"📊 {exchange_code} 거래소에서 {len(symbols_to_process):,}개 종목 로드 완료")
            except Exception as e:
                raise FileNotFoundError(f"❌ symbol_list parquet 로드 실패: {e}")

        # ----------------------------------------------------------------------
        # ✅ 2️⃣ 저장 경로 및 기존 수집 종목 확인
        # ----------------------------------------------------------------------
        kwargs["partition_key_name"] = kwargs.get("partition_key_name", "trd_dt")
        kwargs["geo_key_name"] = kwargs.get("geo_key_name", "exchange_code")
        target_dir, base_metadata = self._get_lake_path_and_metadata(**kwargs)

        existing_symbols = self._get_existing_symbols(target_dir)
        symbols_to_fetch = [s for s in symbols_to_process if s not in existing_symbols]

        if not symbols_to_fetch:
            self.log.info("✅ 모든 종목이 이미 수집되어 있습니다 — 스킵")
            return {"status": "skipped", "records": 0}

        # ----------------------------------------------------------------------
        # ✅ 3️⃣ 종목별 수집 및 저장
        # ----------------------------------------------------------------------
        total_saved = 0
        for sym in symbols_to_fetch:
            try:
                data = self.fetch(symbol=f"{sym}.{exchange_code}")
                if not data:
                    self.log.warning(f"⚠️ {sym} 데이터 없음 — 건너뜀")
                    continue

                records, meta = self._standardize_fetch_output(data)
                if not records:
                    continue

                file_path = target_dir / f"{sym}.json"
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(records[0], f, ensure_ascii=False)

                total_saved += 1
            except Exception as e:
                self.log.error(f"❌ {sym} 수집 실패: {e}")
                continue

        # ----------------------------------------------------------------------
        # ✅ 4️⃣ 메타 저장
        # ----------------------------------------------------------------------
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
            f"🎯 Fundamentals 완료 ({exchange_code}, {self.trd_dt}) - {total_saved:,}/{len(symbols_to_process):,} symbols"
        )
        return {"status": "success", "records": total_saved}

    # ------------------------------------------------------------------
    # ✅ 기존 심볼 확인 (이미 저장된 파일 탐색)
    # ------------------------------------------------------------------
    def _get_existing_symbols(self, target_dir: Path) -> set[str]:
        symbols = set()
        if not target_dir.exists():
            return symbols

        for f in target_dir.glob("*.json"):
            symbols.add(f.stem.upper())

        self.log.info(f"🧭 기존 수집 종목 {len(symbols):,}건 확인됨")
        return symbols
