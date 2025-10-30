# plugins/pipelines/fundamental_pipeline.py
from typing import Dict, List
import math
import json
from pathlib import Path
from plugins.hooks.eodhd_hook import EODHDHook
from plugins.pipelines.base_equity_pipeline import BaseEquityPipeline


class FundamentalPipeline(BaseEquityPipeline):
    """
    펀더멘털(Fundamentals) 데이터 수집 및 적재 파이프라인
    --------------------------------------------------------
    ✅ 다른 파이프라인(ExchangeInfoPipeline 등)과 동일한 구조 유지
    ✅ 단, 종목별 호출 및 배치 단위 저장(batch_0001.jsonl 등)만 차별화
    """

    def __init__(self, data_domain: str, exchange_code: str, trd_dt: str):
        super().__init__(data_domain, exchange_code, trd_dt)
        self.hook = EODHDHook()
        self.batch_size = 40  # ✅ 배치 단위 저장 (40종목씩)

    # ------------------------------------------------------------------
    # ✅ 1️⃣ Fetch
    # ------------------------------------------------------------------
    def fetch(self, **kwargs):
        tickers = kwargs.get("tickers")

        assert tickers, '⚠ 수집할 종목 목록이 존재하지 않습니다.'

        all_records = []
        source_meta = None

        for ticker in tickers:
            try:
                data = self.hook.get_fundamentals(
                    symbol=ticker
                )
                records, meta = self._standardize_fetch_output(data)  # ✅ 언패킹
                all_records.extend(records)  # ✅ flatten list[dict]
                source_meta = meta  # 마지막 메타만 기록 (혹은 병합 가능)

            except Exception as e:
                self.log.error(f"❌ {ticker} 펀더멘털 수집 실패: {e}")
                continue

        self.log.info(f"✅ 펀더멘털 수집 완료: 총 {len(all_records)}건")
        return all_records, source_meta

    # ------------------------------------------------------------------
    # ✅ 2️⃣ Load
    # ------------------------------------------------------------------
    def load(self, records: List[Dict], **kwargs):
        """
        수집된 펀더멘털 데이터를 배치 단위로 JSON Lines로 적재합니다.
        다른 파이프라인과 동일한 구조를 유지하며, 파일명만 batch_*.jsonl로 구분합니다.
        """

        if not records:
            self.log.warning("⚠️ 적재할 데이터가 없습니다.")
            return

        # 🔹 공통 메타데이터 및 저장 경로 확보
        kwargs["partition_key_name"] = kwargs.get("partition_key_name", "trd_dt")
        kwargs["geo_key_name"] = kwargs.get("geo_key_name", "exchange_code")

        target_dir, base_metadata = self._get_lake_path_and_metadata(**kwargs)

        total_records = len(records)
        total_batches = math.ceil(total_records / self.batch_size)

        for i in range(total_batches):
            batch_records = records[i * self.batch_size : (i + 1) * self.batch_size]
            file_name = f"batch_{i + 1:04d}.jsonl"

            self.log.info(f"{file_name} >> {batch_records}")

            self._write_records_to_lake(
                records=batch_records,
                target_dir=target_dir,
                base_metadata=base_metadata,
                file_name=file_name,
            )

        meta = kwargs.get("meta", {}) or {}

        # 🔹 _source_meta.json 저장
        self._save_source_meta(
            target_dir=target_dir,
            record_count=total_records,
            source_meta={**meta, "batches": total_batches}
        )

        self.log.info(f"✅ 펀더멘털 데이터 {total_batches}개 배치 저장 완료 ({total_records}건)")

    # ------------------------------------------------------------------
    # ✅ 3️⃣ Fetch + Load 통합 실행
    # ------------------------------------------------------------------
    def fetch_and_load(self, **kwargs):
        """
        펀더멘털 데이터 수집 및 적재 (배치 종목 기반)
        - Airflow Dynamic Task로 전달된 batch_symbols만 처리
        - 이미 수집된 종목 제외 (증분 수집)
        - mode='new_listing' 시 향후 확장 가능
        """

        self.log.info(f"🚀 펀더멘털 수집 파이프라인 시작 ({self.exchange_code}, {self.trd_dt})")

        # ----------------------------------------------------------------------
        # ✅ 0️⃣ 필수 파라미터 확인
        # ----------------------------------------------------------------------
        exchange_code = kwargs.get("exchange_code", self.exchange_code)
        if not exchange_code:
            raise ValueError("exchange_code 값이 없습니다 (예: exchange_code=KO)")
        self.exchange_code = exchange_code

        batch_symbols = kwargs.get("batch_symbols")
        if not batch_symbols:
            raise ValueError("batch_symbols 값이 없습니다 — Dynamic Task에서 전달되어야 합니다.")
        self.log.info(f"📦 전달받은 배치 종목 수: {len(batch_symbols):,}")

        # ✅ load 호출 시에도 명시적으로 전달
        kwargs["exchange_code"] = self.exchange_code

        # ----------------------------------------------------------------------
        # ✅ 1️⃣ 종목 로드 제거 (→ 전달받은 batch_symbols만 사용)
        # ----------------------------------------------------------------------
        mode = kwargs.get("mode", "incremental")

        # 기존 종목(이미 수집된 종목) 조회
        target_dir, base_metadata = self._get_lake_path_and_metadata(**kwargs)
        existing_symbols = self._get_existing_symbols(target_dir)

        # ----------------------------------------------------------------------
        # ✅ 2️⃣ 대상 심볼 결정
        # ----------------------------------------------------------------------
        if mode == "new_listing":
            self.log.info("✨ 신규상장 전용 모드 활성화 — 전달받은 종목 중 미수집된 종목만 수집")
            pending_symbols = [s for s in batch_symbols if s not in existing_symbols]
        else:
            pending_symbols = [s for s in batch_symbols if s not in existing_symbols]

        if not pending_symbols:
            self.log.info(f"✅ 전달받은 {len(batch_symbols)}개 종목 중 모두 이미 수집됨 — 스킵")
            return {"status": "skipped", "records": 0}

        self.log.info(f"📈 수집 대상: {len(pending_symbols)} / {len(batch_symbols)} 종목")

        # ----------------------------------------------------------------------
        # ✅ 3️⃣ API 호출 및 배치 단위 적재
        # ----------------------------------------------------------------------
        batch_index = kwargs.get("batch_index")
        if batch_index is not None:
            batch_name = f"batch_{int(batch_index):04d}.jsonl"
        else:
            batch_name = "batch_dynamic.jsonl"  # fallback (단일 실행 시)

        total_records = 0
        total_batches = 0

        batch_records = []
        for sym in pending_symbols:
            try:
                data = self.hook.get_fundamentals(symbol=f"{sym}.{exchange_code}")
                if not data:
                    self.log.warning(f"⚠️ {sym} 데이터 없음 — 건너뜀")
                    continue

                recs, meta = self._standardize_fetch_output(data)
                batch_records.extend(recs)
            except Exception as e:
                self.log.error(f"❌ {sym} 수집 중 오류: {e}")
                continue

        if not batch_records:
            self.log.warning(f"⚠️ {len(pending_symbols)}개 종목 모두 데이터 없음 — 배치 스킵")
            return {"status": "skipped", "records": 0}

        # 기존 파일이 있으면 삭제
        batch_path = target_dir / batch_name
        if batch_path.exists():
            batch_path.unlink()
            self.log.info(f"🧹 기존 배치 파일 삭제 후 새로 저장: {batch_name}")

        self._write_records_to_lake(
            records=batch_records,
            target_dir=target_dir,
            base_metadata=base_metadata,
            file_name=batch_name,
            mode="overwrite",
        )

        total_records += len(batch_records)
        total_batches += 1
        self.log.info(f"✅ 펀더멘털 데이터 {total_batches}개 배치 저장 완료 ({len(batch_records)}건)")

        # ----------------------------------------------------------------------
        # ✅ 4️⃣ 원천 메타 저장
        # ----------------------------------------------------------------------
        self._save_source_meta(
            target_dir=target_dir,
            record_count=total_records,
            source_meta={
                "vendor": "EODHD",
                "endpoint": "api/fundamentals",
                "batches": total_batches,
                "mode": mode,
                "batch_symbols": len(batch_symbols),
                "pending_symbols": len(pending_symbols),
            },
        )

        self.log.info(
            f"🎯 펀더멘털 파이프라인 완료 ({self.exchange_code}, {self.trd_dt}) - "
            f"{total_records:,}건 / {total_batches}개 배치"
        )

        return {"status": "success", "records": total_records, "batches": total_batches}


    # -------------------------------------------------------------------
    def _get_existing_symbols(self, target_dir: Path) -> set[str]:
        """
        이미 수집된 종목코드를 기존 JSONL 파일에서 추출
        """
        symbols = set()
        if not target_dir.exists():
            return symbols

        for f in target_dir.glob("batch_*.jsonl"):
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    for line in fp:
                        rec = json.loads(line)
                        code = rec.get("General", {}).get("Code")
                        if code:
                            symbols.add(code)
            except Exception as e:
                self.log.warning(f"⚠️ {f} 읽기 오류: {e}")
        self.log.info(f"🧭 기존 수집 종목 {len(symbols):,}건 확인됨")
        return symbols