# plugins/pipelines/fundamental_pipeline.py
from typing import Any, Dict, List
import math
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
        tickers = kwargs.get("tickers", ['TSLA.US', 'AAPL.US', 'TMF.US', 'LABU.US'])
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
        전체 파이프라인 실행
        - API 호출(fetch)
        - 배치 단위 JSONL 저장(load)
        """

        self.log.info(f"🚀 펀더멘털 수집 파이프라인 시작 ({self.exchange_code}, {self.trd_dt})")

        try:
            records, meta = self.fetch(**kwargs)
            if not records:
                return {
                    "exchange_code": self.exchange_code,
                    "data_domain": self.data_domain,
                    "trd_dt": self.trd_dt,
                    "status": "skipped",
                    "record_count": 0,
                }

            kwargs["meta"] = meta
            self.load(records, **kwargs)
            status = "success"
        except Exception as e:
            self.log.error(f"❌ 펀더멘털 파이프라인 실패: {e}")
            status = "failed"
            records = []

        return {
            "exchange_code": self.exchange_code,
            "data_domain": self.data_domain,
            "trd_dt": self.trd_dt,
            "status": status,
            "record_count": len(records),
        }
        #
        #
        # records, meta = self.fetch(**kwargs)
        # if not records:
        #     self.log.warning("⚠️ 수집된 데이터가 없습니다. 적재를 건너뜁니다.")
        #     return
        #
        # kwargs['meta'] = meta
        # self.load(records, **kwargs)
        self.log.info(f"🎯 펀더멘털 파이프라인 완료 ({self.exchange_code}, {self.trd_dt})")
