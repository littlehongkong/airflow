from dataclasses import dataclass
from datetime import timedelta

import json
import pandas as pd
import logging

from plugins.utils.path_manager import DataPathResolver
from plugins.utils.loaders.warehouse.asset_master_loader import load_asset_master_latest

log = logging.getLogger(__name__)


@dataclass
class NewListingCandidateExtractorPipeline:
    trd_dt: str                      # YYYY-MM-DD
    country_code: str               # USA / KOR
    domain_group: str = "equity"

    def _get_prev_trading_day(self) -> str:
        """
        간단한 이전 거래일 계산
        - 기준: 주말(토/일)만 휴일로 가정
        - 필요 시 나중에 country별 휴일 캘린더로 교체
        """
        current_dt = pd.to_datetime(self.trd_dt)
        prev_dt = current_dt - timedelta(days=1)

        # 토(5), 일(6)이면 평일까지 뒤로 이동
        while prev_dt.weekday() >= 5:
            prev_dt -= timedelta(days=1)

        return prev_dt.strftime("%Y-%m-%d")


    def _load_asset_master(self, dt: str) -> pd.DataFrame:
        """Warehouse Asset Master 로드"""
        try:
            df = load_asset_master_latest(
                country_code=self.country_code,
                domain_group=self.domain_group
            )
            assert not df.empty, f"❌ asset_master가 비어있음: {dt}"
            return df
        except Exception as e:
            log.error(f"❌ Asset master load 실패: {e}")
            return pd.DataFrame()

    def detect_candidates(self):
        """warehouse asset_master 기반 신규 상장 탐지"""

        # 1) 오늘 snapshot
        today_df = self._load_asset_master(self.trd_dt)

        # 2) 전일 snapshot (휴일 조정 포함)
        prev_dt = self._get_prev_trading_day()
        yesterday_df = self._load_asset_master(prev_dt)

        # 3) 신규 security_id = today - yesterday
        existing_ids = set(yesterday_df["security_id"])
        today_ids = set(today_df["security_id"])

        new_ids = today_ids - existing_ids
        if not new_ids:
            return []

        # 신규 entries 추출
        candidates = today_df[today_df["security_id"].isin(new_ids)]
        return candidates.to_dict(orient="records")

    def _save(self, candidates):
        """DataPathResolver로 모니터링 경로에 저장"""

        out_dir = DataPathResolver.warehouse_monitoring(
            domain_group=self.domain_group,
            category="new_listing",
            country_code=self.country_code,
            trd_dt=self.trd_dt,
        )
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / "candidates.jsonl"

        with open(out_file, "w", encoding="utf-8") as f:
            for row in candidates:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

        log.info(f"📦 신규상장 {len(candidates)}건 저장 → {out_file}")
        return str(out_file)

    def run(self, context=None, **kwargs):
        candidates = self.detect_candidates()
        out_path = self._save(candidates)

        return {
            "record_count": len(candidates),
            "validated_path": out_path,
            "country_code": self.country_code,
            "trd_dt": self.trd_dt,
            "candidates": candidates,
        }
