from dataclasses import dataclass
from datetime import timedelta

import json
import pandas as pd
import logging

from plugins.utils.path_manager import DataPathResolver
from plugins.utils.loaders.warehouse.asset_master_loader import load_asset_master

log = logging.getLogger(__name__)


@dataclass
class NewListingCandidateExtractorPipeline:
    trd_dt: str                      # YYYY-MM-DD
    country_code: str               # USA / KOR
    domain_group: str = "equity"

    def _get_prev_trading_day(self) -> str:
        """주말 보정 포함한 전 영업일 계산"""
        current_dt = pd.to_datetime(self.trd_dt)
        prev_dt = current_dt - timedelta(days=1)

        while prev_dt.weekday() >= 5:  # Sat=5, Sun=6
            prev_dt -= timedelta(days=1)

        return prev_dt.strftime("%Y-%m-%d")

    def _load_asset_master(self, dt: str) -> pd.DataFrame:
        """Warehouse Asset Master 로드"""
        try:
            df = load_asset_master(
                country_code=self.country_code,
                domain_group=self.domain_group,
                trd_dt=dt
            )
            assert not df.empty, f"❌ asset_master가 비어있음: {dt}"
            return df

        except Exception as e:
            log.error(f"❌ Asset master load 실패: {e}")
            return pd.DataFrame()

    def detect_candidates(self):
        """warehouse asset_master 기반 신규 상장 탐지"""

        # 오늘 기준
        today_df = self._load_asset_master(self.trd_dt)

        # 전일 기준
        prev_dt = self._get_prev_trading_day()
        yesterday_df = self._load_asset_master(prev_dt)

        # 신규 security_id = today - yesterday
        new_ids = set(today_df["security_id"]) - set(yesterday_df["security_id"])
        if not new_ids:
            return {}

        # 신규 entries
        candidates = today_df[today_df["security_id"].isin(new_ids)]

        # 🔥 국가 기준이 아니라 exchange_code 기준 그룹화
        grouped = {}
        for _, row in candidates.iterrows():
            ex = 'US' if row['country_code'] == 'USA' else row["exchange_code"]
            grouped.setdefault(ex, []).append(row.to_dict())

        return grouped   # { "NASDAQ": [...], "NYSE": [...], ... }

    def _save_group(self, exchange_code: str, rows: list):
        """exchange_code 단위로 모니터링 경로 저장"""

        out_dir = DataPathResolver.warehouse_monitoring(
            domain_group=self.domain_group,
            category="new_listing",
            trd_dt=self.trd_dt,
            exchange_code=exchange_code
        )

        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / "candidates.jsonl"

        with open(out_file, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

        log.info(f"📦 신규상장 {exchange_code}: {len(rows)}건 저장 → {out_file}")
        return str(out_file)

    def run(self, context=None, **kwargs):
        grouped = self.detect_candidates()

        saved_paths = {}
        for ex_code, rows in grouped.items():
            p = self._save_group(ex_code, rows)
            saved_paths[ex_code] = p

        return {
            "record_count": sum(len(v) for v in grouped.values()),
            "paths": saved_paths,
            "country_code": self.country_code,
            "trd_dt": self.trd_dt,
            "grouped_candidates": grouped,
        }
