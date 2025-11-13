"""
plugins/pipelines/equity/base_equity_pipeline.py

💾 BaseEquityPipeline
- 주식/ETF/펀더멘털/배당 등 공통 수집 파이프라인의 상위 클래스
- Data Lake (raw/validated) 적재 공통 로직
- vendor 파티셔닝 및 _source_meta.json 자동 관리
"""

import logging
import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Any, Callable, Tuple, Union
from datetime import datetime

from plugins.config import constants as C

# 민감정보 키 목록
REDACT_KEYS = {"api_token", "apikey", "api_key", "authorization", "auth", "token", "access_token", "secret"}


class HookMixin:
    """Hook 객체 혼합용 클래스"""
    hook: Any  # 예: EODHDHook, KRXHook 등


class BaseEquityPipeline(HookMixin, ABC):
    """
    ✅ Equity 계열 공통 파이프라인 베이스 클래스

    [공통 기능]
    1️⃣ fetch → JSONL 파일 저장 (Data Lake 구조)
    2️⃣ vendor/exchange_code/trd_dt 파티셔닝
    3️⃣ _source_meta.json 자동 생성 및 비밀값 마스킹
    """

    def __init__(self, domain: str, exchange_code: str, trd_dt: str, domain_group: str | None = None, allow_empty: bool = False):
        self.domain = domain                  # 예: "symbol_list", "fundamentals"
        self.exchange_code = exchange_code    # 예: "US"
        self.trd_dt = trd_dt                  # 예: "2025-11-03"
        self.layer = "data_lake"
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.allow_empty = allow_empty
        C.DATA_LAKE_ROOT.mkdir(parents=True, exist_ok=True)

        # ✅ 1️⃣ domain_group 자동 인식 (없으면 constants 기반 추론)
        if domain_group:
            self.domain_group = domain_group.lower()
        else:
            self.domain_group = C.DOMAIN_GROUPS.get(domain.lower(), "equity")

    # ============================================================
    # 1️⃣ Domain 정규화
    # ============================================================
    def _normalize_domain(self, domain: str) -> str:
        """
        exchange_list, symbol_list 등 lake 전용 도메인을 warehouse 호환형으로 정규화
        예:
          exchange_list → exchange
          symbol_list → asset_master
        """
        for wh_domain, sources in C.WAREHOUSE_SOURCE_MAP.items():
            if domain in sources:
                return wh_domain
        return domain

    # ============================================================
    # 2️⃣ 비밀값 마스킹
    # ============================================================
    @staticmethod
    def _redact(obj: Any) -> Any:
        """dict/list 내 API 키 등의 민감값 마스킹"""
        if isinstance(obj, dict):
            return {
                k: ("***" if k.lower() in REDACT_KEYS else BaseEquityPipeline._redact(v))
                for k, v in obj.items()
            }
        elif isinstance(obj, list):
            return [BaseEquityPipeline._redact(v) for v in obj]
        else:
            return obj

    # ============================================================
    # 3️⃣ 경로 및 메타데이터 생성
    # ============================================================
    def _get_lake_path_and_metadata(
        self,
        stage: str = "raw",
        vendor: str = None,
        **kwargs,
    ) -> Tuple[Path, dict]:
        """
        Hive-style 파티셔닝 구조 생성
        예:
          /data_lake/raw/symbol_list/vendor=eodhd/exchange_code=US/trd_dt=2025-11-05/
        """
        vendor_value = vendor or getattr(self.hook, "vendor", None)
        if not vendor_value:
            raise ValueError("❌ vendor 값이 없습니다. DAG op_kwargs 또는 Hook.vendor 확인 필요.")

        date_partition_key = kwargs.get("partition_key_name", "trd_dt")
        geo_partition_key = kwargs.get("geo_partition_key", "exchange_code")

        date_value = kwargs.get(date_partition_key, self.trd_dt)
        geo_value = kwargs.get(geo_partition_key, self.exchange_code)

        target_dir = (
            C.DATA_LAKE_ROOT
            / stage
            / self.domain_group
            / self.domain
            / f"vendor={vendor_value.lower()}"
            / f"{geo_partition_key}={geo_value}"
            / f"{date_partition_key}={date_value}"
        )
        target_dir.mkdir(parents=True, exist_ok=True)

        metadata = {
            "stage": stage,
            "vendor": vendor_value.lower(),
            geo_partition_key: geo_value,
            date_partition_key: date_value,
            "source": self.domain,
            "saved_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
        return target_dir, metadata

    # ============================================================
    # 4️⃣ 파일 저장 (JSONL)
    # ============================================================
    def _write_records_to_lake(
            self,
            records: List[Dict],
            target_dir: Path,
            base_metadata: Dict[str, Any],
            file_name: Union[str, Callable[[List[Dict]], str]],
            mode: str = "overwrite",
    ) -> Dict[str, Any]:
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / (file_name if isinstance(file_name, str) else file_name(records))
        meta_path = target_dir / "_source_meta.json"

        # ============================================================
        # 1️⃣ Empty Handling
        # ============================================================
        if not records:
            if getattr(self, "allow_empty", False):
                self.log.warning("⚠️ 저장할 레코드가 없습니다. (allow_empty=True)")
                # ✅ 빈 JSONL 파일 생성
                with open(file_path, "w", encoding="utf-8") as f:
                    pass  # 빈 파일
                record_count = 0
            else:
                self.log.error("❌ 빈 데이터가 허용되지 않는 도메인입니다.")
                raise ValueError("빈 데이터가 허용되지 않는 도메인입니다.")
        else:
            open_mode = "a" if mode == "append" else "w"
            with open(file_path, open_mode, encoding="utf-8") as f:
                for rec in records:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            record_count = len(records)

        # ============================================================
        # 2️⃣ 메타정보 작성 (_source_meta.json)
        # ============================================================
        meta_info = base_metadata.copy()
        meta_info["record_count"] = record_count
        meta_info["saved_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

        with open(meta_path, "w", encoding="utf-8") as mf:
            json.dump(meta_info, mf, indent=2, ensure_ascii=False)

        # ============================================================
        # 3️⃣ 로그 및 반환
        # ============================================================
        self.log.info(f"✅ JSONL 저장 완료: {file_path.name} ({record_count:,}건)")
        return {
            "count": record_count,
            "target_path": str(target_dir),
            "file_path": str(file_path),
        }

    # ============================================================
    # 5️⃣ Fetch 결과 표준화
    # ============================================================
    def _standardize_fetch_output(self, data: Any) -> Tuple[List[Dict], Dict[str, Any]]:
        """
        Fetch 결과를 항상 (records, meta) 형태로 변환
        """
        if data is None:
            records = []
        elif isinstance(data, list):
            records = [r for r in data if isinstance(r, dict)]
        elif isinstance(data, dict):
            records = [data]
        else:
            self.log.warning(f"⚠️ Unexpected fetch type: {type(data)} — 강제 리스트 변환")
            records = [{"value": str(data)}]

        hook_vendor = getattr(self.hook, "vendor", None)
        hook_endpoint = getattr(self.hook, "endpoint", None)
        hook_params = self._redact(getattr(self.hook, "params", None) or {})

        meta = {
            "vendor": (hook_vendor or "unknown").lower(),
            "endpoint": hook_endpoint,
            "params": hook_params,
        }
        return records, meta

    # ============================================================
    # 6️⃣ _source_meta.json 저장
    # ============================================================
    def _save_source_meta(self, target_dir: Path, record_count: int, source_meta: dict | None = None):
        meta_path = target_dir / "_source_meta.json"
        safe_meta = self._redact(source_meta or {})
        envelope = {
            "record_count": record_count,
            "saved_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            **safe_meta,
        }
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(envelope, f, indent=2, ensure_ascii=False)
        self.log.info(f"📘 Source metadata saved: {meta_path}")

    # ============================================================
    # 7️⃣ 표준 실행(fetch → save → meta)
    # ============================================================
    def fetch_and_load(self, **kwargs) -> Dict[str, Any]:
        self.log.info(f"📡 Fetching {self.domain} for exchange={self.exchange_code}")

        raw = self.fetch(**kwargs)
        records, source_meta = self._standardize_fetch_output(raw)
        vendor_override = kwargs.get("vendor")

        target_dir, base_meta = self._get_lake_path_and_metadata(
            stage=C.Stages.get("raw", "raw"),  # 안전 접근
            vendor=vendor_override,
        )

        file_name = f"{self.domain}.jsonl"
        write_result = self._write_records_to_lake(records, target_dir, base_meta, file_name)
        record_count = write_result.get("count", len(records))

        self._save_source_meta(target_dir, record_count, source_meta)
        self.log.info(f"✅ [FETCH COMPLETE] {self.domain} | {record_count:,}건 저장 완료 → {target_dir}")

        return {"record_count": record_count, "target_path": str(target_dir)}

    # ============================================================
    # 8️⃣ 외부에서 직접 적재할 때 사용
    # ============================================================
    def load_to_datalake(self, records: List[Dict], **kwargs) -> Dict[str, Any]:
        kwargs["partition_key_name"] = kwargs.get("partition_key_name", "trd_dt")
        kwargs["geo_partition_key"] = kwargs.get("geo_partition_key", "exchange_code")

        target_dir, base_meta = self._get_lake_path_and_metadata(**kwargs)
        file_name = f"{self.domain}.jsonl"

        save_info = self._write_records_to_lake(
            records=records,
            target_dir=target_dir,
            base_metadata=base_meta,
            file_name=file_name,
        )
        return save_info

    # ============================================================
    # 9️⃣ 추상 메서드 정의
    # ============================================================
    @abstractmethod
    def fetch(self, **kwargs) -> Any:
        """하위 클래스에서 fetch() 구현 필수"""
        pass

    def load(self, records: List[Dict], **kwargs):
        return self.load_to_datalake(records, **kwargs)
