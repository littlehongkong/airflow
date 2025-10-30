import logging
import json
from abc import ABC
from pathlib import Path
from typing import List, Dict, Any, Callable, Tuple
from datetime import datetime
from plugins.config.constants import (
    DATA_LAKE_ROOT,
    LAYERS,
    VENDORS,
    WAREHOUSE_SOURCE_MAP,
)

REDACT_KEYS = {"api_token", "apikey", "api_key", "authorization", "auth", "token", "access_token", "secret"}

class HookMixin:
    hook: Any  # 각 Hook(e.g. EODHDHook, KRXHook)는 최소 vendor/endpoint/params 속성을 제공하도록

class BaseEquityPipeline(HookMixin, ABC):
    """
    Equity 관련 공통 파이프라인
    - 원본(raw) 적재 공통
    - vendor 파티셔닝 지원
    - _source_meta.json 저장 + 비밀값 마스킹
    """

    def __init__(self, data_domain: str, exchange_code: str, trd_dt: str):
        self.data_domain = data_domain  # e.g. ", "symbol_list", "fundamentals"
        self.exchange_code = exchange_code
        self.trd_dt = trd_dt
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        DATA_LAKE_ROOT.mkdir(parents=True, exist_ok=True)


    # =============================
    # Domain 정규화 유틸
    # =============================
    def _normalize_domain(self, data_domain: str) -> str:
        """
        exchange_list, symbol_list 등 lake 전용 도메인을 warehouse 호환형으로 정규화
        예:
          exchange_list → exchange
          symbol_list → asset
        """
        for wh_domain, sources in WAREHOUSE_SOURCE_MAP.items():
            if data_domain in sources:
                return wh_domain
        return data_domain

    # =============================
    # 비밀값 마스킹
    # =============================
    @staticmethod
    def _redact(obj: Any) -> Any:
        if isinstance(obj, dict):
            redacted = {}
            for k, v in obj.items():
                if k.lower() in REDACT_KEYS:
                    redacted[k] = "***"
                else:
                    redacted[k] = BaseEquityPipeline._redact(v)
            return redacted
        if isinstance(obj, list):
            return [BaseEquityPipeline._redact(v) for v in obj]
        return obj


    # ---------------------------
    # 유틸: 비밀값 마스킹
    # ---------------------------
    @staticmethod
    def _redact(obj: Any) -> Any:
        if isinstance(obj, dict):
            redacted = {}
            for k, v in obj.items():
                if k.lower() in REDACT_KEYS:
                    redacted[k] = "***"
                else:
                    redacted[k] = BaseEquityPipeline._redact(v)
            return redacted
        if isinstance(obj, list):
            return [BaseEquityPipeline._redact(v) for v in obj]
        return obj

    # ---------------------------
    # 1) 경로 & 메타 생성
    # ---------------------------
    def _get_lake_path_and_metadata(
        self,
        layer: str = "raw",
        vendor: str = None,
        **kwargs
    ) -> Tuple[Path, dict]:
        """
        Hive-style 파티셔닝 구조 생성
        예: /data_lake/validated/exchange/vendor=eodhd/exchange_code=US/trd_dt=2025-10-15/
        """
        vendor_value = vendor or getattr(self.hook, "vendor", None)
        if not vendor_value:
            raise ValueError("❌ vendor 값이 없습니다. DAG op_kwargs 또는 Hook.vendor 확인 필요.")

        date_partition_key = kwargs.get("partition_key_name", "trd_dt")
        date_value = kwargs.get(date_partition_key, self.trd_dt)
        geo_partition_key = kwargs.get("geo_partition_key", "exchange_code")
        geo_value = kwargs.get(geo_partition_key, self.exchange_code)

        target_dir = (
            DATA_LAKE_ROOT
            / layer
            / self.data_domain
            / f"vendor={vendor_value.lower()}"
            / f"{geo_partition_key}={geo_value}"
            / f"{date_partition_key}={date_value}"
        )
        target_dir.mkdir(parents=True, exist_ok=True)

        metadata = {
            "layer": layer,
            "vendor": vendor_value.lower(),
            geo_partition_key: geo_value,
            date_partition_key: date_value,
            "source": self.data_domain,
            "saved_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
        return target_dir, metadata
    # ---------------------------
    # 2) 파일 저장 (JSONL)
    # ---------------------------
    def _write_records_to_lake(
        self,
        records: List[Dict],
        target_dir: Path,
        base_metadata: Dict[str, str],
        file_name: str | Callable[[List[Dict]], str],
        mode: str = "overwrite",
    ) -> Dict[str, Any]:
        if not records:
            self.log.info("저장할 레코드가 없습니다.")
            return {"count": 0, "target_path": str(target_dir), "file_path": None}

        file_path = target_dir / (file_name if isinstance(file_name, str) else file_name(records))
        open_mode = "a" if mode == "append" else "w"

        with open(file_path, open_mode, encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        meta_path = target_dir / "_source_meta.json"
        meta_info = base_metadata.copy()
        meta_info["record_count"] = len(records)
        meta_info["saved_at"] = datetime.utcnow().isoformat() + "Z"
        with open(meta_path, "w", encoding="utf-8") as mf:
            json.dump(meta_info, mf, indent=2, ensure_ascii=False)

        self.log.info(f"✅ JSONL 저장: {file_path} ({len(records):,}건)")
        return {"count": len(records), "target_path": str(target_dir), "file_path": str(file_path)}

    # ---------------------------
    # 3) fetch → 표준화
    # ---------------------------
    def _standardize_fetch_output(self, data: Any) -> tuple[list[dict], dict]:
        """
        Fetch 결과를 항상 (records, meta)로 표준화
        meta는 hook(vendor/endpoint/params) 기준으로 구성하며 비밀값 마스킹
        """

        print("data type:::::", type(data))
        print("data:::::", data)

        if data is None:
            records = []
        elif isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = [data]
        else:
            self.log.warning(f"⚠️ Unexpected fetch type: {type(data)} — 리스트로 캐스팅")
            records = []

        # hook 메타
        hook_vendor = getattr(self.hook, "vendor", None)
        hook_endpoint = getattr(self.hook, "endpoint", None)
        hook_params = self._redact(getattr(self.hook, "params", None) or {})
        meta = {
            "vendor": (hook_vendor or "unknown").lower(),
            "endpoint": hook_endpoint,
            "params": hook_params,
        }
        return records, meta

    # ---------------------------
    # 4) 메타 파일 저장
    # ---------------------------
    def _save_source_meta(self, target_dir: Path, record_count: int, source_meta: dict | None = None):
        """
        _source_meta.json 저장 (비밀값 마스킹 + record_count 포함)
        """
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

    # ---------------------------
    # 5) 표준 실행 (fetch → save → meta)
    # ---------------------------
    def fetch_and_load(self, **kwargs):
        self.log.info(f"📡 Fetching {self.data_domain} for exchange={self.exchange_code}")

        raw = self.fetch(**kwargs)
        records, source_meta = self._standardize_fetch_output(raw)
        vendor_override = kwargs.get("vendor")

        target_dir, base_meta = self._get_lake_path_and_metadata(layer=LAYERS["raw"], vendor=vendor_override)
        file_name = f"{self.data_domain}.jsonl"
        write_result = self._write_records_to_lake(records, target_dir, base_meta, file_name)
        record_count = write_result.get("count", len(records))

        self._save_source_meta(target_dir, record_count, source_meta)
        self.log.info(f"✅ [FETCH COMPLETE] {self.data_domain} | {record_count:,}건 저장 완료 → {target_dir}")

        return {"record_count": record_count, "target_path": str(target_dir)}

    # ---------------------------
    # 6) 외부에서 직접 적재할 때 사용
    # ---------------------------
    def load_to_datalake(self, records: List[Dict], **kwargs) -> Dict:
        """
        외부에서 records를 직접 넘겨 적재할 때 사용
        - vendor/partition 키 자동 처리
        - _source_meta.json 자동 생성
        """
        kwargs["partition_key_name"] = kwargs.get("partition_key_name", "trd_dt")
        kwargs["geo_partition_key"] = kwargs.get("geo_partition_key", "exchange_code")

        target_dir, base_meta = self._get_lake_path_and_metadata(**kwargs)
        file_name = f"{self.data_domain}.jsonl"

        save_info = self._write_records_to_lake(
            records=records,
            target_dir=target_dir,
            base_metadata=base_meta,
            file_name=file_name,
        )

        # 별도 source_meta envelope가 있다면 여기서도 저장 가능
        # if hasattr(self, "source_meta") and self.source_meta:
        #     self._save_source_meta(Path(save_info["target_path"]), save_info.get("count", 0), self.source_meta)

        return save_info

    # ---------------------------
    # 7) 하위 클래스 필수 구현
    # ---------------------------
    def fetch(self, **kwargs):
        raise NotImplementedError("하위 클래스에서 fetch()를 구현해야 합니다.")

    def load(self, records: List[Dict], **kwargs):
        return self.load_to_datalake(records, **kwargs)
