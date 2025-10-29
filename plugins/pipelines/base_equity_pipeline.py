import logging
import json
from abc import ABC
from pathlib import Path
from typing import List, Dict, Any, Callable, Tuple
from datetime import datetime
from plugins.config.constants import DATA_LAKE_ROOT

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
        layer: str = "raw",  # raw / validated / staging / curated / snapshot
        vendor: str = None,
        **kwargs
    ) -> Tuple[Path, dict]:
        """
        Hive 파티셔닝 + vendor 구분
          예: /opt/airflow/data/data_lake/raw/fundamentals/vendor=eodhd/exchange_code=US/trd_dt=2025-10-15/
        """

        self.log.info(f"[DEBUG] hook type: {type(self.hook)}, vendor: {getattr(self.hook, 'vendor', None)}")

        # 날짜 파티션
        date_partition_key = kwargs.get("partition_key_name", "trd_dt")
        date_value = kwargs.get(date_partition_key) or self.trd_dt
        if not date_value:
            raise ValueError(f"{date_partition_key} 값이 없습니다 (예: trd_dt=YYYY-MM-DD)")

        # 지리 파티션
        geo_partition_key = kwargs.get("geo_partition_key", "exchange_code")
        geo_value = kwargs.get(geo_partition_key) or self.exchange_code
        if not geo_value:
            raise ValueError(f"{geo_partition_key} 값이 없습니다 (예: exchange_code=US)")

        # 벤더 파티션
        vendor_value = vendor or getattr(self.hook, "vendor", None)
        if not vendor_value:
            raise ValueError(
                f"❌ vendor 값이 설정되지 않았습니다. "
                f"pipeline_cls={self.__class__.__name__}, hook={type(self.hook).__name__}. "
                f"DAG의 op_kwargs 또는 Hook.vendor 속성을 확인하세요."
            )
        vendor_value = vendor_value.lower()

        # 경로 생성
        target_dir = (
            DATA_LAKE_ROOT
            / layer
            / self.data_domain
            / f"vendor={vendor_value}"
            / f"{geo_partition_key}={geo_value}"
            / f"{date_partition_key}={date_value}"
        )
        target_dir.mkdir(parents=True, exist_ok=True)

        # 메타데이터
        metadata = {
            "layer": layer,
            "vendor": vendor_value,
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
        """
        JSONL로 저장. 각 레코드에 metadata 필드 병합(얕은 복사)
        """
        if not records:
            self.log.info("저장할 레코드가 없습니다.")
            return {"count": 0, "target_path": str(target_dir), "file_path": None}

        file_path = target_dir / (file_name if isinstance(file_name, str) else file_name(records))
        open_mode = "a" if mode == "append" else "w"

        saved_count = 0
        try:
            with open(file_path, open_mode, encoding="utf-8") as f:
                for rec in records:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    saved_count += 1

            # ✅ 별도의 메타파일 생성
            meta_path = target_dir / "_source_meta.json"
            with open(meta_path, "w", encoding="utf-8") as mf:
                meta_info = base_metadata.copy()
                meta_info["record_count"] = len(records)
                meta_info["saved_at"] = datetime.utcnow().isoformat() + "Z"
                json.dump(meta_info, mf, indent=2, ensure_ascii=False)
            self.log.info(f"✅ JSONL 저장: {file_path} ({saved_count:,}건, mode={mode})")
        except Exception as e:
            self.log.exception(f"❌ JSONL 저장 실패: {file_path} - {e}")
            raise

        return {"count": saved_count, "target_path": str(target_dir), "file_path": str(file_path)}

    # ---------------------------
    # 3) fetch → 표준화
    # ---------------------------
    def _standardize_fetch_output(self, data: Any) -> tuple[list[dict], dict]:
        """
        Fetch 결과를 항상 (records, meta)로 표준화
        meta는 hook(vendor/endpoint/params) 기준으로 구성하며 비밀값 마스킹
        """
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
        """
        공통: 데이터 수집 → 정규화 → 저장 → 원천 메타 기록
        - fetch()가 (records, meta) 튜플을 반환해도 OK
        - fetch()가 원시 응답(list|dict|None)을 반환해도 OK
        """
        self.log.info(f"📡 Fetching {self.data_domain} for exchange={self.exchange_code}, trd_dt={self.trd_dt}")

        raw = self.fetch(**kwargs)

        if isinstance(raw, tuple) and len(raw) == 2:
            records, source_meta = raw
            if records is None:
                records = []
            elif isinstance(records, dict):
                records = [records]
        else:
            records, source_meta = self._standardize_fetch_output(raw)

        # 경로/메타
        vendor_override = kwargs.get("vendor")  # 필요 시 DAG에서 강제 지정 가능
        target_dir, base_meta = self._get_lake_path_and_metadata(layer="raw", vendor=vendor_override, **kwargs)

        # 저장
        file_name = f"{self.data_domain}.jsonl"
        write_result = self._write_records_to_lake(
            records=records,
            target_dir=target_dir,
            base_metadata=base_meta,
            file_name=file_name,
        )
        record_count = write_result.get("count", len(records))

        # 원천 메타 저장
        self._save_source_meta(target_dir, record_count, source_meta)

        result = {
            "record_count": record_count,
            "target_path": write_result.get("target_path"),
            "file_path": write_result.get("file_path"),
            "vendor": base_meta.get("vendor"),
        }
        self.log.info(
            f"✅ [FETCH COMPLETE] {self.data_domain} | "
            f"exchange={self.exchange_code} | vendor={vendor_override} | "
            f"{record_count:,}건 저장 완료 → {target_dir}"
        )
        return result

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
