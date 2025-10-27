import logging
import json
from abc import ABC
from pathlib import Path
from typing import List, Dict, Any, Callable


# Data Lake Root Path: Airflow 컨테이너 내부 절대 경로로 설정 (권한 오류 방지)
LOCAL_DATA_LAKE_PATH = Path("/opt/airflow/data")

class HookMixin:
    hook: Any

class BaseEquityPipeline(HookMixin, ABC):  # DataPipelineInterface 상속 유지 가정
    """
    Equity 관련 데이터 파이프라인의 공통 기반 클래스
    - Data Lake Raw Zone (File System) 공통 적재 로직 제공
    """

    def __init__(self, data_domain: str, exchange_code: str, trd_dt: str):  # table_name 대신 data_domain 사용
        self.data_domain = data_domain
        self.exchange_code = exchange_code
        self.trd_dt = trd_dt
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        # 루트 폴더가 없으면 생성 (권한 오류는 환경 설정으로 해결해야 함)
        LOCAL_DATA_LAKE_PATH.mkdir(parents=True, exist_ok=True)

        # ------------
        # ------------------------
        # 1️⃣ 공통 유틸리티: 파일 경로 생성 및 메타데이터 반환
        # ------------------------------------

    def _get_lake_path_and_metadata(
            self,
            layer: str = "raw",  # raw / validated / staging / curated / snapshot
            **kwargs
    ) -> tuple[Path, dict]:
        """
        Hive 파티셔닝 구조를 따르되, S3 구조와 동일하게 경로를 생성.
        예시:
          raw/equity_prices/exchange_code=US/trd_dt=2025-10-15/
        """

        # 1️⃣ 날짜 파티션 결정
        date_value = self.trd_dt or kwargs.get("ds")
        date_partition_key = kwargs.get("partition_key_name", "trd_dt")

        # 2️⃣ 지리적 파티션 결정
        geo_partition_key = kwargs.get("geo_partition_key", "exchange_code")
        geo_value = kwargs.get(geo_partition_key)
        if not geo_value:
            raise ValueError(f"{geo_partition_key} 값이 없습니다 (예: exchange_code=KO)")

        # 3️⃣ 계층 구조: /data_lake/raw/equity_prices/exchange_code=KO/trd_dt=2025-10-15
        target_dir = (
                LOCAL_DATA_LAKE_PATH
                / "data_lake"
                / layer
                / self.data_domain
                / f"{geo_partition_key}={geo_value}"
                / f"{date_partition_key}={date_value}"
        )
        target_dir.mkdir(parents=True, exist_ok=True)

        # 4️⃣ 메타데이터 구성
        metadata = {
            "layer": layer,
            geo_partition_key: geo_value,
            date_partition_key: date_value,
            "source": self.data_domain,
        }

        return target_dir, metadata



    # ------------------------------------
    # 2️⃣ 공통: 파일 적재 로직 (통합)
    # ------------------------------------
    def _write_records_to_lake(
            self,
            records: List[Dict],
            target_dir: Path,
            base_metadata: Dict[str, str],
            file_name: str | Callable[[List[Dict]], str],
            mode: str = "overwrite",  # ✅ 기본: 덮어쓰기
    ) -> Dict[str, Any]:
        """
        Data Lake에 데이터를 저장하는 공통 함수 (단일 JSONL 버전)
        - 모든 데이터유형 공통: 1개 JSONL 파일에 저장
        - mode:
            - "overwrite": 기존 파일 교체 (기본)
            - "append": 기존 파일 뒤에 추가
        """
        if not records:
            self.log.info("저장할 레코드가 없습니다.")
            return {"count": 0, "target_path": str(target_dir)}

        target_dir.mkdir(parents=True, exist_ok=True)
        saved_count = 0

        # 파일 경로 결정
        file_path = target_dir / (
            file_name if isinstance(file_name, str) else file_name(records)
        )

        # 모드 설정
        open_mode = "a" if mode == "append" else "w"

        try:
            with open(file_path, open_mode, encoding="utf-8") as f:
                for rec in records:
                    rec["metadata"] = base_metadata.copy()
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    saved_count += 1

            self.log.info(
                f"✅ JSONL 파일 저장 완료: {file_path} "
                f"({saved_count:,}건, mode={mode})"
            )

        except Exception as e:
            self.log.error(f"❌ JSONL 저장 실패: {str(e)}")
            raise

        return {"count": saved_count, "target_path": str(target_dir)}


    # --------------------------------------------------
    # ✅ 0️⃣ record_count 강제 삽입 헬퍼
    # --------------------------------------------------
    @staticmethod
    def _enforce_record_count(result: Any, records: list | None = None) -> dict:
        """
        모든 파이프라인에서 반환 시 record_count 누락 방지
        - result: 파이프라인 반환 결과
        - records: (선택) 실제 데이터 목록 (len() 계산용)
        """
        # 결과가 None → 기본값 반환
        if result is None:
            count = len(records) if records is not None else 0
            return {"record_count": count}

        # dict가 아닌 경우 → dict로 변환
        if not isinstance(result, dict):
            result = {"result": result}

        # record_count가 누락된 경우 자동 추가
        if "record_count" not in result:
            count = len(records) if records is not None else 0
            result["record_count"] = count

        return result

    def fetch_and_load(self, **kwargs):
        """
        공통: 데이터 수집 → 정규화 → 저장 → 원천 메타 기록
        - fetch()가 (records, meta) 튜플을 반환해도 OK
        - fetch()가 원시 응답(list|dict|None)을 반환해도 OK
        """
        self.log.info(f"📡 Fetching data for {self.exchange_code} ({self.data_domain})")

        raw = self.fetch(**kwargs)

        # ✅ fetch가 (records, meta) 튜플을 반환하는 경우 지원
        if isinstance(raw, tuple) and len(raw) == 2:
            records, source_meta = raw
            # records가 dict면 list로, None이면 빈 리스트로
            if records is None:
                records = []
            elif isinstance(records, dict):
                records = [records]
        else:
            # ✅ 기존 설계: 원시 응답을 표준화
            records, source_meta = self._standardize_fetch_output(raw)

        record_count = len(records)

        # ✅ 경로/메타 구성
        target_dir, base_meta = self._get_lake_path_and_metadata(layer="raw", **kwargs)
        file_name = f"{self.data_domain}.jsonl"

        # ✅ 저장
        write_result = self._write_records_to_lake(
            records=records,
            target_dir=target_dir,
            base_metadata=base_meta,
            file_name=file_name,
        )

        # ✅ record_count 일관 보정 (write_result를 실제로 활용)
        if isinstance(write_result, dict):
            record_count = write_result.get("count", record_count)

        # ✅ 원천 메타 저장 (시그니처 통일)
        self._save_source_meta(target_dir, record_count, source_meta)

        # ✅ 반환 객체 표준화 (항상 record_count 포함)
        result = {
            "record_count": record_count,
            "target_path": str(target_dir),
        }
        self.log.info(f"✅ Completed fetch_and_load: {result}")
        return result

    def _save_source_meta(self, target_dir: Path, record_count: int, source_meta: dict | None = None):
        """수집 원천 정보를 `_source_meta.json` 형태로 저장"""
        meta_path = target_dir / "_source_meta.json"

        if "api_token" in source_meta.get("params", {}):
            source_meta["params"]["api_token"] = "***"

        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(source_meta, f, indent=2, ensure_ascii=False)
        self.log.info(f"📘 Source metadata saved: {meta_path}")

    def load_to_datalake(self, records: List[Dict], **kwargs) -> Dict:
        """
        공통 Data Lake 적재 로직
        - Hive 파티션 구조
        - JSONL or 개별 JSON 파일 저장
        - _source_meta.json 자동 생성
        """
        kwargs["partition_key_name"] = "trd_dt"
        kwargs["geo_partition_key"] = kwargs.get("geo_partition_key", "exchange_code")

        target_dir, base_metadata = self._get_lake_path_and_metadata(**kwargs)
        file_name = f"{self.data_domain}.jsonl"

        # 1️⃣ 실제 데이터 저장
        save_info = self._write_records_to_lake(
            records=records,
            target_dir=target_dir,
            base_metadata=base_metadata,
            file_name=file_name,
        )

        # 2️⃣ 원천 메타정보 (_source_meta.json) 추가 생성
        if hasattr(self, "source_meta") and self.source_meta:
            meta_path = target_dir / "_source_meta.json"
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(self.source_meta, f, indent=2, ensure_ascii=False)

        return save_info

    def _standardize_fetch_output(self, data: Any) -> tuple[list[dict], dict]:
        """
        Fetch 결과를 항상 (records, meta) 형태로 정규화합니다.
        - data: API 응답 (list or dict)
        - endpoint, params, vendor는 Hook 객체에서 직접 가져옵니다.
        """
        # ✅ data 형식 정규화

        if data is None:
            records = []
        elif isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = [data]
        else:
            self.log.warning(f"⚠️ Unexpected fetch type: {type(data)}")
            records = []

        # ✅ hook으로부터 원천 메타정보 추출
        meta = {
            "vendor": getattr(self.hook, "vendor", "EODHD"),
            "endpoint": getattr(self.hook, "endpoint", None),
            "params": getattr(self.hook, "params", None),
        }

        return records, meta

    # ------------------------------------
    # 3️⃣ 공통: 기본 fetch/load (하위 클래스에서 구현)
    # ------------------------------------
    def fetch(self, **kwargs):
        raise NotImplementedError("하위 클래스에서 fetch()를 구현해야 합니다.")

    def load(self, records: List[Dict], **kwargs):
        """기본 load 로직 — 하위 클래스에서 override 필요 없음"""
        return self.load_to_datalake(records, **kwargs)
