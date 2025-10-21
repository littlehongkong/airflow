import logging
import json
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Callable
import gc

# Data Lake Root Path: Airflow 컨테이너 내부 절대 경로로 설정 (권한 오류 방지)
LOCAL_DATA_LAKE_PATH = Path("/opt/airflow/data")


class BaseEquityPipeline(ABC):  # DataPipelineInterface 상속 유지 가정
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
            is_multi_file: bool = False,
            mode: str = "overwrite",  # ✅ append / overwrite 제어
    ) -> Dict[str, Any]:
        """
        Data Lake에 데이터를 저장하는 공통 함수
        - 일반: JSONL 단일 파일
        - 펀더멘털: 티커별 JSON
        - mode:
            - "overwrite": 기존 파일 교체 (기본)
            - "append": 기존 파일 뒤에 추가
        """
        if not records:
            self.log.info("저장할 레코드가 없습니다.")
            return {"count": 0, "target_path": str(target_dir)}

        target_dir.mkdir(parents=True, exist_ok=True)
        saved_count = 0

        # ✅ Case 1: multi-file 저장
        if is_multi_file:
            for rec in records:
                try:
                    ticker = (
                            rec.get("General", {}).get("Code")
                            or rec.get("code")
                            or rec.get("ticker")
                    )
                    if not ticker:
                        raise ValueError("⚠️ Ticker 정보를 찾을 수 없습니다.")

                    rec["metadata"] = base_metadata.copy()
                    file_path = target_dir / f"{ticker}.json"

                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(rec, f, indent=4, ensure_ascii=False)

                    saved_count += 1
                except Exception as e:
                    self.log.warning(f"⚠️ 개별 JSON 저장 실패: {str(e)}")

            self.log.info(f"✅ {saved_count:,}개 티커별 JSON 파일 저장 완료 ({target_dir})")
            return {"count": saved_count, "target_path": str(target_dir)}

        # ✅ Case 2: JSONL 저장
        file_path = target_dir / (
            file_name if isinstance(file_name, str) else file_name(records)
        )

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


    # ------------------------------------
    # 3️⃣ 공통: 기본 fetch/load (하위 클래스에서 구현)
    # ------------------------------------
    def fetch(self, **kwargs):
        raise NotImplementedError("하위 클래스에서 fetch()를 구현해야 합니다.")

    def load(self, records, **kwargs):
        raise NotImplementedError("하위 클래스에서 load()를 구현해야 합니다.")
