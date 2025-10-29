# plugins/validators/base_validator_interface.py
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pathlib import Path
import logging


class BaseValidatorInterface(ABC):
    """
    ✅ Data Lake & Warehouse 공통 Validator 인터페이스

    모든 Validator가 구현해야 하는 핵심 메서드:
    1. run(): 검증 실행 진입점
    2. _validate(): 실제 검증 로직
    3. _save_validation_result(): 검증 결과 저장

    하위 클래스 종류:
    - BaseDataLakeValidator: Data Lake용 (JSONL → Parquet, Pandera + Soda)
    - BaseWarehouseValidator: Warehouse용 (Parquet 검증, 비즈니스 룰)
    """

    def __init__(self, **kwargs):
        """
        공통 초기화
        - 로거 설정
        - 기본 파라미터 저장
        """
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.kwargs = kwargs

    @abstractmethod
    def build(self, context: Optional[Dict] = None, **kwargs) -> Dict[str, Any]:
        """
        검증 실행 진입점

        Args:
            context: Airflow context (optional)
            **kwargs: 추가 실행 파라미터

        Returns:
            검증 결과 딕셔너리
            {
                "status": "success" | "failed" | "warning" | "skipped",
                "record_count": int,
                "checks": {...},
                "log_file": str,
                ...
            }
        """
        pass

    @abstractmethod
    def _validate(self, **kwargs) -> Dict[str, Any]:
        """
        실제 검증 로직 수행

        Returns:
            검증 상세 결과
        """
        pass

    @abstractmethod
    def _save_validation_result(self, result: Dict[str, Any], **kwargs) -> Path:
        """
        검증 결과를 파일로 저장

        Args:
            result: 검증 결과 딕셔너리

        Returns:
            저장된 파일 경로
        """
        pass

    def _extract_airflow_context(self, context: Optional[Dict]) -> tuple:
        """
        Airflow context에서 dag_run_id, task_id 추출

        Returns:
            (dag_run_id, task_id)
        """
        if not context:
            return None, None

        airflow_ctx = context.get("context", {})
        dag_run_id = airflow_ctx.get("run_id")
        task_instance = airflow_ctx.get("task_instance")
        task_id = getattr(task_instance, "task_id", None) if task_instance else None

        return dag_run_id, task_id

    def _get_validation_metadata(
            self,
            status: str,
            checks: Dict[str, Any],
            dag_run_id: Optional[str] = None,
            task_id: Optional[str] = None,
            **extra_fields
    ) -> Dict[str, Any]:
        """
        표준 검증 메타데이터 구조 생성

        Returns:
            {
                "status": str,
                "checks": dict,
                "dag_run_id": str,
                "task_id": str,
                "validated_at": str (ISO format),
                ...extra_fields
            }
        """
        from datetime import datetime, timezone

        return {
            "status": status,
            "checks": checks,
            "dag_run_id": dag_run_id,
            "task_id": task_id,
            "validated_at": datetime.now(timezone.utc).isoformat(),
            **extra_fields
        }