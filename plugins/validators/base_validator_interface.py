"""
Base Validator Interface (Refactored)
-----------------------------------------
✅ 모든 Validator 공통 인터페이스
- 기존 build(), _validate() → validate() 단일 메서드로 통합
- Airflow Operator 및 Pipeline과의 호환성 강화
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging


class BaseValidatorInterface(ABC):
    def __init__(self, *args, **kwargs):
        self.log = logging.getLogger(self.__class__.__name__)

    # ---------------------------------------------------------------------
    # ✅ 공통 Entry Point (통일된 인터페이스)
    # ---------------------------------------------------------------------
    @abstractmethod
    def validate(self, context: Optional[Dict] = None, **kwargs) -> Dict[str, Any]:
        """
        모든 Validator가 구현해야 하는 표준 인터페이스 메서드
        - Airflow Operator에서 직접 호출됨
        """
        pass

    # ---------------------------------------------------------------------
    # ✅ Airflow context 헬퍼
    # ---------------------------------------------------------------------
    def _extract_airflow_context(self, context: Optional[Dict]) -> tuple[str, str]:
        """Airflow DAG 실행 컨텍스트에서 run_id, task_id 추출"""
        if not context:
            return None, None
        dag_run_id = context.get("run_id")
        ti = context.get("task_instance")
        task_id = ti.task_id if ti else None
        return dag_run_id, task_id

    # ---------------------------------------------------------------------
    # ✅ 공통 Result Formatter (모든 Validator 공통 사용)
    # ---------------------------------------------------------------------
    def _get_validation_metadata(
        self,
        status: str,
        checks: Dict[str, Any],
        dag_run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        record_count: int = 0,
        message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """검증 결과 공통 메타데이터 생성"""
        meta = {
            "status": status,
            "record_count": record_count,
            "checks": checks,
            "dag_run_id": dag_run_id,
            "task_id": task_id,
        }
        if message:
            meta["message"] = message
        return meta
