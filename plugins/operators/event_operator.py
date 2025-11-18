# plugins/operators/event_operator.py

from typing import Dict, Any, Type
from airflow.models import BaseOperator
from airflow.utils.context import Context
from plugins.utils.pipeline_helper import run_and_log


class EventOperator(BaseOperator):
    """
    🎯 이벤트 기반(신규상장, 자본변경, 코드변경 등) 파이프라인 실행 전용 Operator
    - LakeOperator / WarehouseOperator 포맷과 동일한 구성
    - pipeline.run() 또는 pipeline.detect()/pipeline.process() 자동 식별
    - Event 계층의 메타를 event_meta.json 형식으로 저장
    """

    template_fields = ("op_kwargs",)
    ui_color = "#87CEEB"  # Skyblue for event tasks

    def __init__(
        self,
        *,
        pipeline_cls: Type,
        method_name: str = None,       # run() default, detect()/process() 지원
        op_kwargs: Dict[str, Any] = None,
        postgres_conn_id: str = "postgres_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.pipeline_cls = pipeline_cls
        self.op_kwargs = op_kwargs or {}
        self.method_name = method_name
        self.postgres_conn_id = postgres_conn_id

    # ------------------------------------------------------------------
    # Operator 실행
    # ------------------------------------------------------------------
    def execute(self, context: Context) -> Dict[str, Any]:

        self.log.info(
            f"🎯 [EventOperator] Starting Event Pipeline | "
            f"Pipeline={self.pipeline_cls.__name__} | Task={self.task_id}"
        )

        # 렌더링된 args
        rendered_kwargs = self.op_kwargs
        self.log.info(f"📋 Pipeline kwargs: {rendered_kwargs}")

        # 인스턴스 생성
        pipeline = self.pipeline_cls(**rendered_kwargs)

        # 실행 메서드 자동 결정
        func = None

        if self.method_name:
            func = getattr(pipeline, self.method_name, None)
        else:
            # 기본 탐색 순서
            if hasattr(pipeline, "run"):
                func = pipeline.run
            elif hasattr(pipeline, "detect"):
                func = pipeline.detect
            elif hasattr(pipeline, "process"):
                func = pipeline.process

        if not func or not callable(func):
            raise AttributeError(
                f"❌ {self.pipeline_cls.__name__}에 실행 가능한 메서드(run/detect/process)가 없습니다."
            )

        # run_and_log로 실행 (메타 기록 layer='event')
        result = run_and_log(
            func=func,
            postgres_conn_id=self.postgres_conn_id,
            dag_id=context["dag"].dag_id,
            task_id=self.task_id,
            airflow_context=context,
            layer="event",
            **rendered_kwargs,
        )

        self.log.info(
            f"✅ [EventOperator] 완료 | Pipeline={self.pipeline_cls.__name__} | "
            f"Result keys={list(result.keys())}"
        )

        # 파이프라인 cleanup() 지원 (선택)
        if hasattr(pipeline, "cleanup"):
            try:
                pipeline.cleanup()
                self.log.info(f"🧹 Cleanup 완료")
            except Exception as e:
                self.log.warning(f"⚠️ Cleanup 실패: {str(e)}")

        return result
