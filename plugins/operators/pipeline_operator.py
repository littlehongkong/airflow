from airflow.models import BaseOperator
from airflow.utils.context import Context
from plugins.utils.pipeline_helper import run_and_log
import inspect


class PipelineOperator(BaseOperator):
    """
    표준화된 파이프라인 실행용 Operator
    """

    template_fields = ("op_kwargs",)
    template_fields_renderers = {"op_kwargs": "json"}  # ← 이게 핵심

    def __init__(
        self,
        pipeline_cls,
        method_name: str,
        postgres_conn_id: str = "postgres_default",
        op_kwargs: dict = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.pipeline_cls = pipeline_cls
        self.method_name = method_name
        self.op_kwargs = op_kwargs or {}
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context: Context):
        """
        Airflow Task 실행 시 호출됨.
        - 사용자 전달 인자(op_kwargs)만 실제 파이프라인으로 전달
        - Airflow 내부 context 값들은 로깅 및 추적용으로만 사용
        """
        self.log.info(f"▶ PipelineOperator 실행: {self.method_name}")

        # 1️⃣ Airflow에서 Jinja 템플릿 치환된 op_kwargs 복제
        rendered_kwargs = self.op_kwargs.copy()

        # 2️⃣ 사용자 지정 인자만 출력 (context는 숨김)
        self.log.info(f"▶ 사용자 전달 인자: {rendered_kwargs}")

        # 3️⃣ 파이프라인 클래스와 메서드 로드
        # pipeline = self.pipeline_cls()
        # method = getattr(pipeline, self.method_name, None)

        instance = self.pipeline_cls(**self.op_kwargs)
        # ✅ 이후 메서드 호출
        method = getattr(instance, self.method_name)

        if not method or not callable(method):
            raise AttributeError(f"{self.pipeline_cls.__name__}에 '{self.method_name}' 메서드가 없습니다.")

        # 4️⃣ Airflow 내부 context는 run_and_log용으로만 전달
        return run_and_log(
            func=method,
            postgres_conn_id=self.postgres_conn_id,
            dag_id=context['dag'].dag_id,
            task_id=self.task_id,
            **rendered_kwargs,  # ✅ 여기엔 사용자 정의 값만
        )
