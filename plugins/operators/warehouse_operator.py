from typing import Dict, Any, Type
from airflow.models import BaseOperator
from plugins.utils.pipeline_helper import run_and_log


class WarehouseOperator(BaseOperator):
    """
    ✅ Warehouse 빌드 전용 Operator (Airflow 3.x 호환)
    - BaseWarehousePipeline 하위 클래스 실행
    - build() 메서드 자동 호출
    - 템플릿 렌더링은 Airflow가 자동 수행
    """

    template_fields = ("op_kwargs",)
    ui_color = "#FFD700"  # Gold color for warehouse tasks

    def __init__(
        self,
        *,
        pipeline_cls: Type,
        op_kwargs: Dict[str, Any] = None,
        cleanup_on_success: bool = True,
        cleanup_on_failure: bool = True,
        postgres_conn_id: str = "postgres_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.pipeline_cls = pipeline_cls
        self.op_kwargs = op_kwargs or {}
        self.cleanup_on_success = cleanup_on_success
        self.cleanup_on_failure = cleanup_on_failure
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        task_id = context["task_instance"].task_id
        dag_id = context["dag"].dag_id

        self.log.info(
            f"🏭 [WarehouseOperator] Starting warehouse build | "
            f"DAG={dag_id} | Task={task_id} | "
            f"Pipeline={self.pipeline_cls.__name__}"
        )

        # ✅ Airflow가 템플릿을 자동으로 렌더링하므로, 그대로 사용
        rendered_kwargs = self.op_kwargs
        self.log.info(f"📋 Pipeline kwargs: {rendered_kwargs}")

        pipeline = None
        try:
            pipeline = self.pipeline_cls(**rendered_kwargs)

            # build() 실행 + 로그 기록
            result = run_and_log(
                func=pipeline.build,
                context=context,
                op_kwargs=rendered_kwargs,
                postgres_conn_id=self.postgres_conn_id,
                dag_id=context['dag'].dag_id,
                task_id=self.task_id,
                layer='warehouse'
            )


            self.log.info(
                f"✅ [SUCCESS] Warehouse build completed | "
                f"Pipeline={self.pipeline_cls.__name__} | "
                f"Result keys: {list(result.keys())}"
            )

            if self.cleanup_on_success and pipeline:
                pipeline.cleanup()
                self.log.info("🧹 Resources cleaned up (success)")

            return result

        except Exception as e:
            self.log.error(
                f"❌ [FAILURE] Warehouse build failed | "
                f"Pipeline={self.pipeline_cls.__name__} | "
                f"Error: {str(e)}"
            )

            if self.cleanup_on_failure and pipeline:
                try:
                    pipeline.cleanup()
                    self.log.info("🧹 Resources cleaned up (failure)")
                except Exception as cleanup_error:
                    self.log.warning(f"⚠️ Cleanup failed: {cleanup_error}")

            raise


class WarehouseBatchOperator(BaseOperator):
    """
    ✅ Warehouse 배치 빌드 Operator (Airflow 3.x 호환)
    - 여러 국가/파티션 반복 실행
    - 템플릿 자동 렌더링
    """

    template_fields = ("batch_configs",)
    ui_color = "#FFA500"

    def __init__(
        self,
        *,
        pipeline_cls: Type,
        batch_configs: list[Dict[str, Any]],
        fail_fast: bool = True,
        cleanup_on_success: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.pipeline_cls = pipeline_cls
        self.batch_configs = batch_configs
        self.fail_fast = fail_fast
        self.cleanup_on_success = cleanup_on_success

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        self.log.info(
            f"🏭 [WarehouseBatchOperator] Starting batch build | "
            f"Pipeline={self.pipeline_cls.__name__} | "
            f"Batch size={len(self.batch_configs)}"
        )

        results = []
        success_count = 0
        failure_count = 0

        # ✅ Airflow가 이미 템플릿 렌더링을 수행했으므로 그대로 사용
        for idx, rendered_config in enumerate(self.batch_configs, 1):
            config_label = (
                rendered_config.get("country_code")
                or rendered_config.get("partition_value")
                or f"config_{idx}"
            )

            self.log.info(f"📦 [{idx}/{len(self.batch_configs)}] Processing: {config_label}")

            pipeline = None
            try:
                pipeline = self.pipeline_cls(**rendered_config)

                result = run_and_log(
                    func=pipeline.build,
                    context=context,
                    op_kwargs=rendered_config,
                )

                results.append({
                    "config": rendered_config,
                    "status": "success",
                    "result": result,
                })
                success_count += 1

                self.log.info(f"✅ [{config_label}] Success")

                if self.cleanup_on_success and pipeline:
                    pipeline.cleanup()

            except Exception as e:
                error_msg = str(e)
                self.log.error(f"❌ [{config_label}] Failed: {error_msg}")
                results.append({
                    "config": rendered_config,
                    "status": "failure",
                    "error": error_msg,
                })
                failure_count += 1

                if pipeline:
                    try:
                        pipeline.cleanup()
                    except:
                        pass

                if self.fail_fast:
                    raise

        summary = {
            "results": results,
            "success_count": success_count,
            "failure_count": failure_count,
            "total_count": len(self.batch_configs),
        }

        self.log.info(
            f"🏁 [BATCH COMPLETE] Success: {success_count}/{len(self.batch_configs)} | "
            f"Failures: {failure_count}"
        )

        if failure_count > 0 and not self.fail_fast:
            self.log.warning(
                f"⚠️ Batch completed with {failure_count} failures. "
                f"Check individual results."
            )

        return summary
