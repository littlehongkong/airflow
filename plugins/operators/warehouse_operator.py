from typing import Dict, Any, Type
from airflow.models import BaseOperator
from plugins.utils.pipeline_helper import run_and_log


class WarehouseOperator(BaseOperator):
    """
    ✅ Warehouse 빌드/검증 전용 Operator (Airflow 3.x 호환)
    - BaseWarehousePipeline: build()
    - BaseWarehouseValidator: validate()
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
            f"🏭 [WarehouseOperator] Starting warehouse process | "
            f"DAG={dag_id} | Task={task_id} | "
            f"Pipeline={self.pipeline_cls.__name__}"
        )

        op_kwargs = self.op_kwargs
        self.log.info(f"📋 Pipeline kwargs: {op_kwargs}")

        pipeline = None
        try:
            pipeline = self.pipeline_cls(**op_kwargs)

            # ✅ build() 또는 validate() 자동 식별
            if hasattr(pipeline, "build"):
                func = pipeline.build
                self.log.info("🏗️ Detected Pipeline class → executing build()")
            elif hasattr(pipeline, "validate"):
                func = pipeline.validate
                self.log.info("🔍 Detected Validator class → executing validate()")
            else:
                raise AttributeError(
                    f"❌ {self.pipeline_cls.__name__} has no valid method (build/validate)"
                )

            # 실행 + 로그 기록
            result = run_and_log(
                func=func,
                context=context,
                op_kwargs=op_kwargs,
                postgres_conn_id=self.postgres_conn_id,
                dag_id=dag_id,
                task_id=self.task_id,
                layer="warehouse",
            )

            self.log.info(
                f"✅ [SUCCESS] Warehouse process completed | "
                f"Pipeline={self.pipeline_cls.__name__} | "
                f"Result keys: {list(result.keys())}"
            )

            if self.cleanup_on_success and hasattr(pipeline, "cleanup"):
                pipeline.cleanup()
                self.log.info("🧹 Resources cleaned up (success)")

            return result

        except Exception as e:
            self.log.error(
                f"❌ [FAILURE] Warehouse process failed | "
                f"Pipeline={self.pipeline_cls.__name__} | "
                f"Error: {str(e)}"
            )

            if self.cleanup_on_failure and hasattr(pipeline, "cleanup"):
                try:
                    pipeline.cleanup()
                    self.log.info("🧹 Resources cleaned up (failure)")
                except Exception as cleanup_error:
                    self.log.warning(f"⚠️ Cleanup failed: {cleanup_error}")

            raise


class WarehouseBatchOperator(BaseOperator):
    """
    ✅ Warehouse 배치 빌드/검증 Operator (Airflow 3.x 호환)
    - 여러 파티션 반복 실행
    - build()/validate() 자동 감지
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
            f"🏭 [WarehouseBatchOperator] Starting batch process | "
            f"Pipeline={self.pipeline_cls.__name__} | "
            f"Batch size={len(self.batch_configs)}"
        )

        results = []
        success_count = 0
        failure_count = 0

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

                # ✅ build() / validate() 자동 인식
                if hasattr(pipeline, "build"):
                    func = pipeline.build
                elif hasattr(pipeline, "validate"):
                    func = pipeline.validate
                else:
                    raise AttributeError(f"{self.pipeline_cls.__name__} has no valid method")

                result = run_and_log(
                    func=func,
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

                if self.cleanup_on_success and hasattr(pipeline, "cleanup"):
                    pipeline.cleanup()

            except Exception as e:
                self.log.error(f"❌ [{config_label}] Failed: {str(e)}")
                results.append({
                    "config": rendered_config,
                    "status": "failure",
                    "error": str(e),
                })
                failure_count += 1

                if hasattr(pipeline, "cleanup"):
                    try:
                        pipeline.cleanup()
                    except Exception:
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

        return summary
