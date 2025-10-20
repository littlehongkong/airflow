from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from plugins.operators.pipeline_operator import PipelineOperator
from plugins.pipelines.fundamental_pipeline import FundamentalPipeline
from plugins.validators.fundamental_validator import FundamentalValidator

EXCHANGES = Variable.get("exchange_config", default_var=["US", "KO", "KQ"], deserialize_json=True)

with DAG(
    dag_id="fundamental_dag",
    start_date=datetime(2025, 10, 14),
    schedule="0 1 * * 6",  # 매주 토요일 10시 (KST)
    catchup=False,
    tags=["EODHD", "Fundamentals"],
    max_active_tasks=1,
) as dag:

    for ex in EXCHANGES:
        fetch_task = PipelineOperator(
            task_id=f"{ex}_fetch_fundamental_data",
            pipeline_cls=FundamentalPipeline,
            method_name="fetch_and_load",
            op_kwargs={
                "exchange_code": ex,
                "data_domain": "fundamentals",
                "trd_dt": "{{ ds }}"
            },
        )

        validate_task = PipelineOperator(
            task_id=f"{ex}_validate_fundamental_data",
            pipeline_cls=FundamentalValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": ex,
                "trd_dt": "{{ ds }}",
                "data_domain": "fundamentals"
            },
        )

        fetch_task >> validate_task
