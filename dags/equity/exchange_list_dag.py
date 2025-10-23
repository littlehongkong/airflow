from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from plugins.operators.pipeline_operator import PipelineOperator
from plugins.pipelines.exchange_list_pipeline import ExchangeInfoPipeline
from plugins.validators.exchange_list_validator import ExchangeInfoValidator

with DAG(
    dag_id="exchange_list_dag",
    start_date=datetime(2025, 10, 1),
    schedule="0 18 1 * *",  # 매월 1일 03:00 KST 실행 (UTC 18:00)
    catchup=False,
    tags=["EODHD", "metadata", "exchange_list"],
) as dag:

    start_task = EmptyOperator(task_id="start_pipeline")

    fetch_exchange_list = PipelineOperator(
        task_id="fetch_exchange_list",
        pipeline_cls=ExchangeInfoPipeline,
        method_name="fetch_and_load",
        op_kwargs={"data_domain": "exchange_list", "trd_dt": "{{ ds }}", 'exchange_code': 'ALL'},
    )

    validate_exchange_list = PipelineOperator(
        task_id="validate_exchange_list",
        pipeline_cls=ExchangeInfoValidator,
        method_name="validate",
        op_kwargs={"data_domain": "exchange_list", "trd_dt": "{{ ds }}", 'exchange_code': 'ALL'},
    )

    end_task = EmptyOperator(task_id="end_pipeline")

    start_task >> fetch_exchange_list >> validate_exchange_list >> end_task
