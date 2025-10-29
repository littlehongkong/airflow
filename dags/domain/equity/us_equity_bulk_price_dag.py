from airflow import DAG
from datetime import datetime
from plugins.operators.pipeline_operator import PipelineOperator
from plugins.pipelines.lake.equity.equity_price_pipeline import EquityPricePipeline
from plugins.validators.lake.equity.equity_price_validator import EquityPriceValidator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from plugins.config.constants import VENDORS, DATA_DOMAINS

EXCHANGE_CODE = "US"

with DAG(
    dag_id="us_equity_price_pipeline",
    start_date=datetime(2025, 10, 14),
    schedule="0 23 * * *",  # 매일 새벽 3시 (수집)
    catchup=False,
    tags=["EODHD", "Price", "Soda"],
) as dag:

    start_task = EmptyOperator(task_id="start_pipeline")

    # 1️⃣ 데이터 수집 (raw)
    fetch_and_load = PipelineOperator(
        task_id=f"{EXCHANGE_CODE}_fetch_and_load_price",
        pipeline_cls=EquityPricePipeline,
        method_name="fetch_and_load",
        op_kwargs={
            "exchange_code": "{{ params.exchange_code }}",
            "trd_dt": "{{ ds }}",
            "data_domain": "{{ params.data_domain }}",
        },
        params={
            "exchange_code": EXCHANGE_CODE,
            "data_domain": DATA_DOMAINS['PRICES']
        },
    )

    # 2️⃣ 데이터 검증 (Pandera + Soda)
    validate_data = PipelineOperator(
        task_id=f"{EXCHANGE_CODE}_validate_price_data",
        pipeline_cls=EquityPriceValidator,
        method_name="validate",
        op_kwargs={
            "exchange_code": "{{ params.exchange_code }}",
            "trd_dt": "{{ ds }}",
            "data_domain": "{{ params.data_domain }}",
            "allow_empty": True,
            "vendor": VENDORS['EODHD']
        },
        params={
            "exchange_code": EXCHANGE_CODE,
            "data_domain": DATA_DOMAINS["PRICES"],
        },
    )

    # 3️⃣ Corporate Actions DAG 트리거 (거래소 코드 전달)
    trigger_corporate_actions = TriggerDagRunOperator(
        task_id=f"{EXCHANGE_CODE}_trigger_corporate_actions",
        trigger_dag_id="corporate_actions_dag",
        conf={
            "exchange_code": EXCHANGE_CODE,
            "trd_dt": "{{ ds }}",
            "triggered_by": "{{ dag.dag_id }}",
        },
        wait_for_completion=False,  # 비동기 실행
        poke_interval=30,
    )

    end_task = EmptyOperator(task_id="end_pipeline")

    start_task >> fetch_and_load >> validate_data >> trigger_corporate_actions >> end_task