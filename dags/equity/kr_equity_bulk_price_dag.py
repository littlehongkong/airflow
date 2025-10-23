from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from plugins.operators.pipeline_operator import PipelineOperator
from plugins.pipelines.equity_price_pipeline import EquityPricePipeline
from plugins.validators.equity_price_validator import EquityPriceValidator

DATA_DOMAIN = "equity_prices"

with DAG(
    dag_id="kr_us_equity_price_pipeline",
    description="Fetch, validate and trigger corporate actions for KRX & KOSDAQ prices",
    start_date=datetime(2025, 10, 1),
    schedule="0 19 * * 1-5",  # KST 새벽 4시 실행 (UTC+0 19:00)
    catchup=False,
    max_active_runs=1,
    tags=["EODHD", "equity", "price"],
) as dag:

    # ✅ 1️⃣ Start marker
    start_task = EmptyOperator(task_id="start_pipeline")

    # ✅ 2️⃣ 거래소별 Task 생성
    for exchange_code in ["KO", "KQ"]:  # 한국거래소 / 코스닥

        # 🟦 Fetch & Load
        fetch_and_load = PipelineOperator(
            task_id=f"{exchange_code}_fetch_and_load_price",
            pipeline_cls=EquityPricePipeline,
            method_name="fetch_and_load",
            op_kwargs={
                "exchange_code": "{{ params.exchange_code }}",
                "trd_dt": "{{ macros.ds_add(ds, -1) }}",  # 전일 데이터 수집
                "data_domain": "{{ params.data_domain }}",
            },
            params={
                "exchange_code": exchange_code,
                "data_domain": DATA_DOMAIN,
            },
        )

        # 🟩 Validate
        validate_data = PipelineOperator(
            task_id=f"{exchange_code}_validate_price_data",
            pipeline_cls=EquityPriceValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": "{{ params.exchange_code }}",
                "trd_dt": "{{ macros.ds_add(ds, -1) }}",
                "data_domain": "{{ params.data_domain }}",
                "allow_empty": True,
            },
            params={
                "exchange_code": exchange_code,
                "data_domain": DATA_DOMAIN,
            },
        )

        # 🟥 Trigger Corporate Actions DAG
        trigger_corporate_actions = TriggerDagRunOperator(
            task_id=f"{exchange_code}_trigger_corporate_actions",
            trigger_dag_id="corporate_actions_dag",
            conf={
                "exchange_code": exchange_code,
                "trd_dt": "{{ macros.ds_add(ds, -1) }}",
                "triggered_by": "{{ dag.dag_id }}",
            },
            wait_for_completion=False,
            poke_interval=30,
        )

        # DAG Dependency (각 거래소별 라인)
        start_task >> fetch_and_load >> validate_data >> trigger_corporate_actions

    # ✅ 3️⃣ End marker
    end_task = EmptyOperator(task_id="end_pipeline")

    # ✅ 4️⃣ 모든 거래소 DAG 종료를 end_task로 집결
    [trigger_corporate_actions] >> end_task
