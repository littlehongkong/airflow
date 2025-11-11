from airflow import DAG
from datetime import datetime
from plugins.operators.lake_operator import LakeOperator
from plugins.pipelines.lake.equity.equity_price_pipeline import EquityPricePipeline
from plugins.validators.lake_data_validator import LakeDataValidator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from plugins.config import constants as C

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
    fetch_and_load = LakeOperator(
        task_id=f"{EXCHANGE_CODE}_fetch_and_load_price",
        pipeline_cls=EquityPricePipeline,
        method_name="fetch_and_load",
        op_kwargs={
            "exchange_code": "{{ params.exchange_code }}",
            "trd_dt": "{{ data_interval_end | ds }}",
            "domain": "{{ params.domain }}",
            "domain_group": C.DOMAIN_GROUPS["equity"]
        },
        params={
            "exchange_code": EXCHANGE_CODE,
            "domain": C.DATA_DOMAINS['prices']
        },
    )

    # 2️⃣ 데이터 검증 (Pandera + Soda)
    validate_data = LakeOperator(
        task_id=f"{EXCHANGE_CODE}_validate_price_data",
        pipeline_cls=LakeDataValidator,
        method_name="validate",
        op_kwargs={
            "exchange_code": "{{ params.exchange_code }}",
            "trd_dt": "{{ data_interval_end | ds }}",
            "domain": "{{ params.domain }}",
            "allow_empty": True,
            "vendor": C.VENDORS["eodhd"],
            "domain_group": C.DOMAIN_GROUPS["equity"]
        },
        params={
            "exchange_code": EXCHANGE_CODE,
            "domain": C.DATA_DOMAINS["prices"],
        },
    )

    # 3️⃣ Corporate Actions DAG 트리거 (거래소 코드 전달)
    trigger_corporate_actions = TriggerDagRunOperator(
        task_id=f"{EXCHANGE_CODE}_trigger_corporate_actions",
        trigger_dag_id="corporate_actions_dag",
        conf={
            "exchange_code": EXCHANGE_CODE,
            "trd_dt": "{{ data_interval_end | ds }}",
            "triggered_by": "{{ dag.dag_id }}",
        },
        wait_for_completion=False,  # 비동기 실행
        poke_interval=30,
    )

    trigger_price_warehouse = TriggerDagRunOperator(
        task_id=f"{EXCHANGE_CODE}_trigger_price_warehouse",
        trigger_dag_id="price_warehouse_dag",
        conf={
            "country_code": "USA",  # 🇰🇷 or "US", "JP" 등 다른 국가 가능
            "trd_dt": "{{ data_interval_end | ds }}",
            "triggered_by": "{{ dag.dag_id }}",
            "vendor": C.VENDORS['eodhd'],
            "domain_group": C.DOMAIN_GROUPS['equity']
        },
        wait_for_completion=False,
        poke_interval=30,
    )

    end_task = EmptyOperator(task_id="end_pipeline")

    start_task >> fetch_and_load >> validate_data >> trigger_corporate_actions >> trigger_price_warehouse >> end_task