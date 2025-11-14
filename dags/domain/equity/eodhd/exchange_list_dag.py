from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from plugins.operators.lake_operator import LakeOperator
from plugins.pipelines.lake.equity.exchange_list_pipeline import ExchangeInfoPipeline
from plugins.config import constants as C
from plugins.validators.lake_data_validator import LakeDataValidator

with DAG(
    dag_id="exchange_list_dag",
    start_date=datetime(2025, 10, 1),
    schedule="0 18 1 * *",  # 매월 1일 03:00 KST 실행 (UTC 18:00)
    catchup=False,
    tags=["EODHD", "metadata", "exchange_list"],
) as dag:

    start_task = EmptyOperator(task_id="start_pipeline")

    fetch_exchange_list = LakeOperator(
        task_id="fetch_exchange_list",
        pipeline_cls=ExchangeInfoPipeline,
        method_name="fetch_and_load",
        op_kwargs={
            "domain": C.DATA_DOMAINS["exchange_list"],
            "trd_dt": "{{ data_interval_end | ds }}",
            'exchange_code': 'ALL',
            "domain_group": C.DOMAIN_GROUPS["equity"],
            "allow_empty": False
        }
    )

    validate_exchange_list = LakeOperator(
        task_id="validate_exchange_list",
        pipeline_cls=LakeDataValidator,
        method_name="validate",
        op_kwargs={"domain": C.DATA_DOMAINS["exchange_list"], "trd_dt": "{{ data_interval_end | ds }}", 'exchange_code': 'ALL', "vendor": C.VENDORS["eodhd"], "domain_group": C.DOMAIN_GROUPS["equity"]},
    )

    # -------------------------------------------------------------------------
    # 4️⃣ 거래소 웨어하우스 파이프라인 트리거
    # -------------------------------------------------------------------------
    trigger_exchange_warehouse = TriggerDagRunOperator(
        task_id="trigger_exchange_warehouse",
        trigger_dag_id="build_exchange_master_dag",
        conf={
            "exchange_code": "ALL",
            "trd_dt": "{{ data_interval_end | ds }}",
            "triggered_by": "{{ dag.dag_id }}",
            "vendor": C.VENDORS["eodhd"],
            "domain_group": C.DOMAIN_GROUPS["equity"],
        },
        wait_for_completion=False,  # 병렬 파이프라인으로 즉시 넘김
        poke_interval=30,
    )


    end_task = EmptyOperator(task_id="end_pipeline")

    start_task >> fetch_exchange_list >> validate_exchange_list >> trigger_exchange_warehouse >> end_task
