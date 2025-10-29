"""
Airflow DAG: Exchange Master Warehouse Build + Validation + Publish
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from plugins.operators.warehouse_operator import WarehouseOperator
from plugins.pipelines.warehouse.exchange_master_pipeline import ExchangeMasterPipeline
from plugins.validators.warehouse.exchange_master_validator import ExchangeMasterValidator
from plugins.config.constants import DATA_DOMAINS

# 기본 설정
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="exchange_master_dag",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["warehouse", "exchange_master"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 1️⃣ Warehouse Build
    build_exchange_master = WarehouseOperator(
        task_id="build_exchange_master",
        pipeline_cls=ExchangeMasterPipeline,
        op_kwargs={
            "snapshot_dt": "{{ ds }}",
        },
    )

    # 2️⃣ Validation Task (유효성검증 + validated 이관)
    validate_exchange_master = WarehouseOperator(
        task_id="validate_exchange_master",
        pipeline_cls=ExchangeMasterValidator,  # ✅ BaseWarehouseValidator 상속
        op_kwargs={
            "snapshot_dt": "{{ ds }}"
        },
    )

    end = EmptyOperator(task_id="end")

    start >> build_exchange_master >> validate_exchange_master >> end
