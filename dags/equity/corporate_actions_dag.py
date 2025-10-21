# dags/corporate_actions_dag.py
from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from plugins.operators.pipeline_operator import PipelineOperator
from plugins.pipelines.equity_split_pipeline import EquitySplitPipeline
from plugins.pipelines.equity_dividend_pipeline import EquityDividendPipeline
from plugins.validators.equity_split_validator import EquitySplitValidator
from plugins.validators.equity_dividend_validator import EquityDividendValidator

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="corporate_actions_dag",
    default_args=default_args,
    description="Collect and validate corporate actions (splits, dividends) - Triggered by price DAGs",
    schedule=None,  # 매일 오전 6시
    start_date=datetime(2025, 10, 19),
    catchup=False,
    max_active_runs=1,
    tags=["corporate_actions", "splits", "dividends", "triggered"]
) as dag:
    # dag_run.conf에서 exchange_code 가져오기
    exchange_code = "{{ dag_run.conf.get('exchange_code') }}"
    trd_dt = "{{ dag_run.conf.get('trd_dt', ds) }}"

    assert exchange_code is not None and exchange_code != '', "exchange_code is required"

    with TaskGroup(group_id=f"{exchange_code}_actions") as exchange_group:

        # ✅ 1️⃣ Splits 파이프라인
        fetch_splits = PipelineOperator(
            task_id=f"{exchange_code}_fetch_splits",
            pipeline_cls=EquitySplitPipeline,
            method_name="fetch_and_load",
            op_kwargs={"exchange_code": exchange_code, "data_domain": "splits", "trd_dt": trd_dt},
        )

        validate_splits = PipelineOperator(
            task_id=f"{exchange_code}_validate_splits",
            pipeline_cls=EquitySplitValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": exchange_code,
                "trd_dt": trd_dt,
                "data_domain": "splits",
            },
        )

        # ✅ 2️⃣ Dividends 파이프라인
        fetch_dividends = PipelineOperator(
            task_id=f"{exchange_code}_fetch_dividends",
            pipeline_cls=EquityDividendPipeline,
            method_name="fetch_and_load",
            op_kwargs={"exchange_code": exchange_code, "data_domain": "dividends", "trd_dt": trd_dt}
        )

        validate_dividends = PipelineOperator(
            task_id=f"{exchange_code}_validate_dividends",
            pipeline_cls=EquityDividendValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": exchange_code,
                "trd_dt": trd_dt, # "{{ ds }}"
                "data_domain": "dividends",
            },
        )

        # ✅ 관계 정의: fetch → validate (각 도메인별)
        fetch_splits >> validate_splits
        fetch_dividends >> validate_dividends
