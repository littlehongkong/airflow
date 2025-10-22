# dags/corporate_actions_dag.py
from airflow import DAG
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
}

with DAG(
    dag_id="corporate_actions_dag",
    default_args=default_args,
    description="Triggered by price DAGs to collect & validate splits/dividends",
    schedule=None,
    start_date=datetime(2025, 10, 19),
    catchup=False,
    max_active_runs=3,
    tags=["corporate_actions", "splits", "dividends", "triggered"],
) as dag:

    exchange_code = "{{ dag_run.conf.get('exchange_code') }}"
    trd_dt = "{{ dag_run.conf.get('trd_dt', ds) }}"

    with TaskGroup(group_id="actions") as tg:
        fetch_splits = PipelineOperator(
            task_id="fetch_splits",
            pipeline_cls=EquitySplitPipeline,
            method_name="fetch_and_load",
            op_kwargs={
                "exchange_code": exchange_code,
                "data_domain": "splits",
                "trd_dt": trd_dt,
            },
        )

        validate_splits = PipelineOperator(
            task_id="validate_splits",
            pipeline_cls=EquitySplitValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": exchange_code,
                "trd_dt": trd_dt,
                "data_domain": "splits",
                "allow_empty": True,
            },
        )

        fetch_dividends = PipelineOperator(
            task_id="fetch_dividends",
            pipeline_cls=EquityDividendPipeline,
            method_name="fetch_and_load",
            op_kwargs={
                "exchange_code": exchange_code,
                "data_domain": "dividends",
                "trd_dt": trd_dt,
            },
        )

        validate_dividends = PipelineOperator(
            task_id="validate_dividends",
            pipeline_cls=EquityDividendValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": exchange_code,
                "trd_dt": trd_dt,
                "data_domain": "dividends",
                "allow_empty": True,
            },
        )

        fetch_splits >> validate_splits
        fetch_dividends >> validate_dividends
