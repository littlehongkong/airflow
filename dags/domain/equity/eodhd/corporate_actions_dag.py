# dags/corporate_actions_dag.py
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

from plugins.operators.lake_operator import LakeOperator
from plugins.pipelines.lake.equity.equity_split_pipeline import EquitySplitPipeline
from plugins.pipelines.lake.equity.equity_dividend_pipeline import EquityDividendPipeline
from plugins.pipelines.lake.equity.symbol_changes_pipeline import SymbolChangePipeline
from plugins.config import constants as C
from plugins.validators.lake_data_validator import LakeDataValidator

# -----------------------------------------------------------
# ✅ BranchPythonOperator용 함수: 미국 거래소만 실행
# -----------------------------------------------------------
def should_run_symbol_changes(**context):
    ex = context["dag_run"].conf.get("exchange_code")
    return "run_symbol_changes" if ex == "US" else "skip_symbol_changes"

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
}

# -----------------------------------------------------------
# ✅ DAG 정의
# -----------------------------------------------------------
with DAG(
    dag_id="corporate_actions_dag",
    default_args=default_args,
    description="Triggered by price DAGs to collect & validate splits/dividends/symbol changes",
    schedule=None,
    start_date=datetime(2025, 10, 19),
    catchup=False,
    max_active_runs=3,
    tags=["corporate_actions", "splits", "dividends", "symbol_changes", "triggered"],
) as dag:

    exchange_code = "{{ dag_run.conf.get('exchange_code') }}"
    trd_dt = "{{ dag_run.conf.get('trd_dt', ds) }}"

    # ===========================================================
    # 🎯 1️⃣ Splits & Dividends TaskGroup
    # ===========================================================
    with TaskGroup(group_id="actions") as tg:
        fetch_splits = LakeOperator(
            task_id="fetch_splits",
            pipeline_cls=EquitySplitPipeline,
            method_name="fetch_and_load",
            op_kwargs={
                "exchange_code": exchange_code,
                "domain": C.DATA_DOMAINS["splits"],
                "domain_group": C.DOMAIN_GROUPS["equity"],
                "trd_dt": trd_dt,
                "allow_empty": True
            },
        )

        validate_splits = LakeOperator(
            task_id="validate_splits",
            pipeline_cls=LakeDataValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": exchange_code,
                "trd_dt": trd_dt,
                "domain": C.DATA_DOMAINS["splits"],
                "domain_group": C.DOMAIN_GROUPS["equity"],
                "vendor": C.VENDORS["eodhd"]
            },
        )

        fetch_dividends = LakeOperator(
            task_id="fetch_dividends",
            pipeline_cls=EquityDividendPipeline,
            method_name="fetch_and_load",
            op_kwargs={
                "exchange_code": exchange_code,
                "domain": C.DATA_DOMAINS["dividends"],
                "domain_group": C.DOMAIN_GROUPS["equity"],
                "trd_dt": trd_dt,
                "allow_empty": True
            },
        )

        validate_dividends = LakeOperator(
            task_id="validate_dividends",
            pipeline_cls=LakeDataValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": exchange_code,
                "trd_dt": trd_dt,
                "domain": C.DATA_DOMAINS["dividends"],
                "domain_group": C.DOMAIN_GROUPS["equity"],
                "vendor": C.VENDORS["eodhd"]
            },
        )

        fetch_splits >> validate_splits
        fetch_dividends >> validate_dividends

    # ===========================================================
    # 🎯 2️⃣ Symbol Changes (미국 전용)
    # ===========================================================
    branch = BranchPythonOperator(
        task_id="check_exchange_for_symbol_changes",
        python_callable=should_run_symbol_changes,
    )

    run_symbol_changes = LakeOperator(
        task_id="run_symbol_changes",
        pipeline_cls=SymbolChangePipeline,
        method_name="fetch_and_load",
        op_kwargs={
            "exchange_code": exchange_code,
            "domain": C.DATA_DOMAINS['symbol_changes'],
            "domain_group": C.DOMAIN_GROUPS["equity"],
            "trd_dt": trd_dt,
            "allow_empty": True
        },
    )

    validate_symbol_changes = LakeOperator(
        task_id="validate_symbol_changes",
        pipeline_cls=LakeDataValidator,
        method_name="validate",
        op_kwargs={
            "exchange_code": exchange_code,
            "domain": C.DATA_DOMAINS['symbol_changes'],
            "domain_group": C.DOMAIN_GROUPS["equity"],
            "trd_dt": trd_dt,
            "vendor": C.VENDORS["eodhd"]
        },
    )

    skip_symbol_changes = EmptyOperator(task_id="skip_symbol_changes")

    # ===========================================================
    # 🎯 3️⃣ DAG 실행 순서 정의
    # ===========================================================
    # 1. 기본 Corporate Actions (Splits/Dividends)
    tg >> branch

    # 2. 미국 거래소일 경우만 Symbol Change 실행
    branch >> run_symbol_changes >> validate_symbol_changes

    # 3. 그 외 거래소는 Skip 후 종료
    branch >> skip_symbol_changes
