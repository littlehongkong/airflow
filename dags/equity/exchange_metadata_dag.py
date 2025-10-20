# dags/exchange_metadata_dag.py
from datetime import datetime
from airflow import DAG
from airflow.models import Variable

from plugins.operators.pipeline_operator import PipelineOperator
from plugins.pipelines.symbol_list_pipeline import SymbolListPipeline
from plugins.pipelines.symbol_change_pipeline import SymbolChangePipeline
from plugins.pipelines.exchange_holiday_pipeline import ExchangeHolidayPipeline
from plugins.validators.symbol_list_validator import SymbolListValidator
from plugins.validators.symbol_change_validator import SymbolChangeValidator
from plugins.validators.exchange_holiday_validator import ExchangeHolidayValidator


with DAG(
    dag_id="exchange_metadata_dag",
    start_date=datetime(2025, 10, 15),
    schedule="0 19 * * 1-5",  # KST 기준 새벽 4시 실행(서버 TZ에 맞게 운영환경에서 조정)
    catchup=False,
    tags=["EODHD", "metadata", "exchange"],
) as dag:

    EXCHANGES = Variable.get("exchange_config", default_var=["US"], deserialize_json=True)

    symbol_tasks = []
    for ex in EXCHANGES:
        fetch_symbols = PipelineOperator(
            task_id=f"{ex}_fetch_symbol_list",
            pipeline_cls=SymbolListPipeline,
            method_name="fetch_and_load",
            op_kwargs={"exchange_code": ex, "data_domain": "symbol_list", "trd_dt": "{{ ds }}"},
        )

        validate_symbols = PipelineOperator(
            task_id=f"{ex}_validate_symbol_list",
            pipeline_cls=SymbolListValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": ex,
                "trd_dt": "{{ ds }}",
                "data_domain": "symbol_list",
            },
        )

        fetch_symbols >> validate_symbols
        symbol_tasks.append(validate_symbols)

    # 심볼 변경 이력 (US만)
    change_task = PipelineOperator(
        task_id="US_fetch_symbol_changes",
        pipeline_cls=SymbolChangePipeline,
        method_name="fetch_and_load",
        op_kwargs={"exchange_code": "US", "data_domain": "symbol_changes", "trd_dt": "{{ ds }}"},
    )

    validate_change = PipelineOperator(
        task_id="US_validate_symbol_changes",
        pipeline_cls=SymbolChangeValidator,
        method_name="validate",
        op_kwargs={
            "exchange_code": "US",
            "trd_dt": "{{ ds }}",
            "data_domain": "symbol_changes",
        },
    )

    change_task >> validate_change

    fetch_holiday = PipelineOperator(
        task_id="US_fetch_exchange_holidays",
        pipeline_cls=ExchangeHolidayPipeline,
        method_name="fetch_and_load",
        op_kwargs={"exchange_code": 'US', "data_domain": "exchange_holiday", "trd_dt": "{{ ds }}"},
    )

    validate_holiday = PipelineOperator(
        task_id="US_validate_exchange_holidays",
        pipeline_cls=ExchangeHolidayValidator,
        method_name="validate",
        op_kwargs={
            "exchange_code": 'US',
            "trd_dt": "{{ ds }}",
            "data_domain": "exchange_holiday",
        },
    )

    fetch_holiday >> validate_holiday

    # 순서 연결
    for s in symbol_tasks:
        s >> change_task >> validate_change >> fetch_holiday >> validate_holiday
