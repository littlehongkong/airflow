from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from datetime import datetime

from plugins.operators.pipeline_operator import PipelineOperator
from plugins.pipelines.symbol_list_pipeline import SymbolListPipeline
from plugins.validators.symbol_list_validator import SymbolListValidator
from plugins.pipelines.exchange_holiday_pipeline import ExchangeHolidayPipeline
from plugins.validators.exchange_holiday_validator import ExchangeHolidayValidator

with DAG(
    dag_id="exchange_metadata_dag",
    description="Collect and validate exchange metadata: symbol list, symbol changes, holidays",
    start_date=datetime(2025, 10, 15),
    schedule="0 19 * * 1-5",  # KST 새벽 4시 (UTC 19시)
    catchup=False,
    max_active_runs=1,
    tags=["EODHD", "metadata", "exchange"],
) as dag:

    # ✅ 1️⃣ Start marker
    start_task = EmptyOperator(task_id="start_pipeline")

    # ✅ 2️⃣ 거래소 목록 (Airflow Variable에서 관리)
    EXCHANGES = Variable.get("exchange_config", default_var=["US"], deserialize_json=True)

    # ✅ 3️⃣ 심볼 리스트 수집 및 검증
    symbol_validate_tasks = []

    for exchange_code in EXCHANGES:

        # 🟦 Symbol List Fetch
        fetch_symbols = PipelineOperator(
            task_id=f"{exchange_code}_fetch_symbol_list",
            pipeline_cls=SymbolListPipeline,
            method_name="fetch_and_load",
            op_kwargs={
                "exchange_code": exchange_code,
                "data_domain": "symbol_list",
                "trd_dt": "{{ ds }}",
            },
        )

        # 🟩 Symbol List Validate
        validate_symbols = PipelineOperator(
            task_id=f"{exchange_code}_validate_symbol_list",
            pipeline_cls=SymbolListValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": exchange_code,
                "trd_dt": "{{ ds }}",
                "data_domain": "symbol_list",
                "allow_empty": False,
            },
        )

        start_task >> fetch_symbols >> validate_symbols
        symbol_validate_tasks.append(validate_symbols)


    # ✅ 5️⃣ Exchange Holiday (미국 거래소만)
    fetch_holidays = PipelineOperator(
        task_id="US_fetch_exchange_holidays",
        pipeline_cls=ExchangeHolidayPipeline,
        method_name="fetch_and_load",
        op_kwargs={
            "exchange_code": "US",
            "data_domain": "exchange_holiday",
            "trd_dt": "{{ ds }}",
        },
    )

    validate_holidays = PipelineOperator(
        task_id="US_validate_exchange_holidays",
        pipeline_cls=ExchangeHolidayValidator,
        method_name="validate",
        op_kwargs={
            "exchange_code": "US",
            "trd_dt": "{{ ds }}",
            "data_domain": "exchange_holiday",
            "allow_empty": True,
        },
    )

    # ✅ 6️⃣ End marker
    end_task = EmptyOperator(task_id="end_pipeline")

    # ✅ 7️⃣ DAG dependency 설정
    for validate_symbols in symbol_validate_tasks:
        validate_symbols >> fetch_holidays >> validate_holidays >> end_task
