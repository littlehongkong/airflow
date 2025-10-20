from airflow import DAG
from datetime import datetime
from plugins.operators.pipeline_operator import PipelineOperator
from plugins.pipelines.equity_price_pipeline import EquityPricePipeline
from plugins.validators.equity_price_validator import EquityPriceValidator

EXCHANGE_CODE = "US"
DATA_DOMAIN = "equity_prices"

with DAG(
    dag_id="us_equity_price_pipeline",
    start_date=datetime(2025, 10, 14),
    schedule="0 3 * * *",  # 매일 새벽 3시 (수집)
    catchup=False,
    tags=["EODHD", "Price", "Soda"],
) as dag:

    # 1️⃣ 데이터 수집 (raw)
    fetch_and_load = PipelineOperator(
        task_id=f"{EXCHANGE_CODE}_fetch_and_load_price",
        pipeline_cls=EquityPricePipeline,
        method_name="fetch_and_load",
        op_kwargs={
            "exchange_code": "{{ params.exchange_code }}",
            "trd_dt": "{{ macros.ds_add(ds, -1) }}",
            "data_domain": "{{ params.data_domain }}",
        },
        params={
            "exchange_code": EXCHANGE_CODE,
            "data_domain": DATA_DOMAIN,
        },
    )

    # 2️⃣ 데이터 검증 (Pandera + Soda)
    validate_data = PipelineOperator(
        task_id=f"{EXCHANGE_CODE}_validate_price_data",
        pipeline_cls=EquityPriceValidator,
        method_name="validate",
        op_kwargs={
            "exchange_code": "{{ params.exchange_code }}",
            "trd_dt": "{{ macros.ds_add(ds, -1) }}",
            "data_domain": "{{ params.data_domain }}",
        },
        params={
            "exchange_code": EXCHANGE_CODE,
            "data_domain": DATA_DOMAIN,
        },
    )

    fetch_and_load >> validate_data