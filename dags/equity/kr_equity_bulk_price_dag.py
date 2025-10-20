# dags/equity/kr_equity_bulk_price_dag.py

from airflow import DAG
from datetime import datetime, timedelta
from pendulum import timezone
from plugins.operators.pipeline_operator import PipelineOperator
from plugins.pipelines.equity_price_pipeline import EquityPricePipeline
from plugins.validators.equity_price_validator import EquityPriceValidator

KST = timezone("Asia/Seoul")

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
}

DATA_DOMAIN = "equity_prices"

with DAG(
    dag_id="kr_equity_bulk_price_dag",
    start_date=datetime(2025, 10, 10, tzinfo=KST),
    schedule="0 10 * * 1-5",  # ✅ 한국시간 평일 오후 7시 실행
    catchup=False,
    tags=["EODHD", "Equity", "Korea"],
    default_args=default_args,
) as dag:

    for exchange_code in ["KO", "KQ"]:  # 한국거래소, 코스닥

        fetch_and_load = PipelineOperator(
            task_id=f"{exchange_code}_fetch_and_load_price",
            pipeline_cls=EquityPricePipeline,
            method_name="fetch_and_load",
            op_kwargs={
                "exchange_code": "{{ params.exchange_code }}",
                "trd_dt": "{{ macros.ds_add(ds, -1) }}",
                "data_domain": "{{ params.data_domain }}",
            },
            params={
                "exchange_code": exchange_code,
                "data_domain": DATA_DOMAIN,
            },
        )

        # 2️⃣ 데이터 검증 (Pandera + Soda)
        validate_data = PipelineOperator(
            task_id=f"{exchange_code}_validate_price_data",
            pipeline_cls=EquityPriceValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": "{{ params.exchange_code }}",
                "trd_dt": "{{ macros.ds_add(ds, -1) }}",
                "data_domain": "{{ params.data_domain }}",
            },
            params={
                "exchange_code": exchange_code,
                "data_domain": DATA_DOMAIN,
            },
        )

        fetch_and_load >> validate_data
