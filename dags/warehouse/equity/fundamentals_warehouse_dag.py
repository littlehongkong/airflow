"""
Airflow DAG: Fundamentals Warehouse Build (Ticker-Split Key Partition)
-----------------------------------------------------------------------
💡 기능 요약
- Data Lake에 있는 fundamentals JSON 파일을 읽어서
  각 종목별 폴더(ticker=XXX)로 key별 parquet 파일로 저장
- exchange_code, trd_dt 파티션 구조 유지
- 후속 DAG(AssetMaster, Validation 등)에서 warehouse 데이터를 재활용
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.empty import EmptyOperator
from plugins.operators.warehouse_operator import WarehouseOperator
from plugins.pipelines.warehouse.fundamentals_ticker_split_pipeline import FundamentalsTickerSplitPipeline
from plugins.config.constants import DOMAIN_GROUPS, VENDORS

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
    dag_id="fundamentals_warehouse_dag",
    default_args=default_args,
    schedule=None,  # 수동/Trigger 기반
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["warehouse", "fundamentals"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")

    # 1️⃣ Warehouse Build (Ticker-Split)
    build_fundamentals = WarehouseOperator(
        task_id="build_fundamentals_ticker_split",
        pipeline_cls=FundamentalsTickerSplitPipeline,
        op_kwargs={
            "domain_group": "{{ dag_run.conf.get('domain_group', '') }}",
            "vendor":  "{{ dag_run.conf.get('exchange_code', '') }}",
            "exchange_code": "{{ dag_run.conf.get('exchange_code', '') }}",   # ✅ 국가별 거래소
            "trd_dt": "{{ dag_run.conf.get('trd_dt', '') }}",
            "trigger_source": "{{ dag_run.conf.get('trigger_source', 'manual') }}"
        },
    )

    # (Optional) 2️⃣ Validation Task (General key only)
    # 필요시 아래 주석 해제
    # validate_fundamentals = WarehouseOperator(
    #     task_id="validate_fundamentals_general",
    #     pipeline_cls=WarehouseDataValidator,
    #     op_kwargs={
    #         "domain": "fundamentals_general",
    #         "domain_group": DOMAIN_GROUPS["equity"],
    #         "trd_dt": "{{ ds }}",
    #     },
    # )

    end = EmptyOperator(task_id="end_pipeline")

    # 실행 순서
    start >> build_fundamentals >> end
