from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from plugins.operators.pipeline_operator import PipelineOperator
from plugins.pipelines.fundamental_pipeline import FundamentalPipeline
from plugins.validators.fundamental_validator import FundamentalValidator

# 거래소 목록 (Variable 또는 상수)
EXCHANGES = ["US", "KO", "KQ"]

with DAG(
    dag_id="fundamental_dag",
    description="Fetch and validate fundamentals for multiple exchanges",
    start_date=datetime(2025, 10, 14),
    schedule="0 1 * * 6",  # 매주 토요일 오전 10시 (KST) → UTC 기준 금요일 01시
    catchup=False,
    max_active_runs=1,
    tags=["EODHD", "metadata", "fundamentals"],
) as dag:

    # ✅ 1️⃣ Start marker
    start_task = EmptyOperator(task_id="start_pipeline")

    # ✅ 2️⃣ 거래소별 수집 및 검증 태스크
    #for exchange_code in EXCHANGES:
    for exchange_code in ['US']:

        # 🟦 Fetch fundamentals
        fetch_fundamentals = PipelineOperator(
            task_id=f"{exchange_code}_fetch_fundamental_data",
            pipeline_cls=FundamentalPipeline,
            method_name="fetch_and_load",
            op_kwargs={
                "exchange_code": exchange_code,
                "data_domain": "fundamentals",
                "trd_dt": "{{ ds }}"
            },
        )

        # 🟩 Validate fundamentals
        validate_fundamentals = PipelineOperator(
            task_id=f"{exchange_code}_validate_fundamental_data",
            pipeline_cls=FundamentalValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": exchange_code,
                "trd_dt": "{{ ds }}",
                "data_domain": "fundamentals",
                "allow_empty": True,  # 일부 거래소 데이터가 비어 있을 수 있음
            },
        )

        # DAG dependency
        start_task >> fetch_fundamentals >> validate_fundamentals

    # ✅ 3️⃣ End marker
    end_task = EmptyOperator(task_id="end_pipeline")

    # ✅ 4️⃣ 모든 거래소의 검증 완료 후 종료
    [validate_fundamentals] >> end_task
