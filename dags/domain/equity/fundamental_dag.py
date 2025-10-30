from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime
from plugins.operators.pipeline_operator import PipelineOperator
from plugins.pipelines.lake.equity.fundamental_pipeline import FundamentalPipeline
from plugins.validators.lake.equity.fundamental_validator import FundamentalValidator
from plugins.utils.symbol_loader import load_symbols_from_datalake_pd
import json
from plugins.config.constants import VENDORS, DATA_DOMAINS

# ---------------------------------------------------------------------
# ⚙️ Variable 로드 (Airflow UI → Admin → Variables)
# ---------------------------------------------------------------------
EXCHANGES = json.loads(Variable.get("exchange_config", default_var='["US","KO","KQ"]'))

BATCH_SIZE = int(Variable.get("FUNDAMENTALS_BATCH_SIZE", default_var=30))

# ---------------------------------------------------------------------
# 🧭 DAG 정의
# ---------------------------------------------------------------------
with DAG(
    dag_id="fundamental_dag",
    description="Parallel fundamentals fetch & validate (Variable-controlled)",
    start_date=datetime(2025, 10, 14),
    schedule="0 1 * * 6",  # 매주 토요일 오전 10시 (KST)
    catchup=False,
    max_active_runs=1,
    max_active_tasks=60,
    tags=["EODHD", "metadata", "fundamentals"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")
    end = EmptyOperator(task_id="end_pipeline")

    # -------------------------------------------------------------
    # ✅ 거래소별 동적 Task 구성
    # -------------------------------------------------------------
    for exchange_code in EXCHANGES:

        # ---------------------------------------------------------
        # 1️⃣ 거래소 종목 로드
        # ---------------------------------------------------------
        @task(task_id=f"load_symbols_{exchange_code}")
        def load_symbols(trd_dt: str) -> list:
            df = load_symbols_from_datalake_pd(exchange_code=exchange_code, trd_dt=trd_dt, vendor=VENDORS["EODHD"])
            symbols = df["Code"].dropna().astype(str).unique().tolist()
            # symbols = ['AAPL', 'TSLA', 'LABU', 'TMF']
            return symbols

        # ---------------------------------------------------------
        # 2️⃣ 배치 분할
        # ---------------------------------------------------------
        @task(task_id=f"split_batches_{exchange_code}")
        def split_batches(symbols: list, batch_size: int = BATCH_SIZE) -> list[dict]:
            batches = []
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                batches.append({
                    "batch_symbols": batch,
                    "batch_index": i // batch_size + 1
                })
            return batches

        # ---------------------------------------------------------
        # 3️⃣ 병렬 수집 (Dynamic Task Mapping)
        # ---------------------------------------------------------
        @task(task_id=f"fetch_batch_{exchange_code}")
        def fetch_batch(batches: dict, trd_dt: str, exchange_code: str) -> dict:
            batch_symbols = batches["batch_symbols"]
            batch_index = batches["batch_index"]

            pipeline = FundamentalPipeline(
                data_domain="fundamentals",
                exchange_code=exchange_code,
                trd_dt=trd_dt,
            )
            return pipeline.fetch_and_load(
                batch_symbols=batch_symbols,
                batch_index=batch_index,
                exchange_code=exchange_code,
                trd_dt=trd_dt
            )


        # ---------------------------------------------------------
        # 4️⃣ 검증 Task
        # ---------------------------------------------------------
        validate_task = PipelineOperator(
            task_id=f"{exchange_code}_validate_fundamental_data",
            pipeline_cls=FundamentalValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": exchange_code,
                "data_domain": DATA_DOMAINS['fundamentals'],
                "trd_dt": "{{ ds }}",
                "vendor": VENDORS["EODHD"]
            },
        )

        # ---------------------------------------------------------
        # 5️⃣ DAG Task 연결
        # ---------------------------------------------------------
        symbols = load_symbols(trd_dt="{{ ds }}")
        filter_symbols = symbols
        batches = split_batches(filter_symbols)
        fetched_batches = (
            fetch_batch.partial(
                trd_dt="{{ ds }}",
                exchange_code=exchange_code
            ).expand(batches=batches)
        )

        start >> symbols >> batches >> fetched_batches >> validate_task >> end
