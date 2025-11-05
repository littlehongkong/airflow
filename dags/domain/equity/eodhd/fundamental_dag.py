from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import json
from plugins.operators.lake_operator import LakeOperator
from plugins.pipelines.lake.equity.fundamental_pipeline import FundamentalPipeline
from plugins.validators.lake.equity.fundamental_data_validator import FundamentalDataValidator
from plugins.utils.exchange_loader import load_exchanges_by_country
from plugins.config import constants as C

MASTER_COUNTRIES = json.loads(Variable.get("master_countries", default_var='["USA","KOR"]'))
BATCH_SIZE = int(Variable.get("FUNDAMENTALS_BATCH_SIZE", default_var=30))

with DAG(
    dag_id="fundamental_dag",
    description="Fundamentals fetch & validate by exchange (simple loop version)",
    start_date=datetime(2025, 10, 14),
    schedule="0 1 * * 6",  # 매주 토요일 10시 (KST)
    catchup=False,
    max_active_runs=1,
    tags=["EODHD", "metadata", "fundamentals"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")
    end = EmptyOperator(task_id="end_pipeline")

    # -------------------------------------------------------------
    # 1️⃣ 거래소 코드 로드 (exchange_master 기반)
    # -------------------------------------------------------------
    exchange_map = load_exchanges_by_country(MASTER_COUNTRIES)

    # 예: {"KOR": ["KO", "KQ"], "USA": ["US"]}
    exchanges = [ex for ex_list in exchange_map.values() for ex in ex_list]

    # -------------------------------------------------------------
    # 2️⃣ 거래소별 Task 생성 루프
    # -------------------------------------------------------------
    for exchange_code in exchanges:

        # ✅ 펀더멘털 수집
        def _fetch_wrapper(exchange_code=exchange_code):
            def _fetch(**context):
                pipeline = FundamentalPipeline(
                    domain="fundamentals",
                    exchange_code=exchange_code,
                    trd_dt=context["ds"],
                    domain_group=C.DOMAIN_GROUPS["equity"],
                )
                pipeline.fetch_and_load(
                    exchange_code=exchange_code,
                    trd_dt=context["ds"],
                )
            return _fetch

        fetch_task = PythonOperator(
            task_id=f"fetch_fundamentals_{exchange_code}",
            python_callable=_fetch_wrapper(exchange_code),
        )

        # ✅ 유효성 검증
        validate_task = LakeOperator(
            task_id=f"validate_fundamental_data_{exchange_code}",
            pipeline_cls=FundamentalDataValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": exchange_code,
                "domain": C.DATA_DOMAINS["fundamentals"],
                "domain_group": C.DOMAIN_GROUPS["equity"],
                "trd_dt": "{{ ds }}",
                "vendor": C.VENDORS["eodhd"],
            },
        )

        trigger_warehouse = TriggerDagRunOperator(
            task_id=f"trigger_fundamentals_warehouse_{exchange_code}",
            trigger_dag_id="fundamentals_warehouse_dag",
            conf={
                "trigger_source": "fundamental_validation",
                "exchange_code": exchange_code,
                "domain_group": C.DOMAIN_GROUPS["equity"],
                "vendor": C.VENDORS["eodhd"],
                "trd_dt": "{{ ds }}",
            },
            wait_for_completion=False,  # ✅ 비동기 실행 (Lake 완료 후 병렬 가능)
            reset_dag_run=True,
        )

        start >> fetch_task >> validate_task >> trigger_warehouse >> end
