# fundamental_dag_schedule.py

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime
import json

from plugins.config.constants import DOMAIN_GROUPS
from plugins.config import constants as C
from plugins.operators.lake_operator import LakeOperator
from plugins.pipelines.lake.equity.fundamental_pipeline import FundamentalPipeline
from plugins.validators.lake.equity.fundamental_data_validator import FundamentalDataValidator
from plugins.utils.loaders.lake.exchange_loader import load_exchange_list


MASTER_COUNTRIES = json.loads(Variable.get("master_countries", default_var='["USA","KOR"]'))

with DAG(
    dag_id="fundamental_dag_schedule",
    description="Scheduled fundamentals fetch & validate by exchange",
    start_date=datetime(2025, 10, 14),
    schedule="0 1 * * 6",  # 매주 토요일 10시 (한국시간)
    catchup=False,
    max_active_runs=1,
    tags=["EODHD", "fundamentals", "schedule"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")
    end = EmptyOperator(task_id="end_pipeline")

    # ==========================
    # 1️⃣ 거래소 목록 로드
    # ==========================
    exchange_df = load_exchange_list(
        domain_group=DOMAIN_GROUPS["equity"],
        vendor=C.VENDORS["eodhd"],
        trd_dt="{{ data_interval_end | ds }}"
    )
    exchange_df = exchange_df[exchange_df["CountryISO3"].isin(MASTER_COUNTRIES)]
    exchange_codes = list(exchange_df["Code"])

    # ==========================
    # 2️⃣ 거래소별 Task LOOP
    # ==========================
    for exchange_code in exchange_codes:

        # FETCH
        def _fetch_wrapper(exchange_code=exchange_code):
            def _fetch(**context):
                trd_dt = context["ds"]

                pipeline = FundamentalPipeline(
                    domain="fundamentals",
                    exchange_code=exchange_code,
                    trd_dt=trd_dt,
                    domain_group=DOMAIN_GROUPS["equity"],
                    mode="schedule",
                )
                return pipeline.fetch_and_load()
            return _fetch

        fetch_task = PythonOperator(
            task_id=f"fetch_fundamentals_{exchange_code}",
            python_callable=_fetch_wrapper(exchange_code)
        )

        # VALIDATE
        validate_task = LakeOperator(
            task_id=f"validate_fundamental_data_{exchange_code}",
            pipeline_cls=FundamentalDataValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": exchange_code,
                "domain": C.DATA_DOMAINS["fundamentals"],
                "domain_group": DOMAIN_GROUPS["equity"],
                "trd_dt": "{{ data_interval_end | ds }}",
                "vendor": C.VENDORS["eodhd"],
            },
        )

        # Warehouse build trigger
        trigger_warehouse = TriggerDagRunOperator(
            task_id=f"trigger_fundamentals_warehouse_{exchange_code}",
            trigger_dag_id="build_fundamentals_dag",
            conf={
                "trigger_source": "fundamental_validation",
                "exchange_code": exchange_code,
                "trd_dt": "{{ data_interval_end | ds }}",
                "domain_group": DOMAIN_GROUPS["equity"],
                "vendor": C.VENDORS["eodhd"],
            },
            wait_for_completion=False,
            reset_dag_run=True,
        )

        start >> fetch_task >> validate_task >> trigger_warehouse >> end
