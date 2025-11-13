"""
Airflow DAG: Exchange Warehouse Build (Master + Holiday) + Validation
"""

from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.sdk import TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator
from plugins.operators.warehouse_operator import WarehouseOperator
from plugins.pipelines.warehouse.exchange_master_pipeline import ExchangeMasterPipeline
from plugins.pipelines.warehouse.exchange_holiday_pipeline import ExchangeHolidayPipeline
from plugins.validators.warehouse_data_validator import WarehouseDataValidator
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
    dag_id="build_exchange_warehouse_dag",
    default_args=default_args,
    schedule="0 19 * * 1-5",  # 평일 04:00 KST (19:00 UTC)
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["warehouse", "exchange_master", "exchange_holiday"],
    description="Build exchange_master & exchange_holiday_master warehouse tables daily (Mon–Fri).",
) as dag:

    start = EmptyOperator(task_id="start")

    # ---------------------------------------------------------------------
    # 1️⃣ Build Exchange Master (거래소 기본 정보 + 영업시간 등)
    # ---------------------------------------------------------------------
    build_exchange_master = WarehouseOperator(
        task_id="build_exchange_master",
        pipeline_cls=ExchangeMasterPipeline,
        op_kwargs={
            "domain": "exchange",  # pipeline 내부에서 exchange_master로 변환
            "domain_group": DOMAIN_GROUPS["equity"],
            "trd_dt": "{{ dag_run.conf.get('trd_dt', ds) }}",  # 기본값: 실행일
            "vendor": VENDORS["eodhd"],
            "master_countries" : Variable.get("master_countries", ["USA", "KOR"], deserialize_json=True)

    },
    )

    # ---------------------------------------------------------------------
    # 2️⃣ Validate Exchange Master
    # ---------------------------------------------------------------------
    validate_exchange_master = WarehouseOperator(
        task_id="validate_exchange_master",
        pipeline_cls=WarehouseDataValidator,
        op_kwargs={
            "domain": "exchange",
            "domain_group": DOMAIN_GROUPS["equity"],
            "trd_dt": "{{ dag_run.conf.get('trd_dt', ds) }}",
        },
    )

    # ======================================================
    # 2️⃣ Country-based Holiday Pipeline (국가별 병렬 실행)
    # ======================================================
    master_countries = Variable.get(
        "master_countries",
        default_var=["USA", "KOR"],
        deserialize_json=True
    )

    holiday_tasks = []

    for country in master_countries:

        with TaskGroup(group_id=f"group_holiday_{country}") as tg:

            build_holiday = WarehouseOperator(
                task_id=f"build_exchange_holiday_master_{country}",
                pipeline_cls=ExchangeHolidayPipeline,
                op_kwargs={
                    "domain": "holiday",
                    "domain_group": DOMAIN_GROUPS["equity"],
                    "trd_dt": "{{ dag_run.conf.get('trd_dt', ds) }}",
                    "vendor": VENDORS["eodhd"],
                    "country_code": country,
                },
            )

            validate_holiday = WarehouseOperator(
                task_id=f"validate_exchange_holiday_master_{country}",
                pipeline_cls=WarehouseDataValidator,
                op_kwargs={
                    "domain": "holiday",
                    "domain_group": DOMAIN_GROUPS["equity"],
                    "trd_dt": "{{ dag_run.conf.get('trd_dt', ds) }}",
                    "country_code": country,   # validator도 국가단위 검증
                },
            )

            build_holiday >> validate_holiday

            holiday_tasks.append(tg)


    end = EmptyOperator(task_id="end")

    # ---------------------------------------------------------------------
    # 📈 DAG 흐름 정의
    # ---------------------------------------------------------------------
    start >> build_exchange_master >> validate_exchange_master
    for tg in holiday_tasks:
        validate_exchange_master >> tg

    for tg in holiday_tasks:
        tg >> end