from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from plugins.config.constants import DATA_DOMAINS, DOMAIN_GROUPS
from plugins.operators.warehouse_operator import WarehouseOperator
from plugins.pipelines.warehouse.price_warehouse_pipeline import PriceWarehousePipeline
from plugins.validators.warehouse_data_validator import WarehouseDataValidator
from plugins.config import constants as C


# ==========================================================
# 💾 Price Warehouse DAG
# ----------------------------------------------------------
# 목적:
#   - validated lake price 데이터를 warehouse snapshot으로 적재
#   - Pandera + Soda Core 기반 검증 수행
#   - 상위 Lake DAG에서 TriggerDagRunOperator로 국가단위 호출
# ==========================================================

with DAG(
    dag_id="price_warehouse_dag",
    description="Lake → Warehouse: 일자별 가격 데이터 적재 및 검증 (국가 단위)",
    start_date=datetime(2025, 10, 1),
    schedule=None,  # ✅ 수동 or TriggerDagRunOperator 호출
    catchup=False,
    max_active_runs=1,
    tags=["warehouse", "equity", "price"],
) as dag:

    # ✅ 1️⃣ 시작 마커
    start_task = EmptyOperator(task_id="start_pipeline")

    # ✅ 2️⃣ 가격 웨어하우스 빌드
    def build_price_warehouse(**context):
        conf = context["dag_run"].conf or {}
        trd_dt = conf.get("trd_dt", context["ds"])
        country_code = conf.get("country_code")
        vendor = C.VENDORS["eodhd"]

        print(f"🚀 [Build] Price Warehouse Start — {country_code} @ {trd_dt}")

        assert country_code is not None, "country_code 정보가 없습니다."

        with PriceWarehousePipeline(
            trd_dt=trd_dt,
            vendor=vendor,
            country_code=country_code,
            domain="price",
            domain_group=DOMAIN_GROUPS['equity'],
        ) as pipeline:
            result = pipeline.build()
            print(f"✅ [Build] Warehouse Build Complete: {result}")
            return result

    build_task = PythonOperator(
        task_id="build_price_warehouse",
        python_callable=build_price_warehouse,
    )

    # 2️⃣ Validation Task (유효성검증 + validated 이관)
    validate_price_warehouse = WarehouseOperator(
        task_id="validate_price_warehouse",
        pipeline_cls=WarehouseDataValidator,  # ✅ 단일 통합 Validator 사용
        op_kwargs={
            "domain": "price",
            "domain_group": "{{ dag_run.conf.get('domain_group', '') }}",
            "country_code": "{{ dag_run.conf.get('country_code', '') }}",
            "trd_dt": "{{ dag_run.conf.get('trd_dt', '') }}",
            "vendor": "{{ dag_run.conf.get('vendor', '') }}"
        },
    )

    # ✅ 4️⃣ 종료 마커
    end_task = EmptyOperator(task_id="end_pipeline")

    # DAG 흐름
    start_task >> build_task >> validate_price_warehouse >> end_task