"""
Airflow DAG: Fundamentals Warehouse Build & Validation
-----------------------------------------------------------------------
💡 기능 요약
1️⃣ Data Lake의 fundamentals JSON을 warehouse 구조로 재정리
   - exchange_code / trd_dt 기준으로 ticker별(security_id별) 폴더 생성
2️⃣ Warehouse-level 유효성 검증 수행
   - Pandera + Soda Core 기반 검증 (type별: stock / etf / fund)
"""

from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.empty import EmptyOperator
from pathlib import Path

from plugins.operators.warehouse_operator import WarehouseOperator
from plugins.pipelines.warehouse.fundamentals_ticker_split_pipeline import FundamentalsTickerSplitPipeline
from plugins.validators.warehouse.fundamentals_warehouse_validator import FundamentalsWarehouseValidator
from plugins.config.constants import DATA_WAREHOUSE_ROOT, DOMAIN_GROUPS, VENDORS


with DAG(
    dag_id="fundamentals_warehouse_dag",
    start_date=datetime(2025, 10, 1),
    schedule=None,  # 수동/Trigger 기반
    catchup=False,
    tags=["warehouse", "fundamentals"],
) as dag:

    # ------------------------------------------------------------------
    # 🟩 Start
    # ------------------------------------------------------------------
    start = EmptyOperator(task_id="start_pipeline")

    # ------------------------------------------------------------------
    # 🏗️ 1️⃣ Fundamentals Warehouse Build (Ticker-Split)
    # ------------------------------------------------------------------
    build_fundamentals = WarehouseOperator(
        task_id="build_fundamentals_ticker_split",
        pipeline_cls=FundamentalsTickerSplitPipeline,
        op_kwargs={
            "domain_group": "{{ dag_run.conf.get('domain_group', '') }}",
            "vendor": "{{ dag_run.conf.get('vendor', '') }}",
            "exchange_code": "{{ dag_run.conf.get('exchange_code', '') }}",
            "trd_dt": "{{ dag_run.conf.get('trd_dt', '') }}",
            "trigger_source": "{{ dag_run.conf.get('trigger_source', 'manual') }}",
            "country_code": "{{ dag_run.conf.get('country_code', '') }}",
        },
    )

    # ------------------------------------------------------------------
    # ✅ 2️⃣ Fundamentals Warehouse Validation (Pandera + Soda Core)
    # ------------------------------------------------------------------
    validate_fundamentals = WarehouseOperator(
        task_id="validate_fundamentals_warehouse",
        pipeline_cls=FundamentalsWarehouseValidator,  # ✅ 새 Validator 클래스
        op_kwargs={
            "trd_dt": "{{ dag_run.conf.get('trd_dt', '') }}",
            "country_code": "{{ dag_run.conf.get('country_code', '') }}",
            "domain_group": "{{ dag_run.conf.get('domain_group', '') }}",
            "vendor": "{{ dag_run.conf.get('vendor', '') }}",
            # ✅ dataset_path는 trd_dt 단위 snapshot 루트 지정
            "allow_empty": True,
        },
    )

    # ------------------------------------------------------------------
    # 🟪 End
    # ------------------------------------------------------------------
    end = EmptyOperator(task_id="end_pipeline")

    # DAG 실행 순서
    start >> build_fundamentals >> validate_fundamentals >> end
