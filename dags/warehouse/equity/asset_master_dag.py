# dags/warehouse/asset_master_dag.py
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from pathlib import Path
from plugins.operators.warehouse_operator import WarehouseOperator
from plugins.pipelines.warehouse.asset_master_pipeline import AssetMasterPipeline
from plugins.validators.warehouse_data_validator import WarehouseDataValidator


with DAG(
    dag_id="build_asset_master_dag",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=["warehouse", "asset_master.json"],
) as dag:
    from plugins.config.constants import WAREHOUSE_DOMAINS, DATA_WAREHOUSE_ROOT

    start = EmptyOperator(task_id="start_pipeline")

    build_asset_master = WarehouseOperator(
        task_id="build_asset_master",
        pipeline_cls=AssetMasterPipeline,
        op_kwargs={
            "trd_dt": "{{ dag_run.conf.get('trd_dt', '') }}",
            "country_code": "{{ dag_run.conf.get('country_code', '') }}",
            "domain_group": "{{ dag_run.conf.get('domain_group', '') }}",
            "vendor":  "{{ dag_run.conf.get('vendor', '') }}"
        }
    )

    # 2️⃣ Validation Task (유효성검증 + validated 이관)
    validate_asset_master = WarehouseOperator(
        task_id="validate_asset_master",
        pipeline_cls=WarehouseDataValidator,  # ✅ 단일 통합 Validator 사용
        op_kwargs={
            "domain": "asset",
            "domain_group": "{{ dag_run.conf.get('domain_group', '') }}",
            "country_code": "{{ dag_run.conf.get('country_code', '') }}",
            "trd_dt": "{{ dag_run.conf.get('trd_dt', '') }}",
            "allow_empty": True,
        }
    )

    end = EmptyOperator(task_id="end_pipeline")

    start >> build_asset_master >> validate_asset_master >> end
