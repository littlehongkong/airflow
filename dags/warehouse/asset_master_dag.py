# dags/warehouse/asset_master_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from plugins.operators.warehouse_operator import WarehouseOperator
from plugins.pipelines.warehouse.asset_master_pipeline import AssetMasterPipeline
from plugins.validators.warehouse.asset_master_validator import AssetMasterValidator

with DAG(
    dag_id="asset_master_dag",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=["warehouse", "asset_master"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")

    build_asset_master = WarehouseOperator(
        task_id="build_asset_master",
        pipeline_cls=AssetMasterPipeline,
        op_kwargs={"snapshot_dt": "{{ ds }}"},
    )

    validate_asset_master = WarehouseOperator(
        task_id="validate_asset_master",
        pipeline_cls=AssetMasterValidator,
        op_kwargs={"snapshot_dt": "{{ ds }}"},
    )

    end = EmptyOperator(task_id="end_pipeline")

    start >> build_asset_master >> validate_asset_master >> end
