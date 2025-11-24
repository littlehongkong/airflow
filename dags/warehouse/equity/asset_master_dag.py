# dags/warehouse/asset_master_dag.py
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from plugins.operators.warehouse_operator import WarehouseOperator
from plugins.operators.event_operator import EventOperator
from plugins.pipelines.warehouse.asset_master_pipeline import AssetMasterPipeline
from plugins.pipelines.events.new_listing.candidate_extractor_pipeline import NewListingCandidateExtractorPipeline
# from plugins.pipelines.events.new_listing.fetch_fundamentals_for_candidates import NewListingFundamentalCollector
from plugins.validators.warehouse_data_validator import WarehouseDataValidator


with DAG(
    dag_id="build_asset_master_dag",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=["warehouse", "asset_master.json"],
) as dag:

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

    extract_warehouse_new_listing = EventOperator(
        task_id="extract_warehouse_new_listing",
        pipeline_cls=NewListingCandidateExtractorPipeline,  # Warehouse-level pipeline
        method_name="run",
        op_kwargs={
            "country_code": "{{ dag_run.conf.get('country_code', '') }}",
            "domain_group": "{{ dag_run.conf.get('domain_group', '') }}",
            "trd_dt": "{{ dag_run.conf.get('trd_dt', '') }}",
        }
    )

    # collect_new_listing_fundamentals = EventOperator(
    #     task_id="collect_new_listing_fundamentals",
    #     pipeline_cls=NewListingFundamentalCollector,  # Warehouse-level pipeline
    #     method_name="run",
    #     op_kwargs={
    #         "country_code": "{{ dag_run.conf.get('country_code', '') }}",
    #         "domain_group": "{{ dag_run.conf.get('domain_group', '') }}",
    #         "trd_dt": "{{ dag_run.conf.get('trd_dt', '') }}",
    #     }
    # )

    trigger_event_fundamentals = TriggerDagRunOperator(
        task_id="trigger_event_fundamentals",
        trigger_dag_id="fundamental_dag_event",  # ← 이벤트 DAG으로 변경
        conf={
            "mode": "event",
            "country_code": "{{ dag_run.conf.get('country_code') }}",
            "domain_group": "{{ dag_run.conf.get('domain_group') }}",
            "trd_dt": "{{ dag_run.conf.get('trd_dt') }}",
            "vendor": "{{ dag_run.conf.get('vendor') }}",
        },
        reset_dag_run=True,
        wait_for_completion=False,
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

    start >> build_asset_master >> extract_warehouse_new_listing >> trigger_event_fundamentals >> validate_asset_master >> end
