from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import TaskGroup
from airflow.task.trigger_rule import TriggerRule

from plugins.pipelines.events.new_listing.candidate_extractor_pipeline import (
    NewListingCandidateExtractorPipeline,
)
from plugins.config.constants import DOMAIN_GROUPS, VENDORS
from plugins.operators.lake_operator import LakeOperator
from plugins.operators.event_operator import EventOperator
from plugins.pipelines.lake.equity.symbol_list_pipeline import SymbolListPipeline
from plugins.pipelines.lake.equity.exchange_detail_pipeline import ExchangeDetailPipeline
from plugins.pipelines.lake.equity.symbol_changes_pipeline import SymbolChangePipeline

from plugins.config import constants as C
from plugins.validators.lake_data_validator import LakeDataValidator
from plugins.validators.lake.equity.exchange_detail_validator import ExchangeDetailValidator
import json

all_fetch_tasks = []


# =========================================================
# 📘 Warehouse에서 국가-거래소 매핑 로드
# =========================================================
def _load_country_exchange_map_from_warehouse() -> dict:
    wh_root = C.DATA_WAREHOUSE_ROOT / "exchange"
    meta_files = sorted(wh_root.glob("trd_dt=*/_build_meta.json"), reverse=True)
    if not meta_files:
        raise FileNotFoundError(f"❌ exchange_master 메타파일이 없습니다: {wh_root}")

    latest_meta_path = meta_files[0]
    with open(latest_meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    mapping = meta.get("country_exchange_map", {})
    print(f"📘 최신 exchange_master 스냅샷: {latest_meta_path}")
    print(f"📊 국가-거래소 매핑 로드 완료 (총 {len(mapping)}개국)")
    return mapping



# =========================================================
# 🧩 심볼 / 심볼 변경 / 신규상장 후보 추출
# =========================================================
def _build_symbol_tasks_for_country(dag, country_code: str, exchanges: list):
    symbol_tasks = {}

    for exchange_code in exchanges:

        # ------------------------
        # 1) 심볼 수집
        # ------------------------
        fetch_symbol = LakeOperator(
            task_id=f"{country_code}_{exchange_code}_fetch_symbol_list",
            pipeline_cls=SymbolListPipeline,
            method_name="fetch_and_load",
            op_kwargs={
                "exchange_code": exchange_code,
                "domain": C.DATA_DOMAINS["symbol_list"],
                "domain_group": C.DOMAIN_GROUPS["equity"],
                "trd_dt": "{{ data_interval_end | ds }}",
                "allow_empty": False,
            },
            dag=dag,
        )

        validate_symbol = LakeOperator(
            task_id=f"{country_code}_{exchange_code}_validate_symbol_list",
            pipeline_cls=LakeDataValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": exchange_code,
                "trd_dt": "{{ data_interval_end | ds }}",
                "domain": C.DATA_DOMAINS["symbol_list"],
                "domain_group": C.DOMAIN_GROUPS["equity"],
                "vendor": C.VENDORS["eodhd"],
            },
            dag=dag,
        )

        # ------------------------
        # ⭐ 미국 거래소만 symbol_changes 실행
        # ------------------------
        if country_code == "USA":
            run_symbol_changes = LakeOperator(
                task_id=f"{country_code}_{exchange_code}_run_symbol_changes",
                pipeline_cls=SymbolChangePipeline,
                method_name="fetch_and_load",
                op_kwargs={
                    "exchange_code": exchange_code,
                    "domain": C.DATA_DOMAINS["symbol_changes"],
                    "domain_group": C.DOMAIN_GROUPS["equity"],
                    "trd_dt": "{{ data_interval_end | ds }}",
                    "allow_empty": True,
                },
                dag=dag,
            )

            validate_symbol_changes = LakeOperator(
                task_id=f"{country_code}_{exchange_code}_validate_symbol_changes",
                pipeline_cls=LakeDataValidator,
                method_name="validate",
                op_kwargs={
                    "exchange_code": exchange_code,
                    "domain": C.DATA_DOMAINS["symbol_changes"],
                    "domain_group": C.DOMAIN_GROUPS["equity"],
                    "trd_dt": "{{ data_interval_end | ds }}",
                    "vendor": C.VENDORS["eodhd"],
                },
                dag=dag,
            )
        else:
            run_symbol_changes = EmptyOperator(task_id=f"{country_code}_{exchange_code}_skip_symbol_changes", dag=dag)
            validate_symbol_changes = EmptyOperator(task_id=f"{country_code}_{exchange_code}_skip_symbol_changes_ok", dag=dag)

        # ------------------------
        # 3) 신규상장 후보 추출
        # ------------------------
        extract_candidates = EventOperator(
            task_id=f"{country_code}_{exchange_code}_extract_new_listing_candidates",
            pipeline_cls=NewListingCandidateExtractorPipeline,
            method_name="run",
            op_kwargs={
                "exchange_code": exchange_code,
                "trd_dt": "{{ data_interval_end | ds }}",
                "domain_group": C.DOMAIN_GROUPS["equity"],
                "vendor": C.VENDORS["eodhd"],
                "country_code": country_code,
            },
            postgres_conn_id="postgres_default",
            dag=dag,
        )

        # ------------------------
        # 실행 흐름 구성
        # ------------------------
        fetch_symbol >> validate_symbol \
                     >> run_symbol_changes \
                     >> validate_symbol_changes \
                     >> extract_candidates

        symbol_tasks[exchange_code] = extract_candidates
        all_fetch_tasks.append(fetch_symbol)

    return symbol_tasks



# =========================================================
# 휴장일 수집
# =========================================================
def _build_exchange_detail_tasks_for_country(dag, country_code: str, exchanges: list):
    detail_tasks = {}

    for exchange_code in exchanges:
        fetch_task = LakeOperator(
            task_id=f"{country_code}_{exchange_code}_fetch_exchange_detail",
            pipeline_cls=ExchangeDetailPipeline,
            method_name="fetch_and_load",
            op_kwargs={
                "exchange_code": exchange_code,
                "domain": C.DATA_DOMAINS["exchange_detail"],
                "domain_group": C.DOMAIN_GROUPS["equity"],
                "trd_dt": "{{ data_interval_end | ds }}",
                "allow_empty": True,
            },
            dag=dag,
        )

        validate_task = LakeOperator(
            task_id=f"{country_code}_{exchange_code}_validate_exchange_detail",
            pipeline_cls=ExchangeDetailValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": exchange_code,
                "trd_dt": "{{ data_interval_end | ds }}",
                "domain": C.DATA_DOMAINS["exchange_detail"],
                "domain_group": C.DOMAIN_GROUPS["equity"],
                "vendor": C.VENDORS["eodhd"],
            },
            dag=dag,
        )

        fetch_task >> validate_task
        detail_tasks[exchange_code] = validate_task

        all_fetch_tasks.append(fetch_task)

    return detail_tasks



# =========================================================
# DAG 정의
# =========================================================
with DAG(
    dag_id="exchange_metadata_dag",
    description="Collect & validate symbol_list / symbol_changes / holidays, then trigger exchange warehouse",
    start_date=datetime(2025, 10, 15),
    schedule="0 19 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["EODHD", "metadata", "symbol_changes", "listing"],
) as dag:

    start_task = EmptyOperator(task_id="start_pipeline")
    end_task = EmptyOperator(task_id="end_pipeline")

    # 국가-거래소 매핑 로드
    try:
        country_exchange_map = _load_country_exchange_map_from_warehouse()
    except FileNotFoundError:
        print("⚠️ exchange_master 메타파일 없음 → 기본값 사용")
        country_exchange_map = {"KOR": ["KO", "KQ"], "USA": ["US"]}

    active_countries = Variable.get("master_countries", default_var=["USA", "KOR"], deserialize_json=True)
    filtered_map = {c: country_exchange_map.get(c, []) for c in active_countries if country_exchange_map.get(c)}

    all_symbol_tasks = {}

    # ⭐ SYMBOL → SYMBOL CHANGES → CANDIDATES
    for country, exchanges in filtered_map.items():
        symbol_tasks = _build_symbol_tasks_for_country(dag, country, exchanges)
        all_symbol_tasks[country] = symbol_tasks

        with TaskGroup(group_id=f"group_trigger_master_{country}", dag=dag):
            trigger_asset_master = TriggerDagRunOperator(
                task_id=f"trigger_asset_master_{country}",
                trigger_dag_id="build_asset_master_dag",
                trigger_rule=TriggerRule.ALL_SUCCESS,
                conf={
                    "trigger_source": "symbol_list_validation",
                    "country_code": country,
                    "domain_group": DOMAIN_GROUPS['equity'],
                    "trd_dt": "{{ data_interval_end | ds }}",
                    "vendor": VENDORS['eodhd'],
                },
                reset_dag_run=True,
                wait_for_completion=False,
                dag=dag,
            )

            for t in symbol_tasks.values():
                t >> trigger_asset_master


    # ⭐ HOLIDAYS
    all_exchange_detail_tasks = {}
    for country, exchanges in filtered_map.items():
        detail_tasks = _build_exchange_detail_tasks_for_country(dag, country, exchanges)
        all_exchange_detail_tasks[country] = detail_tasks

        for sym in all_symbol_tasks.get(country, {}).values():
            for d in detail_tasks.values():
                sym >> d


    # ⭐ Trigger Exchange Warehouse
    trigger_exchange_warehouse = TriggerDagRunOperator(
        task_id="trigger_exchange_warehouse_dag",
        trigger_dag_id="build_exchange_warehouse_dag",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        conf={
            "trigger_source": "exchange_detail_validation",
            "domain_group": DOMAIN_GROUPS['equity'],
            "trd_dt": "{{ data_interval_end | ds }}",
            "vendor": VENDORS['eodhd'],
        },
        reset_dag_run=True,
        wait_for_completion=False,
        dag=dag,
    )

    for country, tlist in all_exchange_detail_tasks.items():
        for t in tlist.values():
            t >> trigger_exchange_warehouse

    start_task >> all_fetch_tasks
    trigger_exchange_warehouse >> end_task
