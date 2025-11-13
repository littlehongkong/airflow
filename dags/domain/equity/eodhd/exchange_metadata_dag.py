from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import TaskGroup
from airflow.task.trigger_rule import TriggerRule

from plugins.config.constants import DOMAIN_GROUPS, VENDORS
from plugins.operators.lake_operator import LakeOperator
from plugins.pipelines.lake.equity.symbol_list_pipeline import SymbolListPipeline
from plugins.pipelines.lake.equity.exchange_detail_pipeline import ExchangeDetailPipeline
from plugins.config import constants as C
from plugins.validators.lake_data_validator import LakeDataValidator
from plugins.validators.lake.equity.exchange_detail_validator import ExchangeDetailValidator
import json


# ✅ Warehouse에서 국가-거래소 매핑 읽기
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


# ✅ 심볼 수집 태스크 생성
def _build_symbol_tasks_for_country(dag, country_code: str, exchanges: list):
    symbol_tasks = {}
    print(f"🌍 [{country_code}] 거래소 수집 대상: {len(exchanges)}개 → {exchanges}")

    for exchange_code in exchanges:
        fetch_task = LakeOperator(
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

        validate_task = LakeOperator(
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

        fetch_task >> validate_task
        symbol_tasks[exchange_code] = validate_task

    return symbol_tasks


# ✅ 휴장일 수집 태스크 생성
def _build_exchange_detail_tasks_for_country(dag, country_code: str, exchanges: list):
    detail_tasks = {}
    print(f"🏖️ [{country_code}] 상세정보 수집 대상: {len(exchanges)}개 → {exchanges}")

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

    return detail_tasks


# =========================================================
# DAG 정의
# =========================================================
with DAG(
        dag_id="exchange_metadata_dag",
        description="Collect & validate exchange metadata, then trigger asset_master master build",
        start_date=datetime(2025, 10, 15),
        schedule="0 19 * * 1-5",  # 평일 KST 04시
        catchup=False,
        max_active_runs=1,
        tags=["EODHD", "metadata", "exchange detail", "holiday"],
) as dag:
    start_task = EmptyOperator(task_id="start_pipeline")
    end_task = EmptyOperator(task_id="end_pipeline")

    # ✅ 1️⃣ 국가-거래소 매핑 로드 (기존 동일)
    try:
        country_exchange_map = _load_country_exchange_map_from_warehouse()
    except FileNotFoundError:
        print("⚠️ exchange_master 메타파일 없음 → 기본값 사용")
        country_exchange_map = {"KOR": ["KO", "KQ"], "USA": ["US"]}

    active_countries = Variable.get("master_countries", default_var=["USA", "KOR"], deserialize_json=True)
    filtered_map = {c: country_exchange_map.get(c, []) for c in active_countries if country_exchange_map.get(c)}

    all_symbol_tasks = {}

    # ✅ 2️⃣ 국가별 심볼 수집
    for country, exchanges in filtered_map.items():
        symbol_tasks = _build_symbol_tasks_for_country(dag, country, exchanges)
        all_symbol_tasks[country] = symbol_tasks

        # 심볼 검증 완료 후 → asset_master 빌드 트리거
        with TaskGroup(group_id=f"group_trigger_master_{country}", dag=dag):
            trigger_asset_master = TriggerDagRunOperator(
                task_id=f"trigger_asset_master_{country}",
                trigger_dag_id="asset_master_dag",
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

            for val_task in symbol_tasks.values():
                val_task >> trigger_asset_master

    # ✅ 3️⃣ 휴장일 수집 (기존 동일)
    all_exchange_detail_tasks = {}
    for country, exchanges in filtered_map.items():
        exchange_detail_tasks = _build_exchange_detail_tasks_for_country(dag, country, exchanges)
        all_exchange_detail_tasks[country] = exchange_detail_tasks

        for val_task in all_symbol_tasks.get(country, {}).values():
            for h_val in exchange_detail_tasks.values():
                val_task >> h_val

    # ✅ 4️⃣ 모든 국가의 exchange_detail 검증 완료 후 → Warehouse DAG 트리거
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

    # ✅ 모든 exchange_detail 검증이 끝나면 Warehouse 트리거
    for country, tasks in all_exchange_detail_tasks.items():
        for h_val in tasks.values():
            h_val >> trigger_exchange_warehouse

    trigger_exchange_warehouse >> end_task
    start_task >> [v for c in all_symbol_tasks.values() for v in c.values()]