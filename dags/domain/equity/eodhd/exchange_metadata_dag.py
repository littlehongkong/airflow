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
from plugins.pipelines.lake.equity.exchange_holiday_pipeline import ExchangeHolidayPipeline
from plugins.config import constants as C
from plugins.validators.lake_data_validator import LakeDataValidator
from plugins.validators.lake.equity.exchange_holiday_validator import ExchangeHolidayValidator
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
                "allow_empty": False,
                "vendor": C.VENDORS["eodhd"],
            },
            dag=dag,
        )

        fetch_task >> validate_task
        symbol_tasks[exchange_code] = validate_task

    return symbol_tasks


# ✅ 휴장일 수집 태스크 생성
def _build_holiday_tasks_for_country(dag, country_code: str, exchanges: list):
    holiday_tasks = {}
    print(f"🏖️ [{country_code}] 휴장일 수집 대상: {len(exchanges)}개 → {exchanges}")

    for exchange_code in exchanges:
        fetch_task = LakeOperator(
            task_id=f"{country_code}_{exchange_code}_fetch_exchange_holiday",
            pipeline_cls=ExchangeHolidayPipeline,
            method_name="fetch_and_load",
            op_kwargs={
                "exchange_code": exchange_code,
                "domain": C.DATA_DOMAINS["exchange_holiday"],
                "domain_group": C.DOMAIN_GROUPS["equity"],
                "trd_dt": "{{ data_interval_end | ds }}",
            },
            dag=dag,
        )

        validate_task = LakeOperator(
            task_id=f"{country_code}_{exchange_code}_validate_exchange_holiday",
            pipeline_cls=ExchangeHolidayValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": exchange_code,
                "trd_dt": "{{ data_interval_end | ds }}",
                "domain": C.DATA_DOMAINS["exchange_holiday"],
                "domain_group": C.DOMAIN_GROUPS["equity"],
                "vendor": C.VENDORS["eodhd"],
            },
            dag=dag,
        )

        fetch_task >> validate_task
        holiday_tasks[exchange_code] = validate_task

    return holiday_tasks


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
        tags=["EODHD", "metadata", "exchange", "holiday"],
) as dag:
    start_task = EmptyOperator(task_id="start_pipeline")
    end_task = EmptyOperator(task_id="end_pipeline")

    # ✅ 1️⃣ 국가-거래소 매핑 로드
    try:
        country_exchange_map = _load_country_exchange_map_from_warehouse()
    except FileNotFoundError:
        print("⚠️ exchange_master 메타파일 없음 → 기본값 사용")
        country_exchange_map = {"KOR": ["KO", "KQ"], "USA": ["US"]}

    active_countries = Variable.get("master_countries", default_var=["USA", "KOR"], deserialize_json=True)
    filtered_map = {c: country_exchange_map.get(c, []) for c in active_countries if country_exchange_map.get(c)}

    all_symbol_tasks = {}

    # ✅ 2️⃣ 국가별 태스크 그룹 생성
    for country, exchanges in filtered_map.items():
        symbol_tasks = _build_symbol_tasks_for_country(dag, country, exchanges)
        all_symbol_tasks[country] = symbol_tasks

        with TaskGroup(group_id=f"group_trigger_master_{country}", dag=dag):
            trigger_master = TriggerDagRunOperator(
                task_id=f"trigger_asset_master_{country}",
                trigger_dag_id="asset_master_dag",
                trigger_rule=TriggerRule.ALL_SUCCESS,
                conf={"trigger_source": "symbol_list_validation", "country_code": country, "domain_group": DOMAIN_GROUPS['equity'], "trd_dt": "{{ data_interval_end | ds }}", "vendor": VENDORS['eodhd']},
                reset_dag_run=True,
                wait_for_completion=False,
                dag=dag,
            )

            for val_task in symbol_tasks.values():
                val_task >> trigger_master

                # ✅ 3️⃣ 휴장일 수집 (국가별 병렬 수행)
    for country, exchanges in filtered_map.items():
        holiday_tasks = _build_holiday_tasks_for_country(dag, country, exchanges)

        # 휴장일은 해당 국가의 모든 심볼 검증 이후에 실행
        for val_task in all_symbol_tasks.get(country, {}).values():
            for h_val in holiday_tasks.values():
                val_task >> h_val

                # 모든 holiday 검증이 끝나면 종료 태스크로 연결
        for h_val in holiday_tasks.values():
            h_val >> end_task

    start_task >> [v for c in all_symbol_tasks.values() for v in c.values()]