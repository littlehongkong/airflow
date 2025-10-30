from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import TaskGroup
from airflow.task.trigger_rule import TriggerRule

from plugins.operators.pipeline_operator import PipelineOperator
from plugins.pipelines.lake.equity.symbol_list_pipeline import SymbolListPipeline
from plugins.validators.lake.equity.symbol_list_validator import SymbolListValidator
from plugins.pipelines.lake.equity.exchange_holiday_pipeline import ExchangeHolidayPipeline
from plugins.validators.lake.equity.exchange_holiday_validator import ExchangeHolidayValidator
from plugins.config.constants import VENDORS, DATA_DOMAINS, DATA_WAREHOUSE_ROOT

import json
from pathlib import Path


# ✅ Warehouse에서 국가-거래소 매핑 읽기
def _load_country_exchange_map_from_warehouse() -> dict:
    wh_root = DATA_WAREHOUSE_ROOT / "exchange_master"
    meta_files = sorted(wh_root.glob("snapshot_dt=*/_build_meta.json"), reverse=True)
    if not meta_files:
        raise FileNotFoundError(f"❌ exchange_master 메타파일이 없습니다: {wh_root}")

    latest_meta_path = meta_files[0]
    with open(latest_meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    mapping = meta.get("country_exchange_map", {})
    print(f"📘 최신 exchange_master 스냅샷: {latest_meta_path}")
    print(f"📊 국가-거래소 매핑 로드 완료 (총 {len(mapping)}개국)")
    return mapping


# ✅ 거래소별 태스크 생성 함수 (국가별로 분리)
def _build_symbol_tasks_for_country(dag, country_code: str, exchanges: list):
    symbol_tasks = {}

    print(f"🌍 [{country_code}] 거래소 수집 대상: {len(exchanges)}개 → {exchanges}")

    for exchange_code in exchanges:
        fetch_task = PipelineOperator(
            task_id=f"{country_code}_{exchange_code}_fetch_symbol_list",
            pipeline_cls=SymbolListPipeline,
            method_name="fetch_and_load",
            op_kwargs={
                "exchange_code": exchange_code,
                "data_domain": DATA_DOMAINS["symbol_list"],
                "trd_dt": "{{ ds }}",
            },
            dag=dag,
        )

        validate_task = PipelineOperator(
            task_id=f"{country_code}_{exchange_code}_validate_symbol_list",
            pipeline_cls=SymbolListValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": exchange_code,
                "trd_dt": "{{ ds }}",
                "data_domain": DATA_DOMAINS["symbol_list"],
                "allow_empty": False,
                "vendor": VENDORS["EODHD"],
            },
            dag=dag,
        )

        # ✅ 수집 결과 요약 로그 (fetch 단계에서 record_count 반환 시)
        fetch_task.log.info(f"✅ [{country_code}-{exchange_code}] Fetch initiated.")
        fetch_task >> validate_task
        symbol_tasks[exchange_code] = validate_task

    return symbol_tasks


# =========================================================
# DAG 정의
# =========================================================
with DAG(
    dag_id="exchange_metadata_dag",
    description="Collect & validate exchange metadata, then trigger asset master build",
    start_date=datetime(2025, 10, 15),
    schedule="0 19 * * 0-4",  # 평일 KST 04시
    catchup=False,
    max_active_runs=1,
    tags=["EODHD", "metadata", "exchange"],
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
                conf={"trigger_source": "symbol_list_validation", "country_code": country},
                reset_dag_run=True,
                wait_for_completion=False,
                dag=dag,
            )

            # 국가 내 모든 거래소 검증 완료 후 마스터 트리거
            for val_task in symbol_tasks.values():
                val_task >> trigger_master

    # ✅ 3️⃣ 미국 휴장일 수집
    if "USA" in filtered_map and "US" in filtered_map["USA"]:
        fetch_holidays = PipelineOperator(
            task_id="US_fetch_exchange_holidays",
            pipeline_cls=ExchangeHolidayPipeline,
            method_name="fetch_and_load",
            op_kwargs={
                "exchange_code": "US",
                "data_domain": DATA_DOMAINS["exchange_holiday"],
                "trd_dt": "{{ ds }}",
            },
            dag=dag,
        )

        validate_holidays = PipelineOperator(
            task_id="US_validate_exchange_holidays",
            pipeline_cls=ExchangeHolidayValidator,
            method_name="validate",
            op_kwargs={
                "exchange_code": "US",
                "trd_dt": "{{ ds }}",
                "data_domain": DATA_DOMAINS["exchange_holiday"],
                "allow_empty": True,
                "vendor": VENDORS["EODHD"],
            },
            dag=dag,
        )

        for val in all_symbol_tasks.get("USA", {}).values():
            val >> fetch_holidays >> validate_holidays >> end_task
    else:
        start_task >> end_task
