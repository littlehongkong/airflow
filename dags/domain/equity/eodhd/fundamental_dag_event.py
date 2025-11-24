from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import json

from plugins.config import constants as C
from plugins.config.constants import DOMAIN_GROUPS
from plugins.pipelines.lake.equity.fundamental_pipeline import FundamentalPipeline
from plugins.validators.lake.equity.fundamental_data_validator import FundamentalDataValidator
from plugins.utils.loaders.lake.exchange_loader import load_exchange_list
from plugins.utils.path_manager import DataPathResolver


with DAG(
    dag_id="fundamental_dag_event",
    description="Event-driven fundamental fetch & validate (new listings)",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["event", "fundamentals", "new_listing"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # -------------------------------------------------------------------
    # 🔥 1) 신규상장 기반 fundamentals 수집 (모든 거래소 loop)
    # -------------------------------------------------------------------
    def _fetch_event_all(**kwargs):
        conf = kwargs["dag_run"].conf
        country_code = conf["country_code"]
        trd_dt = conf["trd_dt"]
        domain_group = conf.get("domain_group", "equity")

        print(f"\n[EVENT] 신규상장 fundamentals 수집 시작 (country={country_code})")

        # 국가에 속한 거래소 목록 로딩
        exch_df = load_exchange_list(
            domain_group=domain_group,
            vendor=C.VENDORS["eodhd"],
            trd_dt=trd_dt
        )
        exch_df = exch_df[exch_df["CountryISO3"] == country_code]
        exchanges = exch_df["Code"].unique().tolist()

        print(f"[EVENT] 거래소 목록({country_code}): {exchanges}")

        for exchange_code in exchanges:

            # 신규상장 파일 경로
            candidates_path = (
                DataPathResolver.warehouse_monitoring(
                    domain_group=domain_group,
                    category=C.EVENT_CATEGORIES["new_listing"],
                    exchange_code=exchange_code,
                    trd_dt=trd_dt,
                )
                / "candidates.jsonl"
            )

            if not candidates_path.exists():
                print(f"[EVENT] {exchange_code}: 신규상장 없음 (파일 없음)")
                continue

            tickers = []
            with open(candidates_path, "r") as f:
                for line in f:
                    row = json.loads(line)
                    tickers.append(row["ticker"])

            if not tickers:
                print(f"[EVENT] {exchange_code}: 신규상장 없음 (빈 파일)")
                continue

            print(f"[EVENT] {exchange_code}: 신규상장 {len(tickers)}개 → {tickers}")

            # pipeline 호출
            pipeline = FundamentalPipeline(
                domain="fundamentals",
                exchange_code=exchange_code,
                trd_dt=trd_dt,
                domain_group=domain_group,
                mode="event",
            )
            pipeline.fetch_and_load(batch_symbols=tickers)

        print(f"[EVENT] 모든 거래소 수집 완료\n")

    fetch_event_all = PythonOperator(
        task_id="fetch_event_all",
        python_callable=_fetch_event_all,
    )


    # -------------------------------------------------------------------
    # 🔍 2) 모든 거래소 fundamentals 검증 (loop)
    # -------------------------------------------------------------------
    def _validate_event_all(**kwargs):
        conf = kwargs["dag_run"].conf
        country_code = conf["country_code"]
        trd_dt = conf["trd_dt"]
        domain_group = conf.get("domain_group", "equity")

        print(f"\n[EVENT] 신규상장 fundamentals 검증 시작 (country={country_code})")

        exch_df = load_exchange_list(
            domain_group=domain_group,
            vendor=C.VENDORS["eodhd"],
            trd_dt=trd_dt
        )
        exch_df = exch_df[exch_df["CountryISO3"] == country_code]
        exchanges = exch_df["Code"].unique().tolist()

        for exchange_code in exchanges:
            try:
                print(f"[EVENT-VALIDATE] {exchange_code} 검증 시작")

                validator = FundamentalDataValidator(
                    domain=C.DATA_DOMAINS["fundamentals"],
                    domain_group=domain_group,
                    trd_dt=trd_dt,
                    vendor=C.VENDORS["eodhd"],
                    exchange_code=exchange_code,
                )
                validator.validate()

            except FileNotFoundError:
                print(f"[EVENT-VALIDATE] {exchange_code} → 수집된 파일 없음, 자동 skip")
                continue

        print(f"[EVENT] 모든 거래소 validation 완료\n")

    validate_event_all = PythonOperator(
        task_id="validate_event_all",
        python_callable=_validate_event_all,
    )


    # -------------------------------------------------------------------
    # 🏭 3) 국가단위로 warehouse 트리거 (최종 1회)
    # -------------------------------------------------------------------
    trigger_warehouse = TriggerDagRunOperator(
        task_id="trigger_warehouse",
        trigger_dag_id="build_fundamentals_dag",
        conf={
            "trigger_source": "event_new_listing",
            "country_code": "{{ dag_run.conf.country_code }}",
            "domain_group": "{{ dag_run.conf.domain_group }}",
            "trd_dt": "{{ dag_run.conf.trd_dt }}",
            "vendor": "{{ dag_run.conf.vendor }}",
            "mode": "event",
        },
        reset_dag_run=True,
        wait_for_completion=False,
    )


    # -------------------------------------------------------------------
    # DAG Flow
    # -------------------------------------------------------------------
    start >> fetch_event_all >> validate_event_all >> trigger_warehouse >> end
