# dags/warehouse/asset_master_dag.py

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime

from plugins.operators.warehouse_operator import WarehouseOperator
from plugins.pipelines.warehouse.asset_master_pipeline import AssetMasterWarehousePipeline
from plugins.validators.warehouse.asset_master_validator import AssetMasterValidator
from plugins.operators.pipeline_operator import PipelineOperator

# =========================================================
# DAG 정의
# =========================================================
with DAG(
        dag_id="asset_master_dag",
        description="Build and validate country-level asset master from validated data",
        start_date=datetime(2025, 10, 1),
        schedule=None,  # ✅ 외부 DAG(symbol_list DAG)에서만 Trigger
        catchup=False,
        max_active_runs=1,
        tags=["warehouse", "master", "asset"],
) as dag:
    # -------------------------------------------------
    # 1️⃣ Airflow Variable에서 운영 대상 국가 목록 불러오기
    # -------------------------------------------------
    ACTIVE_COUNTRIES = Variable.get(
        "master_countries",
        default_var=["USA", "KOR"],
        deserialize_json=True
    )


    # -------------------------------------------------
    # 2️⃣ 국가 확인 헬퍼 함수
    # -------------------------------------------------
    def _resolve_country(**context):
        """TriggerDagRunOperator에서 conf로 전달받은 국가 추출"""
        dag_run = context["dag_run"]
        country_code = dag_run.conf.get("country_code", "USA")
        return country_code.upper()


    def _is_country_active(country_code: str) -> bool:
        """현재 실행된 국가가 master_countries에 포함되어 있는지 확인"""
        return country_code.upper() in [c.upper() for c in ACTIVE_COUNTRIES]


    def _decide_branch(**context):
        """국가 활성화 여부에 따라 실행 경로 결정"""
        country_code = _resolve_country(**context)

        if _is_country_active(country_code):
            print(f"✅ [asset_master_dag] {country_code} is active → proceed with build")
            return "build_asset_master"
        else:
            print(f"⚠️ [asset_master_dag] {country_code} is not in master_countries → skip build")
            return "skip_country_not_active"


    # -------------------------------------------------
    # 3️⃣ 태스크 정의
    # -------------------------------------------------

    # 시작
    start = EmptyOperator(task_id="start")

    # 국가 활성화 체크
    check_country = BranchPythonOperator(
        task_id="check_country_activation",
        python_callable=_decide_branch,
        doc_md="""
        ## 국가 활성화 체크

        **목적**: master_countries Variable에 정의된 국가만 처리

        **로직**:
        - dag_run.conf에서 country_code 추출
        - ACTIVE_COUNTRIES 리스트와 비교
        - 활성화된 경우: build_asset_master 실행
        - 비활성화된 경우: skip_country_not_active로 분기
        """,
    )

    # 비활성 국가 스킵
    skip = EmptyOperator(
        task_id="skip_country_not_active",
        doc_md="국가가 master_countries에 포함되지 않아 스킵됨",
    )

    # ✅ AssetMasterWarehousePipeline 빌드 (개선된 WarehouseOperator 사용)
    build_asset_master = WarehouseOperator(
        task_id="build_asset_master",
        pipeline_cls=AssetMasterWarehousePipeline,
        op_kwargs={
            "snapshot_dt": "{{ ds }}",
            "country_code": "{{ dag_run.conf.get('country_code') }}",
            # vendor_priority는 Pipeline 기본값 사용 (EODHD 우선)
        },
        cleanup_on_success=True,
        cleanup_on_failure=True,
        doc_md="""
        ## Asset Master 빌드

        **입력**:
        - Data Lake validated/symbol_list (국가별 거래소)
        - Data Lake validated/fundamentals (국가별 거래소)
        - Warehouse exchange_master (거래소 매핑용)

        **출력**:
        - warehouse/asset_master/country_code=XXX/snapshot_dt=YYYY-MM-DD/asset_master.parquet

        **처리 과정**:
        1. exchange_master에서 국가-거래소 매핑 로드
        2. 각 거래소별 symbol_list 로드 (vendor 우선순위)
        3. 각 거래소별 fundamentals 로드 (optional)
        4. 데이터 정규화 및 병합
        5. Entity ID 생성
        6. Parquet 저장
        7. 메타데이터 저장 + 레지스트리 갱신
        """,
    )

    # ✅ AssetMasterValidator 검증 (기존 PipelineOperator 유지)
    validate_asset_master = PipelineOperator(
        task_id="validate_asset_master",
        pipeline_cls=AssetMasterValidator,
        method_name="run",
        op_kwargs={
            "snapshot_dt": "{{ ds }}",
            "country_code": "{{ dag_run.conf.get('country_code', 'USA') }}",
        },
        doc_md="""
        ## Asset Master 검증

        **검증 항목**:
        - Row count > 0
        - Entity ID 중복 체크
        - 필수 필드 null 체크 (ticker, exchange_code, country_code)
        - 데이터 타입 검증
        - 참조 무결성 검증 (exchange_code가 exchange_master에 존재하는지)

        **검증 도구**: Pandera + Soda Core
        """,
    )

    # 종료
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",  # skip 또는 validate 둘 중 하나만 성공해도 OK
    )

    # -------------------------------------------------
    # 4️⃣ 태스크 순서 정의
    # -------------------------------------------------
    start >> check_country

    # 활성 국가인 경우: 빌드 → 검증
    check_country >> build_asset_master >> validate_asset_master >> end

    # 비활성 국가인 경우: 스킵
    check_country >> skip >> end

# =========================================================
# 📝 DAG 문서
# =========================================================
dag.doc_md = """
# Asset Master DAG

## 개요
국가별 자산 마스터 테이블을 구축하고 검증하는 DAG입니다.

## 실행 방식
- **스케줄**: None (외부 Trigger 전용)
- **Trigger 예시**:
  ```python
  TriggerDagRunOperator(
      task_id="trigger_asset_master_kr",
      trigger_dag_id="asset_master_dag",
      conf={"country_code": "KOR"},
  )
  ```

## 주요 변경사항 (2025-10 개선)
1. ✅ **WarehouseOperator 도입**: BaseWarehousePipeline 표준 사용
2. ✅ **자동 리소스 관리**: DuckDB 연결 자동 정리
3. ✅ **향상된 로깅**: run_and_log()를 통한 실행 추적
4. ✅ **Vendor 우선순위**: exchange_master 기반 자동 거래소 매핑

## Airflow Variables
- `master_countries`: 처리 대상 국가 리스트 (예: ["USA", "KOR", "JPN"])

## 의존성
- **선행 DAG**: symbol_list_dag, fundamental_dag (각 국가의 validated 데이터 필요)
- **선행 Warehouse**: exchange_master (국가-거래소 매핑 필요)

## 출력
- **경로**: `warehouse/asset_master/country_code={COUNTRY}/snapshot_dt={DATE}/`
- **파일**: 
  - `asset_master.parquet`: 메인 데이터
  - `_build_meta.json`: 빌드 메타데이터

## 모니터링
- Task 실패 시 `pipeline_task_log` 테이블에서 상세 로그 확인
- 전역 스냅샷 레지스트리에서 최신 버전 추적
"""