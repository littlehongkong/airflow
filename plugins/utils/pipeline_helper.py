from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import traceback
import json

def run_and_log(func, postgres_conn_id, dag_id, task_id, airflow_context=None, **kwargs):
    """
    공통 실행 및 로그 적재 유틸리티
    - func: 실행할 파이프라인 메서드 (fetch_and_load 등)
    - dag_id, task_id: Airflow Context에서 전달
    - postgres_conn_id: Airflow Connection ID
    """

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    # ✅ 로그 테이블 보장
    create_sql = """
    CREATE TABLE IF NOT EXISTS pipeline_task_log (
        id SERIAL PRIMARY KEY,
        dag_id TEXT,
        task_id TEXT,
        run_time TIMESTAMP,
        status TEXT,
        result_info JSONB,
        error_message TEXT
    );
    """
    pg_hook.run(create_sql)

    # ✅ op_kwargs만 추출 (Airflow context 제외)
    op_kwargs = kwargs.get("op_kwargs", {}) if isinstance(kwargs.get("op_kwargs"), dict) else kwargs

    result_info = {}
    status = "SUCCESS"
    error_message = None
    layer = kwargs.get("layer")

    assert layer is not None, '로그테이블에 적재할 layer 정보를 기입하지 않았습니다.'

    try:
        # 실제 task 함수 실행
        result = func(context=airflow_context, **op_kwargs)

        # ✅ 결과 정리: op_kwargs + 공통 필드만
        result_info = {
            **op_kwargs,
            "status": "success",
            "record_count": result.get("record_count"),
            "validated_path": result.get("validated_path"),
        }

        # 기본값 채우기
        if "record_count" not in result_info:
            result_info["record_count"] = None

        # ✅ 성공 시 로그 남기기
        _insert_pipeline_log(pg_hook, dag_id, task_id, status, result_info, error_message, layer)
        print(f"📘 로그 저장 완료 [SUCCESS] - {result_info}")


    except Exception as e:
        status = "FAILED"
        error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"

        # 실패한 경우에도 입력값 로그 남김
        result_info = {**op_kwargs, "status": "failed"}

        # ✅ 로그 남기고 재-raise
        _insert_pipeline_log(pg_hook, dag_id, task_id, status, result_info, error_message, layer)
        print(f"📘 로그 저장 완료 [FAILED] - {result_info}")

        raise

# psql -U postgres -d postgres

    return result_info

def _insert_pipeline_log(pg_hook, dag_id, task_id, status, result_info, error_message, layer: str = 'lake'):
    """공통 로그 insert 함수"""
    insert_sql = """
        INSERT INTO pipeline_task_log(dag_id, task_id, run_time, status, result_info, error_message, layer)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    pg_hook.run(
        insert_sql,
        parameters=(dag_id, task_id, datetime.utcnow(), status, json.dumps(result_info, ensure_ascii=False),
                    error_message, layer),
    )