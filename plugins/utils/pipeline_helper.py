from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import traceback
import json

def run_and_log(func, postgres_conn_id, dag_id, task_id, airflow_context=None, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

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

    op_kwargs = kwargs.get("op_kwargs", {}) if isinstance(kwargs.get("op_kwargs"), dict) else kwargs
    result_info, status, error_message = {}, "SUCCESS", None
    layer = kwargs.get("layer")

    assert layer is not None, "로그테이블에 적재할 layer 정보를 기입하지 않았습니다."

    try:
        # ✅ build() or validate() 구분 실행
        if func.__name__ == "validate":
            result = func(context=airflow_context)
        else:
            result = func(context=airflow_context, **op_kwargs)

        result_info = {
            **op_kwargs,
            "status": "success",
            "record_count": result.get("record_count"),
            "validated_path": result.get("validated_path"),
        }

        if "record_count" not in result_info:
            result_info["record_count"] = None

        _insert_pipeline_log(pg_hook, dag_id, task_id, status, result_info, error_message, layer)
        print(f"📘 로그 저장 완료 [SUCCESS] - {result_info}")

    except Exception as e:
        status = "FAILED"
        error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        result_info = {**op_kwargs, "status": "failed"}
        _insert_pipeline_log(pg_hook, dag_id, task_id, status, result_info, error_message, layer)
        print(f"📘 로그 저장 완료 [FAILED] - {result_info}")
        raise

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