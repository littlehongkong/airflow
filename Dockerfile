# Dockerfile
# Airflow 공식 이미지를 기반으로 사용합니다. (버전에 맞춰 수정하세요)
ARG AIRFLOW_VERSION=3.1.0
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11

# 💡 Pandera 및 Soda Core 설치
# 필요한 경우 'soda-core' 대신 'soda-core-scientific' 또는
# 데이터 소스에 맞는 soda-core-xxx 패키지를 설치합니다.
USER airflow
RUN pip install --no-cache-dir \
    pandera \
    soda-core

# 필요한 경우, Airflow 사용자(airflow)에게 권한을 다시 위임합니다.
USER airflow