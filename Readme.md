# 📘 EODHD 기반 데이터 파이프라인 구축

### *Airflow · Docker · PostgreSQL · DuckDB · Pandera · Soda Core (2025-11 최신)*

이 레포지토리는 **EODHD API 기반 주식·ETF·거래소·펀더멘털·배당·액면분할 데이터 플랫폼**을
**Airflow 3.x + Docker + PostgreSQL + DuckDB** 환경에서 자동화하기 위한
엔드투엔드 파이프라인을 제공합니다.

본 문서는 LLM 또는 개발자가 전체 아키텍처와 파이프라인 설계를 명확히 이해하고
운영·디버깅·확장 작업을 일관되게 수행할 수 있도록 작성된 **기술 문서**입니다.

---

# 🚀 Features

* 🌐 **EODHD 기반 멀티 도메인 데이터 수집**
* 🔍 **Pandera + Soda Core 기반 2-Layer 품질 검증**
* 🧺 **Lake → Warehouse(Snapshot) → Mart 정규화 계층**
* 📦 **AssetMaster / PriceMaster / FundamentalsMaster 자동 생성**
* 🗂️ **Vendor / Exchange / trd_dt 파티셔닝 표준화**
* 🧱 **Deterministic security_id 생성 (base32 hash)**
* 🔄 **백필(backfill) 대응 Snapshot Metadata 시스템**
* 🔒 **FileLock 기반 컨커런시 제어**
* 📝 **PostgreSQL(JSONB) 기반 실행 로그 자동 적재**
* 🐳 로컬 개발용 **Docker Compose 환경 포함**

---

# 🧭 Architecture Overview

```
EODHD API
   ↓ fetch(JSON)
Data Lake (raw)
   ↓ Pandera / Soda Validation
Data Lake (validated)
   ↓ Warehouse Pipelines (정규화/병합/ID 생성)
Data Warehouse (snapshot)
   ↓ 분석/모델링
Data Mart
```

---

# 🗂️ Directory Structure (2025-11 Latest)

```
/opt/airflow
├── dags/
│   ├── domain/equity/eodhd/*.py
│   └── warehouse/equity/*.py
│
├── data/
│   ├── data_lake/
│   │   ├── raw/
│   │   ├── validated/
│   │   ├── latest_validated_meta.lock
│   │   └── _metadata/validation_logs/
│   │
│   ├── data_warehouse/
│   │   ├── snapshot/
│   │   ├── latest_snapshot_meta.lock
│   │   └── _event_logs/
│   │
│   ├── meta/security_id_map.lock
│   └── validation_logs/
│
├── plugins/
│   ├── pipelines/
│   ├── validators/
│   ├── utils/
│   ├── operators/
│   ├── hooks/
│   └── config/constants.py
│
├── docker-compose.yaml
└── Readme.md
```

---

# ⚙️ Pipeline Architecture

## 1️⃣ Standard Pipeline Interface

```python
class DataPipelineInterface(ABC):
    def fetch(...): ...
    def validate(...): ...
    def load(...): ...
```

Airflow에서는:

```python
PipelineOperator(
    pipeline_cls=FundamentalPipeline,
    method_name="fetch_and_validate",
    op_kwargs={...}
)
```

---

## 2️⃣ Data Lake Validation (Pandera + Soda)

### 🔍 단계별 흐름

```
RAW JSON
   ↓ Pandera (Row-level schema)
merged_temp.parquet
   ↓ Soda Core (DuckDB SQL)
VALIDATED 저장
```

### Pandera 예시

```python
columns={
  "code": Column(str),
  "date": Column(str),
  "close": Column(float, Check(lambda s: s >= 0)),
}
```

### Soda 예시 (`prices.yml`)

```yaml
checks for prices in my_duckdb:
  - row_count > 0
  - missing_percent(close) < 5
```

---

## 3️⃣ Warehouse Layer (Snapshot Architecture)

### Snapshot 구조

```
data_warehouse/snapshot/equity/asset_master/
  └── country_code=USA/
      └── snapshot_dt=2025-11-10/
```

### 최신 스냅샷 메타 파일

`data_warehouse/latest_snapshot_meta.json`

```json
{
  "asset_master": {
    "latest_snapshot_dt": "2025-11-10",
    "meta_file": ".../_build_meta.json"
  }
}
```

### Lock Files

| 파일                         | 역할                    |
| -------------------------- | --------------------- |
| latest_validated_meta.lock | Lake 검증 메타 점유         |
| latest_snapshot_meta.lock  | Warehouse snapshot 점유 |
| security_id_map.lock       | AssetMaster ID 충돌 방지  |

---

# 🧱 Warehouse Pipelines

| Pipeline                          | Input                                  | Output             | Notes          |
| --------------------------------- | -------------------------------------- | ------------------ | -------------- |
| **AssetMasterPipeline**           | symbol + fundamentals + symbol_changes | asset_master       | security_id 생성 |
| **PriceWarehousePipeline**        | validated prices                       | price_master       | 국가단위 snapshot  |
| **FundamentalsWarehousePipeline** | fundamentals(stock/etf)                | fundamental_master | ticker 병합      |
| **ExchangeMasterPipeline**        | exchange_list + exchange_detail        | exchange_master    | 거래소 메타 통합      |
| **HolidayMasterPipeline**         | exchange_detail holiday                | holiday_master     | 국가 휴장일         |

---

# 🔑 Deterministic ID Generator

```python
AST-{country}|{exchange}|{ticker}
 → sha256 hash
 → base32 encode
 → 10~12 chars
```

재실행해도 동일한 security_id가 생성됨.

---

# 📜 Example DAG

```python
with DAG(
    dag_id="fundamentals_warehouse",
    schedule=None,
    catchup=False,
    start_date=datetime(2025,10,1)
) as dag:

    validate = PipelineOperator(
        task_id="validate_fundamentals",
        pipeline_cls=FundamentalPipeline,
        method_name="fetch_and_validate",
        op_kwargs={"exchange_codes": ["US", "KO", "KQ"]},
    )

    build_snapshot = PipelineOperator(
        task_id="build_fundamentals_snapshot",
        pipeline_cls=FundamentalsWarehousePipeline,
        method_name="build",
        op_kwargs={"country_code": "USA"},
    )

    validate >> build_snapshot
```

---

# 📊 Logging System

모든 PipelineOperator는 `run_and_log()`로 감싸서
결과를 PostgreSQL(`pipeline_task_log`)에 JSONB로 저장.

```sql
CREATE TABLE pipeline_task_log (
  id SERIAL PRIMARY KEY,
  dag_id TEXT,
  task_id TEXT,
  run_time TIMESTAMP,
  status TEXT,
  result_info JSONB,
  error_message TEXT
);
```

로그 예시:

```json
{
  "exchange_code": "US",
  "trd_dt": "2025-11-05",
  "record_count": 2345,
  "validated_path": ".../validated/prices/..."
}
```

---

# 📁 Path Conventions

```
data_lake/validated/{domain}/{vendor}/exchange_code=US/trd_dt=2025-11-10/

data_warehouse/snapshot/equity/asset_master/
  └── country_code=USA/
      └── snapshot_dt=2025-11-10/
```

---

# 🧩 Metadata System

| 구성                | 설명                  |
| ----------------- | ------------------- |
| Validation Logs   | Lake 검증 결과 저장       |
| Snapshot Metadata | 최신 snapshot_dt 관리   |
| Lock File         | 동시 작업 방지 + 상태 저장    |
| Event Logs        | Warehouse 빌드 이벤트 기록 |

---

# 🧱 Tech Stack

* **Airflow 3.x**
* **Docker Compose / ECS Fargate**
* **RDS PostgreSQL**
* **DuckDB**
* **Pandera**
* **Soda Core**
* **Redis (Queue/Cache)**
* Python 3.11

