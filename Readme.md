# 📘 EODHD 기반 데이터 파이프라인 Airflow 구축 문서 (2025-10 최신)

## 🧭 개요

이 문서는 **EODHD API**를 기반으로 주식·ETF·펀더멘털·배당금 등 금융 데이터를 자동 수집·적재하는
**Airflow 3.1.0 + PostgreSQL + Docker 기반 데이터 파이프라인 시스템**의 전체 설계 및 검증 프로세스를 설명합니다.

본 문서는 **LLM에게 시스템의 전체 맥락을 전달하기 위한 내부 기술 문서**이며,
이 내용을 바탕으로 LLM은 코드 수정·설계 변경·운영 자동화 질의에 일관된 답변을 제공할 수 있어야 합니다.

---

## ⚙️ 시스템 개요

### 🔹 기술 스택

| 구성요소       | 기술 스택                                     |
| ---------- | ----------------------------------------- |
| 오케스트레이션    | **Apache Airflow 3.1.0 (CeleryExecutor)** |
| 인프라        | AWS ECS Fargate (개발환경은 Docker Compose)    |
| 데이터베이스     | AWS RDS PostgreSQL                        |
| 캐시/큐       | AWS ElastiCache Redis                     |
| 데이터 소스     | [EODHD API](https://eodhd.com)            |
| 품질 검증      | **Pandera + Soda Core**                   |
| 알림 (추가 예정) | Slack Webhook                             |
| CI/CD      | AWS CodePipeline + ECR 배포 자동화             |

---

## 🗂️ 디렉토리 구조

```
/opt/airflow
├── dags/
│   ├── domain/
│   │   ├── equity/
│   │   │   ├── eodhd/
│   │   │   │   ├── equity_price_dag.py
│   │   │   │   ├── equity_dividend_dag.py
│   │   │   │   └── fundamentals_dag.py
│   │   │   ├── krx/
│   │   │   │   ├── equity_price_dag.py
│   │   │   │   ├── disclosure_dag.py
│   │   │   │   └── fundamentals_dag.py
│   │   │   └── investing/
│   │   │       ├── equity_index_dag.py
│   │   │       └── sector_dag.py
│   │   ├── macro/
│   │   │   ├── eodhd/
│   │   │   ├── fred/
│   │   │   └── statistic_korea/
│   │   ├── fx/
│   │   │   ├── eodhd/
│   │   │   └── boj/
│   │   └── crypto/
│   │       ├── coingecko/
│   │       ├── binance/
│   │       └── upbit/
│   │
│   ├── warehouse/
│   │   ├── equity/
│   │   │   ├── equity_master_dag.py
│   │   │   ├── fundamentals_merge_dag.py   # ✅ EODHD + KRX 통합
│   │   │   └── price_normalization_dag.py
│   │   ├── fx/
│   │   ├── macro/
│   │   ├── crypto/
│   │   └── news/
│   │
│   └── mart/
│       ├── portfolio/
│       ├── sentiment/
│       └── indicator/
│
├── plugins/
│   ├── validators/
│   │   ├── schemas/
│   │   │   ├── lake/
│   │   │   │   ├── equity/
│   │   │   │   │   ├── eodhd/
│   │   │   │   │   │   ├── equity_price.json
│   │   │   │   │   │   ├── equity_dividend.json
│   │   │   │   │   │   └── fundamentals.json
│   │   │   │   │   ├── krx/
│   │   │   │   │   │   ├── equity_price.json
│   │   │   │   │   │   ├── disclosure.json
│   │   │   │   │   │   └── fundamentals.json
│   │   │   │   ├── macro/
│   │   │   │   │   ├── eodhd/
│   │   │   │   │   ├── fred/
│   │   │   │   │   └── statistic_korea/
│   │   │   └── warehouse/
│   │   │       ├── equity/
│   │   │       ├── fx/
│   │   │       ├── macro/
│   │   │       ├── crypto/
│   │   │       └── news/
│   │   │
│   │   ├── checks/
│   │   │   ├── lake/
│   │   │   │   ├── equity/
│   │   │   │   │   ├── eodhd/
│   │   │   │   │   ├── krx/
│   │   │   │   │   └── investing/
│   │   │   │   ├── fx/
│   │   │   │   └── macro/
│   │   │   └── warehouse/
│   │   │       ├── equity/
│   │   │       ├── fx/
│   │   │       ├── macro/
│   │   │       └── crypto/
│   │
│   ├── operators/
│   └── pipelines/
│
├── data/
│   ├── data_lake/
│   │   ├── raw/
│   │   │   ├── equity/
│   │   │   │   ├── eodhd/
│   │   │   │   ├── krx/
│   │   │   │   └── investing/
│   │   │   ├── macro/
│   │   │   └── fx/
│   │   └── validated/
│   │       ├── equity/
│   │       │   ├── eodhd/
│   │       │   └── krx/
│   │       └── macro/
│   │
│   ├── data_warehouse/
│   │   ├── snapshot/
│   │   │   ├── equity/
│   │   │   └── fx/
│   │   └── validated/
│   │       ├── equity/
│   │       │   └── merged/
│   │       └── macro/
│   │
│   └── data_mart/
│       └── portfolio/

```

---

## 🧩 핵심 설계 개념

### 1️⃣ 표준화된 Pipeline 인터페이스

모든 데이터 파이프라인은 `DataPipelineInterface`를 상속받으며,
`fetch → validate → load` 구조를 갖습니다.

```python
class DataPipelineInterface(ABC):
    @abstractmethod
    def fetch(self, **kwargs): ...
    @abstractmethod
    def validate(self, **kwargs): ...
    @abstractmethod
    def load(self, **kwargs): ...
```

* **Data Lake 중심 구조**

  * `fetch`: 원천 수집 (EODHD API → JSON/Parquet 저장)
  * `validate`: Pandera + Soda Core 병행 검증
  * `load`: 검증 통과 파일만 Data Lake → PostgreSQL 적재

---

### 2️⃣ Validator 구조 (2025 최신 버전)

| 구분                       | 역할                       | 검증 도구               | 출력 경로                                |
| ------------------------ | ------------------------ | ------------------- | ------------------------------------ |
| **EquityPriceValidator** | 종가·거래량 등 시세 데이터 정합성 검증   | Pandera + Soda Core | `/data_lake/validated/equity_prices` |
| **FundamentalValidator** | 주식·ETF 펀더멘털 검증 (다층 JSON) | Pandera + Soda Core | `/data_lake/validated/fundamentals`  |
| **SymbolListValidator**  | 거래소 종목 리스트 필수 필드 검증      | Pandera             | `/data_lake/validated/symbols`       |

모든 Validator는 `BaseDataValidator`를 상속받으며,
Airflow Task 실행 시 `validate()` 진입점으로 호출됩니다.

---

## 🔍 검증 로직 세부 구조

### 1️⃣ Pandera 기반 정합성 검사

Pandera는 **DataFrame 단위 스키마 유효성 검사**를 수행합니다.

예: `EquityPriceValidator`

```python
class EquityPriceValidator(BaseDataValidator):
    def _get_schema(self):
        return pa.DataFrameSchema(
            columns={
                "code": Column(str),
                "exchange_short_name": Column(str),
                "date": Column(str),
                "close": Column(float, Check(lambda s: s >= 0)),
                "volume": Column(float, nullable=True),
            },
            checks=[
                Check(lambda df: df["high"] >= df["low"], error="고가<저가 오류")
            ],
            strict=False,
        )
```

* ✅ Pandera 검증은 Batch 단위(기본 1000건)로 수행
* ✅ 통과된 레코드는 `valid_records`로 누적
* ❌ 실패 시 에러 로그에 batch index 및 컬럼명 출력

---

### 2️⃣ FundamentalValidator (다층 JSON 파싱)

EODHD의 펀더멘털 API는 다음과 같은 **중첩 JSON 구조**를 가집니다:

```
Financials → Income_Statement / Balance_Sheet / Cash_Flow → quarterly/yearly → 각 항목
```

`FundamentalValidator`는 이 구조를 평탄화(flatten)하여 Pandera 검증 스키마에 맞게 변환합니다.

```python
data = {
    "code": "AAPL",
    "totalRevenue": 94036000000.0,
    "operatingIncome": 28202000000.0,
    "netIncome": 23434000000.0,
    "totalAssets": 331495000000.0,
    "totalLiab": 287880000000.0,
    "totalStockholderEquity": 43615000000.0,
    ...
}
```

또한 ETF의 경우 `"ETF_Data"` 블록을 별도로 파싱하여
`etf_total_assets`, `etf_expense_ratio`, `etf_holdings_count`, `etf_yield` 등 필드를 검증합니다.

---

### 3️⃣ Pandera 검증 후 Soda Core 병행 검증

Soda Core는 **DuckDB 기반 SQL 수준 데이터 품질 규칙**을 실행합니다.
(예: 평균, 누락 비율, 자산=부채+자본 등)

#### ✅ 예시: `plugins/soda/checks/fundamentals_stock_checks.yml`

```yaml
checks for fundamentals in my_duckdb:
  - row_count > 0
  - duplicate_count(code) = 0
  - missing_percent(totalRevenue) < 10
  - avg(totalRevenue) > 0
  - missing_percent(operatingIncome) < 10
  - avg(totalAssets) > 0
  - avg(totalLiab) >= 0
  - avg(totalStockholderEquity) >= 0
```

#### 실행 로직

```python
scan = Scan()
scan.set_data_source_name("my_duckdb")
scan.add_configuration_yaml_file(tmp_config_path)
scan.add_sodacl_yaml_files(soda_check_file)
scan._data_source_manager.get_data_source("my_duckdb").connection.execute(
    f"CREATE TABLE fundamentals AS SELECT * FROM read_parquet('{parquet_path}')"
)
exit_code = scan.execute()
```

* **exit_code=0** → 모든 체크 통과
* **exit_code≠0** → 실패, DAG 태스크 실패로 처리됨

---

### 4️⃣ 검증 흐름 요약 (시세/펀더멘털 공통)

```
개별 JSON 파일 수집 (raw)
         ↓
Batch 단위 Pandera 스키마 검증
         ↓
통과 레코드 → 단일 병합파일(_merged_temp.parquet)
         ↓
Soda Core (DuckDB 기반 SQL 검증)
         ↓
exit_code=0 → validated 경로 저장
```

✅ **검증 통과 파일만**
→ `/data_lake/validated/{data_domain}/exchange_code=XX/trd_dt=YYYY-MM-DD/`

---

## 🧠 로그 관리 및 실행 추적

### 1️⃣ run_and_log() — 실행 및 DB 로그 적재

모든 `PipelineOperator` 실행은 `pipeline_helper.run_and_log()`을 통해 래핑됩니다.

```python
result_info = func(**op_kwargs) or {}
```

수정 후 구조:

```python
result = func(**op_kwargs) or {}
result_info = {
    **op_kwargs,
    "status": "success",
    "record_count": result.get("record_count"),
    "validated_path": result.get("validated_path"),
}
```

* Airflow context (`dag`, `ti`, 등) 제외
* JSONB 형태로 PostgreSQL 테이블(`pipeline_task_log`)에 저장

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

---

### 2️⃣ Validator 결과 반환 표준화

모든 Validator는 `validate()` 끝에 아래 형식으로 반환합니다.

```python
return {
    "record_count": len(merged_df),
    "validated_path": validated_path,
}
```

이를 통해 로그 테이블에 다음과 같이 기록됩니다:

```json
{
  "exchange_code": "US",
  "trd_dt": "2025-10-18",
  "data_domain": "fundamentals",
  "status": "success",
  "record_count": 3,
  "validated_path": "/opt/airflow/data/data_lake/validated/fundamentals/exchange_code=US/trd_dt=2025-10-18/fundamentals.parquet"
}
```

---

## 💾 주요 파이프라인 구성 요약

| 파이프라인                 | 데이터          | 원천 API                           | 검증 방식          | 결과 저장                   |
| --------------------- | ------------ | -------------------------------- | -------------- | ----------------------- |
| `SymbolListPipeline`  | 거래소 종목       | `/api/exchange-symbol-list/{EX}` | Pandera        | validated/symbols       |
| `EquityPricePipeline` | 주가/시세        | `/api/eod-bulk-last-day/{EX}`    | Pandera + Soda | validated/equity_prices |
| `FundamentalPipeline` | 펀더멘털(주식/ETF) | `/api/fundamentals/{TICKER}`     | Pandera + Soda | validated/fundamentals  |

---

## 🧱 DAG 구성 예시

`dags/equity/fundamental_dag.py`

```python
with DAG(
    dag_id="equity_fundamental_pipeline",
    start_date=datetime(2025, 10, 14),
    schedule=None,
    catchup=False,
) as dag:

    exchanges = Variable.get("EXCHANGES", default_var=["US", "KO", "KQ"], deserialize_json=True)

    fetch_symbol = [
        PipelineOperator(
            task_id=f"{ex}_fetch_symbol_list",
            pipeline_cls=SymbolListPipeline,
            method_name="fetch_and_load",
            op_kwargs={"exchange_code": ex}
        )
        for ex in exchanges
    ]

    fetch_fundamental = PipelineOperator(
        task_id="fetch_fundamental_data",
        pipeline_cls=FundamentalPipeline,
        method_name="fetch_and_load_all",
        op_kwargs={"exchange_codes": exchanges}
    )

    fetch_symbol >> fetch_fundamental
```

---

## 🧠 운영 규칙

| 항목    | 규칙                                         |
| ----- | ------------------------------------------ |
| 실행 시간 | EODHD 기준 장 마감 후 2~3시간                      |
| 재실행 시 | 기존 날짜 데이터 삭제 후 재적재                         |
| 실패 시  | Slack 알림 (추후 추가 예정)                        |
| 검증 로직 | Pandera 1차 + Soda Core 2차                  |
| 로그 관리 | PostgreSQL(`pipeline_task_log`)에 JSONB로 저장 |
| 모니터링  | exit_code 기반 태스크 상태 반영                     |

---

## 🚀 확장 계획

| 항목    | 설명                              |
| ----- | ------------------------------- |
| ✅ 현재  | Symbol / Price / Fundamental    |
| 🔜 예정 | Dividend / Split / ETF Holdings |
| 🧩 확장 | Great Expectations 대시보드 연동      |
| 📊 운영 | DuckDB + Soda SQL 기반 QA 리포트 자동화 |
