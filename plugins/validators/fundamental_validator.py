import os
import json
import tempfile
import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check
from plugins.validators.base_validator import BaseDataValidator
from soda.scan import Scan
import duckdb
import yaml


class FundamentalValidator(BaseDataValidator):
    """주식/ETF 재무제표 통합 검증 Validator"""

    BATCH_SIZE = 1000

    def __init__(self, exchange_code: str, trd_dt: str, data_domain: str = "fundamentals"):
        super().__init__(exchange_code, trd_dt, data_domain)
        self.stock_schema = self._get_stock_schema()
        self.etf_schema = self._get_etf_schema()

    # ============================================================
    # 1️⃣ JSON flatten 헬퍼
    # ============================================================
    @staticmethod
    def _to_float(v):
        if v in [None, "", "NA", "NaN", "N/A", "null", "None"]:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            try:
                return float(str(v).replace(",", ""))
            except Exception:
                return None

    def _extract_financials(self, record: dict):
        fin = record.get("Financials", {})
        general = record.get("General", {})
        code = general.get("Code")
        stock_type = general.get("Type", "").lower()

        data = {"code": code, "stock_type": stock_type}

        if stock_type == 'common stock':

            # ------------------------
            # 📊 손익계산서 (Income Statement)
            # ------------------------
            income_q = fin.get("Income_Statement", {}).get("quarterly", {})
            if income_q:
                latest_q = max(income_q.keys())
                item = income_q[latest_q]
                data["totalRevenue"] = self._to_float(item.get("totalRevenue"))
                data["operatingIncome"] = self._to_float(item.get("operatingIncome"))
                data["netIncome"] = self._to_float(item.get("netIncome"))
            else:
                data["totalRevenue"] = None
                data["operatingIncome"] = None
                data["netIncome"] = None

            # ------------------------
            # 💰 대차대조표 (Balance Sheet)
            # ------------------------
            bs_q = fin.get("Balance_Sheet", {}).get("quarterly", {})
            if bs_q:
                latest_q = max(bs_q.keys())
                item = bs_q[latest_q]
                data["totalAssets"] = self._to_float(item.get("totalAssets"))
                data["totalLiab"] = self._to_float(item.get("totalLiab"))
                data["totalStockholderEquity"] = self._to_float(item.get("totalStockholderEquity"))
            else:
                data["totalAssets"] = None
                data["totalLiab"] = None
                data["totalStockholderEquity"] = None

            # ------------------------
            # 💵 현금흐름표 (Cash Flow)
            # ------------------------
            cf_q = fin.get("Cash_Flow", {}).get("quarterly", {})
            if cf_q:
                latest_q = max(cf_q.keys())
                item = cf_q[latest_q]

                # ✅ 영업활동 현금흐름
                data["cashflow_operating"] = self._to_float(
                    item.get("totalCashFromOperatingActivities")
                    or item.get("cashFlowsOtherOperating")
                    or item.get("freeCashFlow")
                )

                # ✅ 투자활동 현금흐름
                data["cashflow_investing"] = self._to_float(
                    item.get("totalCashflowsFromInvestingActivities")
                    or item.get("investments")
                    or item.get("capitalExpenditures")
                    or item.get("otherCashflowsFromInvestingActivities")
                    or item.get("salePurchaseOfStock")
                )

                # ✅ 재무활동 현금흐름
                data["cashflow_financing"] = self._to_float(
                    item.get("totalCashFromFinancingActivities")
                    or item.get("dividendsPaid")
                    or item.get("netBorrowings")
                    or item.get("issuanceOfCapitalStock")
                    or item.get("otherCashflowsFromFinancingActivities")
                )
            else:
                data["cashflow_operating"] = None
                data["cashflow_investing"] = None
                data["cashflow_financing"] = None

        elif "ETF_Data" in record:

            etf_data = record.get('ETF_Data', {})

            data["etf_total_assets"] = self._to_float(etf_data.get('TotalAssets'))
            data["etf_holdings_count"] = self._to_float(etf_data.get('Holdings_Count'))
            data["etf_expense_ratio"] = self._to_float(etf_data.get('NetExpenseRatio'))
            data["etf_yield"] = self._to_float(etf_data.get('Yield'))

        return data

    # ============================================================
    # 2️⃣ Pandera Schema 정의
    # ============================================================
    def _get_stock_schema(self):
        return pa.DataFrameSchema(
            columns={
                "totalRevenue": Column(float, nullable=True),
                "operatingIncome": Column(float, nullable=True),
                "netIncome": Column(float, nullable=True),
                "totalAssets": Column(float, nullable=True),
                "totalLiab": Column(float, nullable=True),
                "totalStockholderEquity": Column(float, nullable=True),
                # 새로운 현금흐름 항목
                "cashflow_operating": Column(float, nullable=True),
                "cashflow_investing": Column(float, nullable=True),
                "cashflow_financing": Column(float, nullable=True)
            },
            checks=[
                Check(
                    lambda df: (
                            abs(df["totalAssets"].fillna(0)
                                - (df["totalLiab"].fillna(0) + df["totalStockholderEquity"].fillna(0)))
                            <= (df["totalAssets"].abs() * 0.1)
                    ),
                    error="자산=부채+자본 불일치 (5% 이상 차이)"
                )
            ],
            strict=False,
        )

    def _get_etf_schema(self):
        return pa.DataFrameSchema(
            columns={
                "etf_total_assets": Column(float, nullable=True),
                "etf_holdings_count": Column(float, nullable=True),
                "etf_expense_ratio": Column(float, nullable=True),
                "etf_yield": Column(float, nullable=True),
            },
            strict=False,
        )

    # ============================================================
    # 3️⃣ 전체 검증 실행
    # ============================================================
    def validate(self, **kwargs):
        raw_dir = self._get_lake_path("raw")
        files = [f for f in os.listdir(raw_dir) if f.endswith(".json")]
        if not files:
            raise FileNotFoundError(f"⚠️ 검증 대상 JSON 파일이 없습니다: {raw_dir}")

        all_records, valid_records = [], []
        for file in files:
            path = os.path.join(raw_dir, file)
            with open(path, "r", encoding="utf-8") as f:
                record = json.load(f)
                all_records.append(record)

        print(f"📦 총 {len(all_records)}개 종목 데이터 검증 시작 (batch={self.BATCH_SIZE})")

        # ✅ Batch 단위 Pandera 검증
        for i in range(0, len(all_records), self.BATCH_SIZE):
            batch_records = all_records[i:i + self.BATCH_SIZE]
            df = pd.DataFrame([self._extract_financials(r) for r in batch_records])
            print('df.columns :: ' ,df.columns)
            # 종목 타입별 분리
            df_stock = df[df["stock_type"].str.contains("common stock", case=False, na=False)]
            df_etf = df[df["stock_type"].str.contains("etf", case=False, na=False)]

            print('df_stock :: ' ,df_stock)
            print('df_etf :: ' ,df_etf)

            try:
                if not df_stock.empty:
                    self.stock_schema.validate(df_stock)
                if not df_etf.empty:
                    self.etf_schema.validate(df_etf)
                valid_records.extend(df.to_dict("records"))
                print(f"✅ Pandera 검증 통과 ({len(df)}/{len(all_records)})")
            except Exception as e:
                print(f"❌ Pandera 검증 실패 (batch_{i // self.BATCH_SIZE}): {e}")

        if not valid_records:
            raise AssertionError("❌ 모든 종목의 Pandera 검증 실패")

        merged_df = pd.DataFrame(valid_records)
        temp_path = os.path.join(raw_dir, "_merged_temp.parquet")
        merged_df.to_parquet(temp_path, index=False)
        print(f"📄 단일 병합 파일 생성 완료: {temp_path}")

        # ✅ ETF / STOCK 분리 Soda 검증
        stock_df = merged_df[merged_df["stock_type"].str.contains("Common Stock", case=False, na=False)]
        etf_df = merged_df[merged_df["stock_type"].str.contains("ETF", case=False, na=False)]

        if not stock_df.empty:
            stock_temp_path = os.path.join(raw_dir, "_stock_temp.parquet")
            stock_df.to_parquet(stock_temp_path, index=False)
            self._run_soda_on_parquet(stock_temp_path, mode="stock")

        if not etf_df.empty:
            etf_temp_path = os.path.join(raw_dir, "_etf_temp.parquet")
            etf_df.to_parquet(etf_temp_path, index=False)
            self._run_soda_on_parquet(etf_temp_path, mode="etf")

        validated_path = os.path.join(
            self._get_lake_path("validated"),
            "fundamentals.parquet"
        )
        merged_df.to_parquet(validated_path, index=False)
        print(f"🎯 검증 완료 데이터 저장: {validated_path}")

    # ============================================================
    # 4️⃣ Soda Core 실행 로직
    # ============================================================
    def _run_soda_on_parquet(self, parquet_path: str, mode: str = "stock"):
        """Soda Core로 parquet 파일 검증 (mode: stock / etf)"""
        base_dir = "/opt/airflow/plugins/soda/checks"
        soda_check_file = os.path.join(base_dir, f"fundamentals_{mode}_checks.yml")

        if not os.path.exists(soda_check_file):
            print(f"⚠️ {soda_check_file} 없음 — 건너뜀.")
            return

        tmp_config = {
            "data_source my_duckdb": {
                "type": "duckdb",
                "path": ":memory:",
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as tmp_file:
            yaml.dump(tmp_config, tmp_file)
            tmp_config_path = tmp_file.name

        scan = Scan()
        scan.set_data_source_name("my_duckdb")
        scan.add_configuration_yaml_file(tmp_config_path)
        scan.add_sodacl_yaml_files(soda_check_file)
        scan._data_source_manager.get_data_source("my_duckdb").connection.execute(
            f"CREATE TABLE fundamentals AS SELECT * FROM read_parquet('{parquet_path}')"
        )

        exit_code = scan.execute()
        print(f"🧪 Soda Scan 완료 (exit_code={exit_code}, mode={mode})")

        # 🚨 Soda 검증 실패 시 예외 발생시켜 Task 실패 처리
        if exit_code != 0:
            raise AssertionError(f"❌ Soda 검증 실패: exit_code={exit_code}, mode={mode}")

        os.unlink(tmp_config_path)

