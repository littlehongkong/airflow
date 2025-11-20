import json
import pandas as pd
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import psutil, gc
from plugins.pipelines.warehouse.base_warehouse_pipeline import BaseWarehousePipeline
from plugins.utils.duckdb_manager import DuckDBManager
from plugins.config.constants import (
    EXCLUDED_EXCHANGES_BY_COUNTRY,
    VENDORS,
    DATA_WAREHOUSE_ROOT,
)
from plugins.utils.id_generator import _load_id_map, _save_id_map, _b32, generate_or_reuse_entity_id
from plugins.utils.transform_utils import normalize_columns, safe_merge

# ✅ loader import
from plugins.utils.loaders.lake.symbol_loader import load_symbol_list
from plugins.utils.loaders.lake.fundamentals_loader import load_fundamentals_latest
from plugins.utils.loaders.lake.exchange_detail_loader import load_exchange_detail_list
from plugins.utils.loaders.lake.exchange_loader import load_exchange_list
from plugins.utils.loaders.lake.symbol_change import load_symbol_change_list

from plugins.utils.loaders.warehouse.asset_master_loader import load_asset_master


class AssetMasterPipeline(BaseWarehousePipeline):
    """
    ✅ 자산 마스터 파이프라인 (Warehouse Domain: asset_master)
    --------------------------------------------------------
    - Symbol / Fundamentals / Exchange 데이터를 병합하여 국가 단위 마스터 생성
    - Symbol Change 발생 시, 이전 스냅샷과 식별자 매칭으로 security_id 재사용
    - 신규 상장/심볼변경 이벤트를 이벤트 로그에 기록
    """

    def __init__(self, trd_dt: str, domain_group: str, country_code: Optional[str] = None, vendor: str = None):
        super().__init__(
            domain="asset",
            domain_group=domain_group,
            trd_dt=trd_dt,
            vendor=vendor,
            country_code=country_code
        )
        self.country_code = country_code
        self.exchanges: List[str] = []

    # ============================================================
    # 📘 0️⃣ 유틸: 이벤트 로깅 / 이전 스냅샷 로드 / 최신 스냅샷 탐색
    # ============================================================
    def _log_event(self, event_type: str, data: Dict[str, Any]):
        """
        이벤트를 JSONL로 기록하여 대시보드에서 모니터링 가능하게 함
        """
        event_dir = DATA_WAREHOUSE_ROOT / "_event_logs"
        event_dir.mkdir(parents=True, exist_ok=True)
        log_file = event_dir / "_warehouse_events.jsonl"

        record = {
            "event_type": event_type,
            "domain": self.domain,
            "country": self.country_code,
            "snapshot_dt": self.trd_dt,
            "timestamp": datetime.utcnow().isoformat(),
            **data,
        }
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        self.log.info(f"🧩 Event logged: {event_type} | {data.get('count', '-') } items")


    # ============================================================
    # 📘 1️⃣ 거래소 매핑 로드
    # ============================================================
    def _load_exchange_codes(self) -> List[str]:
        if not self.country_code:
            raise ValueError("❌ country_code 값이 누락되었습니다. (예: 'KOR', 'USA')")
        try:
            df = load_exchange_list(domain_group=self.domain_group, vendor=self.vendor, trd_dt=self.trd_dt)
            # 원본 컬럼명 보정 (loader 결과가 케이스/컬럼 다를 수 있어 normalize)
            dfn = normalize_columns(df)
            # 일반화된 컬럼에서 Code / CountryISO3 찾기
            code_col = next((c for c in ("code", "exchange_code") if c in dfn.columns), None)
            ctry_col = next((c for c in ("countryiso3", "country_code") if c in dfn.columns), None)
            if not code_col or not ctry_col:
                raise ValueError("❌ exchange_list에서 code/countryiso3 컬럼을 찾지 못했습니다.")
            exchanges = dfn.loc[dfn[ctry_col].astype(str).str.upper() == self.country_code.upper(), code_col] \
                           .dropna().astype(str).str.upper().unique().tolist()
            if not exchanges:
                raise ValueError(f"❌ {self.country_code} 국가에 대한 거래소 정보가 없습니다.")
            self.log.info(f"🌍 {self.country_code} 거래소 코드: {exchanges}")
            return exchanges
        except Exception as e:
            raise RuntimeError(f"⚠️ 거래소 매핑 로드 실패: {e}")

    # ============================================================
    # 📘 2️⃣ Symbol 리스트 정규화
    # ============================================================
    def _normalize_symbol_list(self, df: pd.DataFrame) -> pd.DataFrame:
        df = normalize_columns(df)
        df_norm = pd.DataFrame({
            "ticker": df.get("code", df.get("symbol", "")).astype(str).str.upper(),
            "name": df.get("name", ""),
            "security_type": df.get("type", ""),
            "exchange_code": df.get("exchange", df.get("exchange_code", "")),
            "currency_code": df.get("currency", ""),
            "country_code": df.get("countryiso2", self.country_code),
        }).drop_duplicates(subset=["ticker", "exchange_code"])

        # ✅ 거래소 필터링
        exclude_exchanges = EXCLUDED_EXCHANGES_BY_COUNTRY.get(self.country_code, [])
        if exclude_exchanges:
            before = len(df_norm)
            df_norm = df_norm[~df_norm["exchange_code"].isin(exclude_exchanges)]
            after = len(df_norm)
            self.log.info(f"🚫 Excluded {before - after:,} symbols where exchange_code in {exclude_exchanges}")

        print('df_norm.columns:: ', df_norm.columns)
        print('df_norm :: ', df_norm.head(5))

        return df_norm

    # ============================================================
    # 📘 3️⃣ Fundamentals 정규화
    # ============================================================
    def _normalize_fundamentals(self, df: pd.DataFrame) -> pd.DataFrame:
        return normalize_columns(df)

    # ============================================================
    # 📘 4️⃣ Exchange Detail 정규화
    # ============================================================
    def _normalize_exchange_detail(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            self.log.warning("⚠️ Empty exchange_list received for normalization.")
            return df
        df = normalize_columns(df)
        rename_map = {
            "code": "exchange_code",
            "name": "exchange_name",
            "country": "country_code",
            "currency": "currency_code",
            "timezone": "time_zone",
            "operatingmic": "operating_mic",
        }
        valid_cols = [c for c in rename_map if c in df.columns]
        if not valid_cols:
            return pd.DataFrame()
        df_out = df[valid_cols].rename(columns={k: rename_map[k] for k in valid_cols})
        return df_out.drop_duplicates(subset=["exchange_code"]).reset_index(drop=True)

    # ============================================================
    # 📘 5️⃣ 병합 및 변환 로직 (security_id/이벤트 포함)
    # ============================================================
    def _assign_security_id_and_events(
            self,
            merged: pd.DataFrame,
            prev_master: Optional[pd.DataFrame],
            symbol_changes_df: pd.DataFrame,
    ) -> pd.DataFrame:

        df = merged.copy()
        df["ticker"] = df["ticker"].astype(str).str.upper()

        # ============================================================
        # 1) symbol_change 정규화 (ticker + name 동시 매핑)
        # ============================================================
        change_map = {}  # old → new ticker
        name_change_map = {}  # old → new name

        if symbol_changes_df is not None and not symbol_changes_df.empty:
            ch = normalize_columns(symbol_changes_df)
            ch["old_symbol"] = ch["old_symbol"].astype(str).str.upper()
            ch["new_symbol"] = ch["new_symbol"].astype(str).str.upper()

            change_map = dict(zip(ch["old_symbol"], ch["new_symbol"]))
            name_change_map = dict(zip(ch["old_symbol"], ch["company_name"]))  # name 변경

        self.log.info(f"🔄 symbol_change map = {change_map}")
        self.log.info(f"🔄 name_change_map = {name_change_map}")

        # ============================================================
        # 2) prev_master mapping: ticker → security_id
        # ============================================================
        prev_map = {}
        if prev_master is not None and not prev_master.empty:
            prev = normalize_columns(prev_master)
            prev["ticker"] = prev["ticker"].astype(str).str.upper()
            prev_map = prev.set_index("ticker")["security_id"].to_dict()

        # ============================================================
        # 3) apply ticker rename (old → new)
        # ============================================================
        df["old_ticker"] = df["ticker"]
        df["new_ticker"] = df["old_ticker"].map(change_map)

        # ticker rename 적용
        df.loc[df["new_ticker"].notna(), "ticker"] = df["new_ticker"]

        # name 변경까지 반영 (company_name 사용)
        df.loc[df["new_ticker"].notna(), "name"] = (
            df["old_ticker"].map(name_change_map)
        )

        # 변경 여부 저장 (event 용)
        df["prev_ticker_matched"] = df["old_ticker"].where(df["new_ticker"].notna(), pd.NA)

        # ============================================================
        # 4) security_id 승계
        # ============================================================
        df["security_id"] = pd.NA

        # ledger 불러오기
        id_map = _load_id_map()

        for idx, row in df.iterrows():

            old_ticker = row["old_ticker"]
            new_ticker = row["ticker"]

            # ----- (1) prev_master에서 old_ticker 승계 -----
            if old_ticker in prev_map:
                sec_id = prev_map[old_ticker]
                df.at[idx, "security_id"] = sec_id
                id_map[new_ticker] = sec_id  # ledger에 new 추가
                continue

            # ----- (2) ledger에서 old_ticker 승계 -----
            if old_ticker in id_map:
                sec_id = id_map[old_ticker]
                df.at[idx, "security_id"] = sec_id
                id_map[new_ticker] = sec_id
                continue

            # ----- (3) ledger에서 new_ticker 직접 매칭 -----
            if new_ticker in id_map:
                df.at[idx, "security_id"] = id_map[new_ticker]
                continue

            # ----- (4) 신규 생성 -----
            new_id = generate_or_reuse_entity_id(
                prefix="AST",
                country=row["country_code"],
                ticker=new_ticker,
                exchange=row['exchange_code']
            )
            df.at[idx, "security_id"] = new_id
            id_map[new_ticker] = new_id

        # ============================================================
        # 5) SYMBOL_CHANGE 이벤트 기록
        # ============================================================
        changed = df.loc[df["prev_ticker_matched"].notna(), ["prev_ticker_matched", "ticker"]]

        if not changed.empty:
            self._log_event("SYMBOL_CHANGE", {
                "count": len(changed),
                "changes": changed.to_dict(orient="records"),
            })

        # ledger 저장
        _save_id_map(id_map)

        return df.drop(columns=["old_ticker", "new_ticker"], errors="ignore")

    def _get_prev_business_day(self) -> str:
        dt = pd.to_datetime(self.trd_dt)
        dt -= timedelta(days=1)
        while dt.weekday() >= 5:  # 5=토, 6=일
            dt -= timedelta(days=1)
        return dt.strftime("%Y-%m-%d")

    def _transform_business_logic(self, symbol_list, fundamentals=None, exchange_list=None, symbol_changes_df=None) -> pd.DataFrame:
        """

        :param symbol_list:
        :param fundamentals:
        :param exchange_list:
        :return:
        """

        merged = symbol_list.copy()

        if fundamentals is not None and not fundamentals.empty:
            merged = safe_merge(merged, fundamentals, on=["ticker", "exchange_code"], how="left")

        if exchange_list is not None and not exchange_list.empty:
            merged = safe_merge(merged, exchange_list, on=["exchange_code"], how="left")

        merged = merged.drop_duplicates(subset=["ticker", "exchange_code"])

        prev_dt = self._get_prev_business_day()

        # ✅ 이전 스냅샷 기반 security_id 재사용 + 이벤트 기록
        prev_master = load_asset_master(
            domain_group=self.domain_group,
            country_code=self.country_code,
            trd_dt=prev_dt
        )

        assert not prev_master.empty, f"⚠ prev_master({prev_dt}) snapshot not found or empty"

        merged = self._assign_security_id_and_events(merged, prev_master, symbol_changes_df)

        return merged

    # ============================================================
    # 📘 6️⃣ 데이터 로더
    # ============================================================
    def _load_source_datasets(self, exchanges: list) -> dict[str, pd.DataFrame]:
        exclude_values = EXCLUDED_EXCHANGES_BY_COUNTRY.get(self.country_code, [])
        symbol_df = load_symbol_list(
            domain_group=self.domain_group,
            vendor=self.vendor,
            exchange_codes=exchanges,
            trd_dt=self.trd_dt,
            exclude_field="exchange_code",
            exclude_values=exclude_values,
        )

        fundamentals_df = load_fundamentals_latest(
            domain_group=self.domain_group,
            vendor=self.vendor,
            exchange_codes=exchanges,
        )

        symbol_changes_df = load_symbol_change_list(
            domain_group=self.domain_group,
            vendor=self.vendor,
            exchange_code=exchanges[0],
            trd_dt=self.trd_dt
        )

        exchange_frames = []
        for exchange_code in exchanges:
            df = load_exchange_detail_list(
                domain_group=self.domain_group,
                vendor=self.vendor,
                trd_dt=self.trd_dt,
                exchange_code=exchange_code,
            )
            if df is not None and not df.empty:
                cols = ["Name", "Code", "OperatingMIC", "Country", "Currency", "Timezone"]
                df = df[[c for c in cols if c in df.columns]].copy()
                df["exchange_code"] = exchange_code
                exchange_frames.append(df)

        exchange_df = pd.concat(exchange_frames, ignore_index=True) if exchange_frames else pd.DataFrame()
        return {"symbol_list": symbol_df, "fundamentals": fundamentals_df, "exchange_list": exchange_df, "symbol_changes_list": symbol_changes_df}

    # ============================================================
    # 📘 7️⃣ 전체 빌드 실행
    # ============================================================
    def build(self, **kwargs) -> Dict[str, Any]:

        with DuckDBManager(self.domain) as conn:

            self.log.info(f"🏗️ Building AssetMasterPipeline | trd_dt={self.trd_dt}, country={self.country_code}")
            self.exchanges = self._load_exchange_codes()

            sources = self._load_source_datasets(self.exchanges)
            symbol_df = self._normalize_symbol_list(sources["symbol_list"])
            fundamental_df = self._normalize_fundamentals(sources["fundamentals"])
            exchange_df = self._normalize_exchange_detail(sources["exchange_list"])

            if symbol_df.empty:
                raise FileNotFoundError("❌ symbol_list 데이터가 없습니다.")

            # ✅ 병합 (DuckDB 없이 pandas 병합으로도 충분하나, 기존 형태 유지 시 아래처럼 DuckDB 사용 가능)
            conn.register("symbol_df", symbol_df)
            conn.register("fundamental_df", fundamental_df)
            conn.register("exchange_df", exchange_df)

            query = f"""
            WITH merged AS (
              SELECT 
                upper(s.ticker) AS ticker,
                s.name,
                s.exchange_code,
                s.security_type,
                COALESCE(s.country_code, '{self.country_code}') AS country_code,
                s.currency_code,
    
                -- fundamentals 필드 확장
                f.isin,
                f.cusip,
                f.lei,
                f.open_figi,
                f.cik,
                f.fiscal_year_end,
                f.primary_ticker,
                f.logo_url,
                f.last_fundamental_update,
                f.sector,
                f.industry,
                f.gic_sector,
                f.gic_group,
                f.gic_industry,
                f.gic_sub_industry,
                f.ipo_date,
                f.is_delisted,
    
                -- exchange info
                e.exchange_name,
    
                -- 메타정보
                now() AT TIME ZONE 'UTC' AS ingested_at,
                '{VENDORS.get("eodhd", "eodhd")}' AS source_vendor,
                '{self.trd_dt}'::DATE AS snapshot_date
              FROM symbol_df s
              LEFT JOIN fundamental_df f ON s.ticker = f.ticker
              LEFT JOIN exchange_df e ON s.exchange_code = e.exchange_code
            )
            SELECT * FROM merged;
            """

            self.log.info("🧩 Executing DuckDB join query ...")
            merged_df = conn.execute(query).df().replace("None", pd.NA)

            # ✅ 비즈니스 로직(+ security_id 및 이벤트) 적용
            final_df = self._transform_business_logic(
                symbol_list=merged_df,
                fundamentals=None,
                exchange_list=None,
                symbol_changes_df=sources["symbol_changes_list"]
            )

            reorder_df = self._reorder_columns(df=final_df)

            # ✅ 저장
            self.save_parquet(reorder_df)
            self.log.info(f"✅ asset_master build complete: {len(reorder_df):,} rows")

            meta = self.save_metadata(
                row_count=len(reorder_df),
                source_datasets=["symbol_list", "fundamentals", "exchange_list"],
                metrics={
                    "symbol_count": len(reorder_df),
                    "vendor": self.vendor,
                    "exchanges": self.exchanges
                },
                context=kwargs.get("context"),
            )

            gc.collect()
            return meta
