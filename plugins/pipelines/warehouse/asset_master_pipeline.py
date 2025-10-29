# plugins/pipelines/warehouse/asset_master_pipeline.py
import json
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional

from plugins.config.constants import (
    DATA_DOMAINS, LAYERS, VENDORS,
    MASTER_DOMAIN, ENTITY_PREFIX,
    DATA_WAREHOUSE_ROOT
)
from plugins.utils.partition_finder import latest_trd_dt_path, parquet_file_in
from plugins.utils.id_generator import generate_entity_id
from plugins.utils.snapshot_registry import get_latest_snapshot_meta, update_global_snapshot_registry


class AssetMasterWarehousePipeline:
    """
    ✅ 국가 단위 자산 마스터 스냅샷 생성 파이프라인
      - 입력: Data Lake 'validated' (symbol_list, fundamentals), Warehouse 'exchange_master'
      - 출력: data_warehouse/asset_master/country_code=XX/snapshot_dt=YYYY-MM-DD/asset_master.parquet

    📌 거래소 매핑은 exchange_master의 최신 스냅샷 메타(또는 parquet)에서 가져온다.
       (더 이상 exchange_mapper.py 사용하지 않음)
    """

    def __init__(self, snapshot_dt: str, country_code: str):
        self.snapshot_dt = snapshot_dt  # 'YYYY-MM-DD'
        self.country_code = country_code.upper()

        # ✅ 최신 exchange_master에서 국가-거래소 매핑 로드 & exchanges 설정
        country_exchange_map = self._load_latest_exchange_master()
        if self.country_code not in country_exchange_map:
            raise ValueError(f"❌ 최신 exchange_master에서 {self.country_code} 국가를 찾을 수 없습니다.")
        self.exchanges: List[str] = country_exchange_map[self.country_code]

        # ✅ 벤더 우선순위 (필요 시 향후 국가별 세분화 가능)
        self.vendor_priority: List[str] = [VENDORS["EODHD"]]

        # ✅ 출력 경로
        self.output_dir = (
            DATA_WAREHOUSE_ROOT / MASTER_DOMAIN / f"country_code={self.country_code}" / f"snapshot_dt={self.snapshot_dt}"
        )
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / f"{MASTER_DOMAIN}.parquet"
        self.meta_file = self.output_dir / "_build_meta.json"

    # -------------------------
    # 내부 헬퍼
    # -------------------------
    def _choose_best_partition(self, domain: str, exchange_code: str) -> Optional[Path]:
        """벤더 우선순위에 따라 최신 validated 파티션 선택"""
        for vendor in self.vendor_priority:
            pdir = latest_trd_dt_path(domain, exchange_code, vendor, layer=LAYERS["VALIDATED"])
            if pdir:
                return pdir
        return None

    def _load_latest_exchange_master(self) -> Dict[str, List[str]]:
        """
        ✅ 전역 레지스트리에서 최신 exchange_master 메타파일을 찾아
           국가 → [거래소코드] 매핑을 반환한다.
        1) meta의 country_exchange_map 사용
        2) 없으면 exchange_master.parquet을 읽어 groupby로 생성 (fallback)
        """
        meta_info = get_latest_snapshot_meta("exchange_master")
        if not meta_info:
            raise FileNotFoundError("❌ latest_snapshot_meta.json에서 exchange_master 항목을 찾지 못했습니다.")

        meta_path = Path(meta_info.get("meta_file", ""))
        if not meta_path.exists():
            raise FileNotFoundError(f"❌ exchange_master 메타파일을 찾을 수 없습니다: {meta_path}")

        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)

        # 1) 메타에 이미 매핑이 있으면 그대로 사용
        mapping = meta.get("country_exchange_map")
        if isinstance(mapping, dict) and mapping:
            # 값이 list가 아닐 경우 보정
            cleaned = {k.upper(): sorted({str(x).upper() for x in v}) for k, v in mapping.items() if v}
            return cleaned

        # 2) fallback: parquet에서 재구성
        pq_path = meta_path.parent / "exchange_master.parquet"
        if not pq_path.exists():
            raise FileNotFoundError(
                f"❌ exchange_master parquet이 없어 매핑을 구성할 수 없습니다: {pq_path}"
            )

        conn = duckdb.connect(database=":memory:")
        df = conn.execute(f"SELECT country_code, exchange_code FROM read_parquet('{pq_path.as_posix()}')").df()
        conn.close()

        if df.empty:
            raise ValueError("❌ exchange_master parquet이 비어 있어 매핑을 구성할 수 없습니다.")

        df.columns = df.columns.str.lower()
        if not {"country_code", "exchange_code"}.issubset(df.columns):
            raise ValueError("❌ exchange_master parquet에 필요한 컬럼(country_code, exchange_code)이 없습니다.")

        df["country_code"] = df["country_code"].astype(str).str.strip().str.upper()
        df["exchange_code"] = df["exchange_code"].astype(str).str.strip().str.upper()

        mapping = (
            df.groupby("country_code")["exchange_code"]
              .apply(lambda s: sorted(set(s.dropna().astype(str))))
              .to_dict()
        )
        return mapping

    @staticmethod
    def _normalize_symbol_df(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = df.columns.str.lower()

        ticker_col = "ticker" if "ticker" in df.columns else ("code" if "code" in df.columns else ("symbol" if "symbol" in df.columns else None))
        if not ticker_col:
            raise ValueError("심볼 데이터에 ticker/code/symbol 컬럼이 없습니다.")

        name_col = next((c for c in ("name", "companyname", "securityname", "issuer_name", "기업명") if c in df.columns), None)
        ex_col = next((c for c in ("exchange", "exchange_code", "market", "거래소", "시장구분") if c in df.columns), None)

        out = pd.DataFrame({
            "ticker": df[ticker_col].astype(str).str.strip(),
            "name": df[name_col].astype(str).str.strip() if name_col else None,
            "exchange_code": df[ex_col].astype(str).str.strip() if ex_col else None,
        })
        return out.drop_duplicates(subset=["ticker"], keep="first").reset_index(drop=True)

    @staticmethod
    def _normalize_fund_df(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = df.columns.str.lower()
        ticker_col = "code" if "code" in df.columns else ("ticker" if "ticker" in df.columns else None)
        if not ticker_col:
            raise ValueError("펀더멘털 데이터에 code/ticker 컬럼이 없습니다.")

        pick = {
            "ticker": df[ticker_col].astype(str).str.strip(),
            "type": df.get("type"),
            "sector": df.get("sector"),
            "industry": df.get("industry"),
            "currency_code": df.get("currencycode", df.get("currency")),
            "currency_name": df.get("currencyname"),
            "currency_symbol": df.get("currencysymbol"),
            "country_name": df.get("countryname"),
            "country_iso": df.get("countryiso"),
            "isin": df.get("isin"),
            "cik": df.get("cik"),
            "cusip": df.get("cusip"),
            "lei": df.get("lei"),
            "openfigi": df.get("openfigi"),
            "ipo_date": df.get("ipodate"),
            "gic_sector": df.get("gicsector"),
            "gic_group": df.get("gicgroup"),
            "gic_industry": df.get("gicindustry"),
            "gic_subindustry": df.get("gicsubindustry"),
            "is_delisted": df.get("isdelisted"),
            "primary_ticker": df.get("primaryticker"),
            "home_category": df.get("homecategory"),
            "fiscal_year_end": df.get("fiscalyearend"),
        }
        out = pd.DataFrame(pick)
        return out.drop_duplicates(subset=["ticker"], keep="first").reset_index(drop=True)

    @staticmethod
    def _normalize_exchange_df(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = df.columns.str.lower()
        code_col = next((c for c in ("code", "exchange_code", "mic", "operatingmic") if c in df.columns), None)
        if not code_col:
            raise ValueError("거래소 데이터에 code/exchange_code 컬럼이 없습니다.")

        out = pd.DataFrame({
            "exchange_code": df[code_col].astype(str).str.strip(),
            "exchange_name": df.get("name"),
            "country": df.get("country"),
            "currency": df.get("currency"),
            "country_iso2": df.get("countryiso2"),
            "country_iso3": df.get("countryiso3"),
        })
        return out.drop_duplicates(subset=["exchange_code"], keep="first").reset_index(drop=True)

    # -------------------------
    # 메인 로직
    # -------------------------
    def build(self, **kwargs) -> Dict:
        """
        1) (국가 내 여러 거래소에 대해) vendor 우선순위로 최신 symbol/fund 파티션 선택
        2) 읽기 → 정규화 → 병합
        3) entity_id 발급
        4) 저장 + 메타 + 전역 레지스트리 갱신
        """
        sym_dfs, fund_dfs, ex_dfs = [], [], []
        conn = duckdb.connect(database=":memory:")

        # --- 거래소별 파티션 선택/로딩
        for ex in self.exchanges:
            # symbol_list
            sp = self._choose_best_partition(DATA_DOMAINS["SYMBOL"], ex)
            if sp:
                sym_pq = parquet_file_in(sp, DATA_DOMAINS["SYMBOL"])
                if sym_pq:
                    df = conn.execute(f"SELECT * FROM read_parquet('{sym_pq.as_posix()}')").df()
                    df["__exchange_chosen__"] = ex
                    sym_dfs.append(df)

            # fundamentals
            fp = self._choose_best_partition(DATA_DOMAINS["FUNDAMENTALS"], ex)
            if fp:
                fund_pq = parquet_file_in(fp, DATA_DOMAINS["FUNDAMENTALS"])
                if fund_pq:
                    df = conn.execute(f"SELECT * FROM read_parquet('{fund_pq.as_posix()}')").df()
                    df["__exchange_chosen__"] = ex
                    fund_dfs.append(df)

            # exchange_list (옵션)
            ep = self._choose_best_partition(DATA_DOMAINS["EXCHANGE_LIST"], ex)
            if ep:
                ex_pq = parquet_file_in(ep, DATA_DOMAINS["EXCHANGE_LIST"])
                if ex_pq:
                    df = conn.execute(f"SELECT * FROM read_parquet('{ex_pq.as_posix()}')").df()
                    df["__exchange_chosen__"] = ex
                    ex_dfs.append(df)

        conn.close()

        if not sym_dfs:
            raise RuntimeError(f"[{self.country_code}] 심볼 데이터가 없습니다. 벤더/검증 여부 확인 필요.")
        symbols_raw = pd.concat(sym_dfs, ignore_index=True)
        symbols = self._normalize_symbol_df(symbols_raw)

        funds = None
        if fund_dfs:
            funds_raw = pd.concat(fund_dfs, ignore_index=True)
            funds = self._normalize_fund_df(funds_raw)

        exchanges_df = None
        if ex_dfs:
            exchanges_raw = pd.concat(ex_dfs, ignore_index=True)
            exchanges_df = self._normalize_exchange_df(exchanges_raw)

        # --- 병합 (심볼 기준 LEFT)
        master = symbols
        if funds is not None:
            master = master.merge(funds, on="ticker", how="left", suffixes=("", "_fund"))

        if exchanges_df is not None:
            master = master.merge(
                exchanges_df[["exchange_code", "exchange_name", "country", "currency", "country_iso2", "country_iso3"]],
                on="exchange_code",
                how="left"
            )

        # --- 국가 코드 & entity_id
        master["country_code"] = self.country_code
        master["entity_id"] = master.apply(
            lambda r: generate_entity_id(
                ENTITY_PREFIX["EQUITY"],
                country=self.country_code,
                exchange=r.get("exchange_code") or "",
                ticker=r.get("ticker") or "",
            ),
            axis=1
        )

        # --- 칼럼 순서 정리
        preferred_cols = [
            "entity_id", "ticker", "name", "exchange_code", "country_code",
            "type", "sector", "industry",
            "currency_code", "currency_name", "currency_symbol",
            "country_name", "country_iso",
            "isin", "cusip", "cik", "lei", "openfigi",
            "ipo_date",
            "gic_sector", "gic_group", "gic_industry", "gic_subindustry",
            "is_delisted", "primary_ticker", "home_category", "fiscal_year_end",
            "exchange_name", "country", "currency", "country_iso2", "country_iso3",
        ]
        for c in preferred_cols:
            if c not in master.columns:
                master[c] = None
        master = master[preferred_cols]

        # --- 저장
        import pyarrow as pa
        import pyarrow.parquet as pq
        table = pa.Table.from_pandas(master)
        pq.write_table(table, self.output_file.as_posix())

        # --- 메타 저장
        meta = {
            "dataset": MASTER_DOMAIN,
            "country_code": self.country_code,
            "snapshot_dt": self.snapshot_dt,
            "exchanges": self.exchanges,
            "vendor_priority": self.vendor_priority,
            "row_count": len(master),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "sources": {
                "symbol_list": True,
                "fundamentals": bool(fund_dfs),
                "exchange_list": bool(ex_dfs),
            },
            "output_file": self.output_file.as_posix(),
        }
        with open(self.meta_file, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)

        # --- 전역 스냅샷 레지스트리 갱신 (asset_master[KOR] / [USA] ...)
        update_global_snapshot_registry(
            domain="asset_master",
            snapshot_dt=self.snapshot_dt,
            meta_file=self.meta_file,
            country_code=self.country_code,
        )

        return meta
