import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import psutil, gc
from plugins.pipelines.warehouse.base_warehouse_pipeline import BaseWarehousePipeline
from plugins.config.constants import (
    WAREHOUSE_DOMAINS,
    EXCLUDED_EXCHANGES_BY_COUNTRY,
    VENDORS, DATA_WAREHOUSE_ROOT
)
from plugins.utils.loaders.exchange_holiday_loader import load_exchange_holiday_list
from plugins.utils.transform_utils import normalize_columns, safe_merge
from plugins.utils.id_generator import generate_or_reuse_entity_id

from plugins.utils.loaders.symbol_loader import load_symbol_list
from plugins.utils.loaders.fundamentals_loader import load_fundamentals_latest
from plugins.utils.loaders.exchange_loader import load_exchange_list


class AssetMasterPipeline(BaseWarehousePipeline):
    """
    ✅ 자산 마스터 파이프라인 (Warehouse Domain: asset_master)
    --------------------------------------------------------
    - Symbol / Fundamentals / Exchange 데이터를 병합하여 국가 단위 마스터 생성
    - 고유 ID 생성은 id_generator 유틸 사용
    - Exchange 매핑은 latest_snapshot_meta.json 기반 동적 로드
    """

    def __init__(
        self,
        trd_dt: str,
        domain_group: str,
        country_code: Optional[str] = None,
        vendor: str = None,
    ):
        super().__init__(
            domain=WAREHOUSE_DOMAINS["asset"],
            domain_group=domain_group,
            trd_dt=trd_dt,
            vendor=vendor,
        )
        self.country_code = country_code
        self.exchanges = []

    # ============================================================
    # 📘 1️⃣ 거래소 매핑 로드 (latest_snapshot_meta 기반)
    # ============================================================
    def _load_exchange_codes(self) -> List[str]:
        """latest_snapshot_meta.json에서 해당 국가의 거래소 목록 로드"""
        if not self.country_code:
            raise ValueError("❌ country_code 값이 누락되었습니다. (예: 'KOR', 'USA')")
        try:
            df = load_exchange_list(domain_group=self.domain_group, vendor=self.vendor, trd_dt=self.trd_dt)

            exchanges = df.loc[
                df["CountryISO3"].astype(str).str.upper() == self.country_code.upper(),
                "Code"
            ].dropna().unique().tolist()

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
            "security_name": df.get("name", ""),
            "security_type": df.get("type", ""),
            "exchange_code": df.get("exchange", df.get("exchange_code", "")),
            "currency_code": df.get("currency", ""),
            "country_code": df.get("countryiso2", self.country_code)
        }).drop_duplicates(subset=["ticker", "exchange_code"])

        # ✅ 거래소 필터링 (국가별 제외 리스트 반영)
        exclude_exchanges = EXCLUDED_EXCHANGES_BY_COUNTRY.get(self.country_code, [])
        if exclude_exchanges and "exchange_code" in df_norm.columns:
            before_rows = len(df_norm)
            df_norm = df_norm[~df_norm["exchange_code"].isin(exclude_exchanges)]
            df_norm["exchange_code"] = (
                df_norm["exchange_code"]
                .astype(str)
                .str.upper()
                .str.strip()  # 앞뒤 공백 제거
                .str.replace(r"[^A-Z0-9]", "", regex=True)  # 공백, 콜론, 하이픈 제거
            )
            after_rows = len(df_norm)
            self.log.info(
                f"🚫 Excluded {before_rows - after_rows:,} symbols "
                f"where exchange_code in {exclude_exchanges} "
                f"({before_rows:,} → {after_rows:,})"
            )

        return df_norm

    # ============================================================
    # 📘 3️⃣ Fundamentals 정규화
    # ============================================================
    # ============================================================
    # 📘 3️⃣ Fundamentals 정규화 (General-only parquet 기반)
    # ============================================================
    def _normalize_fundamentals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        fundamentals_general_latest.parquet 파일을 기반으로 간소화된 정규화
        """
        df = normalize_columns(df)
        return df

    # ============================================================
    # 📘 4️⃣ Exchange Detail(Holiday) 정규화
    # ============================================================
    def _normalize_exchange_detail(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        ✅ 거래소 상세정보 정규화 (Lake → Warehouse용)
        - 여러 거래소(KO, KQ, etc.) 데이터 포함 가능
        - nested 필드는 제외하고 주요 메타 컬럼만 유지
        """

        assert df.empty is False, "⚠️ Empty exchange_list received for normalization."

        # 1️⃣ 컬럼 이름 통일 (대소문자, 공백 제거 등)
        df = normalize_columns(df)  # 예: 'Code' → 'code', 'OperatingMIC' → 'operatingmic'

        # 2️⃣ 매핑 정의
        rename_map = {
            "code": "exchange_code",
            "name": "exchange_name",
            "country": "country_code",
            "currency": "currency_code",
            "timezone": "time_zone",
            "operatingmic": "operating_mic",
        }

        # 3️⃣ 존재하는 컬럼만 추출
        valid_cols = [col for col in rename_map.keys() if col in df.columns]
        if not valid_cols:
            self.log.warning("⚠️ No valid exchange columns found to normalize.")
            return pd.DataFrame()

        # 4️⃣ 선택된 컬럼 rename
        df_out = df[valid_cols].rename(columns={k: rename_map[k] for k in valid_cols})

        # 5️⃣ 중복 제거 및 정렬
        df_out = df_out.drop_duplicates(subset=["exchange_code"]).reset_index(drop=True)

        return df_out

    # ============================================================
    # 📘 5️⃣ 병합 및 변환 로직
    # ============================================================
    def _transform_business_logic(
        self,
        symbol_list: pd.DataFrame,
        fundamentals: Optional[pd.DataFrame] = None,
        exchange_list: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        merged = symbol_list.copy()

        # ✅ fundamentals 병합
        if fundamentals is not None and not fundamentals.empty:
            merged = safe_merge(merged, fundamentals, on=["ticker", "exchange_code"], how="left")

        # ✅ exchange 정보 병합
        if exchange_list is not None and not exchange_list.empty:
            merged = safe_merge(merged, exchange_list, on=["exchange_code"], how="left")

        merged = merged.drop_duplicates(subset=["ticker", "exchange_code"])


        # ✅ 날짜 메타필드
        merged["trd_dt"] = self.trd_dt
        merged["last_symbol_update"] = self.trd_dt
        merged["ingested_at"] = datetime.now(timezone.utc).isoformat()
        merged["source_vendor"] = VENDORS["EODHD"]

        # ✅ 컬럼 순서 정렬
        preferred_cols = [
            "security_id", "ticker", "security_name", "exchange_code", "country_code",
            "security_type", "isin", "cusip", "lei",
            "sector", "industry", "gic_sector", "gic_group",
            "gic_industry", "gic_sub_industry", "ipo_date", "is_delisted",
            "currency_code", "last_fundamental_update", "last_symbol_update",
            "ingested_at", "source_vendor", "snapshot_date"
        ]
        for col in preferred_cols:
            if col not in merged.columns:
                merged[col] = None
        return merged[preferred_cols]

    import pandas as pd

    def _load_source_datasets(self, exchanges: list) -> dict[str, pd.DataFrame]:
        """
        ✅ Exchange-Holiday 기반 통합 데이터 로더 (불필요한 nested 필드 제거)
        - validated parquet 기준: 이미 평탄화되어 있음
        - 실제 warehouse에서 사용하는 주요 컬럼만 추출
        """
        # 1️⃣ Symbol List
        symbol_df = load_symbol_list(
            domain_group=self.domain_group,
            vendor=self.vendor,
            exchange_codes=exchanges,
            trd_dt=self.trd_dt
        )

        # 2️⃣ Fundamentals (최신 스냅샷)
        fundamentals_df = load_fundamentals_latest(
            domain_group=self.domain_group,
            vendor=self.vendor,
            exchange_codes=exchanges
        )

        # 3️⃣ Exchange 상세정보 (Holiday API 기반)
        exchange_frames = []
        for exchange_code in exchanges:
            df = load_exchange_holiday_list(
                domain_group=self.domain_group,
                vendor=self.vendor,
                trd_dt=self.trd_dt,
                exchange_code=exchange_code
            )

            if df is not None and not df.empty:
                # ✅ 필요한 컬럼만 남기기 (nested 필드는 제외)
                used_cols = [
                    "Name", "Code", "OperatingMIC", "Country", "Currency",
                    "Timezone", "isOpen", "ActiveTickers", "UpdatedTickers"
                ]
                available_cols = [c for c in used_cols if c in df.columns]
                df = df[available_cols].copy()

                # ✅ exchange_code 명시 추가 (통합 시 중복 방지)
                df["exchange_code"] = exchange_code

                exchange_frames.append(df)

        # 4️⃣ 통합 Exchange DF 생성
        exchange_list_df = (
            pd.concat(exchange_frames, ignore_index=True)
            if exchange_frames else pd.DataFrame()
        )

        # ✅ 최종 반환
        return {
            "symbol_list": symbol_df,
            "fundamentals": fundamentals_df,
            "exchange_list": exchange_list_df,
        }

    # ============================================================
    # 📘 6️⃣ 전체 빌드 실행
    # ============================================================
    def build(self, **kwargs) -> Dict[str, Any]:
        self.log.info(f"🏗️ Building AssetMasterPipeline | trd_dt={self.trd_dt}, country={self.country_code}")

        # ✅ 거래소 매핑
        self.exchanges = self._load_exchange_codes()
        conn = self._get_duckdb_connection()

        # ✅ Symbol 및 Exchange 로드
        sources = self._load_source_datasets(exchanges=self.exchanges)
        symbol_df = self._normalize_symbol_list(sources.get("symbol_list"))
        exchange_detail_df = self._normalize_exchange_detail(sources.get("exchange_list"))
        fundamental_df = self._normalize_fundamentals(sources.get("fundamentals"))

        if symbol_df.empty:
            raise FileNotFoundError("❌ symbol_list 데이터가 없습니다.")


        # ✅ DuckDB 병합
        conn.register("symbol_df", symbol_df)
        conn.register("fundamental_df", fundamental_df)
        conn.register("exchange_detail_df", exchange_detail_df)

        # ✅ DuckDB SQL에서 병합 및 ID 생성
        country_fallback = self.country_code or "XX"
        query = f"""
        WITH merged AS (
        SELECT 
            -- ✅ symbol_df 원본
            upper(s.ticker) AS ticker,
            s.security_name,                         -- symbol_list 원본 필드
            s.exchange_code,
            s.security_type,
            COALESCE(s.country_code, '{country_fallback}') AS country_code,
            s.currency_code,
            f.isin,                                          -- symbol_df에도 존재하는 경우
            f.cusip,
            f.lei,
            f.open_figi,
            f.cik,
            f.fiscal_year_end,
            f.primary_ticker,
            f.sector,
            f.industry,
            f.gic_sector,
            f.gic_group,
            f.gic_industry,
            f.gic_sub_industry,
            f.ipo_date,
            f.is_delisted,
            f.logo_url,
    
            -- ✅ fundamentals (최신 스냅샷 관련)
            f.last_fundamental_update,
    
            -- ✅ exchange_list (거래소 정보)
            e.exchange_name AS exchange_name,
    
            -- ✅ 메타정보
            '{self.trd_dt}'::DATE AS last_symbol_update,
            now() AT TIME ZONE 'UTC' AS ingested_at,
            '{VENDORS["eodhd"]}' AS source_vendor,
            '{self.trd_dt}'::DATE AS snapshot_date
    
        FROM symbol_df s
        LEFT JOIN fundamental_df f
            ON s.ticker = f.ticker AND s.exchange_code = f.exchange_code
        LEFT JOIN exchange_detail_df e
            ON s.exchange_code = e.exchange_code
    )
    SELECT
        ticker,
        security_name AS name,
        exchange_code,
        country_code,
        COALESCE(security_type, NULL) AS security_type,   -- 일부 vendor만 제공
        isin,
        cusip,
        lei,
        open_figi,
        cik,
        fiscal_year_end,
        primary_ticker,
        sector,
        industry,
        gic_sector,
        gic_group,
        gic_industry,
        gic_sub_industry,
        ipo_date,
        is_delisted,
        currency_code,
        logo_url,
        last_fundamental_update,
        last_symbol_update,
        ingested_at,
        source_vendor,
        snapshot_date
    FROM merged;

        """

        # ✅ 출력 경로(국가 파티션) 동일 규칙 적용
        output_dir = self.output_file.parent
        if self.country_code:
            output_dir = output_dir / f"country_code={self.country_code}"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = (output_dir / self.output_file.name).as_posix()
        else:
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = self.output_file.as_posix()

        # ✅ DuckDB로 병합 결과 DataFrame만 가져오기
        self.log.info("🧩 Executing DuckDB join query ...")
        merged_df = conn.execute(query).df()

        # ✅ Python에서 기존 ID 유지 + 신규 ID 생성
        merged_df = self._assign_persistent_security_id(merged_df)

        # ✅ 저장
        self.save_parquet(merged_df)

        row_count = len(merged_df)
        file_size = Path(output_file).stat().st_size
        self.log.info(f"✅ Parquet saved: {output_file} ({row_count:,} rows, {file_size:,} bytes)")

        meta = self.save_metadata(
            row_count=row_count,
            source_datasets=["symbol_list", "fundamentals_general_latest", "exchange_list"],
            metrics={
                "symbol_count": row_count,
                "vendor": self.vendor,
                "exchanges": self.exchanges,
            },
            context=kwargs.get("context"),
        )

        self.log.info(f"✅ [BUILD COMPLETE] asset_master | {row_count:,} symbols ({self.country_code})")
        gc.collect()
        return meta


    # ============================================================
    # ♻️ 기존 security_id 유지 + 신규 발급 함수
    # ============================================================
    def _assign_persistent_security_id(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        기존 스냅샷에서 security_id를 불러와 동일 종목에 동일 ID를 재사용하고,
        신규 종목만 새 ID를 생성한다.
        """

        if df.empty:
            self.log.warning("⚠️ No records to assign security_id (empty DataFrame).")
            return df

        existing_map = {}
        latest_snapshot_dir = (
            Path(DATA_WAREHOUSE_ROOT)
            / "snapshot"
            / self.domain_group
            / self.domain
        )

        if latest_snapshot_dir.exists():
            snapshots = sorted(latest_snapshot_dir.glob("trd_dt=*"), reverse=True)
            if snapshots:
                latest_snapshot_path = snapshots[0] / f"{self.domain}.parquet"
                if latest_snapshot_path.exists():
                    try:
                        old_df = pd.read_parquet(latest_snapshot_path)
                        existing_map = (
                            old_df[["ticker", "exchange_code", "security_id"]]
                            .drop_duplicates()
                            .set_index(["ticker", "exchange_code"])["security_id"]
                            .to_dict()
                        )
                        self.log.info(
                            f"♻️ Loaded {len(existing_map):,} existing security_id mappings "
                            f"from {latest_snapshot_path}"
                        )
                    except Exception as e:
                        self.log.warning(f"⚠️ Failed to load previous snapshot for ID mapping: {e}")

        def assign_security_id(row):
            key = (str(row.get("ticker", "")).upper(), str(row.get("exchange_code", "")).upper())
            if key in existing_map:
                return existing_map[key]
            return generate_or_reuse_entity_id(
                prefix="AST",
                country=row.get("country_code", self.country_code),
                exchange=row.get("exchange_code", ""),
                ticker=row.get("ticker", "")
            )

        df["security_id"] = df.apply(assign_security_id, axis=1)
        self.log.info(f"🔑 Assigned deterministic security_id for {len(df):,} records")

        return df
