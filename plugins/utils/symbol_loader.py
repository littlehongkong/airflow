# plugins/utils/symbol_loader.py
import pandas as pd
from plugins.config.constants import DATA_LAKE_ROOT
import logging

log = logging.getLogger(__name__)

def load_symbols_from_datalake_pd(
    exchange_code: str,
    trd_dt: str,
    domain_group: str = "equity",
    exclude_markets=None,
    filter_dict=None,
    vendor=None,
) -> pd.DataFrame:
    """
    🧭 pandas 기반 심볼 로더 (validated 레이어 + parquet 지원)
    ------------------------------------------------------------
    - validated/equity/symbol_list/vendor=eodhd/exchange_code=US/trd_dt=YYYY-MM-DD/symbol_list.parquet
    """

    if vendor is None:
        raise ValueError("❌ vendor 값이 필요합니다 (예: vendor='eodhd')")

    # ✅ 파일 경로 구성
    base_path = (
        DATA_LAKE_ROOT
        / "validated"
        / domain_group
        / "symbol_list"
        / f"vendor={vendor}"
        / f"exchange_code={exchange_code}"
        / f"trd_dt={trd_dt}"
        / "symbol_list.parquet"
    )

    # ✅ 파일 존재 여부 확인
    if not base_path.exists():
        raise FileNotFoundError(f"⚠️ 파일이 존재하지 않습니다: {base_path}")

    # ✅ Parquet → DataFrame 로드
    df = pd.read_parquet(base_path)
    log.info(f"📦 {exchange_code} 거래소 {len(df):,}행의 종목정보 로드 완료")

    # ✅ OTC / 비상장 시장 제외
    exclude_markets = exclude_markets or [
        "OTCQB", "PINK", "OTCQX", "OTCMKTS", "NMFQS",
        "NYSE MKT", "OTCBB", "OTCGREY", "OTC"
    ]

    if "Exchange" in df.columns:
        before = len(df)
        df = df[~df["Exchange"].astype(str).str.upper().isin(exclude_markets)]
        log.info(f"🏛️ Exchange 필터 적용: {before:,} → {len(df):,} 행")

    # ✅ 추가 조건 필터 (예: 국가 코드, 섹터 등)
    if filter_dict:
        for key, allowed_values in filter_dict.items():
            if key in df.columns:
                before = len(df)
                df = df[df[key].astype(str).isin(allowed_values)]
                log.info(f"🔍 {key} 필터 적용: {before:,} → {len(df):,} 행")

    assert not df.empty, f"⚠️ 데이터프레임이 비어있습니다 ({base_path})"

    # ✅ 심볼 컬럼 탐색
    for key in ["Code", "symbol", "Ticker", "ticker"]:
        if key in df.columns:
            symbols = df[key].dropna().astype(str).unique().tolist()
            log.info(f"✅ {exchange_code} 거래소 {len(symbols):,}건의 심볼 로드 완료")
            break
    else:
        raise KeyError("❌ 심볼 컬럼(Code/symbol/Ticker)이 존재하지 않습니다.")


    # todo 테스트용도 향후 df로 교체필요.
    filter_df = df.sample(n=10)

    return filter_df
