# plugins/utils/symbol_loader.py
import pandas as pd
from plugins.config.constants import DATA_LAKE_ROOT
import logging

log = logging.getLogger(__name__)

def load_symbols_from_datalake_pd(
    exchange_code: str,
    trd_dt: str,
    exclude_markets=None,
    filter_dict=None,
    vendor=None,
) -> pd.DataFrame:
    """
    pandas 기반 심볼 로더 (빠르고 간결)
    """
    base_path = (
        DATA_LAKE_ROOT
        / "raw"
        / "symbol_list"
        / f"vendor={vendor}"
        / f"exchange_code={exchange_code}"
        / f"trd_dt={trd_dt}"
        / "symbol_list.jsonl"
    )

    assert base_path.exists(), f"⚠️ 파일이 존재하지 않습니다: {base_path}"

    exclude_markets = exclude_markets or ['OTCQB', 'PINK', 'OTCQX', 'OTCMKTS', 'NMFQS', 'NYSE MKT','OTCBB', 'OTCGREY', 'OTC']

    # ✅ JSONL → DataFrame 로드
    df = pd.read_json(base_path, lines=True)

    log.info(f"{df.shape[0]} 행의 종목정보를 읽었습니다.")

    # ✅ OTC / 비상장 제외
    for col in ["Exchange"]:
        if col in df.columns:
            df = df[~df[col].astype(str).str.upper().isin(exclude_markets)]

    log.info(f"Exchange 필터를 거져 {df.shape[0]} 행의 종목정보가 남았습니다.")

    # ✅ filter_dict 조건 적용
    if filter_dict:
        for key, allowed_values in filter_dict.items():
            if key in df.columns:
                df = df[df[key].astype(str).isin(allowed_values)]

    log.info(f"filter_dict 조건을 거져 {df.shape[0]} 행의 종목정보가 남았습니다.")


    assert df.empty is False, f"⚠️ 데이터프레임이 비어있습니다."


    # ✅ symbol 컬럼 탐색
    for key in ["Code", "symbol", "Ticker", "ticker"]:
        if key in df.columns:
            symbols = df[key].dropna().astype(str).unique().tolist()
            log.info(f"✅ {exchange_code} 거래소 {len(symbols):,}건 로드 완료")

    return df