"""
plugins/utils/warehouse_utils.py

📦 Warehouse 공통 유틸
- exchange_list에서 country_code 기준 거래소코드 목록 조회
- 각 거래소별 exchange_detail 데이터 합치기
"""

import pandas as pd
from plugins.utils.loaders.lake.exchange_loader import load_exchange_list
from plugins.utils.loaders.lake.exchange_detail_loader import load_exchange_detail_list


def load_exchange_details_by_country(
    domain_group: str,
    vendor: str,
    trd_dt: str,
    country_code: str,
) -> pd.DataFrame:
    """
    특정 국가(country_code)에 속한 거래소들의 exchange_detail 데이터를 모두 로드 후 병합
    """
    exchange_df = load_exchange_list(domain_group, vendor, trd_dt)
    if exchange_df.empty:
        raise FileNotFoundError(f"❌ exchange_list 데이터가 없습니다. trd_dt={trd_dt}")

    # country_code 기준 거래소코드 추출
    country_col = "CountryISO3"
    code_col = "Code"

    if not country_col or not code_col:
        raise ValueError("❌ exchange_list에 countryiso3/code 컬럼이 없습니다.")


    print(exchange_df.columns)

    exchanges = (
        exchange_df.loc[
            exchange_df[country_col].astype(str).str.upper() == country_code.upper(), code_col
        ]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    if not exchanges:
        raise ValueError(f"⚠️ country_code={country_code}에 해당하는 거래소코드가 없습니다.")

    all_details = []
    for exchange_code in exchanges:
        df = load_exchange_detail_list(
            domain_group=domain_group,
            vendor=vendor,
            trd_dt=trd_dt,
            exchange_code=exchange_code,
        )
        if df is not None and not df.empty:
            df["exchange_code"] = str(exchange_code).upper().strip()
            all_details.append(df)

    if not all_details:
        return pd.DataFrame()

    combined = pd.concat(all_details, ignore_index=True)
    return combined
