import json
import pandas as pd
from pathlib import Path
from plugins.config.constants import DATA_WAREHOUSE_ROOT


def load_exchanges_by_country(countries: list[str]) -> dict[str, list[str]]:
    """
    ✅ 국가 단위 → 거래소 코드 매핑 로더
    - 최신 스냅샷 메타파일(latest_snapshot_meta.json)을 참조하여 exchange.parquet 로드
    """

    meta_file = Path(DATA_WAREHOUSE_ROOT) / "latest_snapshot_meta.json"
    if not meta_file.exists():
        raise FileNotFoundError(f"❌ latest_snapshot_meta.json 파일을 찾을 수 없습니다: {meta_file}")

    # ✅ latest_snapshot_meta.json 로드
    with open(meta_file, "r", encoding="utf-8") as f:
        meta = json.load(f)

    # ✅ exchange 도메인의 최신 파일 경로 확인
    exchange_meta = meta.get("exchange", {})
    exchange_path_str = exchange_meta.get("meta_file")
    if not exchange_path_str:
        raise ValueError(f"❌ latest_snapshot_meta.json 내에 exchange.meta_file 정보가 없습니다: {meta}")

    exchange_path = Path(exchange_path_str)
    if not exchange_path.exists():
        raise FileNotFoundError(f"❌ exchange.parquet 파일이 존재하지 않습니다: {exchange_path}")

    # ✅ parquet 로드
    df = pd.read_parquet(exchange_path)

    # ✅ 국가별 거래소 매핑 구성
    result = {}
    for country in countries:
        country_exchanges = (
            df[df["country_code"].astype(str).str.upper() == country.upper()]
            ["exchange_code"].dropna().unique().tolist()
        )
        result[country] = country_exchanges

    return result
