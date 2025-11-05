"""
plugins/utils/transform_utils.py

공통 변환 유틸리티 모듈
- 컬럼명 표준화
- 안전 병합(safe merge)
- 결측 컬럼 자동 추가
- 타입 변환 등
"""

import pandas as pd
from typing import Dict, List


# ============================================================
# ✅ 컬럼 정규화
# ============================================================
def normalize_columns(
    df: pd.DataFrame,
    rename_map: Dict[str, str] | None = None,
    lower: bool = True
) -> pd.DataFrame:
    """
    컬럼명을 소문자화하고 rename_map 기반으로 통일
    """
    df = df.copy()
    if lower:
        df.columns = df.columns.str.lower()

    if rename_map:
        df = df.rename(columns=rename_map)
    return df


# ============================================================
# ✅ 안전 병합 (존재 컬럼만 사용)
# ============================================================
def safe_merge(df1, df2, **kwargs):
    """
    ✅ Pandas merge 안전 버전 (None, Empty 자동 처리)
    사용 예시: safe_merge(df1, df2, on="Code", how="left")
    """
    if df1 is None or df1.empty:
        return df2 if df2 is not None else pd.DataFrame()
    if df2 is None or df2.empty:
        return df1

    try:
        return df1.merge(df2, **kwargs)
    except Exception as e:
        print(f"⚠️ safe_merge failed ({e}), returning left dataframe only.")
        return df1


# ============================================================
# ✅ 결측 컬럼 자동 추가
# ============================================================
def ensure_columns(df: pd.DataFrame, required_cols: List[str]) -> pd.DataFrame:
    """
    DataFrame에 누락된 컬럼이 있으면 None으로 추가
    """
    df = df.copy()
    for col in required_cols:
        if col not in df.columns:
            df[col] = None
    return df


# ============================================================
# ✅ 타입 정규화
# ============================================================
def cast_column_types(df: pd.DataFrame, type_map: Dict[str, str]) -> pd.DataFrame:
    """
    지정된 컬럼의 dtype 강제 변환
    """
    df = df.copy()
    for col, dtype in type_map.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception:
                df[col] = pd.NA
    return df
