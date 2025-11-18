# plugins/utils/symbol_diff.py

from __future__ import annotations
import pandas as pd
import logging

log = logging.getLogger(__name__)


def normalize_code(t: str | None) -> str | None:
    """
    대소문자, 공백, 공백문자 등을 정규화하기 위한 유틸
    """
    if t is None:
        return None
    return t.strip().upper()


def to_code_set(df: pd.DataFrame) -> set[str]:
    """
    Symbol List DataFrame → Ticker Set 변환
    """
    if df is None or df.empty:
        return set()

    if "Code" not in df.columns:
        raise ValueError("❌ DataFrame에 'Code' 컬럼이 없습니다.")

    return set(df["Code"].astype(str).map(normalize_code))


def detect_new_codes(today_df: pd.DataFrame,
                       yesterday_df: pd.DataFrame | None) -> list[str]:
    """
    today_df 와 yesterday_df 비교해 신규 생성 Code 추출
    """
    today = to_code_set(today_df)
    yesterday = to_code_set(yesterday_df)

    new_values = sorted(today - yesterday)

    log.info(f"🆕 신규 등장 Code 수: {len(new_values)}")
    if new_values:
        log.info(f"🆕 신규 Code 목록: {new_values}")

    return new_values


def detect_removed_tickers(today_df: pd.DataFrame,
                           yesterday_df: pd.DataFrame | None) -> list[str]:
    """
    오늘 사라진 ticker 목록
    """
    today = to_code_set(today_df)
    yesterday = to_code_set(yesterday_df)

    removed = sorted(yesterday - today)

    if removed:
        log.info(f"⚠️ {len(removed)}개 ticker가 제거됨: {removed}")

    return removed


def detect_symbol_changes(now_df: pd.DataFrame,
                          old_df: pd.DataFrame) -> dict:
    """
    단순 비교로 symbol change 패턴 탐지
    (정교한 로직은 corporate_actions_dag의 symbol_changes 로 처리)
    """
    now_set = to_code_set(now_df)
    old_set = to_code_set(old_df)

    disappeared = sorted(old_set - now_set)
    appeared = sorted(now_set - old_set)

    return {"disappeared": disappeared, "appeared": appeared}
