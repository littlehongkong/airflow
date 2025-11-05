# plugins/pipelines/symbol_changes_pipeline.py
from datetime import datetime
from typing import Any, Dict, List, Tuple

from plugins.pipelines.base_equity_pipeline import BaseEquityPipeline
from plugins.hooks.eodhd_hook import EODHDHook
from psycopg2.extras import execute_values
import json

class SymbolChangePipeline(BaseEquityPipeline):
    """
    거래소별 심볼 변경 이력 수집 파이프라인
    - EODHD: /api/symbol-change-history/{EXCHANGE_CODE}
    - 결과는 symbol_change_history 테이블에 upsert
    """

    def __init__(self, domain: str, exchange_code: str, trd_dt: str, domain_group: str):
        super().__init__(domain=domain, exchange_code=exchange_code, trd_dt=trd_dt, domain_group=domain_group)
        self.hook = EODHDHook()

    def fetch(self, **kwargs):
        data = self.hook.get_symbol_changes(trd_dt=self.trd_dt)
        return data