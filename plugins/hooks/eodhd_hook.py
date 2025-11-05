from airflow.providers.http.hooks.http import HttpHook
from typing import Any, Dict, List
from plugins.config.constants import VENDORS


class EODHDHook(HttpHook):
    """
    EODHD API Hook (통합형)
    - Airflow Connection: eodhd_api
      - Conn Type: HTTP
      - Host: https://eodhd.com
      - Extra JSON: {"api_token": "YOUR_TOKEN_VALUE"}
    """

    def __init__(self, http_conn_id="eodhd_api", method="GET"):
        super().__init__(http_conn_id=http_conn_id, method=method)
        self.connection = self.get_connection(http_conn_id)
        self.api_token = self.connection.extra_dejson.get("api_token")

        # ✅ Hook의 “현재 호출 맥락” 저장용
        self.vendor = VENDORS["eodhd"]
        self.endpoint = None
        self.params = None

    def _run_api(self, endpoint: str, params: Dict[str, Any] = None) -> Any:
        """공통 API 실행 로직"""
        if not self.api_token:
            raise ValueError("Airflow Connection 'eodhd_api' Extra에 'api_token'이 없습니다.")

        params = params or {}
        params["api_token"] = self.api_token
        params["fmt"] = "json"

        headers = {"Accept": "application/json"}
        self.log.info(f"Requesting: {endpoint} | params={params}")

        # ✅ 메타정보 저장
        self.endpoint = endpoint
        self.params = params

        response = self.run(endpoint=endpoint, headers=headers, data=params)

        text = response.text.strip()

        # ✅ 정상: 빈 리스트 ([])
        if text in ("[]", ""):
            self.log.info(f"✅ Empty response. Returning [].")
            return []

        response.raise_for_status()

        if isinstance(response.json(), list):
            print("API 호출결과 row 건수 :", len(response.json()))

        return response.json()

    # ------------------------------
    # 가격 (국가 단위)
    # ------------------------------
    def get_prices(self, trd_dt: str = None, exchange_code: str = None) -> List[Dict[str, Any]]:

        assert exchange_code is not None, 'exchange code 확인이 필요합니다.'
        endpoint = f"api/eod-bulk-last-day/{exchange_code}"

        # 기술적 지표데이터도 함께 수집
        params: dict = {'filter': 'extended', 'date': trd_dt}

        self.log.info(f"Fetching EODHD prices for {exchange_code}, date={trd_dt}, filter=extended")
        return self._run_api(endpoint=endpoint, params=params)

    # ------------------------------
    # 펀더멘털 (심볼 단위)
    # ------------------------------
    def get_fundamentals(self, symbol: str) -> Dict[str, Any]:
        endpoint = f"api/fundamentals/{symbol}"
        return self._run_api(endpoint)

    # -------------------------------------------
    # ✅ 신규 메서드 ①: Splits (액면분할)
    # -------------------------------------------
    def get_splits(self, exchange_code: str = None, trd_dt: str = None):
        endpoint = f"api/eod-bulk-last-day/{exchange_code}"

        params = {
            "type": "splits",
            "date": trd_dt
        }

        self.log.info(f"Fetching splits for {exchange_code}")
        return self._run_api(endpoint, params=params)

    # -------------------------------------------
    # ✅ 신규 메서드 ②: Dividends (배당)
    # -------------------------------------------
    def get_dividends(self, exchange_code: str = None, trd_dt: str = None):
        endpoint = f"api/eod-bulk-last-day/{exchange_code}"

        params = {
            "type": "dividends",
            "date": trd_dt
        }

        self.log.info(f"Fetching dividends for {exchange_code}")
        return self._run_api(endpoint, params=params)


    # ------------------------------
    # 거래소 리스트
    # ------------------------------
    def get_exchanges_list(self):
        """거래소 코드 리스트"""
        endpoint = f"api/exchanges-list"
        self.log.info(f"Fetching exchange list")
        return self._run_api(endpoint)


    # ------------------------------
    # 심볼 리스트
    # ------------------------------
    def get_exchange_symbols(self, exchange_code: str):
        """거래소별 종목 코드 리스트"""
        endpoint = f"api/exchange-symbol-list/{exchange_code}"
        self.log.info(f"Fetching symbol list for exchange: {exchange_code}")
        return self._run_api(endpoint)

    def get_symbol_changes(self, trd_dt):
        """
        심볼 변경 이력
        문서 예: /api/symbol-change-history/{EXCHANGE_CODE}
        """
        endpoint = f"api/symbol-change-history"
        self.log.info(f"Fetching symbol changes")
        params = {'from': trd_dt}
        return self._run_api(endpoint, params=params)

    def get_exchange_holidays(self, exchange_code: str, year: int = None):
        """
        휴장일
        문서 예: /api/exchange-holidays/{EXCHANGE_CODE}?year=YYYY
        """
        endpoint = f"api/exchange-details/{exchange_code}"
        params = {}
        if year:
            params["year"] = year
        self.log.info(f"Fetching holidays for exchange: {exchange_code}, year={year}")
        return self._run_api(endpoint, params=params)
