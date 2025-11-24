from plugins.config import constants as C

class DataPathResolver:
    """
    📁 Layer별 데이터 경로 생성기
    ------------------------------------------------------------
    - lake/raw
    - lake/validated
    - lake/validated/latest
    - warehouse/snapshot
    - warehouse/validated
    - warehouse/monitoring
    ------------------------------------------------------------
    모든 모듈(FundamentalPipeline / Validator / Asset Master 등)이
    오직 이 클래스만을 통해 경로를 생성하도록 표준화.
    """

    # ============================================================
    # 📌 LAKE - RAW (공통)
    # ============================================================
    @staticmethod
    def lake_raw(domain_group, domain, vendor, exchange_code, trd_dt):
        return (
            C.DATA_LAKE_ROOT
            / "raw"
            / domain_group
            / domain
            / f"vendor={vendor}"
            / f"exchange_code={exchange_code}"
            / f"trd_dt={trd_dt}"
        )

    # ============================================================
    # 📌 LAKE - VALIDATED (공통)
    # ============================================================
    @staticmethod
    def lake_validated(domain_group, domain, vendor, exchange_code, trd_dt):
        return (
            C.DATA_LAKE_ROOT
            / "validated"
            / domain_group
            / domain
            / f"vendor={vendor}"
            / f"exchange_code={exchange_code}"
            / f"trd_dt={trd_dt}"
        )

    # ============================================================
    # 📌 Warehouse Snapshot
    # ============================================================
    @staticmethod
    def warehouse_snapshot(domain_group, domain, country_code=None, trd_dt=None):
        if domain not in C.WAREHOUSE_DOMAINS:
            raise ValueError(f"❌ Unknown domain: {domain}")

        domain_dir = C.WAREHOUSE_DOMAINS[domain]
        path = C.DATA_WAREHOUSE_ROOT / "snapshot" / domain_group / domain_dir

        if country_code:
            path /= f"country_code={country_code}"
        if trd_dt:
            path /= f"trd_dt={trd_dt}"

        return path

    # ============================================================
    # 📌 Warehouse Validated
    # ============================================================
    @staticmethod
    def warehouse_validated(domain_group, domain, country_code=None, trd_dt=None):
        if domain not in C.WAREHOUSE_DOMAINS:
            raise ValueError(f"❌ Unknown domain: {domain}")

        domain_dir = C.WAREHOUSE_DOMAINS[domain]
        path = C.DATA_WAREHOUSE_ROOT / "validated" / domain_group / domain_dir

        if country_code:
            path /= f"country_code={country_code}"
        if trd_dt:
            path /= f"trd_dt={trd_dt}"

        return path

    # ============================================================
    # 📌 Warehouse Monitoring (event 기반)
    # ============================================================
    @staticmethod
    def warehouse_monitoring(domain_group: str, category: str,
                             exchange_code: str, trd_dt: str):
        """
        monitoring/equity/new_listing/exchange_code=US/trd_dt=2025-11-13
        """
        return (
            C.DATA_MONITORING_ROOT
            / domain_group
            / category
            / f"exchange_code={exchange_code}"
            / f"trd_dt={trd_dt}"
        )

    # ============================================================
    # 📌 Fundamentals: LAKE RAW
    # ============================================================
    @staticmethod
    def lake_raw_fundamentals(domain_group: str, vendor: str, exchange_code: str, trd_dt: str):
        return (
            C.DATA_LAKE_ROOT
            / "raw"
            / domain_group
            / "fundamentals"
            / f"vendor={vendor}"
            / f"exchange_code={exchange_code}"
            / f"trd_dt={trd_dt}"
        )

    # ============================================================
    # 📌 Fundamentals: VALIDATED root (exchange root)
    # ============================================================
    @staticmethod
    def lake_validated_fundamentals_root(domain_group: str, vendor: str, exchange_code: str):
        return (
            C.DATA_LAKE_ROOT
            / "validated"
            / domain_group
            / "fundamentals"
            / f"vendor={vendor}"
            / f"exchange_code={exchange_code}"
        )

    # ============================================================
    # 📌 Fundamentals: VALIDATED + trd_dt (작업용)
    # ============================================================
    @staticmethod
    def lake_validated_fundamentals_trd_dt(domain_group: str, vendor: str,
                                           exchange_code: str, trd_dt: str):
        return (
            DataPathResolver.lake_validated_fundamentals_root(
                domain_group, vendor, exchange_code
            )
            / f"trd_dt={trd_dt}"
        )

    # ============================================================
    # 📌 Fundamentals: latest (ticker 최신 스냅샷)
    # ============================================================
    @staticmethod
    def fundamentals_latest_root(domain_group: str, vendor: str, exchange_code: str):
        """
        /lake/validated/equity/fundamentals/vendor=eodhd/latest/exchange_code=NASDAQ/
        """
        return (
            C.DATA_LAKE_ROOT
            / "validated"
            / domain_group
            / "fundamentals"
            / f"vendor={vendor}"
            / f"exchange_code={exchange_code}"
            / "latest"
        )
