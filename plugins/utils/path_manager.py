from plugins.config import constants as C

class DataPathResolver:
    """
    📁 Layer별 데이터 경로 생성기
    - lake, warehouse, mart 등 모든 경로 생성 책임을 이곳으로 통합
    """

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

    @staticmethod
    def warehouse_snapshot(domain_group, domain, country_code=None, trd_dt=None):
        if domain not in C.WAREHOUSE_DOMAINS:
            raise ValueError(f"❌ Unknown domain: {domain}")

        domain_dir = C.WAREHOUSE_DOMAINS[domain]
        path = C.DATA_WAREHOUSE_ROOT / "snapshot" / domain_group / domain_dir

        if country_code:
            path = path / f"country_code={country_code}"
        if trd_dt:
            path = path / f"trd_dt={trd_dt}"

        return path

    @staticmethod
    def warehouse_validated(domain_group, domain, country_code=None, trd_dt=None):
        if domain not in C.WAREHOUSE_DOMAINS:
            raise ValueError(f"❌ Unknown domain: {domain}")

        domain_dir = C.WAREHOUSE_DOMAINS[domain]
        path = C.DATA_WAREHOUSE_ROOT / "validated" / domain_group / domain_dir

        if country_code:
            path = path / f"country_code={country_code}"
        if trd_dt:
            path = path / f"trd_dt={trd_dt}"

        return path


    @staticmethod
    def warehouse_monitoring(domain_group: str, category: str,
                             country_code: str, trd_dt: str):
        """
        📁 Warehouse Monitoring 경로 생성기
        예:
          /data_warehouse/monitoring/equity/new_listing/country_code=USA/trd_dt=2025-11-13
        """
        return (
            C.DATA_MONITORING_ROOT
            / domain_group
            / category
            / f"country_code={country_code}"
            / f"trd_dt={trd_dt}"
        )

    # RAW
    @staticmethod
    def lake_raw_fundamentals(domain_group: str, vendor: str, exchange_code: str, trd_dt: str):
        return (
            C.DATA_LAKE_ROOT / "raw" / domain_group / "fundamentals"
            / f"vendor={vendor}"
            / f"exchange_code={exchange_code}"
            / f"trd_dt={trd_dt}"
        )

    # VALIDATED
    @staticmethod
    def lake_validated_fundamentals(domain_group: str, vendor: str, exchange_code: str, trd_dt: str):
        return (
            C.DATA_LAKE_ROOT / "validated" / domain_group / "fundamentals"
            / f"vendor={vendor}"
            / f"exchange_code={exchange_code}"
            / f"trd_dt={trd_dt}"
        )

    # VALIDATED (without trd_dt)
    @staticmethod
    def lake_validated_fundamentals_root(domain_group: str, vendor: str, exchange_code: str):
        return (
            C.DATA_LAKE_ROOT / "validated" / domain_group / "fundamentals"
            / f"vendor={vendor}"
            / f"exchange_code={exchange_code}"
        )

    # LATEST
    @staticmethod
    def fundamentals_latest_root(domain_group: str, vendor: str, exchange_code: str):
        return (
                C.DATA_LAKE_ROOT
                / "validated"
                / domain_group
                / "fundamentals"
                / f"vendor={vendor}"
                / f"exchange_code={exchange_code}"
                / "latest"
        )