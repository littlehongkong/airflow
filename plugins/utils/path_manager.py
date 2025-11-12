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
