"""
WarehouseDataValidator
---------------------------------------
🏭 Data Warehouse 전용 Validator
- DataPathResolver 기반 경로 자동 세팅
- BaseDataValidator 상속
"""

from pathlib import Path
from plugins.validators.base_data_validator import BaseDataValidator
from plugins.utils.path_manager import DataPathResolver
from plugins.config import constants as C
import pandas as pd
import json

class WarehouseDataValidator(BaseDataValidator):
    """
    ✅ Data Warehouse 계층 유효성 검증 Validator
    예시 구조:
      /opt/airflow/data/data_warehouse/snapshot/equity/asset_master/country_code=KOR/trd_dt=2025-11-11/asset_master.parquet
    """

    def __init__(
        self,
        domain: str,
        trd_dt: str,
        vendor: str = "eodhd",
        exchange_code: str = "ALL",
        domain_group: str = "equity",
        country_code: str | None = None,
        allow_empty: bool = False,
        **kwargs,
    ):
        """
        - domain: 검증 대상 도메인 (예: exchange, asset, price 등)
        - trd_dt: 검증 기준일
        - vendor: 데이터 벤더 (예: eodhd)
        - country_code: 국가코드 (선택)
        """

        # ✅ warehouse 경로 자동 생성
        dataset_path = DataPathResolver.warehouse_snapshot(
            domain_group=domain_group,
            domain=domain,
            country_code=country_code,
            trd_dt=trd_dt,
        )

        super().__init__(
            domain=domain,
            domain_group=domain_group,
            layer="warehouse",
            trd_dt=trd_dt,
            vendor=vendor,
            exchange_code=exchange_code,
            allow_empty=allow_empty,
            **kwargs,
        )

        self.dataset_path = dataset_path
        self.country_code = country_code
        self.log.info(f"📦 Warehouse dataset path: {self.dataset_path}")

    # -------------------------------------------------------------------------
    # ✅ 결과 저장 (Parquet + Meta)
    # -------------------------------------------------------------------------
    def _save_result(self, result: dict, df: pd.DataFrame) -> Path:
        """
        ✅ Warehouse 전용 parquet 저장
        DataPathResolver 기반:
          /data_warehouse/snapshot/{group}/{domain_name}/country_code=XXX/trd_dt=YYYY-MM-DD/{domain_name}.parquet
        """

        snapshot_dir = DataPathResolver.warehouse_snapshot(
            domain_group=self.domain_group,
            domain=self.domain,
            country_code=self.country_code,
            trd_dt=self.trd_dt,
        )
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        # 도메인명 표준화 (ex: asset → asset_master)
        domain_name = C.WAREHOUSE_DOMAINS.get(self.domain, self.domain)
        parquet_path = snapshot_dir / f"{domain_name}.parquet"

        # ✅ parquet 저장
        df.to_parquet(parquet_path, index=False)
        self.log.info(f"✅ Parquet 저장 완료: {parquet_path} ({len(df):,}행)")

        return parquet_path



    def _load_dataset(self) -> pd.DataFrame:
        """
        ✅ domain별 파일 포맷 구분 로직
        - 일반 도메인(asset, price 등): Parquet 파일
        - fundamentals: security_id 단위 JSON 폴더
        """
        if not self.dataset_path.exists():
            self.log.warning(f"⚠️ Dataset path not found: {self.dataset_path}")
            return pd.DataFrame()

        # -------------------------------------------------------------
        # 1️⃣ Fundamentals 전용 처리 (JSON 구조)
        # -------------------------------------------------------------
        if self.domain in ["fundamentals", "fundamental_master"]:
            security_dirs = [p for p in self.dataset_path.glob("security_id=*") if p.is_dir()]
            if not security_dirs:
                self.log.warning(f"⚠️ No security_id folders found in {self.dataset_path}")
                return pd.DataFrame()

            dfs = []
            for d in security_dirs:
                general_path = d / "General.json"
                if not general_path.exists():
                    continue
                try:
                    data = json.load(open(general_path, "r", encoding="utf-8"))
                    df = pd.json_normalize(data, sep="_")
                    df["security_id"] = d.name.split("=")[-1]
                    dfs.append(df)
                except Exception as e:
                    self.log.warning(f"⚠️ Failed to load {general_path.name}: {e}")

            if not dfs:
                self.log.warning(f"⚠️ No valid JSON files in {self.dataset_path}")
                return pd.DataFrame()

            combined = pd.concat(dfs, ignore_index=True)
            self.log.info(f"✅ Loaded {len(combined):,} rows from {len(dfs)} security_id folders")
            return combined

        # -------------------------------------------------------------
        # 2️⃣ 일반 Warehouse 도메인 처리 (Parquet)
        # -------------------------------------------------------------
        elif self.dataset_path.is_dir():
            parquet_files = [f for f in self.dataset_path.glob("*.parquet") if not f.name.startswith("_")]
            if not parquet_files:
                self.log.warning(f"⚠️ No Parquet files in {self.dataset_path}")
                return pd.DataFrame()

            dfs = []
            for f in parquet_files:
                try:
                    df = pd.read_parquet(f)
                    if not df.empty:
                        dfs.append(df)
                except Exception as e:
                    self.log.warning(f"⚠️ Failed to load {f.name}: {e}")

            if not dfs:
                self.log.warning(f"⚠️ No valid data in {self.dataset_path}")
                return pd.DataFrame()

            combined = pd.concat(dfs, ignore_index=True)
            self.log.info(f"✅ Loaded {len(combined):,} rows from {len(parquet_files)} Parquet file(s)")
            return combined

        elif self.dataset_path.suffix.lower() == ".parquet":
            df = pd.read_parquet(self.dataset_path)
            self.log.info(f"✅ Loaded single parquet file: {self.dataset_path}")
            return df

        else:
            self.log.warning(f"⚠️ Unsupported file type: {self.dataset_path}")
            return pd.DataFrame()
