# plugins/utils/duckdb_manager.py
import duckdb
import tempfile
from pathlib import Path
import logging
import gc


class DuckDBManager:
    """
    🦆 DuckDB Manager
    ----------------------------------------------------
    - Validator / Pipeline 공용 DuckDB 세션 관리
    - 임시 DB 파일 기반의 안전한 쿼리 실행/정리 지원
    - 에러 및 리소스 정리 일관화
    """

    def __init__(self, domain: str, tmp_dir: Path | None = None):
        self.domain = domain
        self.tmp_path = Path(tmp_dir or tempfile.gettempdir()) / f"{domain}_temp.duckdb"
        self.log = logging.getLogger(f"DuckDBManager[{domain}]")
        self.conn = None

    # ------------------------------------------------------------------
    # ✅ 연결 관리
    # ------------------------------------------------------------------
    def connect(self):
        """DuckDB 연결 생성"""
        self.log.info(f"🦆 Connecting to DuckDB → {self.tmp_path}")
        self.conn = duckdb.connect(database=str(self.tmp_path), read_only=False)
        self.conn.execute("PRAGMA threads=4;")
        self.conn.execute("SET memory_limit='4GB';")
        return self.conn

    def close(self):
        """DuckDB 연결 종료 + 임시파일 정리"""
        try:
            if self.conn:
                self.conn.close()
                self.log.info("🧹 DuckDB connection closed.")
        except Exception as e:
            self.log.warning(f"⚠️ DuckDB close failed: {e}")
        finally:
            # temp 파일 정리
            for p in [self.tmp_path, f"{self.tmp_path}.wal"]:
                if Path(p).exists():
                    Path(p).unlink(missing_ok=True)
            gc.collect()

    # ------------------------------------------------------------------
    # ✅ 쿼리 유틸
    # ------------------------------------------------------------------
    def register_df(self, name: str, df):
        """pandas DataFrame을 DuckDB 테이블로 등록"""
        self.conn.register(name, df)
        self.log.info(f"🪶 Registered DataFrame → {name} ({len(df):,} rows)")

    def query(self, sql: str):
        """SQL 쿼리 실행 및 결과 반환"""
        self.log.debug(f"🔍 Executing SQL: {sql[:80]}...")
        return self.conn.execute(sql).fetch_df()

    # 컨텍스트 매니저 지원
    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
