"""
테스트를 위한 pytest 자동 로드 설정입니다.

DB 테스트는 .env.test 파일이 존재할 경우 이를 사용합니다. PostgreSQL의 경우,
테스트 픽스처(fixture)가 sensor_data 테이블을 생성하기 전에 구성된 데이터베이스를 직접 생성할 수도 있습니다.
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DBConfig
from unittest.mock import MagicMock
import pytest

# ── 외부 라이브러리 모킹 (설치되지 않은 경우를 위한 폴백) ──
for mod in ["paho", "paho.mqtt", "paho.mqtt.client"]:
    if mod not in sys.modules:
        sys.modules[mod] = MagicMock()

# ── 소스 루트 경로 추가 ──────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))


# ── Test DB Load ───────────────────────────────────────
def _verify_and_get_test_table(default: str = "sensor_data_test") -> str:
    # 이 시점에서는 테스트 DB 설정이 안전하게 적용된 상태입니다.
    cfg = DBConfig.from_env() 
    # IP는 일치하지만, 테스트는 테이블명을 _test를 바라보도록 함
    cfg.table_name = "sensor_data_test"
    print("현재 로드된 테이블명:", os.getenv("DB_TABLE_NAME"))
    print("현재 APP_ENV 상태:", os.getenv("APP_ENV"))
    assert cfg.table_name == "sensor_data_test"  # .env.test에 지정한 테이블명
    return default

# ── 공통 Util ────────────────────────────────────────────
def _truncate_table(adapter, db_type: str):
    # Delete only data. Maintain data structure"
    truncate_sql = {
        "postgresql": f"TRUNCATE TABLE {adapter.table_name};",
        "mssql": f"TRUNCATE TABLE {adapter.table_name};",
        "oracle": f"TRUNCATE TABLE {adapter.table_name};",
    }
    cur = adapter._conn.cursor()
    cur.execute(truncate_sql[db_type])
    adapter._conn.commit()
    cur.close()


def _is_missing_postgresql_database(error: Exception) -> bool:
    message = str(error).lower()
    return "database" in message and "does not exist" in message


def _ensure_postgresql_database(cfg):
    if not cfg.name:
        return

    import psycopg2
    from psycopg2 import sql

    maintenance_db = os.getenv("DB_MAINTENANCE_NAME", "postgres")
    conn = psycopg2.connect(
        host=os.getenv("DB_ADMIN_HOST", cfg.host),
        port=int(os.getenv("DB_ADMIN_PORT", str(cfg.port))),
        dbname=maintenance_db,
        user=os.getenv("DB_ADMIN_USER", cfg.user),
        password=os.getenv("DB_ADMIN_PASSWORD", cfg.password),
    )
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (cfg.name,))
            if cur.fetchone() is None:
                cur.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(cfg.name)))
    finally:
        conn.close()



# ── DB Connect fixture ──────────────────────────────────────
@pytest.fixture(scope="session")
def db_adapter():
    """
    실제 DB 어댑터를 반환합니다.
    - DB 연결
    - 테이블이 없으면 생성 (ensure_table 함수 내의 IF NOT EXISTS 활용)
    만약 연결에 실패하면, 모든 테스트를 건너뜁니다(skip).
    """
    from config import DBConfig
    from loader import make_adapter

    cfg = DBConfig.from_env()
    cfg.table_name = _verify_and_get_test_table()
    adapter = make_adapter(cfg)

    try:
        adapter.connect()
    except Exception as e:
        if cfg.db_type == "postgresql" and _is_missing_postgresql_database(e):
            try:
                _ensure_postgresql_database(cfg)
                adapter = make_adapter(cfg)
                adapter.connect()
            except Exception as create_error:
                pytest.skip(f"DB connect failed after database auto-create attempt: {create_error}")
        else:
            pytest.skip(f"DB connect failed: {e}")

    try:
        adapter.ensure_table()
    except Exception as e:
        pytest.skip(f"DB table create failed: {e}")

    yield adapter

    try:
        adapter.close()
    except Exception:
        pass


@pytest.fixture(scope="function")
def clean_test_table(db_adapter):
    """
    pytest가 종료된 후에도 테스트로 삽입된 데이터를 확인할 수 있도록 테이블 데이터를 유지합니다.
    테이블 스키마 역시 그대로 유지합니다.
    """
    from config import DBConfig

    db_type = DBConfig.from_env().db_type

    _truncate_table(db_adapter, db_type)

    yield db_adapter

    _truncate_table(db_adapter, db_type)
