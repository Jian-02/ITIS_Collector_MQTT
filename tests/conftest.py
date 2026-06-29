"""
pytest auto-load settings for tests.

DB tests use .env.test when present. For PostgreSQL, the test fixture can also
create the configured database before creating the sensor_data table.
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

# 2. 최상단: .env.test 파일을 시스템 환경 변수에 즉시 주입 (override=True로 운영값 덮어쓰기)
env_test_path = Path(__file__).parent.parent / ".env.test"
if env_test_path.exists():
    load_dotenv(dotenv_path=env_test_path, override=True)
    os.environ["APP_ENV"] = "test"  # config.py 분기용 스위치

from config import DBConfig
from unittest.mock import MagicMock
import pytest

# ── Mock external libraries (fallback for missing installations) ──
for mod in ["paho", "paho.mqtt", "paho.mqtt.client"]:
    if mod not in sys.modules:
        sys.modules[mod] = MagicMock()

# ── Source root path addition ──────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))


# ── Test DB Load ───────────────────────────────────────
def test_db_loader(default: str = "sensor_data_test") -> str:
    # 이제 여기서는 안전하게 테스트 DB 설정이 적용됩니다.
    cfg = DBConfig.from_env() 
    print("현재 로드된 테이블명:", os.getenv("DB_TABLE_NAME"))
    print("현재 APP_ENV 상태:", os.getenv("APP_ENV"))
    assert cfg.table_name == "sensor_data_test"  # .env.test에 지정한 테이블명
    return default

# ── Common Util ────────────────────────────────────────────
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
    Return readl DB Adapter.
    - DB Connect
    - Create Table If not exist (IF NOT EXISTS in function ensure_table)
    If connection fails, All test skip.
    """
    from config import DBConfig
    from loader import make_adapter

    cfg = DBConfig.from_env()
    cfg.table_name = test_db_loader()
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
    Keep table data so test inserts can be inspected after pytest finishes.
    Maintain table schema.
    """
    from config import DBConfig

    db_type = DBConfig.from_env().db_type

    _truncate_table(db_adapter, db_type)

    yield db_adapter

    _truncate_table(db_adapter, db_type)
