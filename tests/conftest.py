"""
tests/conftest.py
pytest 자동 로드 — 외부 라이브러리 mock 및 공통 fixture 설정.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from dotenv import load_dotenv

# ── 외부 라이브러리 mock (미설치 환경 대비) ──────────────
for mod in ["paho", "paho.mqtt", "paho.mqtt.client"]:
    if mod not in sys.modules:
        sys.modules[mod] = MagicMock()

# ── 소스 루트 경로 추가 ──────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))


# ── .env.test 로드 ───────────────────────────────────────
def pytest_configure(config):
    env_test = Path(__file__).parent.parent / ".env.test"
    if env_test.exists():
        load_dotenv(env_test, override=True)


# ── 공통 유틸 ────────────────────────────────────────────

def _truncate_table(adapter, db_type: str):
    """테이블 데이터만 삭제한다. 테이블 구조는 유지."""
    truncate_sql = {
        "postgresql": "TRUNCATE TABLE sensor_data;",
        "mssql":      "TRUNCATE TABLE sensor_data;",
        "oracle":     "TRUNCATE TABLE sensor_data;",
    }
    cur = adapter._conn.cursor()
    cur.execute(truncate_sql[db_type])
    adapter._conn.commit()
    cur.close()


# ── DB 연결 fixture ──────────────────────────────────────

@pytest.fixture(scope="session")
def db_adapter():
    """
    실제 DB 어댑터를 반환한다.
    - DB 연결
    - 테이블 없으면 생성, 있으면 패스 (ensure_table이 IF NOT EXISTS로 처리)
    연결 실패 시 해당 테스트 전체를 skip 처리한다.
    """
    from config import DBConfig
    from loader import make_adapter

    cfg     = DBConfig.from_env()
    adapter = make_adapter(cfg)

    try:
        adapter.connect()
    except Exception as e:
        pytest.skip(f"DB 연결 실패 — 테스트 skip: {e}")

    # 테이블 없으면 생성, 있으면 패스
    adapter.ensure_table()

    yield adapter

    try:
        adapter.close()
    except Exception:
        pass


@pytest.fixture(scope="function")
def clean_test_table(db_adapter):
    """
    각 테스트 함수 실행 전 테이블 데이터만 초기화한다.
    테이블 구조(스키마)는 유지한다.
    """
    from config import DBConfig
    db_type = DBConfig.from_env().db_type

    _truncate_table(db_adapter, db_type)

    yield db_adapter

    _truncate_table(db_adapter, db_type)