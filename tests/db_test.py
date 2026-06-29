"""
tests/db_test.py
Integration tests for real database connections.
Uses connection settings from .env.test and automatically skips when the connection fails.
"""

import json
import sys
import tempfile
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DBConfig, LoaderConfig, QueueConfig
from file_queue import FileQueue
from loader import DBLoader


# ══════════════════════════════════════════════════════════
# DB connection tests
# ══════════════════════════════════════════════════════════

class DBConnectionTest:

    def test_connection_success(self, db_adapter):
        """Database connection should succeed."""
        assert db_adapter._conn is not None

    def test_ensure_table_creates_sensor_data(self, clean_test_table):
        """sensor_data_test table should be created."""
        adapter    = clean_test_table
        db_type    = DBConfig.from_env().db_type
        table_name = DBConfig.from_env().table_name
        cur        = adapter._conn.cursor()

        check_sql = {
            "postgresql": f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}';",
            "mssql":      f"SELECT COUNT(*) FROM sysobjects WHERE name='{table_name}' AND xtype='U';",
            "oracle":     f"SELECT COUNT(*) FROM user_tables WHERE table_name = '{table_name.upper()}';",
        }

        cur.execute(check_sql[db_type])
        count = cur.fetchone()[0]
        cur.close()
        assert count == 1


# ══════════════════════════════════════════════════════════
# INSERT tests
# ══════════════════════════════════════════════════════════

SAMPLE_ROWS = [
    (
        "factory/line1/temp",
        "factory", "line1", "temp",
        23.5,
        "2026-06-01T10:00:00+00:00",
        "2026-06-01T10:00:01+00:00",
        json.dumps({"value": 23.5, "ts": "2026-06-01T10:00:00+00:00"}),
    ),
    (
        "factory/line1/humidity",
        "factory", "line1", "humidity",
        65.0,
        "2026-06-01T10:00:00+00:00",
        "2026-06-01T10:00:01+00:00",
        json.dumps({"value": 65.0, "ts": "2026-06-01T10:00:00+00:00"}),
    ),
]


def _count_rows(adapter, where: str = "") -> int:
    cur = adapter._conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {adapter.table_name}{where};")
    count = cur.fetchone()[0]
    cur.close()
    return count


class DBInsertTest:

    def test_insert_single_row(self, clean_test_table):
        """A single inserted record should be queryable."""
        adapter = clean_test_table
        before  = _count_rows(adapter)
        adapter.insert_batch([SAMPLE_ROWS[0]])
        assert _count_rows(adapter) == before + 1

    def test_insert_multiple_rows(self, clean_test_table):
        """After inserting multiple records, the total count should match."""
        adapter = clean_test_table
        before  = _count_rows(adapter)
        adapter.insert_batch(SAMPLE_ROWS)
        assert _count_rows(adapter) == before + len(SAMPLE_ROWS)

    def test_inserted_value_is_correct(self, clean_test_table):
        """Inserted value should be correct."""
        adapter = clean_test_table
        before  = _count_rows(adapter, " WHERE topic = 'factory/line1/temp' AND value = 23.5")
        adapter.insert_batch([SAMPLE_ROWS[0]])
        after   = _count_rows(adapter, " WHERE topic = 'factory/line1/temp' AND value = 23.5")
        assert after == before + 1

    def test_insert_empty_batch_does_not_raise(self, clean_test_table):
        """Inserting an empty batch should not raise an error."""
        adapter = clean_test_table
        before  = _count_rows(adapter)
        try:
            adapter.insert_batch([])
        except Exception as e:
            pytest.fail(f"빈 배치 INSERT 에러: {e}")
        assert _count_rows(adapter) == before

    def test_insert_null_value_allowed(self, clean_test_table):
        """Rows with value=None should be inserted."""
        adapter  = clean_test_table
        before   = _count_rows(adapter, " WHERE value IS NULL")
        null_row = (
            "factory/line1/temp",
            "factory", "line1", "temp",
            None,
            "2026-06-01T10:00:00+00:00",
            "2026-06-01T10:00:01+00:00",
            json.dumps({}),
        )
        adapter.insert_batch([null_row])
        assert _count_rows(adapter, " WHERE value IS NULL") == before + 1

    def test_insert_large_batch(self, clean_test_table):
        """A large batch of 1,000 rows should be inserted successfully."""
        adapter = clean_test_table
        before  = _count_rows(adapter)
        rows    = [
            (
                f"factory/line1/sensor{i}",
                "factory", "line1", f"sensor{i}",
                float(i),
                "2026-06-01T10:00:00+00:00",
                "2026-06-01T10:00:01+00:00",
                json.dumps({"value": i}),
            )
            for i in range(1000)
        ]
        adapter.insert_batch(rows)
        assert _count_rows(adapter) == before + 1000


# ══════════════════════════════════════════════════════════
# DB Failover & FileQueue Backup Tests
# ══════════════════════════════════════════════════════════

class DBFailoverTest:

    def test_queue_backup_on_db_failure(self, tmp_path):
        """DB 연결 실패 시 데이터가 FileQueue에 안전하게 보존돼야 한다."""
        bad_db_cfg      = DBConfig.from_env()
        bad_db_cfg.host = "invalid_host_1234.com"
        bad_db_cfg.port = 9999

        queue_cfg    = QueueConfig.from_env()
        queue        = FileQueue(queue_cfg)
        sample       = {"topic": "test", "value": 23.5, "payload": {}}
        queue.append(sample)

        loader = DBLoader(bad_db_cfg, LoaderConfig(batch_size=1, poll_interval=1), queue)
        with pytest.raises(Exception):
            loader._connect(max_attempts=1)
        try:
            loader._process()
        except Exception:
            pass

        assert queue_cfg.path.exists()
        assert queue_cfg.path.stat().st_size > 0


# ══════════════════════════════════════════════════════════
# PQ → DB 적재 테스트
# (파일에 기존 데이터가 있을 때 DB에 정상적으로 밀어넣어지는지)
# ══════════════════════════════════════════════════════════

SAMPLE_RECORDS = [
    {
        "topic":       "factory/line1/temp",
        "site":        "factory",
        "device":      "line1",
        "sensor":      "temp",
        "value":       23.5,
        "ts":          "2026-06-01T10:00:00+00:00",
        "received_at": "2026-06-01T10:00:01+00:00",
        "payload":     {"value": 23.5},
    },
    {
        "topic":       "factory/line1/humidity",
        "site":        "factory",
        "device":      "line1",
        "sensor":      "humidity",
        "value":       65.0,
        "ts":          "2026-06-01T10:00:00+00:00",
        "received_at": "2026-06-01T10:00:01+00:00",
        "payload":     {"value": 65.0},
    },
]


class PQToDBTest:

    def test_existing_pq_data_is_inserted_to_db(self, clean_test_table, tmp_path):
        """
        프로그램 시작 전에 PQ 파일에 쌓인 데이터가
        _process() 호출 시 DB에 정상 INSERT되어야 한다.
        """
        adapter   = clean_test_table
        queue_cfg = QueueConfig.from_env()
        queue     = FileQueue(queue_cfg)
 
        # SAMPLE_RECORDS 추가 전, PQ에 이미 있는 건수를 미리 파악
        existing  = queue.peek()
        queue.rollback()
        expected  = len(existing) + len(SAMPLE_RECORDS)
 
        for record in SAMPLE_RECORDS:
            queue.append(record)
        assert queue_cfg.path.stat().st_size > 0, "PQ 파일에 데이터가 없음"

        before = _count_rows(adapter)

        # DBLoader로 처리
        db_cfg = DBConfig.from_env()
        db_cfg.table_name = adapter.table_name
        loader = DBLoader(db_cfg, LoaderConfig(batch_size=500, poll_interval=1), queue)
        loader._adapter = adapter   # 이미 연결된 어댑터 재사용
        loader._process()

        # DB에 들어갔는지 확인
        assert _count_rows(adapter) == before + expected, \
            "PQ에 있던 레코드가 DB에 INSERT되지 않음"
        assert queue_cfg.path.stat().st_size == 0, \
            "INSERT 성공 후 PQ 파일이 비워지지 않음"

    def test_pq_file_preserved_on_db_failure(self, tmp_path):
        """
        DB INSERT 실패 시 PQ 파일의 데이터가 Queued 상태로 복원돼야 한다.
        """
        queue_cfg = QueueConfig.from_env()
        queue     = FileQueue(queue_cfg)
 
        # 테스트 전 PQ에 이미 있는 건수 파악
        existing = queue.peek()
        queue.rollback()
        expected = len(existing) + len(SAMPLE_RECORDS)
 
        for record in SAMPLE_RECORDS:
            queue.append(record)

        bad_db_cfg      = DBConfig.from_env()
        bad_db_cfg.host = "invalid_host_1234.com"
        bad_db_cfg.port = 9999

        loader = DBLoader(bad_db_cfg, LoaderConfig(batch_size=500, poll_interval=1), queue)
        with pytest.raises(Exception):
            loader._connect(max_attempts=1)
        try:
            loader._process()
        except Exception:
            pass

        # 파일이 남아있고, Queued 상태로 복원됐는지 확인
        assert queue_cfg.path.stat().st_size > 0, "실패 후 PQ 파일이 비워짐 (데이터 유실)"

        lines = queue_cfg.path.read_text(encoding="utf-8").splitlines()
        objs  = [json.loads(l) for l in lines if l.strip()]
        assert all(o["_s"] == "Q" for o in objs), \
            "실패 후 Pending(_s=P) 줄이 남아있음 (rollback 미동작)"
        assert len(objs) == len(SAMPLE_RECORDS), \
            f"복원된 레코드 수 불일치: {len(objs)} != {len(SAMPLE_RECORDS)}"

    def test_pq_data_inserted_after_reconnect(self, clean_test_table, tmp_path):
        """
        DB 재연결 후 PQ에 남아있던 데이터가 정상 INSERT돼야 한다.
        (rollback → 재연결 → 재처리 시나리오)
        """
        adapter   = clean_test_table
        queue_cfg = QueueConfig.from_env()
        queue     = FileQueue(queue_cfg)
 
        # 테스트 전 PQ에 이미 있는 건수 파악
        existing = queue.peek()
        queue.rollback()
        expected = len(existing) + len(SAMPLE_RECORDS)

        # 1차: DB 연결 실패 → rollback
        bad_db_cfg      = DBConfig.from_env()
        bad_db_cfg.host = "invalid_host_1234.com"
        bad_db_cfg.port = 9999
        bad_loader = DBLoader(bad_db_cfg, LoaderConfig(batch_size=500, poll_interval=1), queue)
        with pytest.raises(Exception):
            bad_loader._connect(max_attempts=1)
        try:
            bad_loader._process()
        except Exception:
            pass
        
        before = _count_rows(adapter)

        # 2차: 정상 DB로 재처리
        db_cfg        = DBConfig.from_env()
        db_cfg.table_name = adapter.table_name
        good_loader   = DBLoader(db_cfg, LoaderConfig(batch_size=500, poll_interval=1), queue)
        good_loader._adapter = adapter
        good_loader._process()

        assert _count_rows(adapter) == before + len(SAMPLE_RECORDS), \
            "재연결 후 PQ 데이터가 DB에 INSERT되지 않음"
        assert queue_cfg.path.stat().st_size == 0, \
            "재연결 후 INSERT 성공했지만 PQ 파일이 비워지지 않음"


# ══════════════════════════════════════════════════════════
# Inject fixtures into pytest classes
# ══════════════════════════════════════════════════════════

class TestDBConnection(DBConnectionTest):
    pass

class TestDBInsert(DBInsertTest):
    pass

class TestDBFailover(DBFailoverTest):
    pass

class TestPQToDB(PQToDBTest):
    pass