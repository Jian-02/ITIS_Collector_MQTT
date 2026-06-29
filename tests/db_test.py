"""
tests/db_test.py
Integration tests for real database connections.
Uses connection settings from .env.test and automatically skips when the connection fails.
"""

import json
import sys
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
        adapter = clean_test_table
        db_type = DBConfig.from_env().db_type
        table_name = DBConfig.from_env().table_name
        cur     = adapter._conn.cursor()

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
        "factory",
        "line1",
        "temp",
        23.5,
        "2026-06-01T10:00:00+00:00",
        "2026-06-01T10:00:01+00:00",
        json.dumps({"value": 23.5, "ts": "2026-06-01T10:00:00+00:00"}),
    ),
    (
        "factory/line1/humidity",
        "factory",
        "line1",
        "humidity",
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
        before = _count_rows(adapter)
        adapter.insert_batch([SAMPLE_ROWS[0]])

        assert _count_rows(adapter) == before + 1

    def test_insert_multiple_rows(self, clean_test_table):
        """After inserting multiple records, the total count should match."""
        adapter = clean_test_table
        before = _count_rows(adapter)
        adapter.insert_batch(SAMPLE_ROWS)

        assert _count_rows(adapter) == before + len(SAMPLE_ROWS)

    def test_inserted_value_is_correct(self, clean_test_table):
        """Inserted value should be correct."""
        adapter = clean_test_table
        before = _count_rows(adapter, " WHERE topic = 'factory/line1/temp' AND value = 23.5")
        adapter.insert_batch([SAMPLE_ROWS[0]])

        after = _count_rows(adapter, " WHERE topic = 'factory/line1/temp' AND value = 23.5")
        assert after == before + 1

    def test_insert_empty_batch_does_not_raise(self, clean_test_table):
        """Inserting an empty batch should not raise an error."""
        adapter = clean_test_table
        before = _count_rows(adapter)
        try:
            if SAMPLE_ROWS:  # Explicitly call with an empty list
                adapter.insert_batch([])
        except Exception as e:
            pytest.fail(f"빈 배치 INSERT 에러: {e}")
        assert _count_rows(adapter) == before

    def test_insert_null_value_allowed(self, clean_test_table):
        """Rows with value=None should be inserted."""
        adapter = clean_test_table
        before = _count_rows(adapter, " WHERE value IS NULL")
        null_row = (
            "factory/line1/temp",
            "factory", "line1", "temp",
            None,                           # value = NULL
            "2026-06-01T10:00:00+00:00",
            "2026-06-01T10:00:01+00:00",
            json.dumps({}),
        )
        adapter.insert_batch([null_row])

        assert _count_rows(adapter, " WHERE value IS NULL") == before + 1

    def test_insert_large_batch(self, clean_test_table):
        """A large batch of 1,000 rows should be inserted successfully."""
        adapter = clean_test_table
        before = _count_rows(adapter)
        rows = [
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
        """if DB connection fails, data must remain safe in FileQueue"""
        # Inject invalid connection info
        bad_db_cfg = DBConfig.from_env()
        bad_db_cfg.host = "invalid_host_1234.com"
        bad_db_cfg.port = 9999

        queue_cfg = QueueConfig.from_env()
        queue = FileQueue(queue_cfg)

        target_path = queue_cfg.path
        
        # 3. Safety Check: If the real file already has data, we back it up first
        original_content = b""
        if target_path.exists():
            original_content = target_path.read_bytes()

        queue = FileQueue(queue_cfg)
        
        try:
            # Insert a sample record into the queue
            sample = {"topic": "test", "value": 23.5, "payload": {}}
            queue.append(sample)

            # Initialize loader
            loader = DBLoader(bad_db_cfg, LoaderConfig(batch_size=1, poll_interval=1), queue)

            try:
                loader._connect()
            except Exception:
                pass
            # Trigger process loop and catch the expected DB exception
            try:
                loader._process()
            except Exception:
                pass  # Ignore DB connection error intentionally

            # 4. Verify using standard pytest assert (No assertTrue syntax error)
            assert target_path.exists(), f"PQ file must exist at {target_path} even if DB fails."
            assert target_path.stat().st_size > 0, "Data must be preserved in the queue."
            
        finally:
            # 5. Rollback: Restore the original file state completely
            if original_content:
                target_path.write_bytes(original_content)
            elif target_path.exists():
                target_path.unlink()  # If it didn't exist before, remove it


# ══════════════════════════════════════════════════════════
# Inject fixtures into pytest classes
# ══════════════════════════════════════════════════════════

class TestDBConnection(DBConnectionTest):
    pass


class TestDBInsert(DBInsertTest):
    pass

class TestDBFailover(DBFailoverTest):
    pass
