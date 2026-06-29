"""
tests/db_test.py
Integration tests for real database connections.
Uses connection settings from .env.test and automatically skips when the connection fails.
"""

import json
import sys
import unittest
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DBConfig
from loader import make_adapter


# ══════════════════════════════════════════════════════════
# DB connection tests
# ══════════════════════════════════════════════════════════

class DBConnectionTest:

    def test_connection_success(self, db_adapter):
        """Database connection should succeed."""
        assert db_adapter._conn is not None

    def test_ensure_table_creates_sensor_data(self, clean_test_table):
        """sensor_data table should be created."""
        adapter = clean_test_table
        db_type = DBConfig.from_env().db_type
        cur     = adapter._conn.cursor()

        check_sql = {
            "postgresql": "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'sensor_data';",
            "mssql":      "SELECT COUNT(*) FROM sysobjects WHERE name='sensor_data' AND xtype='U';",
            "oracle":     "SELECT COUNT(*) FROM user_tables WHERE table_name = 'SENSOR_DATA';",
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


class DBInsertTest:

    def test_insert_single_row(self, clean_test_table):
        """A single inserted record should be queryable."""
        adapter = clean_test_table
        adapter.insert_batch([SAMPLE_ROWS[0]])

        cur = adapter._conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sensor_data;")
        count = cur.fetchone()[0]
        cur.close()
        assert count == 1

    def test_insert_multiple_rows(self, clean_test_table):
        """After inserting multiple records, the total count should match."""
        adapter = clean_test_table
        adapter.insert_batch(SAMPLE_ROWS)

        cur = adapter._conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sensor_data;")
        count = cur.fetchone()[0]
        cur.close()
        assert count == len(SAMPLE_ROWS)

    def test_inserted_value_is_correct(self, clean_test_table):
        """Inserted value should be correct."""
        adapter = clean_test_table
        adapter.insert_batch([SAMPLE_ROWS[0]])

        cur = adapter._conn.cursor()
        cur.execute("SELECT value FROM sensor_data;")
        row = cur.fetchone()
        cur.close()
        assert row[0] == 23.5

    def test_insert_empty_batch_does_not_raise(self, clean_test_table):
        """Inserting an empty batch should not raise an error."""
        adapter = clean_test_table
        try:
            if SAMPLE_ROWS:  # Explicitly call with an empty list
                adapter.insert_batch([])
        except Exception as e:
            pytest.fail(f"빈 배치 INSERT 에러: {e}")

    def test_insert_null_value_allowed(self, clean_test_table):
        """Rows with value=None should be inserted."""
        adapter = clean_test_table
        null_row = (
            "factory/line1/temp",
            "factory", "line1", "temp",
            None,                           # value = NULL
            "2026-06-01T10:00:00+00:00",
            "2026-06-01T10:00:01+00:00",
            json.dumps({}),
        )
        adapter.insert_batch([null_row])

        cur = adapter._conn.cursor()
        cur.execute("SELECT value FROM sensor_data;")
        row = cur.fetchone()
        cur.close()
        assert row[0] is None

    def test_insert_large_batch(self, clean_test_table):
        """A large batch of 1,000 rows should be inserted successfully."""
        adapter = clean_test_table
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

        cur = adapter._conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sensor_data;")
        count = cur.fetchone()[0]
        cur.close()
        assert count == 1000


# ══════════════════════════════════════════════════════════
# Inject fixtures into pytest classes
# ══════════════════════════════════════════════════════════

class TestDBConnection(DBConnectionTest):
    pass


class TestDBInsert(DBInsertTest):
    pass
