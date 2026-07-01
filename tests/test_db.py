"""
tests/db_test.py
실제 데이터베이스 연결을 검증하는 통합 테스트입니다.
.env.test의 연결 설정을 사용하며, 연결에 실패할 경우 자동으로 테스트를 건너뜁니다.
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
# 공통 헬퍼 함수
# ══════════════════════════════════════════════════════════

def _count_rows(adapter, where: str = "") -> int:
    cur = adapter._conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {adapter.table_name}{where};")
    count = cur.fetchone()[0]
    cur.close()
    return count

# ══════════════════════════════════════════════════════════
# DB Connection Tests
# ══════════════════════════════════════════════════════════

class DBConnectionTest:

    def test_connection_success(self, db_adapter):
        """데이터베이스 연결에 성공해야 합니다."""
        assert db_adapter._conn is not None

    def test_ensure_table_creates_sensor_data(self, clean_test_table):
        """sensor_data_test 테이블이 정상적으로 생성되어야 합니다."""
        adapter = clean_test_table
        db_type = DBConfig.from_env().db_type
        table_name = DBConfig.from_env().table_name
        cur = adapter._conn.cursor()

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
# INSERT Tests
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


class DBInsertTest:

    def test_insert_single_row(self, clean_test_table):
        """단일 삽입된 레코드가 정상적으로 조회되어야 합니다."""
        before = _count_rows(clean_test_table)
        clean_test_table.insert_batch([SAMPLE_ROWS[0]])
        assert _count_rows(clean_test_table) == before + 1

    def test_insert_multiple_rows(self, clean_test_table):
        """여러 레코드를 삽입한 후, 전체 개수가 일치해야 합니다."""
        before = _count_rows(clean_test_table)
        clean_test_table.insert_batch(SAMPLE_ROWS)
        assert _count_rows(clean_test_table) == before + len(SAMPLE_ROWS)

    def test_inserted_value_is_correct(self, clean_test_table):
        """삽입된 값이 올바르게 저장 및 조회되어야 합니다."""
        adapter = clean_test_table
        before = _count_rows(adapter, " WHERE topic = 'factory/line1/temp' AND value = 23.5")
        adapter.insert_batch([SAMPLE_ROWS[0]])
        after = _count_rows(adapter, " WHERE topic = 'factory/line1/temp' AND value = 23.5")
        assert after == before + 1

    def test_insert_empty_batch_does_not_raise(self, clean_test_table):
        """빈 배치를 삽입해도 에러가 발생하지 않아야 합니다."""
        before = _count_rows(clean_test_table)
        clean_test_table.insert_batch([])
        assert _count_rows(clean_test_table) == before

    def test_insert_null_value_allowed(self, clean_test_table):
        """value가 None인 행(Row)도 정상적으로 삽입되어야 합니다."""
        null_row = (
            "factory/line1/temp",
            "factory", "line1", "temp",
            None,
            "2026-06-01T10:00:00+00:00",
            "2026-06-01T10:00:01+00:00",
            json.dumps({}),
        )
        before = _count_rows(clean_test_table, " WHERE value IS NULL")
        clean_test_table.insert_batch([null_row])
        assert _count_rows(clean_test_table, " WHERE value IS NULL") == before + 1

    def test_insert_large_batch(self, clean_test_table):
        """1,000행의 대규모 배치가 정상적으로 삽입되어야 합니다."""
        before = _count_rows(clean_test_table)
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
        clean_test_table.insert_batch(rows)
        assert _count_rows(clean_test_table) == before + 1000


# ══════════════════════════════════════════════════════════
# DB Failover & PQ To DB Tests
# ══════════════════════════════════════════════════════════

class DBFailoverAndPQTest:

    def test_queue_backup_on_db_failure(self, tmp_path):
        """DB 연결 실패 시, 데이터가 파일 큐에 안전하게 보존되어야 합니다."""
        bad_db_cfg = DBConfig.from_env()
        bad_db_cfg.host, bad_db_cfg.port = "invalid.host", 9999
        
        cfg = FileQueue(QueueConfig.from_env())
        cfg.path = tmp_path / "queue_jsonl"
        queue = FileQueue(cfg)
        queue.append({"topic": "test", "value": 23.5, "payload": {}})

        loader = DBLoader(bad_db_cfg, LoaderConfig(batch_size=1, poll_interval=1), queue)
        with pytest.raises(Exception):
            loader._connect(max_attempts=1)
        
        # 파일 존재 여부 확인
        assert cfg.path.exists()
        assert cfg.path.stat().st_size > 0

    def test_existing_pq_data_is_inserted(self, clean_test_table):
        """기존 큐 데이터가 프로세스 실행 시 DB로 삽입되어야 합니다."""
        queue = FileQueue(QueueConfig.from_env())
        # 샘플 레코드 추가
        for record in [{"topic": "f/l1/t", "value": 23.5, "payload": {}}]:
            queue.append(record)

        before = _count_rows(clean_test_table)
        loader = DBLoader(DBConfig.from_env(), LoaderConfig(batch_size=500), queue)
        loader._adapter = clean_test_table
        loader._process()

        assert _count_rows(clean_test_table) > before
        assert QueueConfig.from_env().path.stat().st_size == 0