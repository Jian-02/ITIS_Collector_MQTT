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
# DB connection tests
# ══════════════════════════════════════════════════════════

class DBConnectionTest:

    def test_connection_success(self, db_adapter):
        """데이터베이스 연결에 성공해야 합니다."""
        assert db_adapter._conn is not None

    def test_ensure_table_creates_sensor_data(self, clean_test_table):
        """sensor_data_test 테이블이 정상적으로 생성되어야 합니다."""
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
        """단일 삽입된 레코드가 정상적으로 조회되어야 합니다."""
        adapter = clean_test_table
        before  = _count_rows(adapter)
        adapter.insert_batch([SAMPLE_ROWS[0]])
        assert _count_rows(adapter) == before + 1

    def test_insert_multiple_rows(self, clean_test_table):
        """여러 레코드를 삽입한 후, 전체 개수가 일치해야 합니다."""
        adapter = clean_test_table
        before  = _count_rows(adapter)
        adapter.insert_batch(SAMPLE_ROWS)
        assert _count_rows(adapter) == before + len(SAMPLE_ROWS)

    def test_inserted_value_is_correct(self, clean_test_table):
        """삽입된 값이 올바르게 저장 및 조회되어야 합니다."""
        adapter = clean_test_table
        before  = _count_rows(adapter, " WHERE topic = 'factory/line1/temp' AND value = 23.5")
        adapter.insert_batch([SAMPLE_ROWS[0]])
        after   = _count_rows(adapter, " WHERE topic = 'factory/line1/temp' AND value = 23.5")
        assert after == before + 1

    def test_insert_empty_batch_does_not_raise(self, clean_test_table):
        """빈 배치를 삽입해도 에러가 발생하지 않아야 합니다."""
        adapter = clean_test_table
        before  = _count_rows(adapter)
        try:
            adapter.insert_batch([])
        except Exception as e:
            pytest.fail(f"Empty batch INSERT error: {e}")
        assert _count_rows(adapter) == before

    def test_insert_null_value_allowed(self, clean_test_table):
        """value가 None인 행(Row)도 정상적으로 삽입되어야 합니다."""
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
        """1,000행의 대규모 배치가 정상적으로 삽입되어야 합니다."""
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
# DB 장애 조치(Failover) 및 파일 큐(FileQueue) 백업 테스트
# ══════════════════════════════════════════════════════════

class DBFailoverTest:

    def test_queue_backup_on_db_failure(self, tmp_path):
        """DB 연결이 실패했을 때, 데이터가 파일 큐(FileQueue)에 안전하게 보존되어야 합니다."""
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
# PQ → DB 로드 테스트
# (파일에 이미 존재하는 데이터가 DB로 올바르게 푸시되는지 검증)
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
        프로그램이 시작되기 전에 파일 큐(PQ) 파일에 이미 누적되어 있던 데이터가
        _process()가 호출될 때 DB에 성공적으로 INSERT되어야 합니다.
        """
        adapter   = clean_test_table
        queue_cfg = QueueConfig.from_env()
        queue     = FileQueue(queue_cfg)
 
        # Figure out the count already in the PQ before adding SAMPLE_RECORDS
        existing  = queue.peek()
        queue.rollback()
        expected  = len(existing) + len(SAMPLE_RECORDS)
 
        for record in SAMPLE_RECORDS:
            queue.append(record)
        assert queue_cfg.path.stat().st_size > 0, "No data in the PQ file"

        before = _count_rows(adapter)

        # DBLoader를 통해 처리
        db_cfg = DBConfig.from_env()
        db_cfg.table_name = adapter.table_name
        loader = DBLoader(db_cfg, LoaderConfig(batch_size=500, poll_interval=1), queue)
        loader._adapter = adapter   # 이미 연결된 어댑터 재사용
        loader._process()

        # DB에 정상적으로 반영되었는지 검증
        assert _count_rows(adapter) == before + expected, \
            "Records that were in the PQ were not INSERTed into the DB"
        assert queue_cfg.path.stat().st_size == 0, \
            "PQ file was not emptied after a successful INSERT"

    def test_pq_file_preserved_on_db_failure(self, tmp_path):
        """
        # DB INSERT가 실패했을 때, 파일 큐(PQ) 파일의 데이터가 다시 대기(Queued) 상태로 복구되어야 합니다.
        """
        queue_cfg = QueueConfig.from_env()
        queue     = FileQueue(queue_cfg)
 
        # 테스트 시작 전 파일 큐(PQ)에 이미 존재하는 데이터 개수 확인
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

        # 파일이 그대로 남아 있고 대기(Queued) 상태로 복구되었는지 검증
        assert queue_cfg.path.stat().st_size > 0, "PQ file was emptied after a failure (data loss)"

        lines = queue_cfg.path.read_text(encoding="utf-8").splitlines()
        objs  = [json.loads(l) for l in lines if l.strip()]
        assert all(o["_s"] == "Q" for o in objs), \
            "Pending(_s=P) line(s) remain after a failure (rollback did not run)"
        assert len(objs) == len(SAMPLE_RECORDS), \
            f"Restored record count mismatch: {len(objs)} != {len(SAMPLE_RECORDS)}"

    def test_pq_data_inserted_after_reconnect(self, clean_test_table, tmp_path):
        """
        DB 재연결 후, 파일 큐(PQ)에 남아 있는 데이터가 성공적으로 INSERT되어야 합니다.
        (rollback → reconnect → reprocess scenario)
        """
        adapter   = clean_test_table
        queue_cfg = QueueConfig.from_env()
        queue     = FileQueue(queue_cfg)
 
        # 테스트 시작 전 파일 큐(PQ)에 이미 존재하는 데이터 개수 확인
        existing = queue.peek()
        queue.rollback()
        expected = len(existing) + len(SAMPLE_RECORDS)

        # 1차 시도: DB 연결 실패 → 롤백
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

        # 2차 시도: 정상 작동하는 DB로 재처리
        db_cfg        = DBConfig.from_env()
        db_cfg.table_name = adapter.table_name
        good_loader   = DBLoader(db_cfg, LoaderConfig(batch_size=500, poll_interval=1), queue)
        good_loader._adapter = adapter
        good_loader._process()

        assert _count_rows(adapter) == before + len(SAMPLE_RECORDS), \
            "PQ data was not INSERTed into the DB after reconnecting"
        assert queue_cfg.path.stat().st_size == 0, \
            "INSERT succeeded after reconnecting but the PQ file was not emptied"


# ══════════════════════════════════════════════════════════
# pytest 클래스에 픽스처(fixture) 주입
# ══════════════════════════════════════════════════════════

class TestDBConnection(DBConnectionTest):
    pass

class TestDBInsert(DBInsertTest):
    pass

class TestDBFailover(DBFailoverTest):
    pass

class TestPQToDB(PQToDBTest):
    pass