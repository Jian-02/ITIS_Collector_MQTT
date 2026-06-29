"""
loader.py
Polls FileQueue and loads records into the database in batches.
Supports PostgreSQL, MSSQL, and Oracle.

2단계 커밋 흐름
──────────────────────────────────────────────────
  1. queue.peek()          → Queued 레코드 읽기 + 파일 내 상태를 Pending으로 전환
  2. adapter.insert_batch()→ DB INSERT 시도
  3. 성공 → queue.commit()  : Pending 줄 파일에서 제거
     실패 → queue.rollback(): Pending 줄을 Queued로 복원 후 재연결 대기
──────────────────────────────────────────────────
"""

import json
import logging
import re
import time
from abc import ABC, abstractmethod

from config import DBConfig, LoaderConfig
from file_queue import FileQueue


# ══════════════════════════════════════════════════════════
# DDL / DML by database
# ══════════════════════════════════════════════════════════

CREATE_TABLE = {
    "postgresql": """
        CREATE TABLE IF NOT EXISTS {table} (
            id          BIGSERIAL PRIMARY KEY,
            topic       TEXT,
            site        TEXT,
            device      TEXT,
            sensor      TEXT,
            value       DOUBLE PRECISION,
            ts          TIMESTAMPTZ,
            received_at TIMESTAMPTZ,
            payload     JSONB
        );
        CREATE INDEX IF NOT EXISTS {idx_prefix}_ts
            ON {table} (ts DESC);
        CREATE INDEX IF NOT EXISTS {idx_prefix}_device
            ON {table} (site, device, sensor);
    """,
    "mssql": """
        IF NOT EXISTS (
            SELECT 1 FROM sysobjects WHERE name='{table}' AND xtype='U'
        )
        BEGIN
            CREATE TABLE {table} (
                id          BIGINT IDENTITY(1,1) PRIMARY KEY,
                topic       NVARCHAR(MAX),
                site        NVARCHAR(255),
                device      NVARCHAR(255),
                sensor      NVARCHAR(255),
                value       FLOAT,
                ts          DATETIMEOFFSET,
                received_at DATETIMEOFFSET,
                payload     NVARCHAR(MAX)
            );
            CREATE INDEX {idx_prefix}_ts
                ON {table} (ts DESC);
            CREATE INDEX {idx_prefix}_device
                ON {table} (site, device, sensor);
        END
    """,
    "oracle": """
        DECLARE
            v_count INTEGER;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM user_tables WHERE table_name = '{table_upper}';
            IF v_count = 0 THEN
                EXECUTE IMMEDIATE '
                    CREATE TABLE {table} (
                        id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        topic       CLOB,
                        site        VARCHAR2(255),
                        device      VARCHAR2(255),
                        sensor      VARCHAR2(255),
                        value       BINARY_DOUBLE,
                        ts          TIMESTAMP WITH TIME ZONE,
                        received_at TIMESTAMP WITH TIME ZONE,
                        payload     CLOB
                    )
                ';
                EXECUTE IMMEDIATE
                    'CREATE INDEX {idx_prefix}_ts ON {table} (ts DESC)';
                EXECUTE IMMEDIATE
                    'CREATE INDEX {idx_prefix}_device ON {table} (site, device, sensor)';
            END IF;
        END;
    """,
}

INSERT_SQL = {
    "postgresql": """
        INSERT INTO {table}
            (topic, site, device, sensor, value, ts, received_at, payload)
        VALUES %s
    """,
    "mssql": """
        INSERT INTO {table}
            (topic, site, device, sensor, value, ts, received_at, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
    "oracle": """
        INSERT INTO {table}
            (topic, site, device, sensor, value, ts, received_at, payload)
        VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
    """,
}

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str) -> str:
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"Invalid DB table name: {name}")
    return name


def _format_sql(template: str, table_name: str) -> str:
    table = _validate_identifier(table_name)
    return template.format(
        table=table,
        table_upper=table.upper(),
        idx_prefix=f"idx_{table}",
    )


# ══════════════════════════════════════════════════════════
# DB adapters (abstract base and implementations)
# ══════════════════════════════════════════════════════════

class BaseDBAdapter(ABC):
    """Encapsulates database-specific connection, DDL, and INSERT behavior."""

    def __init__(self, cfg: DBConfig):
        self.cfg = cfg
        self.table_name = _validate_identifier(cfg.table_name)
        self.log = logging.getLogger(self.__class__.__name__)

    def _create_table_sql(self) -> str:
        return _format_sql(CREATE_TABLE[self.cfg.db_type], self.table_name)

    def _insert_sql(self) -> str:
        return _format_sql(INSERT_SQL[self.cfg.db_type], self.table_name)

    @abstractmethod
    def connect(self): ...

    @abstractmethod
    def ensure_table(self): ...

    @abstractmethod
    def insert_batch(self, rows: list[tuple]): ...

    @abstractmethod
    def close(self): ...


class PostgreSQLAdapter(BaseDBAdapter):

    def connect(self):
        import psycopg2
        cfg = self.cfg
        self._conn = psycopg2.connect(
            host=cfg.host, port=cfg.port,
            dbname=cfg.name, user=cfg.user, password=cfg.password,
        )
        self.log.info("PostgreSQL 연결 성공")

    def ensure_table(self):
        with self._conn.cursor() as cur:
            cur.execute(self._create_table_sql())
        self._conn.commit()

    def insert_batch(self, rows: list[tuple]):
        import psycopg2.extras
        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, self._insert_sql(), rows)
        self._conn.commit()

    def close(self):
        self._conn.close()


class MSSQLAdapter(BaseDBAdapter):

    def connect(self):
        import pyodbc
        cfg = self.cfg
        conn_str = (
            f"DRIVER={{{cfg.mssql_driver}}};"
            f"SERVER={cfg.host},{cfg.port};"
            f"DATABASE={cfg.name};"
            f"UID={cfg.user};"
            f"PWD={cfg.password}"
        )
        self._conn = pyodbc.connect(conn_str)
        self.log.info("MSSQL 연결 성공")

    def ensure_table(self):
        cur = self._conn.cursor()
        cur.execute(self._create_table_sql())
        self._conn.commit()
        cur.close()

    def insert_batch(self, rows: list[tuple]):
        cur = self._conn.cursor()
        cur.executemany(self._insert_sql(), rows)
        self._conn.commit()
        cur.close()

    def close(self):
        self._conn.close()


class OracleAdapter(BaseDBAdapter):

    def connect(self):
        import cx_Oracle
        cfg = self.cfg
        dsn = cx_Oracle.makedsn(cfg.host, cfg.port, service_name=cfg.name)
        self._conn = cx_Oracle.connect(user=cfg.user, password=cfg.password, dsn=dsn)
        self.log.info("Oracle 연결 성공")

    def ensure_table(self):
        cur = self._conn.cursor()
        cur.execute(self._create_table_sql())
        self._conn.commit()
        cur.close()

    def insert_batch(self, rows: list[tuple]):
        cur = self._conn.cursor()
        cur.executemany(self._insert_sql(), rows)
        self._conn.commit()
        cur.close()

    def close(self):
        self._conn.close()


def make_adapter(cfg: DBConfig) -> BaseDBAdapter:
    adapters = {
        "postgresql": PostgreSQLAdapter,
        "mssql":      MSSQLAdapter,
        "oracle":     OracleAdapter,
    }
    return adapters[cfg.db_type](cfg)


# ══════════════════════════════════════════════════════════
# DBLoader
# ══════════════════════════════════════════════════════════

class DBLoader:
    """
    Polls FileQueue and loads records into the database in batches.

    2단계 커밋으로 데이터 유실을 방지:
      peek() → insert_batch() → commit()   (성공 경로)
      peek() → insert_batch() → rollback() (실패 경로)
    """

    def __init__(self, db_cfg: DBConfig, loader_cfg: LoaderConfig, queue: FileQueue):
        self.db_cfg     = db_cfg
        self.loader_cfg = loader_cfg
        self.queue      = queue
        self.log        = logging.getLogger(self.__class__.__name__)
        self._adapter   = None

    def _connect(self, max_attempts: int = 0):
        """
        DB에 연결하고 테이블을 준비한다.

        Parameters
        ----------
        max_attempts : int
            0 (기본값) → 성공할 때까지 무한 재시도 (운영 모드)
            1 이상     → 해당 횟수만큼만 시도 후 마지막 예외를 raise (테스트 모드)
        """
        self._adapter = make_adapter(self.db_cfg)
        attempt = 0
        while True:
            attempt += 1
            try:
                self._adapter.connect()
                self._adapter.ensure_table()
                self.log.info("테이블 준비 완료")
                return
            except Exception as e:
                self.log.warning(f"DB 연결 실패 ({attempt}회): {e}")
                if max_attempts and attempt >= max_attempts:
                    raise
                self.log.warning("5초 후 재시도")
                time.sleep(5)

    def _to_rows(self, records: list) -> list[tuple]:
        return [
            (
                r.get("topic"),
                r.get("site"),
                r.get("device"),
                r.get("sensor"),
                r.get("value"),
                r.get("ts"),
                r.get("received_at"),
                json.dumps(r.get("payload"), ensure_ascii=False),
            )
            for r in records
        ]

    def _process(self):
        # ── Phase 1: 큐에서 읽기 (파일 내 상태: Queued → Pending) ──
        records = self.queue.peek()
        if not records:
            return

        rows = self._to_rows(records)

        try:
            # ── Phase 2: DB INSERT ──────────────────────────────────
            for i in range(0, len(rows), self.loader_cfg.batch_size):
                self._adapter.insert_batch(rows[i : i + self.loader_cfg.batch_size])

            # ── Phase 3 (성공): Pending 줄 파일에서 제거 ────────────
            self.queue.commit()
            self.log.info(f"{len(rows)}건 INSERT 완료")

        except Exception as e:
            # ── Phase 3 (실패): Pending → Queued 복원 ───────────────
            self.log.error(f"INSERT 실패, Pending 레코드를 Queued로 복원: {e}")
            self.queue.rollback()
            # run() 루프가 재연결하도록 예외를 다시 던짐
            raise

    def run(self):
        self._connect()
        self.log.info(f"폴링 시작 (간격: {self.loader_cfg.poll_interval}초, DB: {self.db_cfg.db_type})")

        while True:
            try:
                self._process()
            except Exception as e:
                self.log.error(f"DB 오류: {e} — 재연결 시도")
                self._connect()

            time.sleep(self.loader_cfg.poll_interval)