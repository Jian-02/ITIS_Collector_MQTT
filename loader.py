"""
loader.py
FileQueue를 폴링하여 레코드를 배치 단위로 데이터베이스에 로드합니다.
PostgreSQL, MSSQL, Oracle을 지원합니다.

Two-phase commit flow
──────────────────────────────────────────────────
  1. queue.peek()           → Queued 레코드를 읽고 파일 내 상태를 Pending으로 변경
  2. adapter.insert_batch() → DB INSERT 시도
  3. 성공 시 → queue.commit()   : 파일에서 Pending 라인을 제거
     실패 시 → queue.rollback() : Pending 라인을 Queued로 복구한 후, 재연결을 위해 대기
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
# DB adapters (abstract base 및 구현체)
# ══════════════════════════════════════════════════════════

class BaseDBAdapter(ABC):
    """데이터베이스별 연결, DDL 및 INSERT 동작을 캡슐화합니다."""

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
        self.log.info("PostgreSQL connection successful")

    def ensure_table(self):
        with self._conn.cursor() as cur:
            cur.execute(self._create_table_sql())
        self._conn.commit()

    def insert_batch(self, rows: list[tuple]):
        if not rows:
            return
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
        self.log.info("MSSQL connection successful")

    def ensure_table(self):
        cur = self._conn.cursor()
        cur.execute(self._create_table_sql())
        self._conn.commit()
        cur.close()

    def insert_batch(self, rows: list[tuple]):
        if not rows:
            return
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
        self.log.info("Oracle connection successful")

    def ensure_table(self):
        cur = self._conn.cursor()
        cur.execute(self._create_table_sql())
        self._conn.commit()
        cur.close()

    def insert_batch(self, rows: list[tuple]):
        if not rows:
            return
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
    FileQueue를 폴링하여 레코드를 배치 단위로 데이터베이스에 로드합니다.

    Two-phase commit 방식을 사용하여 데이터 유실을 방지합니다:
    peek() → insert_batch() → commit()   (성공 경로)
    peek() → insert_batch() → rollback() (실패 경로)
    """

    def __init__(self, db_cfg: DBConfig, loader_cfg: LoaderConfig, queue: FileQueue):
        self.db_cfg     = db_cfg
        self.loader_cfg = loader_cfg
        self.queue      = queue
        self.log        = logging.getLogger(self.__class__.__name__)
        self._adapter   = None
        self._running = True
        self._consecutive_failures = 0

    def stop(self):
        self._running = False
        self.log.info("Stopping DBLoader...")

    def start(self):
        self._running = True

    def _connect(self, max_attempts: int = 0):
        """
        DB에 연결하고 테이블을 준비합니다.

        Parameters
        ----------
        max_attempts : int
            0 (기본값) → LoaderConfig.max_retries 정책을 따릅니다
                         (max_retries=0 이면 무한 재시도, 그 외는 해당 횟수만큼 시도 후 예외 발생).
            1 이상     → 지정된 횟수만큼만 시도한 후, 마지막 예외를 발생시킵니다 (테스트 모드 등에서 명시적으로 override).
        """
        effective_max = max_attempts if max_attempts else self.loader_cfg.max_retries

        self._adapter = make_adapter(self.db_cfg)
        attempt = 0
        while True:
            attempt += 1
            try:
                self._adapter.connect()
                self._adapter.ensure_table()
                self.log.info("Table ready")
                return
            except Exception as e:
                self.log.warning(f"DB connection failed (attempt {attempt}): {e}")
                if effective_max and attempt >= effective_max:
                    self.log.error(
                        f"DB connection failed {attempt} times in a row (limit={effective_max}). Giving up."
                    )
                    raise
                self.log.warning(f"Retrying in {self.loader_cfg.retry_interval} seconds")
                time.sleep(self.loader_cfg.retry_interval)

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
        # ── Phase 1: queue에서 읽기 (파일 상태: Queued → Pending) ──
        records = self.queue.peek()
        if not records:
            return

        rows = self._to_rows(records)

        try:
            # ── Phase 2: DB INSERT (batch_size 단위로 분할하여 처리) ──
            for i in range(0, len(rows), self.loader_cfg.batch_size):
                self._adapter.insert_batch(rows[i : i + self.loader_cfg.batch_size])

            # ── Phase 3 (성공): 파일에서 Pending 라인 제거 ────────────
            self.queue.commit()
            self.log.info(f"Inserted {len(rows)} record(s)")

        except Exception as e:
            # ── Phase 3 (실패): Pending → Queued 상태로 복구 ───────────────
            self.log.error(f"INSERT failed, restoring Pending records to Queued: {e}")
            self.queue.rollback()
            # Re-raise so the run() loop will reconnect
            raise

    def run(self):
        self._connect()
        self.log.info(f"Starting polling (interval: {self.loader_cfg.poll_interval}s, DB: {self.db_cfg.db_type})")

        while self._running:
            try:
                self._process()
                self._consecutive_failures = 0
            except Exception as e:
                self._consecutive_failures += 1
                self.log.error(
                    f"DB error ({self._consecutive_failures} consecutive failure(s)): {e} — "
                    f"attempting to reconnect in {self.loader_cfg.retry_interval}s"
                )
                time.sleep(self.loader_cfg.retry_interval)
                try:
                    self._connect()
                except Exception:
                    # max_retries에 도달해 _connect()가 예외를 던진 경우: loader를 중단합니다.
                    self.log.error(
                        "DB reconnection limit reached. Stopping DBLoader "
                        "(queued data remains safely on disk in the PQ file)."
                    )
                    self._running = False
                    break

            time.sleep(self.loader_cfg.poll_interval)

        self.log.info("DBLoader stopped.")
        self._disconnect()  # 루프 종료 후 DB 연결 종료 로직(필요 시)

    def _disconnect(self):
        if self._adapter is not None:
            try:
                self._adapter.close()
            except Exception as e:
                self.log.warning(f"Error while closing DB connection: {e}")