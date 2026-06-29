"""
loader.py
FileQueue를 폴링하여 DB에 배치 적재한다.
PostgreSQL / MSSQL / Oracle 지원.
"""

import json
import logging
import time
from abc import ABC, abstractmethod

from config import DBConfig, LoaderConfig
from file_queue import FileQueue


# ══════════════════════════════════════════════════════════
# DDL / DML (DB별)
# ══════════════════════════════════════════════════════════

CREATE_TABLE = {
    "postgresql": """
        CREATE TABLE IF NOT EXISTS sensor_data (
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
        CREATE INDEX IF NOT EXISTS idx_sensor_data_ts
            ON sensor_data (ts DESC);
        CREATE INDEX IF NOT EXISTS idx_sensor_data_device
            ON sensor_data (site, device, sensor);
    """,
    "mssql": """
        IF NOT EXISTS (
            SELECT 1 FROM sysobjects WHERE name='sensor_data' AND xtype='U'
        )
        BEGIN
            CREATE TABLE sensor_data (
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
            CREATE INDEX idx_sensor_data_ts
                ON sensor_data (ts DESC);
            CREATE INDEX idx_sensor_data_device
                ON sensor_data (site, device, sensor);
        END
    """,
    "oracle": """
        DECLARE
            v_count INTEGER;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM user_tables WHERE table_name = 'SENSOR_DATA';
            IF v_count = 0 THEN
                EXECUTE IMMEDIATE '
                    CREATE TABLE sensor_data (
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
                    'CREATE INDEX idx_sensor_ts ON sensor_data (ts DESC)';
                EXECUTE IMMEDIATE
                    'CREATE INDEX idx_sensor_device ON sensor_data (site, device, sensor)';
            END IF;
        END;
    """,
}

INSERT_SQL = {
    "postgresql": """
        INSERT INTO sensor_data
            (topic, site, device, sensor, value, ts, received_at, payload)
        VALUES %s
    """,
    "mssql": """
        INSERT INTO sensor_data
            (topic, site, device, sensor, value, ts, received_at, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
    "oracle": """
        INSERT INTO sensor_data
            (topic, site, device, sensor, value, ts, received_at, payload)
        VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
    """,
}


# ══════════════════════════════════════════════════════════
# DB 어댑터 (추상 + 구현)
# ══════════════════════════════════════════════════════════

class BaseDBAdapter(ABC):
    """DB별 연결·DDL·INSERT 차이를 캡슐화한다."""

    def __init__(self, cfg: DBConfig):
        self.cfg = cfg
        self.log = logging.getLogger(self.__class__.__name__)

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
            cur.execute(CREATE_TABLE["postgresql"])
        self._conn.commit()

    def insert_batch(self, rows: list[tuple]):
        import psycopg2.extras
        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, INSERT_SQL["postgresql"], rows)
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
        cur.execute(CREATE_TABLE["mssql"])
        self._conn.commit()
        cur.close()

    def insert_batch(self, rows: list[tuple]):
        cur = self._conn.cursor()
        cur.executemany(INSERT_SQL["mssql"], rows)
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
        cur.execute(CREATE_TABLE["oracle"])
        self._conn.commit()
        cur.close()

    def insert_batch(self, rows: list[tuple]):
        cur = self._conn.cursor()
        cur.executemany(INSERT_SQL["oracle"], rows)
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
    """FileQueue를 폴링하여 DB에 배치 적재한다."""

    def __init__(self, db_cfg: DBConfig, loader_cfg: LoaderConfig, queue: FileQueue):
        self.db_cfg     = db_cfg
        self.loader_cfg = loader_cfg
        self.queue      = queue
        self.log        = logging.getLogger(self.__class__.__name__)
        self._adapter   = None

    def _connect(self):
        self._adapter = make_adapter(self.db_cfg)
        while True:
            try:
                self._adapter.connect()
                self._adapter.ensure_table()
                self.log.info("테이블 준비 완료")
                return
            except Exception as e:
                self.log.warning(f"DB 연결 실패: {e} — 5초 후 재시도")
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
        records = self.queue.flush()
        if not records:
            return
        rows = self._to_rows(records)
        for i in range(0, len(rows), self.loader_cfg.batch_size):
            self._adapter.insert_batch(rows[i : i + self.loader_cfg.batch_size])
        self.log.info(f"{len(rows)}건 INSERT 완료")

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