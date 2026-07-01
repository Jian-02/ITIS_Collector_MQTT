"""
config.py
.env 값을 로드하고 application configuration을 제공합니다.
"""

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

env_mode = os.getenv("APP_ENV", "production").strip().lower()

# Check the environment(Production or Test)
if env_mode == "test":
    load_dotenv(dotenv_path=".env.test", override=True)
else:
    load_dotenv(dotenv_path=".env")


def _get(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def _get_bool(key: str, default: bool = True) -> bool:
    return _get(key, str(default)).strip().lower() in ("true", "1", "yes")


# ── MQTT ────────────────────────────────────────────────

@dataclass
class MQTTConfig:
    host:           str = ""
    port:           int = 1883
    topic:          str = "#"
    username:       str = ""
    password:       str = ""
    retry_interval: int = 5   # 연결 실패 시 재시도 대기(초)
    max_retries:    int = 0   # 0 = 무한 재시도. N>0 이면 N회 연속 실패 시 collector를 중단합니다.

    @classmethod
    def from_env(cls) -> "MQTTConfig":
        return cls(
            host           = _get("MQTT_HOST", "localhost"),
            port           = int(_get("MQTT_PORT", "1883")),
            topic          = _get("MQTT_TOPIC", "#"),
            username       = _get("MQTT_USERNAME", ""),
            password       = _get("MQTT_PASSWORD", ""),
            retry_interval = int(_get("MQTT_RETRY_INTERVAL", "5")),
            max_retries    = int(_get("MQTT_MAX_RETRIES", "0")),
        )


# --- File PQ (Persistent Queue) ------------------------------------------------

@dataclass
class QueueConfig:
    path:               Path = Path("./pq/persistent_queue.jsonl")
    size_limit_enabled: bool = True
    max_bytes:          int  = 100 * 1024 * 1024   # size_limit_enabled=True 일 때만 적용됨

    # 최소 용량(MIN_MAX_MB)은 1MB로 설정
    MIN_MAX_MB = 1

    @classmethod
    def from_env(cls) -> "QueueConfig":
        size_limit_enabled = _get_bool("PQ_SIZE_LIMIT_ENABLED", True)
        max_mb = int(_get("PQ_MAX_MB", "100"))
        if size_limit_enabled and max_mb < cls.MIN_MAX_MB:
            logging.getLogger("config").warning(
                f"PQ_MAX_MB={max_mb} is below the minimum ({cls.MIN_MAX_MB}MB). "
                f"Clamping to {cls.MIN_MAX_MB}MB."
            )
            max_mb = cls.MIN_MAX_MB

        return cls(
            path               = Path(_get("PQ_PATH", "./pq/persistent_queue.jsonl")),
            size_limit_enabled = size_limit_enabled,
            max_bytes          = max_mb * 1024 * 1024,
        )


# ── DB ──────────────────────────────────────────────────

SUPPORTED_DB_TYPES = ("postgresql", "mssql", "oracle")


@dataclass
class DBConfig:
    db_type:      str = "postgresql"
    host:         str = "localhost"
    port:         int = 5432
    name:         str = ""
    user:         str = ""
    password:     str = ""
    table_name:   str = "sensor_data"
    mssql_driver: str = "ODBC Driver 17 for SQL Server"  # MSSQL only

    @classmethod
    def from_env(cls) -> "DBConfig":
        db_type = _get("DB_TYPE", "postgresql").lower()
        if db_type not in SUPPORTED_DB_TYPES:
            raise ValueError(f"Unsupported DB_TYPE: {db_type} (supported: {SUPPORTED_DB_TYPES})")

        default_port = {"postgresql": "5432", "mssql": "1433", "oracle": "1521"}

        return cls(
            db_type      = db_type,
            host         = _get("DB_HOST", "localhost"),
            port         = int(_get("DB_PORT", default_port[db_type])),
            name         = _get("DB_NAME", ""),
            user         = _get("DB_USER", ""),
            password     = _get("DB_PASSWORD", ""),
            table_name   = _get("DB_TABLE_NAME", "sensor_data"),
            mssql_driver = _get("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server"),
        )


# ── Loader ──────────────────────────────────────────────

@dataclass
class LoaderConfig:
    batch_size:     int = 500
    poll_interval:  int = 5
    retry_interval: int = 5   # DB 연결/재연결 실패 시 재시도 대기(초)
    max_retries:    int = 0   # 0 = 무한 재시도(운영 기본값). N>0 이면 N회 연속 실패 시 loader를 중단합니다.

    @classmethod
    def from_env(cls) -> "LoaderConfig":
        return cls(
            batch_size     = int(_get("BATCH_SIZE", "500")),
            poll_interval  = int(_get("POLL_INTERVAL", "5")),
            retry_interval = int(_get("LOADER_RETRY_INTERVAL", "5")),
            max_retries    = int(_get("LOADER_MAX_RETRIES", "0")),
        )


# --- Logging -----------------------------------------------

def get_log_level() -> int:
    return getattr(logging, _get("LOG_LEVEL", "INFO").upper(), logging.INFO)


# ── Mapper ──────────────────────────────────────────────

@dataclass
class MapperConfig:
    mapping_path: Path = Path("./mapping.json")

    @classmethod
    def from_env(cls) -> "MapperConfig":
        return cls(
            mapping_path = Path(_get("MAPPING_PATH", "./mapping.json")),
        )


# ── Log ─────────────────────────────────────────────────

@dataclass
class LogConfig:
    log_dir:       Path = Path("./logs")
    max_bytes:     int  = 100 * 1024 * 1024  # 파일당 Maximum size
    max_files:     int  = 30                  # 보관되는 파일의 Maximum 개수
    level:         int  = logging.INFO

    @classmethod
    def from_env(cls) -> "LogConfig":
        return cls(
            log_dir   = Path(_get("LOG_DIR", "./logs")),
            max_bytes = int(_get("LOG_MAX_MB", "100")) * 1024 * 1024,
            max_files = int(_get("LOG_MAX_FILES", "30")),
            level     = getattr(logging, _get("LOG_LEVEL", "INFO").upper(), logging.INFO),
        )