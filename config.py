"""
config.py
.env 파일을 읽어 전체 설정을 제공한다.
"""

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _get(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def _get_bool(key: str, default: bool = True) -> bool:
    return _get(key, str(default)).strip().lower() in ("true", "1", "yes")


# ── MQTT ────────────────────────────────────────────────

@dataclass
class MQTTConfig:
    host:     str = ""
    port:     int = 1883
    topic:    str = "#"
    username: str = ""
    password: str = ""

    @classmethod
    def from_env(cls) -> "MQTTConfig":
        return cls(
            host     = _get("MQTT_HOST", "localhost"),
            port     = int(_get("MQTT_PORT", "1883")),
            topic    = _get("MQTT_TOPIC", "#"),
            username = _get("MQTT_USERNAME", ""),
            password = _get("MQTT_PASSWORD", ""),
        )


# ── 파일 PQ ─────────────────────────────────────────────

@dataclass
class QueueConfig:
    path:               Path = Path("./pq/queue.jsonl")
    size_limit_enabled: bool = True
    max_bytes:          int  = 100 * 1024 * 1024   # size_limit_enabled=True 일 때만 적용

    @classmethod
    def from_env(cls) -> "QueueConfig":
        return cls(
            path               = Path(_get("PQ_PATH", "./pq/queue.jsonl")),
            size_limit_enabled = _get_bool("PQ_SIZE_LIMIT_ENABLED", True),
            max_bytes          = int(_get("PQ_MAX_MB", "100")) * 1024 * 1024,
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
    mssql_driver: str = "ODBC Driver 17 for SQL Server"  # MSSQL 전용

    @classmethod
    def from_env(cls) -> "DBConfig":
        db_type = _get("DB_TYPE", "postgresql").lower()
        if db_type not in SUPPORTED_DB_TYPES:
            raise ValueError(f"지원하지 않는 DB_TYPE: {db_type} (지원: {SUPPORTED_DB_TYPES})")

        default_port = {"postgresql": "5432", "mssql": "1433", "oracle": "1521"}

        return cls(
            db_type      = db_type,
            host         = _get("DB_HOST", "localhost"),
            port         = int(_get("DB_PORT", default_port[db_type])),
            name         = _get("DB_NAME", ""),
            user         = _get("DB_USER", ""),
            password     = _get("DB_PASSWORD", ""),
            mssql_driver = _get("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server"),
        )


# ── Loader ──────────────────────────────────────────────

@dataclass
class LoaderConfig:
    batch_size:    int = 500
    poll_interval: int = 5

    @classmethod
    def from_env(cls) -> "LoaderConfig":
        return cls(
            batch_size    = int(_get("BATCH_SIZE", "500")),
            poll_interval = int(_get("POLL_INTERVAL", "5")),
        )


# ── 로그 ────────────────────────────────────────────────

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
    max_bytes:     int  = 100 * 1024 * 1024  # 파일당 최대 용량
    max_files:     int  = 30                  # 최대 보관 파일 수
    level:         int  = logging.INFO

    @classmethod
    def from_env(cls) -> "LogConfig":
        return cls(
            log_dir   = Path(_get("LOG_DIR", "./logs")),
            max_bytes = int(_get("LOG_MAX_MB", "100")) * 1024 * 1024,
            max_files = int(_get("LOG_MAX_FILES", "30")),
            level     = getattr(logging, _get("LOG_LEVEL", "INFO").upper(), logging.INFO),
        )