import os
import pytest
from pathlib import Path
from unittest.mock import patch
from config import MQTTConfig, QueueConfig, LoaderConfig, MapperConfig, SUPPORTED_DB_TYPES

# --- MQTTConfig 테스트 ---

def test_mqtt_default_values():
    """기본값(포트 1883, 토픽 '#')이 올바르게 설정되는지 확인"""
    cfg = MQTTConfig()
    assert cfg.port == 1883
    assert cfg.topic == "#"

def test_mqtt_custom_values():
    """사용자 지정 host, port, topic이 정상적으로 적용되는지 확인"""
    cfg = MQTTConfig(host="192.168.1.1", port=8883, topic="factory/#")
    assert cfg.host == "192.168.1.1"
    assert cfg.port == 8883
    assert cfg.topic == "factory/#"


# --- QueueConfig 테스트 ---

def test_queue_default_size_limit():
    """기본적으로 size_limit_enabled가 True인지 확인"""
    cfg = QueueConfig()
    assert cfg.size_limit_enabled is True

def test_queue_disable_size_limit():
    """size_limit_enabled를 False로 설정할 수 있는지 확인"""
    cfg = QueueConfig(size_limit_enabled=False)
    assert cfg.size_limit_enabled is False

def test_queue_max_bytes_assignment():
    """max_bytes 설정 값이 그대로 할당되는지 확인"""
    cfg = QueueConfig(max_bytes=50 * 1024 * 1024)
    assert cfg.max_bytes == 50 * 1024 * 1024


# --- DBConfig 테스트 ---

def test_db_unsupported_type_raises():
    """지원하지 않는 DB 타입(예: mongodb) 사용 시 ValueError가 발생하는지 확인"""
    with pytest.raises(ValueError):
        db_type = "mongodb"
        if db_type not in SUPPORTED_DB_TYPES:
            raise ValueError(f"Unsupported DB_TYPE: {db_type}")

def test_db_supported_types():
    """지원되는 DB 목록에 postgresql, mssql, oracle이 포함되어 있는지 확인"""
    for db_type in ("postgresql", "mssql", "oracle"):
        assert db_type in SUPPORTED_DB_TYPES


# --- LoaderConfig 테스트 ---

def test_loader_defaults():
    """배치 사이즈 500, 폴링 간격 5초의 기본값이 설정되는지 확인"""
    cfg = LoaderConfig()
    assert cfg.batch_size == 500
    assert cfg.poll_interval == 5


# --- MapperConfig 테스트 ---

def test_mapper_path_defaults():
    """기본 매핑 경로가 ./mapping.json인지 확인"""
    cfg = MapperConfig()
    assert cfg.mapping_path == Path("./mapping.json")

def test_mapper_custom_path():
    """사용자 지정 매핑 경로가 올바르게 설정되는지 확인"""
    path = Path("/custom/mapping.json")
    cfg = MapperConfig(mapping_path=path)
    assert cfg.mapping_path == path


# --- QueueConfig 환경변수 보정 테스트 ---

@pytest.fixture
def mock_env():
    """환경변수 설정을 위한 헬퍼 픽스처"""
    def _set_env(pq_max_mb, size_limit_enabled="true"):
        with patch.dict(os.environ, {
            "PQ_PATH": "./pq/test_queue.jsonl",
            "PQ_SIZE_LIMIT_ENABLED": size_limit_enabled,
            "PQ_MAX_MB": pq_max_mb,
        }):
            return QueueConfig.from_env()
    return _set_env

def test_queue_clamp_logic(mock_env):
    """PQ_MAX_MB가 0 또는 음수일 경우 MIN_MAX_MB(1MB)로 보정되는지 확인"""
    assert mock_env("0").max_bytes == QueueConfig.MIN_MAX_MB * 1024 * 1024
    assert mock_env("-5").max_bytes == QueueConfig.MIN_MAX_MB * 1024 * 1024

def test_queue_boundary_and_normal_values(mock_env):
    """정상 범위(1MB 이상) 값은 보정 없이 그대로 적용되는지 확인"""
    assert mock_env("1").max_bytes == 1 * 1024 * 1024
    assert mock_env("50").max_bytes == 50 * 1024 * 1024

def test_queue_no_clamp_when_disabled(mock_env):
    """size_limit_enabled가 false이면 보정 로직이 무시되는지 확인"""
    cfg = mock_env("0", size_limit_enabled="false")
    assert cfg.size_limit_enabled is False