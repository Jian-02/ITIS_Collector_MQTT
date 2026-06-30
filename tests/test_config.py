"""
tests/config_test.py
설정(Config) 데이터클래스 테스트
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import MQTTConfig, QueueConfig, LoaderConfig, MapperConfig, SUPPORTED_DB_TYPES


class MQTTConfigTest(unittest.TestCase):

    def test_default_port(self):
        cfg = MQTTConfig()
        self.assertEqual(cfg.port, 1883)

    def test_default_topic(self):
        cfg = MQTTConfig()
        self.assertEqual(cfg.topic, "#")

    def test_custom_values(self):
        cfg = MQTTConfig(host="192.168.1.1", port=8883, topic="factory/#")
        self.assertEqual(cfg.host, "192.168.1.1")
        self.assertEqual(cfg.port, 8883)
        self.assertEqual(cfg.topic, "factory/#")


class QueueConfigTest(unittest.TestCase):

    def test_size_limit_enabled_by_default(self):
        cfg = QueueConfig()
        self.assertTrue(cfg.size_limit_enabled)

    def test_size_limit_can_be_disabled(self):
        cfg = QueueConfig(size_limit_enabled=False)
        self.assertFalse(cfg.size_limit_enabled)

    def test_max_bytes_conversion(self):
        cfg = QueueConfig(max_bytes=50 * 1024 * 1024)
        self.assertEqual(cfg.max_bytes, 50 * 1024 * 1024)


class DBConfigTest(unittest.TestCase):

    def test_unsupported_db_type_raises(self):
        with self.assertRaises(ValueError):
            db_type = "mongodb"
            if db_type not in SUPPORTED_DB_TYPES:
                raise ValueError(f"Unsupported DB_TYPE: {db_type}")

    def test_supported_db_types(self):
        for db_type in ("postgresql", "mssql", "oracle"):
            self.assertIn(db_type, SUPPORTED_DB_TYPES)


class LoaderConfigTest(unittest.TestCase):

    def test_default_batch_size(self):
        cfg = LoaderConfig()
        self.assertEqual(cfg.batch_size, 500)

    def test_default_poll_interval(self):
        cfg = LoaderConfig()
        self.assertEqual(cfg.poll_interval, 5)


class MapperConfigTest(unittest.TestCase):

    def test_default_mapping_path(self):
        cfg = MapperConfig()
        self.assertEqual(cfg.mapping_path, Path("./mapping.json"))

    def test_custom_mapping_path(self):
        cfg = MapperConfig(mapping_path=Path("/custom/mapping.json"))
        self.assertEqual(cfg.mapping_path, Path("/custom/mapping.json"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
