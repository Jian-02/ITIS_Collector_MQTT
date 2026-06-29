"""
tests/mapper_test.py
PayloadMapper 테스트
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from mapper import PayloadMapper


def _make_mapper(mapping: dict) -> PayloadMapper:
    tmp = Path(tempfile.mktemp(suffix=".json"))
    tmp.write_text(json.dumps({"mapping": mapping}), encoding="utf-8")
    return PayloadMapper(tmp)


BASE_ARGS = dict(
    topic      = "factory/line1/temp",
    topic_meta = {"site": "factory", "device": "line1", "sensor": "temp"},
    received_at= "2026-06-01T00:00:00+00:00",
)


class BasicMappingTest(unittest.TestCase):

    def test_topic_field_mapped(self):
        mapper = _make_mapper({"topic": "topic"})
        record = mapper.build_record(payload={}, **BASE_ARGS)
        self.assertEqual(record["topic"], "factory/line1/temp")

    def test_payload_field_mapped(self):
        mapper = _make_mapper({"value": "payload.value"})
        record = mapper.build_record(payload={"value": 23.5}, **BASE_ARGS)
        self.assertEqual(record["value"], 23.5)

    def test_nested_payload_path(self):
        mapper = _make_mapper({"deep": "payload.a.b.c"})
        record = mapper.build_record(payload={"a": {"b": {"c": 42}}}, **BASE_ARGS)
        self.assertEqual(record["deep"], 42)

    def test_missing_nested_path_returns_none(self):
        mapper = _make_mapper({"v": "payload.a.b.c"})
        record = mapper.build_record(payload={"a": {}}, **BASE_ARGS)
        self.assertIsNone(record["v"])


class ReservedKeywordTest(unittest.TestCase):

    def test_received_at_reserved(self):
        mapper = _make_mapper({"recv": "__received_at__"})
        record = mapper.build_record(payload={}, **BASE_ARGS)
        self.assertEqual(record["recv"], BASE_ARGS["received_at"])

    def test_payload_reserved(self):
        payload = {"value": 1, "extra": "x"}
        mapper  = _make_mapper({"raw": "__payload__"})
        record  = mapper.build_record(payload=payload, **BASE_ARGS)
        self.assertEqual(record["raw"], payload)

    def test_topic_site_reserved(self):
        mapper = _make_mapper({"site": "__topic_site__"})
        record = mapper.build_record(payload={}, **BASE_ARGS)
        self.assertEqual(record["site"], "factory")

    def test_topic_device_reserved(self):
        mapper = _make_mapper({"device": "__topic_device__"})
        record = mapper.build_record(payload={}, **BASE_ARGS)
        self.assertEqual(record["device"], "line1")

    def test_topic_sensor_reserved(self):
        mapper = _make_mapper({"sensor": "__topic_sensor__"})
        record = mapper.build_record(payload={}, **BASE_ARGS)
        self.assertEqual(record["sensor"], "temp")


class ValidationTest(unittest.TestCase):

    def test_missing_mapping_key_raises(self):
        tmp = Path(tempfile.mktemp(suffix=".json"))
        tmp.write_text(json.dumps({"wrong_key": {}}), encoding="utf-8")
        with self.assertRaises(ValueError):
            PayloadMapper(tmp)

    def test_empty_mapping_raises(self):
        tmp = Path(tempfile.mktemp(suffix=".json"))
        tmp.write_text(json.dumps({}), encoding="utf-8")
        with self.assertRaises(ValueError):
            PayloadMapper(tmp)


class EndToEndTest(unittest.TestCase):

    def test_full_default_mapping(self):
        """실제 mapping.json 기본값으로 end-to-end 검증"""
        mapping_path = Path(__file__).parent.parent / "mapping.json"
        mapper  = PayloadMapper(mapping_path)
        payload = {"value": 99.9, "ts": "2026-06-01T12:00:00Z"}
        record  = mapper.build_record(
            topic      = "site1/dev1/sensor1",
            payload    = payload,
            topic_meta = {"site": "site1", "device": "dev1", "sensor": "sensor1"},
            received_at= "2026-06-01T12:00:01Z",
        )
        self.assertEqual(record["topic"],       "site1/dev1/sensor1")
        self.assertEqual(record["site"],        "site1")
        self.assertEqual(record["device"],      "dev1")
        self.assertEqual(record["sensor"],      "sensor1")
        self.assertEqual(record["value"],       99.9)
        self.assertEqual(record["ts"],          "2026-06-01T12:00:00Z")
        self.assertEqual(record["received_at"], "2026-06-01T12:00:01Z")
        self.assertEqual(record["payload"],     payload)


if __name__ == "__main__":
    unittest.main(verbosity=2)