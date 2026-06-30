"""
tests/mapper_test.py
PayloadMapper tests
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from data_mapper import PayloadMapper


def _make_mapper(mapping: dict) -> PayloadMapper:
    # delete=False로 설정하여 콘텍스트가 끝나도 파일이 유지되도록 함
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", encoding="utf-8", delete=False) as f:
        json.dump({"mapping": mapping}, f)
        tmp_path = Path(f.name)
        
    return PayloadMapper(tmp_path)


BASE_ARGS = dict(
    topic      = "factory/line1/temp",
    topic_meta = {"site": "factory", "device": "line1", "sensor": "temp"},
    received_at= "2026-06-01T00:00:00+00:00",
)


class BasicMappingTest(unittest.TestCase):

    def test_topic_field_mapped(self):
        """메타데이터인 'topic' 필드로부터 값이 정상적으로 매핑되는지 확인"""
        mapper = _make_mapper({"topic": "topic"})
        record = mapper.build_record(payload={}, **BASE_ARGS)
        self.assertEqual(record["topic"], "factory/line1/temp")

    def test_payload_field_mapped(self):
        """페이로드의 최상위(root) 필드 값이 정상적으로 매핑되는지 확인"""
        mapper = _make_mapper({"value": "payload.value"})
        record = mapper.build_record(payload={"value": 23.5}, **BASE_ARGS)
        self.assertEqual(record["value"], 23.5)

    def test_nested_payload_path(self):
        """온점(.) 표기법을 사용한 객체 내부 깊숙한(Nested) 필드 값이 정상 매핑되는지 확인"""
        mapper = _make_mapper({"deep": "payload.a.b.c"})
        record = mapper.build_record(payload={"a": {"b": {"c": 42}}}, **BASE_ARGS)
        self.assertEqual(record["deep"], 42)

    def test_missing_nested_path_returns_none(self):
        """매핑하려는 중첩 경로의 중간 필드가 존재하지 않을 때 안전하게 None을 반환하는지 확인"""
        mapper = _make_mapper({"v": "payload.a.b.c"})
        record = mapper.build_record(payload={"a": {}}, **BASE_ARGS)
        self.assertIsNone(record["v"])


class ReservedKeywordTest(unittest.TestCase):

    def test_received_at_reserved(self):
        """수신 시간 예약어(__received_at__) 매핑 기능 검증"""
        mapper = _make_mapper({"recv": "__received_at__"})
        record = mapper.build_record(payload={}, **BASE_ARGS)
        self.assertEqual(record["recv"], BASE_ARGS["received_at"])

    def test_payload_reserved(self):
        """전체 페이로드 예약어(__payload__) 매핑 기능 검증"""
        payload = {"value": 1, "extra": "x"}
        mapper  = _make_mapper({"raw": "__payload__"})
        record  = mapper.build_record(payload=payload, **BASE_ARGS)
        self.assertEqual(record["raw"], payload)

    def test_topic_site_reserved(self):
        """토픽 내 사이트(Site) 정보 예약어(__topic_site__) 추출 기능 검증"""
        mapper = _make_mapper({"site": "__topic_site__"})
        record = mapper.build_record(payload={}, **BASE_ARGS)
        self.assertEqual(record["site"], "factory")

    def test_topic_device_reserved(self):
        """토픽 내 디바이스(Device) 정보 예약어(__topic_device__) 추출 기능 검증"""
        mapper = _make_mapper({"device": "__topic_device__"})
        record = mapper.build_record(payload={}, **BASE_ARGS)
        self.assertEqual(record["device"], "line1")

    def test_topic_sensor_reserved(self):
        """토픽 내 센서(Sensor) 정보 예약어(__topic_sensor__) 추출 기능 검증"""
        mapper = _make_mapper({"sensor": "__topic_sensor__"})
        record = mapper.build_record(payload={}, **BASE_ARGS)
        self.assertEqual(record["sensor"], "temp")


class ValidationTest(unittest.TestCase):

    def test_missing_mapping_key_raises(self):
        """매핑 파일에 필수 루트 키("mapping")가 누락되었을 때 ValueError가 발생하는지 테스트"""
        tmp = Path(tempfile.mktemp(suffix=".json"))
        tmp.write_text(json.dumps({"wrong_key": {}}), encoding="utf-8")
        with self.assertRaises(ValueError):
            PayloadMapper(tmp)

    def test_empty_mapping_raises(self):
        """매핑 파일이 완전히 비어 있을 때 ValueError가 발생하는지 테스트"""
        tmp = Path(tempfile.mktemp(suffix=".json"))
        tmp.write_text(json.dumps({}), encoding="utf-8")
        with self.assertRaises(ValueError):
            PayloadMapper(tmp)


class EndToEndTest(unittest.TestCase):

    def test_full_default_mapping(self):
        """기본 mapping.json 파일을 사용하여 전체 매핑 프로세스의 E2E 동작을 검증"""
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
