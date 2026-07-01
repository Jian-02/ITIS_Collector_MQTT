"""
tests/test_mqtt_collector.py
MQTTCollector 단위 테스트

커버리지
  - _on_message 예외 격리: 매핑 실패 / QueueFullError / 예상 밖 예외 / 정상 메시지
  - run() 재시도 한도: max_retries=N 시 N회 후 중단, max_retries=0 시 무한, 성공 시 카운터 리셋
"""

import json
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from config import MQTTConfig, QueueConfig, MapperConfig
from file_queue import FileQueue, QueueFullError
from mqtt_collector import MQTTCollector

# ── 픽스처(Fixture) 정의 ────────────────────────────────────

@pytest.fixture
def tmp_dir():
    path = Path(tempfile.mkdtemp())
    yield path
    shutil.rmtree(path, ignore_errors=True)

@pytest.fixture
def queue(tmp_dir):
    cfg = QueueConfig(path=tmp_dir / "queue.jsonl", size_limit_enabled=False)
    return FileQueue(cfg)

@pytest.fixture
def collector(tmp_dir, queue):
    mapping = {"topic": "topic", "value": "payload.value", "site": "__topic_site__"}
    mapping_path = tmp_dir / "mapping.json"
    with open(mapping_path, "w", encoding="utf-8") as f:
        json.dump({"mapping": mapping}, f)

    return MQTTCollector(
        MQTTConfig(host="localhost", port=1883),
        MapperConfig(mapping_path=mapping_path),
        queue,
    )

# ── 헬퍼 함수 ────────────────────────────────────────────

def _make_fake_msg(topic: str, payload: bytes) -> MagicMock:
    msg = MagicMock()
    msg.topic = topic
    msg.payload = payload
    return msg

# ══════════════════════════════════════════════════════════
# 테스트 클래스들
# ══════════════════════════════════════════════════════════

class OnMessageExceptionIsolationTest:
    """_on_message 예외 격리 테스트"""

    def test_mapping_failure_does_not_raise(self, collector):
        """build_record가 Exception을 던져도 _on_message 자체는 예외를 전파하지 않아야 한다."""
        collector.mapper.build_record = MagicMock(side_effect=RuntimeError("mapping boom"))
        msg = _make_fake_msg("factory/line1/temp", b'{"value": 1}')
        # 예외가 발생하지 않으면 통과
        collector._on_message(None, None, msg)

    def test_mapping_failure_skips_queue_append(self, collector, queue):
        """매핑에 실패하면 queue에 아무것도 들어가서는 안 된다."""
        collector.mapper.build_record = MagicMock(side_effect=RuntimeError("mapping boom"))
        collector._on_message(None, None, _make_fake_msg("factory/line1/temp", b'{"value": 1}'))
        assert queue.flush() == []

    def test_mapping_failure_logs_error(self, collector, caplog):
        """매핑 실패 시 ERROR 레벨로 로그가 찍혀야 한다."""
        collector.mapper.build_record = MagicMock(side_effect=RuntimeError("mapping boom"))
        with caplog.at_level("ERROR", logger="MQTTCollector"):
            collector._on_message(None, None, _make_fake_msg("factory/line1/temp", b'{"value": 1}'))
        assert any("Mapping failed" in record.message for record in caplog.records)

    def test_queue_full_does_not_raise(self, collector, queue):
        """QueueFullError가 발생해도 _on_message 자체는 예외를 전파하지 않아야 한다."""
        queue.append = MagicMock(side_effect=QueueFullError("queue full"))
        collector._on_message(None, None, _make_fake_msg("factory/line1/temp", b'{"value": 1}'))

    def test_queue_full_logs_error(self, collector, queue, caplog):
        """QueueFullError 시 [PQ FULL]이 포함된 ERROR 로그가 찍혀야 한다."""
        queue.append = MagicMock(side_effect=QueueFullError("queue full"))
        with caplog.at_level("ERROR", logger="MQTTCollector"):
            collector._on_message(None, None, _make_fake_msg("factory/line1/temp", b'{"value": 1}'))
        assert any("PQ FULL" in record.message for record in caplog.records)

    def test_unexpected_exception_in_append_does_not_raise(self, collector, queue):
        """append에서 예기치 못한 예외가 나도 콜백이 죽지 않아야 한다."""
        queue.append = MagicMock(side_effect=RuntimeError("unexpected!"))
        collector._on_message(None, None, _make_fake_msg("factory/line1/temp", b'{"value": 1}'))

    def test_normal_message_is_enqueued(self, collector, queue):
        """예외 격리 로직이 추가된 이후에도 정상 메시지는 queue에 들어가야 한다."""
        collector._on_message(None, None, _make_fake_msg("factory/line1/temp", b'{"value": 99.9}'))
        records = queue.flush()
        assert len(records) == 1
        assert records[0]["value"] == 99.9

    def test_good_message_after_bad_message_is_enqueued(self, collector, queue):
        """매핑 실패 메시지가 드롭된 뒤에도 다음 정상 메시지는 queue에 들어가야 한다."""
        original_build = collector.mapper.build_record
        call_count = {"n": 0}

        def side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("first message mapping error")
            return original_build(*args, **kwargs)

        collector.mapper.build_record = side_effect
        collector._on_message(None, None, _make_fake_msg("factory/line1/temp", b'{"value": 1}'))
        collector._on_message(None, None, _make_fake_msg("factory/line1/temp", b'{"value": 42}'))

        records = queue.flush()
        assert len(records) == 1
        assert records[0]["value"] == 42


# ══════════════════════════════════════════════════════════
# run() 재시도 한도
# ══════════════════════════════════════════════════════════

class MQTTRetryLimitTest:
    """MQTT 재시도 로직 테스트"""

    @pytest.fixture
    def collector_retry(self, tmp_dir, queue):
        # 헬퍼 함수를 fixture화
        def _create(max_retries):
            mapping_path = tmp_dir / "mapping.json"
            with open(mapping_path, "w", encoding="utf-8") as f:
                json.dump({"mapping": {"topic": "topic"}}, f)
            return MQTTCollector(
                MQTTConfig(host="bad_host", port=9999, max_retries=max_retries, retry_interval=0),
                MapperConfig(mapping_path=mapping_path),
                queue,
            )
        return _create

    def test_stops_after_max_retries(self, collector_retry):
        """max_retries=3일 때 3회 연속 실패 후 _running이 False가 되고 run()이 종료되어야 한다."""
        collector = collector_retry(max_retries=3)
        with patch.object(collector._client, "connect", side_effect=ConnectionRefusedError("refused")):
            collector.run()
        assert collector._running is False
        assert collector._consecutive_failures == 3

    def test_consecutive_failures_counted_correctly(self, collector_retry):
        """연결 실패 횟수가 정확히 max_retries만큼 쌓여야 한다."""
        collector = collector_retry(max_retries=2)
        with patch.object(collector._client, "connect", side_effect=ConnectionRefusedError("refused")):
            collector.run()
        assert collector._consecutive_failures == 2

    def test_infinite_retry_when_max_retries_zero(self, collector_retry):
        """max_retries=0(무한)일 때 외부에서 stop()을 불러야만 루프가 끊겨야 한다."""
        collector = collector_retry(max_retries=0)
        call_count = {"n": 0}

        def connect_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] >= 5:
                collector.stop()
            raise ConnectionRefusedError("refused")

        with patch.object(collector._client, "connect", side_effect=connect_side_effect):
            collector.run()
        assert call_count["n"] >= 5

    def test_failure_count_resets_on_successful_connect(self, collector_retry):
        """연결 성공 후 consecutive_failures 카운터가 0으로 초기화되어야 한다."""
        collector = collector_retry(max_retries=5)
        call_count = {"n": 0}

        def connect_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise ConnectionRefusedError("first failure")

        with patch.object(collector._client, "connect", side_effect=connect_side_effect), \
             patch.object(collector._client, "loop_forever", side_effect=collector.stop):
            collector.run()
        assert collector._consecutive_failures == 0