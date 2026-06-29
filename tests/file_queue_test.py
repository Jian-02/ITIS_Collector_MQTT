"""
tests/file_queue_test.py
FileQueue tests — 2단계 커밋(peek/commit/rollback) 포함
"""

import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import QueueConfig
from file_queue import FileQueue


def _make_queue(tmp: Path, size_limit_enabled=True, max_bytes=1024 * 1024) -> FileQueue:
    cfg = QueueConfig(
        path=tmp / "queue.jsonl",
        size_limit_enabled=size_limit_enabled,
        max_bytes=max_bytes,
    )
    return FileQueue(cfg)


def _raw_lines(q: FileQueue) -> list[dict]:
    """파일의 원시 JSON 줄을 파싱해 반환 (상태 필드 포함)."""
    lines = q.path.read_text(encoding="utf-8").splitlines()
    return [json.loads(l) for l in lines if l.strip()]


# ══════════════════════════════════════════════════════════
# 기존 append / flush 테스트 (하위 호환 확인)
# ══════════════════════════════════════════════════════════

class AppendFlushTest(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def test_append_and_flush_returns_all_records(self):
        q = _make_queue(self.tmp)
        q.append({"a": 1})
        q.append({"a": 2})
        records = q.flush()
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["a"], 1)
        self.assertEqual(records[1]["a"], 2)

    def test_flush_clears_file(self):
        q = _make_queue(self.tmp)
        q.append({"x": 1})
        q.flush()
        self.assertEqual(q.path.stat().st_size, 0)

    def test_flush_on_empty_queue_returns_empty_list(self):
        q = _make_queue(self.tmp)
        self.assertEqual(q.flush(), [])

    def test_lines_appended_after_flush_are_preserved(self):
        """flush 이후 추가된 줄은 다음 flush 때 반환돼야 한다."""
        q = _make_queue(self.tmp)
        q.append({"pre": 1})
        q.flush()
        q.append({"post": 2})
        remaining = q.flush()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["post"], 2)

    def test_flush_strips_status_field(self):
        """flush 반환값에 내부 상태 필드(_s)가 없어야 한다."""
        q = _make_queue(self.tmp)
        q.append({"v": 42})
        records = q.flush()
        self.assertNotIn("_s", records[0])

    def test_malformed_line_is_skipped(self):
        q = _make_queue(self.tmp)
        # 상태 필드를 수동으로 넣어 Queued 줄처럼 작성
        q.path.write_text(
            '{"_s":"Q","ok":1}\nNOT_JSON\n{"_s":"Q","ok":2}\n',
            encoding="utf-8"
        )
        records = q.flush()
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["ok"], 1)
        self.assertEqual(records[1]["ok"], 2)


# ══════════════════════════════════════════════════════════
# 2단계 커밋 테스트 (peek / commit / rollback)
# ══════════════════════════════════════════════════════════

class TwoPhaseCommitTest(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    # ── peek ──────────────────────────────────────────────

    def test_peek_returns_records_without_status_field(self):
        q = _make_queue(self.tmp)
        q.append({"v": 1})
        records = q.peek()
        self.assertEqual(len(records), 1)
        self.assertNotIn("_s", records[0])

    def test_peek_marks_records_as_pending_in_file(self):
        """peek 후 파일 내 줄의 _s 값이 'P'(Pending)여야 한다."""
        q = _make_queue(self.tmp)
        q.append({"v": 1})
        q.peek()
        objs = _raw_lines(q)
        self.assertTrue(all(o["_s"] == "P" for o in objs))

    def test_peek_on_empty_queue_returns_empty(self):
        q = _make_queue(self.tmp)
        self.assertEqual(q.peek(), [])

    def test_peek_does_not_return_already_pending_records(self):
        """첫 번째 peek 이후 두 번째 peek는 빈 리스트를 반환해야 한다."""
        q = _make_queue(self.tmp)
        q.append({"v": 1})
        q.peek()
        second = q.peek()
        self.assertEqual(second, [])

    # ── commit ────────────────────────────────────────────

    def test_commit_removes_pending_records(self):
        """commit 후 파일이 비어야 한다."""
        q = _make_queue(self.tmp)
        q.append({"v": 1})
        q.peek()
        q.commit()
        self.assertEqual(q.path.stat().st_size, 0)

    def test_commit_preserves_queued_records_added_after_peek(self):
        """peek 이후 append된 새 레코드는 commit 후에도 남아야 한다."""
        q = _make_queue(self.tmp)
        q.append({"v": 1})
        q.peek()
        q.append({"v": 2})   # peek 이후 추가
        q.commit()

        remaining = q.flush()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["v"], 2)

    # ── rollback ──────────────────────────────────────────

    def test_rollback_restores_pending_to_queued(self):
        """rollback 후 파일 내 모든 줄이 'Q'(Queued) 상태여야 한다."""
        q = _make_queue(self.tmp)
        q.append({"v": 1})
        q.peek()
        q.rollback()
        objs = _raw_lines(q)
        self.assertTrue(all(o["_s"] == "Q" for o in objs))

    def test_rollback_allows_re_peek(self):
        """rollback 후 다시 peek하면 동일한 레코드를 받아야 한다."""
        q = _make_queue(self.tmp)
        q.append({"v": 99})
        first  = q.peek()
        q.rollback()
        second = q.peek()
        self.assertEqual(first, second)

    def test_rollback_on_empty_queue_does_not_raise(self):
        q = _make_queue(self.tmp)
        try:
            q.rollback()
        except Exception as e:
            self.fail(f"빈 큐에서 rollback이 예외를 발생시킴: {e}")

    # ── 크래시 복구 ────────────────────────────────────────

    def test_crash_recovery_converts_pending_to_queued_on_init(self):
        """
        프로세스 재시작(FileQueue 재생성) 시
        Pending 줄이 자동으로 Queued로 복원돼야 한다.
        """
        q = _make_queue(self.tmp)
        q.append({"v": 1})
        q.peek()   # Pending 상태로 전환 후 '크래시' 시뮬레이션

        # 재시작: 같은 경로로 FileQueue 재생성
        cfg = QueueConfig(path=self.tmp / "queue.jsonl")
        q2  = FileQueue(cfg)

        objs = _raw_lines(q2)
        self.assertTrue(all(o["_s"] == "Q" for o in objs), "재시작 후 Pending이 남아있음")

    def test_crash_recovery_data_retrievable_after_restart(self):
        """재시작 후 복구된 레코드를 peek/commit으로 정상 처리할 수 있어야 한다."""
        q = _make_queue(self.tmp)
        q.append({"v": 42})
        q.peek()   # 크래시 시뮬레이션

        cfg = QueueConfig(path=self.tmp / "queue.jsonl")
        q2  = FileQueue(cfg)

        records = q2.peek()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["v"], 42)
        q2.commit()
        self.assertEqual(q2.path.stat().st_size, 0)


# ══════════════════════════════════════════════════════════
# 용량 제한 테스트
# ══════════════════════════════════════════════════════════

class SizeLimitTest(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def test_oldest_lines_dropped_when_limit_exceeded(self):
        line_bytes = len(json.dumps({"_s": "Q", "v": "x" * 100}).encode()) + 1
        q = _make_queue(self.tmp, size_limit_enabled=True, max_bytes=line_bytes * 3)

        for i in range(5):
            q.append({"v": "x" * 100, "i": i})

        records = q.flush()
        indices = [r["i"] for r in records]
        self.assertEqual(indices, sorted(indices))
        self.assertNotIn(0, indices)

    def test_all_records_preserved_when_limit_disabled(self):
        q = _make_queue(self.tmp, size_limit_enabled=False, max_bytes=1)
        for i in range(20):
            q.append({"i": i})
        records = q.flush()
        self.assertEqual(len(records), 20)

    def test_pending_records_not_dropped_on_size_limit(self):
        """용량 초과 시 Pending 상태 레코드는 드롭되지 않아야 한다."""
        line_bytes = len(json.dumps({"_s": "Q", "v": "x" * 100}).encode()) + 1
        q = _make_queue(self.tmp, size_limit_enabled=True, max_bytes=line_bytes * 2)

        q.append({"v": "x" * 100, "i": 0})
        q.peek()  # i=0 을 Pending으로 전환

        # 새 레코드 추가 → 용량 초과 → Queued만 드롭돼야 함
        for i in range(1, 4):
            q.append({"v": "x" * 100, "i": i})

        objs = _raw_lines(q)
        pending = [o for o in objs if o["_s"] == "P"]
        self.assertEqual(len(pending), 1, "Pending 레코드가 드롭됨")


# ══════════════════════════════════════════════════════════
# 스레드 안전성 테스트
# ══════════════════════════════════════════════════════════

class ThreadSafetyTest(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def test_concurrent_appends_no_data_loss(self):
        q = _make_queue(self.tmp)
        total  = 200
        errors = []

        def test_writer():
            for i in range(total // 2):
                try:
                    q.append({"i": i})
                except Exception as e:
                    errors.append(e)

        t1 = threading.Thread(target=test_writer)
        t2 = threading.Thread(target=test_writer)
        t1.start(); t2.start()
        t1.join();  t2.join()

        records = q.flush()
        self.assertEqual(len(errors), 0)
        self.assertEqual(len(records), total)


if __name__ == "__main__":
    unittest.main(verbosity=2)