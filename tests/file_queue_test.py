"""
tests/file_queue_test.py
FileQueue 테스트
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
        """flush 이후 append된 줄은 다음 flush 때 반환돼야 한다."""
        q = _make_queue(self.tmp)
        q.append({"pre": 1})
        q.flush()
        q.append({"post": 2})
        remaining = q.flush()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["post"], 2)

    def test_malformed_line_is_skipped(self):
        q = _make_queue(self.tmp)
        q.path.write_text(
            '{"ok": 1}\nNOT_JSON\n{"ok": 2}\n',
            encoding="utf-8"
        )
        records = q.flush()
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["ok"], 1)
        self.assertEqual(records[1]["ok"], 2)


class SizeLimitTest(unittest.TestCase):

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def test_oldest_lines_dropped_when_limit_exceeded(self):
        line_bytes = len(json.dumps({"v": "x" * 100}).encode()) + 1
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