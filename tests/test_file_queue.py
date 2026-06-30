"""
tests/file_queue_test.py
FileQueue 테스트 — 2단계 커밋(peek/commit/rollback) 기능 포함
"""

import json
import os
import sys
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import QueueConfig
from file_queue import FileQueue, QueueFullError

class BaseQueueTest(unittest.TestCase):
    def setUp(self):
        self.tmp = "c:\\itis_collector_mqtt\\tests\\persistent_queue_test.jsonl"
        # 시작하기 전에 비우기
        if os.path.exists(self.tmp):
            os.remove(self.tmp)

    def tearDown(self):
        # 끝난 후에도 비우기
        if os.path.exists(self.tmp):
            try:
                os.remove(self.tmp)
            except Exception:
                pass

def _make_queue(tmp: Path, size_limit_enabled=True, max_bytes=1024 * 1024) -> FileQueue:
    cfg = QueueConfig(
        path=Path(tmp),
        size_limit_enabled=size_limit_enabled,
        max_bytes=max_bytes,
    )
    return FileQueue(cfg)


def _raw_lines(q: FileQueue) -> list[dict]:
    """Parses and returns the raw JSON lines from the file (including the status field)."""
    lines = q.path.read_text(encoding="utf-8").splitlines()
    return [json.loads(l) for l in lines if l.strip()]


# ══════════════════════════════════════════════════════════
# 기존 append / flush 테스트 (이전 버전과의 호환성 검증)
# ══════════════════════════════════════════════════════════

class AppendFlushTest(BaseQueueTest):

    def setUp(self):
        super().setUp()

    def test_append_and_flush_returns_all_records(self):
        """append 후 flush 하면 모든 레코드가 반환되어야 한다."""
        q = _make_queue(self.tmp)
        q.append({"a": 1})
        q.append({"a": 2})
        records = q.flush()
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["a"], 1)
        self.assertEqual(records[1]["a"], 2)

    def test_flush_clears_file(self):
        """flush 이후 파일이 비워져야 한다."""
        q = _make_queue(self.tmp)
        q.append({"x": 1})
        q.flush()
        self.assertEqual(q.path.stat().st_size, 0)

    def test_flush_on_empty_queue_returns_empty_list(self):
        """빈 큐에서 flush 하면 빈 리스트가 반환되어야 한다."""
        q = _make_queue(self.tmp)
        self.assertEqual(q.flush(), [])

    def test_lines_appended_after_flush_are_preserved(self):
        """flush 이후에 append된 라인들은 다음 flush 때 반환되어야 한다."""
        q = _make_queue(self.tmp)
        q.append({"pre": 1})
        q.flush()
        q.append({"post": 2})
        remaining = q.flush()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["post"], 2)

    def test_flush_strips_status_field(self):
        """The flush return value should not contain the internal status field (_s)."""
        q = _make_queue(self.tmp)
        q.append({"v": 42})
        records = q.flush()
        self.assertNotIn("_s", records[0])

    def test_malformed_line_is_skipped(self):
        """잘못된 형식의 라인은 건너뛰고 정상적인 데이터만 처리돼야 한다."""
        q = _make_queue(self.tmp)
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

class TwoPhaseCommitTest(BaseQueueTest):

    def setUp(self):
        super().setUp()

    # ── peek ──────────────────────────────────────────────

    def test_peek_on_empty_queue_returns_empty(self):
        """빈 큐에서 peek 하면 빈 리스트가 반환되어야 한다."""
        q = _make_queue(self.tmp)
        self.assertEqual(q.peek(), [])

    def test_peek_returns_records_without_status_field(self):
        """peek 반환 값에 내부 상태 필드(_s)가 포함되지 않아야 한다."""
        q = _make_queue(self.tmp)
        existing  = q.peek()
        q.append({"v": 1})
        records = q.peek()
        self.assertEqual(len(records)+len(existing), 1+len(existing))
        self.assertNotIn("_s", records[0])

    def test_peek_marks_records_as_pending_in_file(self):
        """peek 이후 파일 내 라인들의 _s 값은 'P'(Pending)여야 한다."""
        q = _make_queue(self.tmp)
        q.append({"v": 1})
        q.peek()
        objs = _raw_lines(q)
        self.assertTrue(all(o["_s"] == "P" for o in objs))

    def test_peek_does_not_return_already_pending_records(self):
        """첫 번째 peek 직후에 호출한 두 번째 peek은 빈 리스트를 반환해야 한다."""
        q = _make_queue(self.tmp)
        q.append({"v": 1})
        q.peek()
        second = q.peek()
        self.assertEqual(second, [])

    # ── commit ────────────────────────────────────────────

    def test_commit_removes_pending_records(self):
        """commit 이후에는 파일이 비어 있어야 한다."""
        q = _make_queue(self.tmp)
        q.append({"v": 1})
        q.peek()
        q.commit()
        self.assertEqual(q.path.stat().st_size, 0)

    def test_commit_preserves_queued_records_added_after_peek(self):
        """peek 이후에 새로 append된 레코드들은 commit 이후에도 그대로 남아 있어야 한다."""
        q = _make_queue(self.tmp)
        q.append({"v": 1})
        q.peek()
        q.append({"v": 2})   # peek 후에 추가
        q.commit()

        remaining = q.flush()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["v"], 2)

    # ── rollback ──────────────────────────────────────────

    def test_rollback_restores_pending_to_queued(self):
        """rollback 이후에는 파일 내의 모든 라인이 'Q'(Queued) 상태여야 한다."""
        q = _make_queue(self.tmp)
        q.append({"v": 1})
        q.peek()
        q.rollback()
        objs = _raw_lines(q)
        self.assertTrue(all(o["_s"] == "Q" for o in objs))

    def test_rollback_allows_re_peek(self):
        """rollback 이후에 다시 peek을 하면 동일한 레코드들이 반환되어야 한다."""
        q = _make_queue(self.tmp)
        q.append({"v": 99})
        first  = q.peek()
        q.rollback()
        second = q.peek()
        self.assertEqual(first, second)

    def test_rollback_on_empty_queue_does_not_raise(self):
        """빈 큐에서 rollback 해도 예외가 발생하지 않아야 한다."""
        q = _make_queue(self.tmp)
        try:
            q.rollback()
        except Exception as e:
            self.fail(f"rollback raised an exception on an empty queue: {e}")

    # ── Crash recovery ────────────────────────────────────────

    def test_crash_recovery_converts_pending_to_queued_on_init(self):
        """
        프로세스 재시작 시(FileQueue 재생성),
        Pending(처리 중) 상태의 라인들이 Queued(대기) 상태로 자동 복구되어야 한다.
        """
        q = _make_queue(self.tmp)
        q.append({"v": 1})
        q.peek()   # Pending 상태로 전환한 후, '크래시' 상황을 시뮬레이션

        # 재시작: 동일한 경로로 FileQueue 재생성
        cfg = QueueConfig.from_env()
        cfg.path = Path(self.tmp)
        q2  = FileQueue(cfg)

        objs = _raw_lines(q2)
        self.assertTrue(all(o["_s"] == "Q" for o in objs), "Pending remained after restart")

    def test_crash_recovery_data_retrievable_after_restart(self):
        """재시작 후 복구된 레코드들은 peek/commit을 통해 정상적으로 처리 가능해야 한다."""
        q = _make_queue(self.tmp)
        existing  = q.peek()
        q.append({"v": 42})
        append_count = q.peek()   # '크래시' 상황을 시뮬레이션

        cfg = QueueConfig.from_env()
        cfg.path = Path(self.tmp)
        q2  = FileQueue(cfg)

        records = q2.peek()
        self.assertEqual(len(records), len(existing) + len(append_count))
        self.assertEqual(records[len(records)-1]["v"], 42)
        q2.commit()
        self.assertEqual(q2.path.stat().st_size, 0)


# ══════════════════════════════════════════════════════════
# 크기 제한 테스트
# ══════════════════════════════════════════════════════════

class SizeLimitTest(BaseQueueTest):

    def setUp(self):
        super().setUp()

    def test_append_raises_error_when_limit_exceeded(self):
        """큐의 최대 용량(max_bytes)을 초과하여 append 시 QueueFullError가 발생하여야 한다."""
        cfg = QueueConfig.from_env()
        cfg.path = Path(self.tmp)
        q  = FileQueue(cfg)

        chunk = "x" * (q.max_bytes // 3)

        q.append({"v": chunk, "i": 0})
        q.append({"v": chunk, "i": 1})

        with self.assertRaises(QueueFullError):
            q.append({"v": chunk, "i": 2})

    def test_all_records_preserved_when_limit_disabled(self):
        """용량 제한이 없을 때(기본값) 많은 레코드를 추가해도 데이터 유실 없이 모두 보존되어야 한다."""
        cfg = QueueConfig.from_env()
        cfg.path = Path(self.tmp)
        q  = FileQueue(cfg)
        for i in range(20):
            q.append({"i": i})
        records = q.flush()
        self.assertEqual(len(records), 20)


# ══════════════════════════════════════════════════════════
# 스레드 안전성(Thread safety) 테스트
# ══════════════════════════════════════════════════════════

def _run_concurrent_workers(self, n_threads, worker_func, *args):
        """지정된 개수의 스레드를 생성하고, 동시에 모두 시작한 뒤 완료될 때까지 대기(join)한다."""
        barrier = threading.Barrier(n_threads)
        errors = []
        err_lock = threading.Lock()

        def target_wrapper(tid):
            try:
                # 모든 스레드가 준비될 때까지 대기한 후, 동시에 시작
                barrier.wait()
                worker_func(tid, errors, err_lock, *args)
            except Exception as e:
                with err_lock:
                    errors.append(e)

        threads = [threading.Thread(target=target_wrapper, args=(i,)) for i in range(n_threads)]
        for t in threads: t.start()
        for t in threads: t.join()

        self.assertEqual(errors, [], f"Exception occurred during concurrent operation: {errors[:3]}")

class ThreadSafetyTest(BaseQueueTest):

    def setUp(self):
        super().setUp()
        

    def test_concurrent_appends_exact_match(self):
        """2~4개의 스레드와 소량의 데이터 사용 — 콘텐츠의 무결성(integrity)을 철저히 검증한다."""
        cfg = QueueConfig.from_env()
        cfg.path = Path(self.tmp)
        q = FileQueue(cfg)

        n_threads = 4
        per_thread = 10
        errors = []
        err_lock = threading.Lock()  # 에러 리스트를 보호하는 락(lock)
        expected = set()

        for tid in range(n_threads):
            for idx in range(per_thread):
                expected.add((tid, idx))

        # 모든 스레드가 준비되었을 때 동시에 시작할 수 있도록 배리어(Barrier) 설정
        barrier = threading.Barrier(n_threads)

        def writer(tid):
            barrier.wait()  # 동시에 시작하는 타이밍을 동기화
            for idx in range(per_thread):
                try:
                    q.append({"tid": tid, "idx": idx})
                except Exception as e:
                    with err_lock:
                        errors.append(e)

        threads = [threading.Thread(target=writer, args=(tid,)) for tid in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], f"Exception occurred during append: {errors}")

        records = q.flush()
        actual = {(r["tid"], r["idx"]) for r in records}

        self.assertEqual(
            len(records), n_threads * per_thread,
            f"Record count mismatch: expected {n_threads * per_thread}, actual {len(records)}"
        )
        self.assertEqual(actual, expected, f"Missing or corrupted records: {expected - actual}")

    def test_high_concurrency_stress(self):
        """20개의 스레드와 대량의 데이터 사용 — 시스템 부하 상황에서 지연이나 에러가 발생하지 않는지 검증"""
        cfg = QueueConfig.from_env()
        cfg.path = Path(self.tmp)
        q = FileQueue(cfg)

        n_threads = 20
        per_thread = 500
        errors = []
        err_lock = threading.Lock()
        barrier = threading.Barrier(n_threads)

        def writer(tid):
            barrier.wait()
            for idx in range(per_thread):
                try:
                    q.append({"tid": tid, "idx": idx})
                    # GIL을 일시적으로 해제하고 스레드 스위칭을 유도하여, 
                    # 동시성 경쟁(concurrency contention)을 극대화하기 위한 미세한 힌트
                    if idx % 10 == 0:
                        time.sleep(0.00001) 
                except Exception as e:
                    with err_lock:
                        errors.append(e)

        threads = [threading.Thread(target=writer, args=(tid,)) for tid in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], f"Exception occurred during high-load append ({len(errors)} occurrence(s)): {errors[:3]}")

        records = q.flush()
        self.assertEqual(
            len(records), n_threads * per_thread,
            f"High-load record count mismatch: expected {n_threads * per_thread}, actual {len(records)}"
        )

    def test_concurrent_append_and_flush(self):
        """append 스레드들과 flush 스레드를 동시에 실행하여 상호작용을 검증한다."""
        cfg = QueueConfig.from_env()
        cfg.path = Path(self.tmp)
        q = FileQueue(cfg)

        n_writers = 3
        per_writer = 50
        errors = []
        err_lock = threading.Lock()
        
        collected = []
        coll_lock = threading.Lock()  # 컬렉션 리스트를 보호하는 락(lock)
        stop_flag = threading.Event()

        def writer(tid):
            for idx in range(per_writer):
                try:
                    q.append({"tid": tid, "idx": idx})
                    time.sleep(0.001)  # flush 스레드가 사이에 끼어들 수 있도록(interleave) 의도적인 지연 추가
                except Exception as e:
                    with err_lock:
                        errors.append(("append", e))

        def flusher():
            while not stop_flag.is_set():
                try:
                    batch = q.flush()
                    if batch:
                        with coll_lock:
                            collected.extend(batch)
                except Exception as e:
                    with err_lock:
                        errors.append(("flush", e))
                time.sleep(0.005)

        writer_threads = [threading.Thread(target=writer, args=(tid,)) for tid in range(n_writers)]
        flush_thread = threading.Thread(target=flusher)

        flush_thread.start()
        for t in writer_threads:
            t.start()
        for t in writer_threads:
            t.join()

        # 모든 쓰기 스레드(writers)가 종료되면 flush 스레드를 안전하게 종료
        stop_flag.set()
        flush_thread.join()

        # 마지막으로 남은 데이터 수집 (모든 스레드가 종료되었으므로 락이 필요 없음)
        collected.extend(q.flush())

        self.assertEqual(errors, [], f"Exception occurred during concurrent append/flush: {errors}")
        self.assertEqual(
            len(collected), n_writers * per_writer,
            f"Concurrent append/flush record count mismatch: expected {n_writers * per_writer}, actual {len(collected)}"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)