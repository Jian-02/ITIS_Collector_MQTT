"""
tests/file_queue_test.py
FileQueue tests — 2단계 커밋(peek/commit/rollback) 포함
"""

import json
import os
import sys
import tempfile
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
        # 시작할 때 깨끗하게 청소!
        if os.path.exists(self.tmp):
            os.remove(self.tmp)

    def tearDown(self):
        # 끝날 때도 깨끗하게 청소!
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
    """파일의 원시 JSON 줄을 파싱해 반환 (상태 필드 포함)."""
    lines = q.path.read_text(encoding="utf-8").splitlines()
    return [json.loads(l) for l in lines if l.strip()]


# ══════════════════════════════════════════════════════════
# 기존 append / flush 테스트 (하위 호환 확인)
# ══════════════════════════════════════════════════════════

class AppendFlushTest(BaseQueueTest):

    def setUp(self):
        super().setUp()

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

class TwoPhaseCommitTest(BaseQueueTest):

    def setUp(self):
        super().setUp()

    # ── peek ──────────────────────────────────────────────

    def test_peek_on_empty_queue_returns_empty(self):
        q = _make_queue(self.tmp)
        self.assertEqual(q.peek(), [])

    def test_peek_returns_records_without_status_field(self):
        # SAMPLE_RECORDS 추가 전, PQ에 이미 있는 건수를 미리 파악
        q = _make_queue(self.tmp)
        existing  = q.peek()
        q.append({"v": 1})
        records = q.peek()
        self.assertEqual(len(records)+len(existing), 1+len(existing))
        self.assertNotIn("_s", records[0])

    def test_peek_marks_records_as_pending_in_file(self):
        """peek 후 파일 내 줄의 _s 값이 'P'(Pending)여야 한다."""
        q = _make_queue(self.tmp)
        q.append({"v": 1})
        q.peek()
        objs = _raw_lines(q)
        self.assertTrue(all(o["_s"] == "P" for o in objs))

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
        cfg = QueueConfig.from_env()
        cfg.path = Path(self.tmp)
        q2  = FileQueue(cfg)

        objs = _raw_lines(q2)
        self.assertTrue(all(o["_s"] == "Q" for o in objs), "재시작 후 Pending이 남아있음")

    def test_crash_recovery_data_retrievable_after_restart(self):
        """재시작 후 복구된 레코드를 peek/commit으로 정상 처리할 수 있어야 한다."""
        q = _make_queue(self.tmp)
        existing  = q.peek()
        q.append({"v": 42})
        append_count = q.peek()   # 크래시 시뮬레이션

        cfg = QueueConfig.from_env()
        cfg.path = Path(self.tmp)
        q2  = FileQueue(cfg)

        records = q2.peek()
        self.assertEqual(len(records), len(existing) + len(append_count))
        self.assertEqual(records[len(records)-1]["v"], 42)
        q2.commit()
        self.assertEqual(q2.path.stat().st_size, 0)


# ══════════════════════════════════════════════════════════
# 용량 제한 테스트
# ══════════════════════════════════════════════════════════

class SizeLimitTest(BaseQueueTest):

    def setUp(self):
        super().setUp()

    def test_append_raises_error_when_limit_exceeded(self):
        cfg = QueueConfig.from_env()
        cfg.path = Path(self.tmp)
        q  = FileQueue(cfg)

        chunk = "x" * (q.max_bytes // 3)

        q.append({"v": chunk, "i": 0})
        q.append({"v": chunk, "i": 1})

        with self.assertRaises(QueueFullError):
            q.append({"v": chunk, "i": 2})

    def test_all_records_preserved_when_limit_disabled(self):
        cfg = QueueConfig.from_env()
        cfg.path = Path(self.tmp)
        q  = FileQueue(cfg)
        for i in range(20):
            q.append({"i": i})
        records = q.flush()
        self.assertEqual(len(records), 20)


# ══════════════════════════════════════════════════════════
# 스레드 안전성 테스트
# ══════════════════════════════════════════════════════════

def _run_concurrent_workers(self, n_threads, worker_func, *args):
        """지정한 개수의 스레드를 생성하고 동시에 출발시킨 후, 조인합니다."""
        barrier = threading.Barrier(n_threads)
        errors = []
        err_lock = threading.Lock()

        def target_wrapper(tid):
            try:
                # 모든 스레드가 준비될 때까지 대기 후 동시 출발
                barrier.wait()
                worker_func(tid, errors, err_lock, *args)
            except Exception as e:
                with err_lock:
                    errors.append(e)

        threads = [threading.Thread(target=target_wrapper, args=(i,)) for i in range(n_threads)]
        for t in threads: t.start()
        for t in threads: t.join()

        self.assertEqual(errors, [], f"동시성 작업 중 예외 발생: {errors[:3]}")

class ThreadSafetyTest(BaseQueueTest):

    def setUp(self):
        super().setUp()
        

    def test_concurrent_appends_exact_match(self):
        """스레드 2~4개, 소량 데이터로 내용물 무결성 완벽 검증."""
        cfg = QueueConfig.from_env()
        cfg.path = Path(self.tmp)
        q = FileQueue(cfg)

        n_threads = 4
        per_thread = 10
        errors = []
        err_lock = threading.Lock()  # 에러 리스트 보호용 락
        expected = set()

        for tid in range(n_threads):
            for idx in range(per_thread):
                expected.add((tid, idx))

        # 모든 스레드가 준비되면 동시에 출발하도록 장벽(Barrier) 설정
        barrier = threading.Barrier(n_threads)

        def writer(tid):
            barrier.wait()  # 동시 출발 타이밍 정렬
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

        self.assertEqual(errors, [], f"append 중 예외 발생: {errors}")

        records = q.flush()
        actual = {(r["tid"], r["idx"]) for r in records}

        self.assertEqual(
            len(records), n_threads * per_thread,
            f"레코드 수 불일치: 기대 {n_threads * per_thread}, 실제 {len(records)}"
        )
        self.assertEqual(actual, expected, f"누락 또는 변조된 레코드: {expected - actual}")

    def test_high_concurrency_stress(self):
        """스레드 20개, 대량 데이터로 시스템 지연·에러 발생 여부 검증."""
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
                    # GIL을 순간적으로 해제하고 스레드 스위칭을 유도하여 
                    # 동시성 경합을 극대화하기 위한 미세한 힌트
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

        self.assertEqual(errors, [], f"고부하 append 중 예외 발생 ({len(errors)}건): {errors[:3]}")

        records = q.flush()
        self.assertEqual(
            len(records), n_threads * per_thread,
            f"고부하 레코드 수 불일치: 기대 {n_threads * per_thread}, 실제 {len(records)}"
        )

    def test_concurrent_append_and_flush(self):
        """append 스레드와 flush 스레드를 동시에 돌려 상호 작용 검증."""
        cfg = QueueConfig.from_env()
        cfg.path = Path(self.tmp)
        q = FileQueue(cfg)

        n_writers = 3
        per_writer = 50
        errors = []
        err_lock = threading.Lock()
        
        collected = []
        coll_lock = threading.Lock()  # 수집 리스트 보호용 락
        stop_flag = threading.Event()

        def writer(tid):
            for idx in range(per_writer):
                try:
                    q.append({"tid": tid, "idx": idx})
                    time.sleep(0.001)  # flusher가 끼어들 틈을 주기 위해 의도적 지연
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

        # 모든 writer 종료 후 flusher 안전하게 정리
        stop_flag.set()
        flush_thread.join()

        # 마지막 잔여 데이터 수집 (락 불필요 - 스레드들 다 종료됨)
        collected.extend(q.flush())

        self.assertEqual(errors, [], f"동시 append/flush 중 예외 발생: {errors}")
        self.assertEqual(
            len(collected), n_writers * per_writer,
            f"동시 append/flush 레코드 수 불일치: 기대 {n_writers * per_writer}, 실제 {len(collected)}"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)