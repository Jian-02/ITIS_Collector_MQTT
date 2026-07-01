"""
tests/file_queue_test.py
FileQueue 테스트 — 2단계 커밋(peek/commit/rollback) 기능 포함
"""

import json
import os
import sys
import threading
import time
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import QueueConfig
from file_queue import FileQueue, QueueFullError

# ── Fixtures ──────────────────────────────────────────

@pytest.fixture
def temp_queue_path():
    path = "c:\\itis_collector_mqtt\\tests\\persistent_queue_test.jsonl"
    if os.path.exists(path):
        os.remove(path)
    yield Path(path)
    if os.path.exists(path):
        try:
            os.remove(path)
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
    """파일에 저장된 원본 JSON Lines 데이터를 파싱하여 반환한다(상태 필드 포함)."""
    lines = q.path.read_text(encoding="utf-8").splitlines()
    return [json.loads(l) for l in lines if l.strip()]


# ── AppendFlushTest ──────────────────────────────────

def test_append_and_flush_returns_all_records(temp_queue_path):
    """append 후 flush 하면 모든 레코드가 반환되어야 한다."""
    q = _make_queue(temp_queue_path)
    q.append({"a": 1})
    q.append({"a": 2})
    records = q.flush()
    assert len(records) == 2
    assert records[0]["a"] == 1
    assert records[1]["a"] == 2

def test_flush_clears_file(temp_queue_path):
    """flush 이후 파일이 비워져야 한다."""
    q = _make_queue(temp_queue_path)
    q.append({"x": 1})
    q.flush()
    assert q.path.stat().st_size == 0

def test_flush_on_empty_queue_returns_empty_list(temp_queue_path):
    """빈 큐에서 flush 하면 빈 리스트가 반환되어야 한다."""
    q = _make_queue(temp_queue_path)
    assert q.flush() == []

def test_lines_appended_after_flush_are_preserved(temp_queue_path):
    """flush 이후에 append된 라인들은 다음 flush 때 반환되어야 한다."""
    q = _make_queue(temp_queue_path)
    q.append({"pre": 1})
    q.flush()
    q.append({"post": 2})
    remaining = q.flush()
    assert len(remaining) == 1
    assert remaining[0]["post"] == 2

def test_flush_strips_status_field(temp_queue_path):
    """flush가 반환하는 값에는 내부 상태 필드(_s)가 포함되지 않아야 한다."""
    q = _make_queue(temp_queue_path)
    q.append({"v": 42})
    records = q.flush()
    assert "_s" not in records[0]

def test_malformed_line_is_skipped(temp_queue_path):
    """잘못된 형식의 라인은 건너뛰고 정상적인 데이터만 처리돼야 한다."""
    q = _make_queue(temp_queue_path)
    q.path.write_text(
        '{"_s":"Q","ok":1}\nNOT_JSON\n{"_s":"Q","ok":2}\n',
        encoding="utf-8"
    )
    records = q.flush()
    assert len(records) == 2
    assert records[0]["ok"] == 1
    assert records[1]["ok"] == 2


# ── TwoPhaseCommitTest ──────────────────────────────────

def test_peek_on_empty_queue_returns_empty(temp_queue_path):
    """빈 큐에서 peek 하면 빈 리스트가 반환되어야 한다."""
    q = _make_queue(temp_queue_path)
    assert q.peek() == []

def test_peek_returns_records_without_status_field(temp_queue_path):
    """peek 반환 값에 내부 상태 필드(_s)가 포함되지 않아야 한다."""
    q = _make_queue(temp_queue_path)
    existing = q.peek()
    q.append({"v": 1})
    records = q.peek()
    assert len(records) + len(existing) == 1 + len(existing)
    assert "_s" not in records[0]

def test_peek_marks_records_as_pending_in_file(temp_queue_path):
    """peek 이후 파일 내 라인들의 _s 값은 'P'(Pending)여야 한다."""
    q = _make_queue(temp_queue_path)
    q.append({"v": 1})
    q.peek()
    objs = _raw_lines(q)
    assert all(o["_s"] == "P" for o in objs)

def test_peek_does_not_return_already_pending_records(temp_queue_path):
    """첫 번째 peek 직후에 호출한 두 번째 peek은 빈 리스트를 반환해야 한다."""
    q = _make_queue(temp_queue_path)
    q.append({"v": 1})
    q.peek()
    second = q.peek()
    assert second == []

def test_commit_removes_pending_records(temp_queue_path):
    """commit 이후에는 파일이 비어 있어야 한다."""
    q = _make_queue(temp_queue_path)
    q.append({"v": 1})
    q.peek()
    q.commit()
    assert q.path.stat().st_size == 0

def test_commit_preserves_queued_records_added_after_peek(temp_queue_path):
    """peek 이후에 새로 append된 레코드들은 commit 이후에도 그대로 남아 있어야 한다."""
    q = _make_queue(temp_queue_path)
    q.append({"v": 1})
    q.peek()
    q.append({"v": 2})
    q.commit()

    remaining = q.flush()
    assert len(remaining) == 1
    assert remaining[0]["v"] == 2

def test_rollback_restores_pending_to_queued(temp_queue_path):
    """rollback 이후에는 파일 내의 모든 라인이 'Q'(Queued) 상태여야 한다."""
    q = _make_queue(temp_queue_path)
    q.append({"v": 1})
    q.peek()
    q.rollback()
    objs = _raw_lines(q)
    assert all(o["_s"] == "Q" for o in objs)

def test_rollback_allows_re_peek(temp_queue_path):
    """rollback 이후에 다시 peek을 하면 동일한 레코드들이 반환되어야 한다."""
    q = _make_queue(temp_queue_path)
    q.append({"v": 99})
    first = q.peek()
    q.rollback()
    second = q.peek()
    assert first == second

def test_rollback_on_empty_queue_does_not_raise(temp_queue_path):
    """빈 큐에서 rollback 해도 예외가 발생하지 않아야 한다."""
    q = _make_queue(temp_queue_path)
    try:
        q.rollback()
    except Exception as e:
        pytest.fail(f"rollback raised: {e}")

def test_crash_recovery_converts_pending_to_queued_on_init(temp_queue_path):
    """
    프로세스 재시작 시(FileQueue 재생성),
    Pending(처리 중) 상태의 라인들이 Queued(대기) 상태로 자동 복구되어야 한다.
    """
    q = _make_queue(temp_queue_path)
    q.append({"v": 1})
    q.peek()  # Pending 상태로 전환한 후, '크래시' 상황을 시뮬레이션

    # 재시작: 동일한 경로로 FileQueue 재생성
    cfg = QueueConfig.from_env()
    cfg.path = Path(temp_queue_path)
    q2 = FileQueue(cfg)

    objs = _raw_lines(q2)
    assert all(o["_s"] == "Q" for o in objs), "Pending remained after restart"

def test_crash_recovery_data_retrievable_after_restart(temp_queue_path):
    """재시작 후 복구된 레코드들은 peek/commit을 통해 정상적으로 처리 가능해야 한다."""
    q = _make_queue(temp_queue_path)
    existing = q.peek()
    q.append({"v": 42})
    append_count = q.peek()  # '크래시' 상황을 시뮬레이션

    cfg = QueueConfig.from_env()
    cfg.path = Path(temp_queue_path)
    q2 = FileQueue(cfg)

    records = q2.peek()
    assert len(records) == len(existing) + len(append_count)
    assert records[-1]["v"] == 42
    q2.commit()
    assert q2.path.stat().st_size == 0


# ── SizeLimitTest ──────────────────────────────────

def test_append_raises_error_when_limit_exceeded(temp_queue_path):
    """큐의 최대 용량(max_bytes)을 초과하여 append 시 QueueFullError가 발생하여야 한다."""
    cfg = QueueConfig.from_env()
    cfg.path = Path(temp_queue_path)
    q = FileQueue(cfg)

    chunk = "x" * (q.max_bytes // 3)

    q.append({"v": chunk, "i": 0})
    q.append({"v": chunk, "i": 1})

    with pytest.raises(QueueFullError):
        q.append({"v": chunk, "i": 2})

def test_all_records_preserved_when_limit_disabled(temp_queue_path):
    """용량 제한이 없을 때(기본값) 많은 레코드를 추가해도 데이터 유실 없이 모두 보존되어야 한다."""
    cfg = QueueConfig.from_env()
    cfg.path = Path(temp_queue_path)
    q = FileQueue(cfg)
    for i in range(20):
        q.append({"i": i})
    records = q.flush()
    assert len(records) == 20


# ── ThreadSafetyTest ──────────────────────────────────

def test_concurrent_appends_exact_match(temp_queue_path):
    """2~4개의 스레드와 소량의 데이터 사용 — 콘텐츠의 무결성(integrity)을 철저히 검증한다."""
    cfg = QueueConfig.from_env()
    cfg.path = Path(temp_queue_path)
    q = FileQueue(cfg)
    n_threads, per_thread = 4, 10
    errors, err_lock = [], threading.Lock()
    expected = set()

    for tid in range(n_threads):
        for idx in range(per_thread):
            expected.add((tid, idx))

    barrier = threading.Barrier(n_threads)

    def writer(tid):
        barrier.wait()
        for idx in range(per_thread):
            try:
                q.append({"tid": tid, "idx": idx})
            except Exception as e:
                with err_lock:
                    errors.append(e)

    threads = [threading.Thread(target=writer, args=(tid,)) for tid in range(n_threads)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert errors == [], f"Exception occurred during append: {errors}"
    records = q.flush()
    actual = {(r["tid"], r["idx"]) for r in records}
    assert len(records) == n_threads * per_thread
    assert actual == expected

def test_high_concurrency_stress(temp_queue_path):
    """20개의 스레드와 대량의 데이터 사용 — 시스템 부하 상황에서 지연이나 에러가 발생하지 않는지 검증"""
    cfg = QueueConfig.from_env()
    cfg.path = Path(temp_queue_path)
    q = FileQueue(cfg)
    n_threads, per_thread = 20, 500
    errors, err_lock = [], threading.Lock()
    barrier = threading.Barrier(n_threads)

    def writer(tid):
        barrier.wait()
        for idx in range(per_thread):
            try:
                q.append({"tid": tid, "idx": idx})
                if idx % 10 == 0:
                    time.sleep(0.00001)
            except Exception as e:
                with err_lock:
                    errors.append(e)

    threads = [threading.Thread(target=writer, args=(tid,)) for tid in range(n_threads)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert errors == [], f"Exception occurred during high-load append"
    records = q.flush()
    assert len(records) == n_threads * per_thread

def test_concurrent_append_and_flush(temp_queue_path):
    """append 스레드들과 flush 스레드를 동시에 실행하여 상호작용을 검증한다."""
    cfg = QueueConfig.from_env()
    cfg.path = Path(temp_queue_path)
    q = FileQueue(cfg)
    n_writers, per_writer = 3, 50
    errors, err_lock = [], threading.Lock()
    collected, coll_lock = [], threading.Lock()
    stop_flag = threading.Event()

    def writer(tid):
        for idx in range(per_writer):
            try:
                q.append({"tid": tid, "idx": idx})
                time.sleep(0.001)
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
    for t in writer_threads: t.start()
    for t in writer_threads: t.join()

    stop_flag.set()
    flush_thread.join()
    collected.extend(q.flush())

    assert errors == []
    assert len(collected) == n_writers * per_writer


# ── CapacityWarningTest ──────────────────────────────────

def test_no_warning_below_threshold(temp_queue_path, monkeypatch):
    """사용량이 80% 미만이면 WARNING이 발생하지 않아야 한다."""
    q = _make_queue(temp_queue_path, max_bytes=1000)
    warning_count = [0]
    
    def counting(msg, *a, **kw):
        if "Queue usage" in str(msg):
            warning_count[0] += 1
    
    monkeypatch.setattr(q.log, "warning", counting)
    q.append({"v": "x"})
    assert warning_count[0] == 0

def test_warning_emitted_above_threshold(temp_queue_path, caplog):
    """사용량이 80%를 넘으면 WARNING 로그가 나와야 한다."""
    q = _make_queue(temp_queue_path, max_bytes=100)
    with caplog.at_level("WARNING", logger="FileQueue"):
        try:
            q.append({"v": "x" * 70})
        except QueueFullError:
            pass
        assert any("Queue usage" in r.message or "PQ FULL" in r.message for r in caplog.records)

def test_warned_full_flag_resets_after_commit(temp_queue_path):
    """commit으로 용량이 줄어들면 _warned_full 플래그가 False로 리셋되어야 한다."""
    q = _make_queue(temp_queue_path, max_bytes=300)
    try:
        q.append({"v": "x" * 200})
    except QueueFullError:
        pass
    q.peek()
    q.commit()
    assert q._warned_full is False

def test_warning_not_duplicated_while_flag_set(temp_queue_path, monkeypatch):
    """_warned_full=True 상태에서 추가 append를 해도 경고가 중복 출력되지 않아야 한다."""
    q = _make_queue(temp_queue_path, max_bytes=500)
    q._warned_full = True
    warning_count = [0]
    
    def counting(msg, *a, **kw):
        if "Queue usage" in str(msg):
            warning_count[0] += 1
            
    monkeypatch.setattr(q.log, "warning", counting)
    try:
        q.append({"v": "x" * 300})
    except QueueFullError:
        pass
    assert warning_count[0] == 0