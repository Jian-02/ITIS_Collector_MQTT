"""
file_queue.py
two-phase commit 방식의 JSONL 기반 persistent queue입니다.
 
──────────────────────────────────────────────────
 File structure (queue.jsonl)
──────────────────────────────────────────────────
 각 line은 다음 두 가지 형태 중 하나입니다:
 
    {"_s":"Q", ...data...}   ← 대기 중 (Queued)
    {"_s":"P", ...data...}   ← 처리 중 (Pending): DB INSERT 시도 중
 
 Normal flow:
    append()  → _s = "Q"
    peek()    → "Q" line만 읽어서 리턴하고, 파일 내에서 _s를 "P"로 변경
    commit()  → "P" line들을 제거
    rollback() → "P" line들을 다시 "Q"로 복구
 
 Crash recovery (재시작 시 __init__ 내부에서 수행):
    "P" line이 남아있으면 자동으로 "Q"로 revert됨
    → 다음 peek() 시점에 reprocessed됨

 용량 초과(Full) 처리 정책:
    append() 시점에 size_limit_enabled=True 이고 max_bytes를 초과하면
    QueueFullError를 발생시켜 "신규 데이터 적재를 거부"합니다 (오래된 데이터를 덮어쓰지 않음).
    호출부(mqtt_collector)는 이 예외를 잡아서 해당 메시지 1건만 드롭하고 ERROR 레벨로 로그를 남기며,
    전체 프로세스는 계속 동작합니다 (= 큐가 가득 찼다고 collector/loader 자체가 죽지는 않습니다).
    또한 사용량이 WARN_THRESHOLD(기본 80%)를 넘으면 사전 경고(WARNING) 로그를 남겨,
    실제로 가득 차서 데이터가 유실되기 전에 운영자가 인지할 수 있도록 합니다.
    (현재는 로그 기반 알림이며, 추후 관리 웹페이지/모니터링 시스템에서 이 로그를 수집해
     Slack/이메일 알람으로 연동하는 것을 권장합니다. 지금 단계에서 알람 채널까지 직접
     구현하는 것은 과한 결합이라 판단하여 로그 레벨 분리까지만 우선 처리했습니다.)
──────────────────────────────────────────────────
"""

import json
import logging
import os
import threading

from config import QueueConfig

_STATUS_QUEUED  = "Q"
_STATUS_PENDING = "P"
_STATUS_KEY     = "_s"

_WARN_THRESHOLD = 0.8  # 용량의 80%를 넘으면 경고 로그


def _mark(record: dict, status: str) -> dict:
    """record에 status field가 추가된 새로운 dict를 리턴합니다 (original은 변경되지 않음)."""
    return {_STATUS_KEY: status, **{k: v for k, v in record.items() if k != _STATUS_KEY}}


def _strip(record: dict) -> dict:
    """status field(_s)가 제거된 pure data dict를 리턴합니다."""
    return {k: v for k, v in record.items() if k != _STATUS_KEY}

class QueueFullError(Exception):
    """queue file capacity가 가득 찼을 때 발생하는 Error입니다."""
    pass

class FileQueue:
    """
    JSONL 기반의 two-phase commit을 지원하는 persistent queue입니다.

    Public API
    ----------
    append(record)   : queue에 record를 추가합니다 (Queued state)
    peek()           : 모든 대기 중인 record를 읽고 Pending 상태로 transition합니다
    commit()         : 파일에서 Pending record를 제거합니다 (DB INSERT 성공 후 호출)
    rollback()       : Pending record를 다시 Queued 상태로 복구합니다 (DB INSERT 실패 시 호출)

    Backward-compatible API
    ------------------------
    flush()          : 한 번의 호출로 peek() + commit()을 함께 수행합니다 (기존 code와의 호환성용)
    """

    def __init__(self, cfg: QueueConfig):
        self.path               = cfg.path
        self.size_limit_enabled = cfg.size_limit_enabled
        self.max_bytes          = cfg.max_bytes
        self._lock              = threading.Lock()
        self.log                = logging.getLogger(self.__class__.__name__)
        self._warned_full       = False  # 경고 로그 중복 방지 플래그

        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.touch()

        # Crash recovery: 재시작 시 Pending line들을 Queued로 revert합니다.
        recovered = self._recover_pending()
        if recovered:
            self.log.warning(f"Crash recovery: restored {recovered} Pending record(s) to Queued")

        if self.size_limit_enabled:
            self.log.info(f"PQ size limit: {self.max_bytes // (1024 * 1024)} MB")
        else:
            self.log.info("PQ size limit: disabled")

    # ── Write ──────────────────────────────────────────────

    def append(self, record: dict):
        """Queued state로 file의 끝에 record를 append합니다. Capacity를 초과하면 error를 발생시킵니다."""
        line = json.dumps(_mark(record, _STATUS_QUEUED), ensure_ascii=False) + "\n"
        line_bytes = len(line.encode("utf-8"))

        with self._lock:
            if self.size_limit_enabled:
                current_size = self.path.stat().st_size if self.path.exists() else 0

                if current_size + line_bytes > self.max_bytes:
                    self.log.error(
                        f"[PQ FULL] Queue capacity exceeded "
                        f"({current_size}/{self.max_bytes} bytes). New data rejected."
                    )
                    raise QueueFullError("Queue storage capacity exceeded.")

                # 용량 임계치(기본 80%) 근접 시 사전 경고 (한 번만, 다시 여유가 생기면 재경고 가능하도록 리셋)
                usage_ratio = (current_size + line_bytes) / self.max_bytes
                if usage_ratio >= _WARN_THRESHOLD:
                    if not self._warned_full:
                        self.log.warning(
                            f"[PQ WARNING] Queue usage at {usage_ratio:.0%} "
                            f"({current_size + line_bytes}/{self.max_bytes} bytes). "
                            f"Check DB connectivity/loader status before it becomes full."
                        )
                        self._warned_full = True
                else:
                    self._warned_full = False

            # file에 쓰고 '즉각적인' flush + disk로의 fsync를 강제합니다.
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())  # Force the write to disk instead of staying in the buffer

    # ── Commit ──────────────────────────────────────────────

    def peek(self) -> list[dict]:
        """
        Queued 레코드를 읽어와 파일 내 상태를 Pending으로 변경합니다.
        반환값은 _s 필드가 제거된 순수 데이터 딕셔너리 리스트입니다.
        DB INSERT 이전에 호출합니다.
        """
        with self._lock:
            lines = self._read_lines()
            if not lines:
                return []

            records  = []
            new_lines = []
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    new_lines.append(line)
                    continue
                try:
                    obj = json.loads(stripped)
                except json.JSONDecodeError:
                    self.log.warning(f"Skipping line that failed to parse: {stripped[:80]}")
                    continue

                if obj.get(_STATUS_KEY) == _STATUS_QUEUED:
                    records.append(_strip(obj))
                    new_lines.append(
                        json.dumps(_mark(obj, _STATUS_PENDING), ensure_ascii=False) + "\n"
                    )
                else:
                    # Pending 라인이나 status 필드가 없는 레거시 라인은 그대로 유지합니다.
                    new_lines.append(line)

            self._write_lines(new_lines)
            return records

    def commit(self):
        """
        파일에서 Pending 레코드를 제거합니다.
        DB INSERT 성공 이후에 호출합니다.
        """
        with self._lock:
            lines = self._read_lines()
            kept  = [
                l for l in lines
                if not self._is_status(l, _STATUS_PENDING)
            ]
            self._write_lines(kept)
            removed = len(lines) - len(kept)
            if removed:
                self.log.debug(f"commit: removed {removed} line(s)")

    def rollback(self):
        """
        Pending 레코드를 다시 Queued 상태로 복구합니다.
        DB INSERT 실패 이후에 호출합니다.
        """
        with self._lock:
            lines     = self._read_lines()
            new_lines = []
            restored  = 0
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    new_lines.append(line)
                    continue
                try:
                    obj = json.loads(stripped)
                except json.JSONDecodeError:
                    new_lines.append(line)
                    continue

                if obj.get(_STATUS_KEY) == _STATUS_PENDING:
                    new_lines.append(
                        json.dumps(_mark(obj, _STATUS_QUEUED), ensure_ascii=False) + "\n"
                    )
                    restored += 1
                else:
                    new_lines.append(line)

            self._write_lines(new_lines)
            if restored:
                self.log.info(f"rollback: restored {restored} record(s) to Queued")

     # ── Backward-compatible API ────────────────────────────────────

    def flush(self) -> list[dict]:
        """
        Atomically performs peek() + commit().
        Kept for backward compatibility with existing code.
        If data protection is needed on INSERT failure, use peek/commit/rollback directly.
        """
        records = self.peek()
        if records:
            self.commit()
        return records

    # ── Internal helpers ────────────────────────────────────────

    def _read_lines(self) -> list[str]:
        if self.path.stat().st_size == 0:
            return []
        with open(self.path, "r", encoding="utf-8") as f:
            return f.readlines()

    def _write_lines(self, lines: list[str]):
        with open(self.path, "w", encoding="utf-8") as f:
            f.writelines(lines)

    def _is_status(self, line: str, status: str) -> bool:
        stripped = line.strip()
        if not stripped:
            return False
        try:
            obj = json.loads(stripped)
            return obj.get(_STATUS_KEY) == status
        except json.JSONDecodeError:
            return False

    def _recover_pending(self) -> int:
        """재시작 시 Pending 라인들을 다시 Queued 상태로 복구합니다. 복구된 레코드 수를 반환합니다."""
        with self._lock:
            lines     = self._read_lines()
            new_lines = []
            recovered = 0
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    new_lines.append(line)
                    continue
                try:
                    obj = json.loads(stripped)
                except json.JSONDecodeError:
                    new_lines.append(line)
                    continue

                if obj.get(_STATUS_KEY) == _STATUS_PENDING:
                    new_lines.append(
                        json.dumps(_mark(obj, _STATUS_QUEUED), ensure_ascii=False) + "\n"
                    )
                    recovered += 1
                else:
                    new_lines.append(line)

            if recovered:
                self._write_lines(new_lines)
            return recovered

    def _drop_oldest(self, needed_bytes: int):
        """용량(capacity)을 초과할 경우, 가장 오래된 Queued 라인부터 순차적으로 제거합니다."""
        lines = self._read_lines()

        dropped, freed = 0, 0
        for line in lines:
            if freed >= needed_bytes:
                break
            # 현재 처리 중인 Pending 라인은 드롭하지 않고 유지합니다.
            if self._is_status(line, _STATUS_PENDING):
                continue
            freed   += len(line.encode())
            dropped += 1

        # 가장 앞쪽에 있는 Queued 라인만 제거합니다 (Pending 라인은 유지됩니다).
        kept, removed = [], 0
        for line in lines:
            if removed < dropped and not self._is_status(line, _STATUS_PENDING):
                removed += 1
            else:
                kept.append(line)

        self._write_lines(kept)
        self.log.warning(f"[PQ] Capacity exceeded - Dropped {removed} lines ({freed / 1024:.1f} KB freed)")