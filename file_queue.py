"""
file_queue.py
JSONL-backed persistent queue with two-phase commit.

──────────────────────────────────────────────────
 파일 구조 (queue.jsonl)
──────────────────────────────────────────────────
 각 줄은 아래 두 형태 중 하나:

   {"_s":"Q", ...데이터...}   ← 대기(Queued)
   {"_s":"P", ...데이터...}   ← 처리 중(Pending) : DB INSERT 시도 중

 정상 흐름:
   append()  → _s = "Q"
   peek()    → _s = "Q" 줄만 읽고 반환, 파일에서 _s를 "P"로 변경
   commit()  → _s = "P" 줄 제거
   rollback()→ _s = "P" 줄을 "Q"로 복원

 크래시 복구 (재시작 시 __init__ 내부):
   _s = "P" 줄이 남아 있으면 자동으로 "Q"로 되돌림
   → 다음 peek() 때 재처리

──────────────────────────────────────────────────
"""

import json
import logging
import threading
from pathlib import Path

from config import QueueConfig

_STATUS_QUEUED  = "Q"
_STATUS_PENDING = "P"
_STATUS_KEY     = "_s"


def _mark(record: dict, status: str) -> dict:
    """레코드에 상태 필드를 추가한 새 dict 반환 (원본 불변)."""
    return {_STATUS_KEY: status, **{k: v for k, v in record.items() if k != _STATUS_KEY}}


def _strip(record: dict) -> dict:
    """상태 필드(_s)를 제거한 순수 데이터 dict 반환."""
    return {k: v for k, v in record.items() if k != _STATUS_KEY}


class FileQueue:
    """
    JSONL-backed persistent queue with two-phase commit.

    Public API
    ----------
    append(record)   : 레코드를 큐에 추가  (Queued 상태)
    peek()           : 대기 중 레코드를 모두 읽고 Pending으로 전환
    commit()         : Pending 레코드를 파일에서 제거  (DB INSERT 성공 후 호출)
    rollback()       : Pending 레코드를 Queued로 복원  (DB INSERT 실패 후 호출)

    하위 호환 API
    -------------
    flush()          : peek() + commit() 를 한 번에 수행 (기존 코드와 호환)
    """

    def __init__(self, cfg: QueueConfig):
        self.path               = cfg.path
        self.size_limit_enabled = cfg.size_limit_enabled
        self.max_bytes          = cfg.max_bytes
        self._lock              = threading.Lock()
        self.log                = logging.getLogger(self.__class__.__name__)

        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.touch()

        # 크래시 복구: 재시작 시 Pending 줄을 Queued로 되돌림
        recovered = self._recover_pending()
        if recovered:
            self.log.warning(f"크래시 복구: {recovered}건의 Pending 레코드를 Queued로 복원")

        if self.size_limit_enabled:
            self.log.info(f"PQ 용량 제한: {self.max_bytes // (1024 * 1024)} MB")
        else:
            self.log.info("PQ 용량 제한: 비활성화")

    # ── 쓰기 ──────────────────────────────────────────────

    def append(self, record: dict):
        """레코드를 Queued 상태로 파일 끝에 추가."""
        line = json.dumps(_mark(record, _STATUS_QUEUED), ensure_ascii=False) + "\n"

        with self._lock:
            if self.size_limit_enabled:
                if self.path.stat().st_size + len(line.encode()) > self.max_bytes:
                    self._drop_oldest(len(line.encode()))

            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line)

    # ── 2단계 커밋 ────────────────────────────────────────

    def peek(self) -> list[dict]:
        """
        Queued 레코드를 읽고 파일 내 상태를 Pending으로 전환.
        반환값은 _s 필드가 제거된 순수 데이터 리스트.
        DB INSERT 전에 호출.
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
                    self.log.warning(f"파싱 실패 라인 스킵: {stripped[:80]}")
                    continue

                if obj.get(_STATUS_KEY) == _STATUS_QUEUED:
                    records.append(_strip(obj))
                    new_lines.append(
                        json.dumps(_mark(obj, _STATUS_PENDING), ensure_ascii=False) + "\n"
                    )
                else:
                    # Pending이거나 상태 필드 없는 레거시 줄은 그대로 유지
                    new_lines.append(line)

            self._write_lines(new_lines)
            return records

    def commit(self):
        """
        Pending 레코드를 파일에서 제거.
        DB INSERT 성공 후 호출.
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
                self.log.debug(f"commit: {removed}줄 제거")

    def rollback(self):
        """
        Pending 레코드를 Queued로 복원.
        DB INSERT 실패 후 호출.
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
                self.log.info(f"rollback: {restored}건 Queued로 복원")

    # ── 하위 호환 API ────────────────────────────────────

    def flush(self) -> list[dict]:
        """
        peek() + commit() 를 원자적으로 수행.
        기존 코드와의 하위 호환을 위해 유지.
        INSERT 실패 시 데이터 보호가 필요하면 peek/commit/rollback을 직접 사용.
        """
        records = self.peek()
        if records:
            self.commit()
        return records

    # ── 내부 헬퍼 ────────────────────────────────────────

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
        """재시작 시 Pending 줄을 Queued로 되돌림. 복구된 건수 반환."""
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
        """용량 초과 시 가장 오래된 Queued 줄부터 제거."""
        lines = self._read_lines()

        dropped, freed = 0, 0
        for line in lines:
            if freed >= needed_bytes:
                break
            # Pending 줄은 처리 중이므로 드롭하지 않음
            if self._is_status(line, _STATUS_PENDING):
                continue
            freed   += len(line.encode())
            dropped += 1

        # 앞에서부터 Queued 줄만 제거 (Pending은 보존)
        kept, removed = [], 0
        for line in lines:
            if removed < dropped and not self._is_status(line, _STATUS_PENDING):
                removed += 1
            else:
                kept.append(line)

        self._write_lines(kept)
        self.log.warning(f"PQ 용량 초과 — {removed}줄 drop ({freed / 1024:.1f} KB 확보)")