"""
file_queue.py
JSONL-backed file priority queue.
"""

import json
import logging
import threading
from pathlib import Path

from config import QueueConfig


class FileQueue:
    """
    JSONL-backed file priority queue.

    - append(): Adds a record to the end of the file.
    - flush(): Returns accumulated records and removes those lines from the file.
    - When size_limit_enabled=True, drops the oldest lines only if the file exceeds the size limit.
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

        if self.size_limit_enabled:
            self.log.info(f"PQ 용량 제한: {self.max_bytes // (1024 * 1024)} MB")
        else:
            self.log.info("PQ 용량 제한: 비활성화")

    def append(self, record: dict):
        line = json.dumps(record, ensure_ascii=False) + "\n"

        with self._lock:
            if self.size_limit_enabled:
                if self.path.stat().st_size + len(line.encode()) > self.max_bytes:
                    self._drop_oldest(len(line.encode()))

            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line)

    def flush(self) -> list[dict]:
        """
        Returns all accumulated records and removes those lines from the file.
        Lines appended after flush starts are preserved.
        """
        with self._lock:
            if self.path.stat().st_size == 0:
                return []

            with open(self.path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            records, consumed = [], 0
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    consumed += 1
                    continue
                try:
                    records.append(json.loads(stripped))
                except json.JSONDecodeError:
                    self.log.warning(f"파싱 실패 라인 스킵: {stripped[:80]}")
                consumed += 1

            remaining = lines[consumed:]
            with open(self.path, "w", encoding="utf-8") as f:
                f.writelines(remaining)

            return records

    def _drop_oldest(self, needed_bytes: int):
        """Removes lines from the beginning of the file to free space."""
        with open(self.path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        dropped, freed = 0, 0
        for line in lines:
            if freed >= needed_bytes:
                break
            freed   += len(line.encode())
            dropped += 1

        with open(self.path, "w", encoding="utf-8") as f:
            f.writelines(lines[dropped:])

        self.log.warning(f"PQ 용량 초과 — {dropped}줄 drop ({freed / 1024:.1f} KB 확보)")
