"""
file_queue.py
JSONL 기반 파일 PQ.
"""

import json
import logging
import threading
from pathlib import Path

from config import QueueConfig


class FileQueue:
    """
    JSONL 기반 파일 PQ.

    - append() : 레코드를 파일 끝에 추가
    - flush()  : 현재까지 쌓인 레코드 반환 + 해당 줄 파일에서 제거
    - size_limit_enabled=True 일 때만 용량 초과 시 오래된 줄부터 drop
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
        현재까지 쌓인 레코드를 모두 반환하고 해당 줄을 파일에서 제거한다.
        flush 이후 append된 줄은 보존된다.
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
        """용량 확보를 위해 파일 앞쪽 줄을 제거한다."""
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