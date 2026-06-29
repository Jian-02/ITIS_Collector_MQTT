"""
logger.py
파일명: collector_YYYYMMDD_HHMMSS.log
- 파일이 max_bytes 초과 시 새 파일 생성 (시간 기준)
- 파일 수가 max_files 초과 시 오래된 파일부터 삭제
"""

import glob
import logging
import logging.handlers
import os
import re
from datetime import datetime
from pathlib import Path

from config import LogConfig

FILE_PREFIX  = "collector_"
FILE_PATTERN = re.compile(r"collector_(\d{8}_\d{6})\.log$")


def _current_filename(log_dir: Path) -> Path:
    """현재 시각 기준 로그 파일 경로를 반환한다."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return log_dir / f"{FILE_PREFIX}{ts}.log"


def _sorted_log_files(log_dir: Path) -> list[Path]:
    """로그 디렉토리 내 파일을 오래된 순으로 정렬해서 반환한다."""
    files = [
        Path(p)
        for p in glob.glob(str(log_dir / f"{FILE_PREFIX}*.log"))
        if FILE_PATTERN.search(p)
    ]
    return sorted(files, key=lambda p: FILE_PATTERN.search(p.name).group(1))


def _purge_old_files(log_dir: Path, max_files: int):
    """max_files 초과 시 오래된 파일부터 삭제한다."""
    files = _sorted_log_files(log_dir)
    while len(files) > max_files:
        oldest = files.pop(0)
        try:
            oldest.unlink()
            logging.getLogger("logger").info(f"오래된 로그 파일 삭제: {oldest.name}")
        except OSError as e:
            logging.getLogger("logger").warning(f"로그 파일 삭제 실패: {oldest.name} — {e}")


class SizeAndTimeRotatingHandler(logging.FileHandler):
    """
    파일 크기가 max_bytes 초과 시 새 파일로 교체한다.
    파일명은 교체 시각 기준으로 생성되며, 오래된 파일은 max_files 기준으로 자동 삭제된다.
    """

    def __init__(self, cfg: LogConfig):
        self.cfg     = cfg
        self.log_dir = cfg.log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)

        current = _current_filename(self.log_dir)
        super().__init__(current, encoding="utf-8")

        _purge_old_files(self.log_dir, cfg.max_files)

    def emit(self, record: logging.LogRecord):
        # 현재 파일이 max_bytes 초과 시 새 파일로 교체
        try:
            if self.stream and Path(self.baseFilename).stat().st_size >= self.cfg.max_bytes:
                self._rotate()
        except OSError:
            pass
        super().emit(record)

    def _rotate(self):
        """현재 파일을 닫고 새 파일을 연다. 오래된 파일 정리도 수행한다."""
        self.stream.flush()
        self.stream.close()

        new_path = _current_filename(self.log_dir)
        self.baseFilename = str(new_path)
        self.stream = self._open()

        _purge_old_files(self.log_dir, self.cfg.max_files)


def setup_logger(cfg: LogConfig) -> None:
    """
    루트 로거에 콘솔 + 파일 핸들러를 설정한다.
    main.py 에서 한 번만 호출하면 된다.
    """
    fmt = logging.Formatter(
        "%(asctime)s [%(name)s] %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler    = SizeAndTimeRotatingHandler(cfg)
    console_handler = logging.StreamHandler()

    file_handler.setFormatter(fmt)
    console_handler.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(cfg.level)
    root.addHandler(file_handler)
    root.addHandler(console_handler)