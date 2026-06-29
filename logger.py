"""
logger.py
Filename: collector_YYYYMMDD_HHMMSS.log
- Creates a new time-based file when the current file exceeds max_bytes.
- Deletes the oldest files first when the file count exceeds max_files.
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
    """Returns the log file path based on the current time."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return log_dir / f"{FILE_PREFIX}{ts}.log"


def _sorted_log_files(log_dir: Path) -> list[Path]:
    """Returns log files in the log directory sorted from oldest to newest."""
    files = [
        Path(p)
        for p in glob.glob(str(log_dir / f"{FILE_PREFIX}*.log"))
        if FILE_PATTERN.search(p)
    ]
    return sorted(files, key=lambda p: FILE_PATTERN.search(p.name).group(1))


def _purge_old_files(log_dir: Path, max_files: int):
    """Deletes the oldest files first when max_files is exceeded."""
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
    Rotates to a new file when the current file exceeds max_bytes.
    The filename is based on the rotation time, and old files are deleted according to max_files.
    """

    def __init__(self, cfg: LogConfig):
        self.cfg     = cfg
        self.log_dir = cfg.log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)

        current = _current_filename(self.log_dir)
        super().__init__(current, encoding="utf-8")

        _purge_old_files(self.log_dir, cfg.max_files)

    def emit(self, record: logging.LogRecord):
        # Rotate when the current file exceeds max_bytes
        try:
            if self.stream and Path(self.baseFilename).stat().st_size >= self.cfg.max_bytes:
                self._rotate()
        except OSError:
            pass
        super().emit(record)

    def _rotate(self):
        """Closes the current file, opens a new one, and purges old files."""
        self.stream.flush()
        self.stream.close()

        new_path = _current_filename(self.log_dir)
        self.baseFilename = str(new_path)
        self.stream = self._open()

        _purge_old_files(self.log_dir, self.cfg.max_files)


def setup_logger(cfg: LogConfig) -> None:
    """
    Configures console and file handlers on the root logger.
    Call this once from main.py.
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
