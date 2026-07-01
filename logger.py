"""
logger.py
파일명: collector_YYYYMMDD_HHMMSS.log
- 현재 파일이 max_bytes를 초과하면 시간 기반의 새 파일을 생성합니다.
- 파일 수가 max_files를 초과하면 가장 오래된 파일부터 순차적으로 삭제합니다.

로그 포맷
---------
2026-06-30 12:00:00 [MQTTCollector] [INFO] MQTT Connection Succesful

레벨(LEVEL)을 대괄호로 감싸 한눈에 INFO/WARNING/ERROR를 구분할 수 있도록 합니다.
- INFO    : 정상 흐름 (연결 성공, N건 적재 완료, polling 시작 등)
- WARNING : 즉시 장애는 아니지만 주의가 필요한 상황 (재시도, 큐 사용량 80% 이상, crash recovery 등)
- ERROR   : 실제 실패/데이터 유실 위험 상황 (DB INSERT 실패, PQ FULL, 매핑 실패, 연결 포기 등)
"""

import glob
import logging
import re
from datetime import datetime
from pathlib import Path

from config import LogConfig

FILE_PREFIX  = "collector_"
FILE_PATTERN = re.compile(r"collector_(\d{8}_\d{6})\.log$")


def _current_filename(log_dir: Path) -> Path:
    """현재 시간을 기반으로 로그 파일 경로를 반환합니다."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return log_dir / f"{FILE_PREFIX}{ts}.log"


def _sorted_log_files(log_dir: Path) -> list[Path]:
    """로그 디렉터리 내의 로그 파일들을 가장 오래된 순부터 가장 최신 순으로 정렬하여 반환합니다."""
    files = [
        Path(p)
        for p in glob.glob(str(log_dir / f"{FILE_PREFIX}*.log"))
        if FILE_PATTERN.search(p)
    ]
    return sorted(files, key=lambda p: FILE_PATTERN.search(p.name).group(1))


def _purge_old_files(log_dir: Path, max_files: int):
    """max_files를 초과할 경우, 가장 오래된 파일부터 순차적으로 삭제합니다."""
    files = _sorted_log_files(log_dir)
    while len(files) > max_files:
        oldest = files.pop(0)
        try:
            oldest.unlink()
            logging.getLogger("logger").info(f"Deleted old log file: {oldest.name}")
        except OSError as e:
            logging.getLogger("logger").warning(f"Failed to delete log file: {oldest.name} — {e}")


class SizeAndTimeRotatingHandler(logging.FileHandler):
    """
    현재 파일이 max_bytes를 초과하면 새로운 파일로 로테이트(교체)합니다.
    파일명은 로테이트 시점의 시간을 기준으로 생성되며, 기존 파일들은 max_files에 따라 오래된 순으로 삭제됩니다.
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
        """현재 파일을 닫고 새 파일을 열며, 오래된 파일들을 정리(삭제)합니다."""
        self.stream.flush()
        self.stream.close()

        new_path = _current_filename(self.log_dir)
        self.baseFilename = str(new_path)
        self.stream = self._open()

        _purge_old_files(self.log_dir, self.cfg.max_files)


def setup_logger(cfg: LogConfig) -> None:
    """
    루트 로거(root logger)에 콘솔 및 파일 핸들러를 설정합니다.
    main.py에서 한 번만 호출하면 됩니다.
    """
    fmt = logging.Formatter(
        "%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
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