"""
main.py
수집기(MQTTCollector)와 적재기(DBLoader)를 띄우는 진입점.

실행: python main.py
"""

import threading

from config import MQTTConfig, QueueConfig, DBConfig, LoaderConfig, MapperConfig, LogConfig
from logger import setup_logger
from file_queue import FileQueue
from collector import MQTTCollector
from loader import DBLoader


def main():
    log_cfg = LogConfig.from_env()
    setup_logger(log_cfg)

    queue     = FileQueue(QueueConfig.from_env())
    collector = MQTTCollector(MQTTConfig.from_env(), MapperConfig.from_env(), queue)
    loader    = DBLoader(DBConfig.from_env(), LoaderConfig.from_env(), queue)

    t = threading.Thread(target=loader.run, daemon=True, name="loader")
    t.start()

    collector.run()  # 메인 스레드 블로킹


if __name__ == "__main__":
    main()