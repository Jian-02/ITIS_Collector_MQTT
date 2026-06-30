"""
main.py
MQTTCollector와 DBLoader를 시작하는 진입점(Entry point)입니다.

실행 방법: python main.py
"""

import threading
import sys
import signal

from config import MQTTConfig, QueueConfig, DBConfig, LoaderConfig, MapperConfig, LogConfig
from logger import setup_logger
from file_queue import FileQueue
from mqtt_collector import MQTTCollector
from loader import DBLoader

# 전역 변수로 관리하여 signal_handler에서 접근 가능하도록 함
collector = None
loader = None

def signal_handler(sig, frame):
    """프로그램 종료 신호(Ctrl+C) 감지 시 호출"""
    print("\n[System] Shutdown signal received. Cleaning up...")
    
    # 여기서 각 객체의 종료 메서드(stop 등)를 호출하여 안전하게 종료
    if collector:
        collector.stop()  # MQTT 구독 해제 등
    if loader:
        loader.stop()     # DB 연결 종료 등
        
    print("[System] Shutdown complete.")
    sys.exit(0)

def main():
    global collector, loader  # 전역 변수 참조
    
    log_cfg = LogConfig.from_env()
    setup_logger(log_cfg)

    queue     = FileQueue(QueueConfig.from_env())
    collector = MQTTCollector(MQTTConfig.from_env(), MapperConfig.from_env(), queue)
    loader    = DBLoader(DBConfig.from_env(), LoaderConfig.from_env(), queue)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    t = threading.Thread(target=loader.run, daemon=True, name="loader")
    t.start()

    #메인 스레드 블로킹
    try:
        collector.run()
    except Exception as e:
        print(f"[Error] Collector crashed: {e}")
    finally:
        # 비정상 종료 시에도 안전한 마무리를 보장
        signal_handler(None, None)


if __name__ == "__main__":
    main()
