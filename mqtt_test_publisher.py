"""
mqtt_test_publisher.py
ITIS_Collector_MQTT 테스트용 MQTT publisher

사용법:
    python mqtt_test_publisher.py
    python mqtt_test_publisher.py --count 50 --interval 0.5
    python mqtt_test_publisher.py --host localhost --port 1883

기본적으로 .env 의 MQTT_HOST / MQTT_PORT 를 읽어 사용하며,
factory/line1/temp, factory/line1/humidity 등의 토픽으로
mapping.json 형식(value, ts)에 맞는 payload를 랜덤하게 발행합니다.
"""

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone

import paho.mqtt.publish as publish
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

# 테스트용 토픽 목록 (site/device/sensor 형식 — 3파츠 이상이어야 정상 파싱됨)
TOPICS = [
    ("factory/line1/temp",     lambda: round(random.uniform(20, 30), 2)),
    ("factory/line1/humidity", lambda: round(random.uniform(40, 70), 2)),
    ("factory/line2/temp",     lambda: round(random.uniform(20, 30), 2)),
    ("factory/line2/pressure", lambda: round(random.uniform(1.0, 2.0), 3)),
]


def build_payload(value_fn) -> str:
    return json.dumps(
        {
            "value": value_fn(),
            "ts": datetime.now(timezone.utc).isoformat(),
        },
        ensure_ascii=False,
    )


def main():
    parser = argparse.ArgumentParser(description="MQTT 테스트 메시지 발행기")
    parser.add_argument("--host", default=os.getenv("MQTT_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    parser.add_argument("--username", default=os.getenv("MQTT_USERNAME", "") or None)
    parser.add_argument("--password", default=os.getenv("MQTT_PASSWORD", "") or None)
    parser.add_argument("--count", type=int, default=20, help="발행할 메시지 개수 (기본 20)")
    parser.add_argument("--interval", type=float, default=1.0, help="발행 간격 초 (기본 1초)")
    args = parser.parse_args()

    auth = None
    if args.username:
        auth = {"username": args.username, "password": args.password}

    print(f"[MQTT Test Publisher] {args.host}:{args.port} 로 {args.count}개 메시지 발행 시작")

    for i in range(args.count):
        topic, value_fn = random.choice(TOPICS)
        payload = build_payload(value_fn)

        publish.single(
            topic=topic,
            payload=payload,
            hostname=args.host,
            port=args.port,
            auth=auth,
        )
        print(f"  [{i + 1}/{args.count}] -> {topic} : {payload}")
        time.sleep(args.interval)

    print("[MQTT Test Publisher] 완료")


if __name__ == "__main__":
    main()