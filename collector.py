"""
collector.py
MQTT 브로커를 구독하여 수신 메시지를 FileQueue에 저장한다.
"""

import json
import logging
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from config import MQTTConfig, MapperConfig
from file_queue import FileQueue
from mapper import PayloadMapper


class MQTTCollector:

    def __init__(self, cfg: MQTTConfig, mapper_cfg: MapperConfig, queue: FileQueue):
        self.cfg    = cfg
        self.queue  = queue
        self.mapper = PayloadMapper(mapper_cfg.mapping_path)
        self.log    = logging.getLogger(self.__class__.__name__)

        self._client = mqtt.Client()
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message

        if cfg.username:
            self._client.username_pw_set(cfg.username, cfg.password)

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.log.info("MQTT 연결 성공")
            client.subscribe(self.cfg.topic)
            self.log.info(f"토픽 구독: {self.cfg.topic}")
        else:
            self.log.error(f"MQTT 연결 실패 (rc={rc})")

    def _on_message(self, client, userdata, msg):
        received_at = datetime.now(timezone.utc).isoformat()
        payload     = self._parse_payload(msg.payload)
        topic_meta  = self._parse_topic(msg.topic)

        record = self.mapper.build_record(
            topic       = msg.topic,
            payload     = payload,
            topic_meta  = topic_meta,
            received_at = received_at,
        )

        self.queue.append(record)
        self.log.debug(f"큐 적재: {msg.topic}")

    def _parse_topic(self, topic: str) -> dict:
        parts = topic.split("/")
        if len(parts) >= 3:
            return {"site": parts[0], "device": parts[1], "sensor": "/".join(parts[2:])}
        return {"site": None, "device": None, "sensor": topic}

    def _parse_payload(self, raw: bytes) -> dict:
        try:
            return json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {"raw": raw.decode("utf-8", errors="replace")}

    def run(self):
        while True:
            try:
                self.log.info(f"MQTT 연결 시도: {self.cfg.host}:{self.cfg.port}")
                self._client.connect(self.cfg.host, self.cfg.port, keepalive=60)
                self._client.loop_forever()
            except (ConnectionRefusedError, OSError) as e:
                self.log.warning(f"연결 실패: {e} — 5초 후 재시도")
                time.sleep(5)