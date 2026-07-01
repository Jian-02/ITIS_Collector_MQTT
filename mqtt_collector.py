"""
collector.py
MQTT 브로커를 구독하고 수신된 메시지를 FileQueue에 저장합니다.
"""

import json
import logging
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from config import MQTTConfig, MapperConfig
from file_queue import FileQueue, QueueFullError
from data_mapper import PayloadMapper


class MQTTCollector:

    def __init__(self, cfg: MQTTConfig, mapper_cfg: MapperConfig, queue: FileQueue):
        self.cfg    = cfg
        self.queue  = queue
        self.mapper = PayloadMapper(mapper_cfg.mapping_path)
        self.log    = logging.getLogger(self.__class__.__name__)

        self._client = mqtt.Client()
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._running = True
        self._consecutive_failures = 0

        if cfg.username:
            self._client.username_pw_set(cfg.username, cfg.password)

    def stop(self):
        self._running = False
        self.log.info("Stopping MQTT Collector...")

    def start(self):
        self._running = True

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.log.info("MQTT Connection Succesful")
            client.subscribe(self.cfg.topic)
            self.log.info(f"Subscribe Topice: {self.cfg.topic}")
            self._consecutive_failures = 0
        else:
            self.log.error(f"MQTT Connection Fail (rc={rc})")

    def _on_message(self, client, userdata, msg):
        """
        메시지 처리 전체를 try/except로 감쌉니다.
        매핑 실패, 큐 적체(QueueFullError) 등으로 인해 이 콜백이 예외를 던지면
        paho-mqtt의 네트워크 루프 스레드 자체가 죽어버릴 수 있으므로,
        예외가 절대 콜백 밖으로 전파되지 않도록 합니다.
        잘못된 메시지 1건 때문에 전체 수집 파이프라인이 멈춰서는 안 됩니다.
        """
        try:
            received_at = datetime.now(timezone.utc).isoformat()
            payload     = self._parse_payload(msg.payload)
            topic_meta  = self._parse_topic(msg.topic)

            try:
                record = self.mapper.build_record(
                    topic       = msg.topic,
                    payload     = payload,
                    topic_meta  = topic_meta,
                    received_at = received_at,
                )
            except Exception as e:
                # 매핑 로직 자체에서 예기치 못한 예외가 발생한 경우: 해당 메시지만 버리고 계속 진행
                self.log.error(
                    f"Mapping failed, dropping message (topic={msg.topic}): {e}"
                )
                return

            try:
                self.queue.append(record)
            except QueueFullError as e:
                # PQ 용량 초과: 데이터 유실이 발생하는 심각한 상황이므로 ERROR로 명확히 남깁니다.
                # (추후 알람/모니터링 시스템과 연동 시 이 로그를 기준으로 alert를 trigger할 수 있습니다.)
                self.log.error(
                    f"[PQ FULL] Queue capacity exceeded. Message dropped (topic={msg.topic}): {e}"
                )
                return

            self.log.debug(f"Enqueued message to topic: {msg.topic}")

        except Exception as e:
            # 그 외 예상하지 못한 모든 예외에 대한 최종 방어선
            self.log.error(f"Unexpected error while handling message (topic={getattr(msg, 'topic', '?')}): {e}")

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
        while self._running:
            try:
                self.log.info(f"MQTT Try Connection: {self.cfg.host}:{self.cfg.port}")
                self._client.connect(self.cfg.host, self.cfg.port, keepalive=60)
                self._consecutive_failures = 0
                self._client.loop_forever()
            except (ConnectionRefusedError, OSError) as e:
                if not self._running:
                    break  # stop() 호출 후 발생한 에러는 무시

                self._consecutive_failures += 1
                self.log.warning(
                    f"Connection failed ({self._consecutive_failures} consecutive failure(s)): {e}. "
                    f"Retrying in {self.cfg.retry_interval} seconds."
                )

                if self.cfg.max_retries and self._consecutive_failures >= self.cfg.max_retries:
                    self.log.error(
                        f"MQTT connection failed {self._consecutive_failures} times in a row "
                        f"(limit={self.cfg.max_retries}). Giving up and stopping the collector."
                    )
                    self._running = False
                    break

                time.sleep(self.cfg.retry_interval)