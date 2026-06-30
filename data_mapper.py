"""
mapper.py
mapping.json을 읽어 MQTT message를 database record로 변환합니다.
"""

import json
import logging
from pathlib import Path


# --- Reserved keywords -------------------------------------
_RESERVED = {
    "__received_at__",
    "__payload__",
    "__topic_site__",
    "__topic_device__",
    "__topic_sensor__",
}


class PayloadMapper:
    """
    mapping.json에 따라 MQTT message를 dict record로 변환합니다.

    지원되는 source path:
    "topic"            -> MQTT topic 문자열
    "payload.field"    -> Nested payload field (payload.a.b.c 형식 지원)
    "__received_at__"  -> Collector의 수신 timestamp
    "__payload__"      -> 원본 full payload
    "__topic_site__"   -> Parsing된 topic site
    "__topic_device__" -> Parsing된 topic device
    "__topic_sensor__" -> Parsing된 topic sensor
    """

    def __init__(self, mapping_path: Path):
        self.log     = logging.getLogger(self.__class__.__name__)
        self.mapping = self._load(mapping_path)
        self.log.info(f"Mapping loaded: {mapping_path} ({len(self.mapping)} columns)")

    def _load(self, path: Path) -> dict:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        mapping = raw.get("mapping")
        if not mapping or not isinstance(mapping, dict):
            raise ValueError(f"mapping.json is missing the 'mapping' key or has an invalid format: {path}")

        return mapping

    def build_record(
        self,
        topic: str,
        payload: dict,
        topic_meta: dict,
        received_at: str,
    ) -> dict:
        """mapping 정의에 따라 record dict를 생성합니다."""
        reserved_values = {
            "__received_at__":  received_at,
            "__payload__":      payload,
            "__topic_site__":   topic_meta.get("site"),
            "__topic_device__": topic_meta.get("device"),
            "__topic_sensor__": topic_meta.get("sensor"),
        }

        record = {}
        for col, src in self.mapping.items():
            if src in _RESERVED:
                record[col] = reserved_values[src]
            elif src == "topic":
                record[col] = topic
            elif src.startswith("payload."):
                record[col] = self._dig(payload, src[len("payload."):])
            else:
                # top-level payload field를 직접 참조합니다.
                record[col] = payload.get(src)

        return record

    def _dig(self, data: dict, path: str):
        """dot으로 구분된 path를 사용하여 nested dict 값을 읽습니다. 값이 없으면 None을 리턴합니다."""
        keys = path.split(".")
        cur  = data
        for key in keys:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        return cur
