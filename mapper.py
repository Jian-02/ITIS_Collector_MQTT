"""
mapper.py
Reads mapping.json and converts MQTT messages into database records.
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
    Converts MQTT messages into dict records according to mapping.json.

    Supported source paths:
      "topic"            -> MQTT topic string
      "payload.field"    -> Nested payload field (payload.a.b.c is supported)
      "__received_at__"  -> Collector receive timestamp
      "__payload__"      -> Original full payload
      "__topic_site__"   -> Parsed topic site
      "__topic_device__" -> Parsed topic device
      "__topic_sensor__" -> Parsed topic sensor
    """

    def __init__(self, mapping_path: Path):
        self.log     = logging.getLogger(self.__class__.__name__)
        self.mapping = self._load(mapping_path)
        self.log.info(f"매핑 로드 완료: {mapping_path} ({len(self.mapping)}개 컬럼)")

    def _load(self, path: Path) -> dict:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        mapping = raw.get("mapping")
        if not mapping or not isinstance(mapping, dict):
            raise ValueError(f"mapping.json 에 'mapping' 키가 없거나 형식이 잘못됐습니다: {path}")

        return mapping

    def build_record(
        self,
        topic: str,
        payload: dict,
        topic_meta: dict,
        received_at: str,
    ) -> dict:
        """Builds a record dict according to the mapping definition."""
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
                # Directly reference a top-level payload field
                record[col] = payload.get(src)

        return record

    def _dig(self, data: dict, path: str):
        """Reads a nested dict value using a dot-separated path. Returns None if missing."""
        keys = path.split(".")
        cur  = data
        for key in keys:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        return cur
