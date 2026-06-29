"""
mapper.py
mapping.json 을 읽어 MQTT 메시지를 DB 레코드로 변환한다.
"""

import json
import logging
from pathlib import Path


# ── 예약어 ───────────────────────────────────────────────
_RESERVED = {
    "__received_at__",
    "__payload__",
    "__topic_site__",
    "__topic_device__",
    "__topic_sensor__",
}


class PayloadMapper:
    """
    mapping.json 정의에 따라 MQTT 메시지 → dict 레코드로 변환한다.

    지원 경로 표현:
      "topic"            → MQTT 토픽 문자열
      "payload.field"    → 페이로드 중첩 필드 (payload.a.b.c 가능)
      "__received_at__"  → 수집기 수신 시각
      "__payload__"      → 원본 페이로드 전체
      "__topic_site__"   → 토픽 파싱 site
      "__topic_device__" → 토픽 파싱 device
      "__topic_sensor__" → 토픽 파싱 sensor
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
        """매핑 정의에 따라 레코드 dict 를 생성한다."""
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
                # 페이로드 최상위 필드 직접 참조
                record[col] = payload.get(src)

        return record

    def _dig(self, data: dict, path: str):
        """점(.) 구분 경로로 중첩 dict 값을 꺼낸다. 없으면 None."""
        keys = path.split(".")
        cur  = data
        for key in keys:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        return cur