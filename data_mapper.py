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

    오류 처리 정책
    --------------
    - mapping.json 자체가 깨졌거나 'mapping' key가 없는 경우 (스키마 오류)
      → 시작 시점에 ValueError를 발생시켜 즉시 기동을 막습니다. (운영 중 잘못된 설정으로
        조용히 잘못된 데이터를 계속 적재하는 것보다, 시작 시점에 바로 알아채는 편이 안전합니다.)
    - 개별 메시지를 record로 변환하는 과정(build_record)에서 특정 컬럼 하나의 값을
      읽다가 예외가 발생하는 경우 → 해당 컬럼만 None으로 채우고 WARNING 로그를 남긴 뒤
      나머지 컬럼은 정상적으로 계속 매핑합니다. (메시지 1건의 필드 하나 때문에
      레코드 전체, 나아가 collector 전체가 죽지 않도록 합니다. 실패 여부는 호출부인
      mqtt_collector에서 다시 한 번 try/except로 감싸 최종 방어선을 둡니다.)
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
        """mapping 정의에 따라 record dict를 생성합니다. 컬럼 단위로 예외를 격리합니다."""
        reserved_values = {
            "__received_at__":  received_at,
            "__payload__":      payload,
            "__topic_site__":   topic_meta.get("site"),
            "__topic_device__": topic_meta.get("device"),
            "__topic_sensor__": topic_meta.get("sensor"),
        }

        record = {}
        for col, src in self.mapping.items():
            try:
                if src in _RESERVED:
                    record[col] = reserved_values[src]
                elif src == "topic":
                    record[col] = topic
                elif isinstance(src, str) and src.startswith("payload."):
                    record[col] = self._dig(payload, src[len("payload."):])
                elif isinstance(src, str):
                    # top-level payload field를 직접 참조합니다.
                    record[col] = payload.get(src) if isinstance(payload, dict) else None
                else:
                    self.log.warning(
                        f"Invalid mapping source for column '{col}': {src!r} (expected a string). Setting None."
                    )
                    record[col] = None
            except Exception as e:
                # 컬럼 하나 처리 중 예기치 못한 예외 → 그 컬럼만 None 처리하고 계속 진행
                self.log.warning(
                    f"Failed to map column '{col}' (src={src!r}, topic={topic}): {e}. Setting None."
                )
                record[col] = None

        return record

    def _dig(self, data, path: str):
        """dot으로 구분된 path를 사용하여 nested dict 값을 읽습니다. 값이 없거나 형식이 dict가 아니면 None을 리턴합니다."""
        keys = path.split(".")
        cur  = data
        for key in keys:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        return cur