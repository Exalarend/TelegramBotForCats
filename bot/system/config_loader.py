from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class SystemImageText:
    text: str
    weight: float = 1.0


@dataclass(frozen=True)
class SystemImage:
    ref: str
    ref_type: str  # file_id | url | path
    weight: float = 1.0
    texts: list[SystemImageText] = field(default_factory=list)


@dataclass(frozen=True)
class SystemRule:
    system_key: str
    title: str
    kind: str  # weekly | interval
    enabled_by_default: bool
    schedule: dict
    images: list[SystemImage]


def load_system_rules(yaml_path: str) -> list[SystemRule]:
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError("PyYAML is required. Install it via `pip install PyYAML`.") from e

    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"System notifications YAML not found: {yaml_path}")

    with open(yaml_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    rules_raw = data.get("rules") or []
    if not isinstance(rules_raw, list) or not rules_raw:
        raise ValueError("YAML: 'rules' must be a non-empty list")

    rules: list[SystemRule] = []
    for r in rules_raw:
        if not isinstance(r, dict):
            raise ValueError("YAML: each rule must be a mapping")
        system_key = str(r.get("system_key") or "").strip()
        if not system_key:
            raise ValueError("YAML: rule.system_key is required")
        title = str(r.get("title") or "").strip()
        if not title:
            # Backward compatible fallback: if title is not provided, use system_key.
            title = system_key
        kind = str(r.get("kind") or "").strip()
        if kind not in {"weekly", "interval"}:
            raise ValueError(f"YAML: rule.kind must be weekly|interval (system_key={system_key})")
        schedule = r.get("schedule") or {}
        if not isinstance(schedule, dict):
            raise ValueError(f"YAML: rule.schedule must be mapping (system_key={system_key})")
        _validate_schedule(kind, schedule, system_key=system_key)

        enabled_by_default = bool(r.get("enabled_by_default", True))

        images_raw = r.get("images") or []
        if not isinstance(images_raw, list) or not images_raw:
            raise ValueError(f"YAML: rule.images must be non-empty list (system_key={system_key})")
        images: list[SystemImage] = []
        for img in images_raw:
            if not isinstance(img, dict):
                raise ValueError(f"YAML: images[] must be mapping (system_key={system_key})")
            ref = str(img.get("ref") or "").strip()
            ref_type = str(img.get("ref_type") or "").strip()
            if not ref or ref_type not in {"file_id", "url", "path"}:
                raise ValueError(f"YAML: image.ref/ref_type invalid (system_key={system_key})")
            weight = float(img.get("weight", 1.0))
            texts_raw = img.get("texts") or []
            if not isinstance(texts_raw, list) or not texts_raw:
                raise ValueError(f"YAML: image.texts must be non-empty list (system_key={system_key})")
            texts: list[SystemImageText] = []
            for t in texts_raw:
                if not isinstance(t, dict):
                    raise ValueError(f"YAML: image.texts[] must be mapping (system_key={system_key})")
                text = str(t.get("text") or "").strip()
                if not text:
                    raise ValueError(f"YAML: image.texts[].text required (system_key={system_key})")
                tw = float(t.get("weight", 1.0))
                texts.append(SystemImageText(text=text, weight=tw))
            images.append(SystemImage(ref=ref, ref_type=ref_type, weight=weight, texts=texts))

        rules.append(
            SystemRule(
                system_key=system_key,
                title=title,
                kind=kind,
                enabled_by_default=enabled_by_default,
                schedule=schedule,
                images=images,
            )
        )

    return rules


def _validate_schedule(kind: str, schedule: dict, *, system_key: str) -> None:
    if kind == "weekly":
        days = schedule.get("days")
        time_hhmm = schedule.get("time_hhmm")
        if not isinstance(days, list) or not days:
            raise ValueError(f"YAML: weekly schedule.days must be non-empty list (system_key={system_key})")
        for d in days:
            if not isinstance(d, int) or d < 0 or d > 6:
                raise ValueError(f"YAML: weekly schedule.days must be ints 0..6 (system_key={system_key})")
        if not isinstance(time_hhmm, str) or ":" not in time_hhmm:
            raise ValueError(f"YAML: weekly schedule.time_hhmm required like '09:30' (system_key={system_key})")
    else:
        interval = schedule.get("interval_minutes")
        if not isinstance(interval, int) or interval < 1:
            raise ValueError(f"YAML: interval schedule.interval_minutes must be int >= 1 (system_key={system_key})")

