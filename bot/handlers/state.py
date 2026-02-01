import time
from typing import Any


DEFAULT_DRAFT_TTL_S = 30 * 60


def _now_ts() -> int:
    return int(time.time())


def is_expired(draft: dict) -> bool:
    try:
        exp = int(draft.get("expires_at_ts") or 0)
    except Exception:
        exp = 0
    return exp > 0 and _now_ts() > exp


def clear_flow(context) -> None:
    # Clears both legacy containers.
    context.user_data.pop("draft_rule", None)
    context.user_data.pop("awaiting_photo", None)


def clear_awaiting_photo(context) -> None:
    context.user_data.pop("awaiting_photo", None)


def get_draft(context, *, chat_id: int, actor_user_id: int | None = None) -> dict | None:
    draft = context.user_data.get("draft_rule")
    if not isinstance(draft, dict):
        return None
    if int(draft.get("chat_id") or 0) != int(chat_id):
        clear_flow(context)
        return None
    if is_expired(draft):
        clear_flow(context)
        return None
    if actor_user_id is not None:
        try:
            a = int(draft.get("actor_user_id") or 0)
        except Exception:
            a = 0
        if a and a != int(actor_user_id):
            return None
    return draft


def set_draft(context, draft: dict) -> None:
    context.user_data["draft_rule"] = draft


def touch_or_init_draft(draft: dict, *, chat_id: int, actor_user_id: int | None) -> dict:
    d = dict(draft)
    d["chat_id"] = int(chat_id)
    if actor_user_id is not None:
        d["actor_user_id"] = int(actor_user_id)
    d.setdefault("created_at_ts", _now_ts())
    d.setdefault("expires_at_ts", int(d["created_at_ts"]) + DEFAULT_DRAFT_TTL_S)
    return d


def set_stage_after_prompt(draft: dict, *, stage: str, prompt_message_id: int) -> dict:
    d = dict(draft)
    d["stage"] = str(stage)
    d["prompt_message_id"] = int(prompt_message_id)
    return d


def get_awaiting_photo(context, *, chat_id: int, actor_user_id: int | None = None) -> dict | None:
    awaiting = context.user_data.get("awaiting_photo")
    if not isinstance(awaiting, dict):
        return None
    if int(awaiting.get("chat_id") or 0) != int(chat_id):
        context.user_data.pop("awaiting_photo", None)
        return None
    if is_expired(awaiting):
        context.user_data.pop("awaiting_photo", None)
        return None
    if actor_user_id is not None:
        try:
            a = int(awaiting.get("actor_user_id") or 0)
        except Exception:
            a = 0
        if a and a != int(actor_user_id):
            return None
    return awaiting


def set_awaiting_photo(context, awaiting: dict) -> None:
    context.user_data["awaiting_photo"] = awaiting


def touch_or_init_awaiting(awaiting: dict, *, chat_id: int, actor_user_id: int | None) -> dict:
    a = dict(awaiting)
    a["chat_id"] = int(chat_id)
    if actor_user_id is not None:
        a["actor_user_id"] = int(actor_user_id)
    a.setdefault("created_at_ts", _now_ts())
    a.setdefault("expires_at_ts", int(a["created_at_ts"]) + DEFAULT_DRAFT_TTL_S)
    return a

