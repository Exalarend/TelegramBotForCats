from bot.db.schema import _conn
from bot.db.repo import ensure_system_rule_interval, ensure_system_rule_weekly
from bot.system.config_loader import SystemRule


def sync_system_rules_for_chat(*, chat_id: int, rules: list[SystemRule], logger) -> None:
    """
    Ensures that all configured system rules exist in DB for this chat.
    - Updates pools/probabilities.
    - Does NOT overwrite user-changed schedule or enabled flag for existing rules.
    - Applies enabled_by_default only on first creation.
    """
    configured_keys = {str(r.system_key) for r in rules if str(r.system_key)}
    _cleanup_stale_system_rules(chat_id=chat_id, configured_keys=configured_keys, logger=logger)

    for r in rules:
        existed, rule_id, enabled = _get_system_rule_state(chat_id, r.system_key)

        images_payload = []
        for img in r.images:
            images_payload.append(
                {
                    "ref": img.ref,
                    "ref_type": img.ref_type,
                    "weight": img.weight,
                    "texts": [(t.text, t.weight) for t in (img.texts or [])],
                }
            )

        if r.kind == "weekly":
            ensure_system_rule_weekly(
                chat_id=chat_id,
                system_key=r.system_key,
                title=r.title,
                days=list(r.schedule["days"]),
                time_hhmm=str(r.schedule["time_hhmm"]),
                images=images_payload,
            )
        else:
            ensure_system_rule_interval(
                chat_id=chat_id,
                system_key=r.system_key,
                title=r.title,
                interval_minutes=int(r.schedule["interval_minutes"]),
                images=images_payload,
            )

        if not existed and not r.enabled_by_default:
            _set_rule_enabled(chat_id, r.system_key, enabled=0)
            logger.info("System rule created disabled: chat_id=%s system_key=%s", chat_id, r.system_key)
        elif not existed:
            logger.info("System rule created: chat_id=%s system_key=%s", chat_id, r.system_key)


def _cleanup_stale_system_rules(*, chat_id: int, configured_keys: set[str], logger) -> None:
    """
    Prevent system rules from "piling up" when YAML keys are renamed/removed
    or when legacy system rules exist without a system_key.

    Rules removed by this cleanup are reproducible from YAML anyway.
    """
    if not configured_keys:
        return
    with _conn() as con:
        # Remove legacy/system duplicates that can't be matched by key.
        con.execute(
            """
            DELETE FROM rules
            WHERE chat_id = ?
              AND is_system = 1
              AND (
                system_key IS NULL
                OR TRIM(system_key) = ''
              )
            """,
            (int(chat_id),),
        )

        placeholders = ",".join("?" for _ in configured_keys)
        # Remove system rules that are no longer present in YAML (e.g. system_key renamed).
        cur = con.execute(
            f"""
            DELETE FROM rules
            WHERE chat_id = ?
              AND is_system = 1
              AND system_key IS NOT NULL
              AND system_key NOT IN ({placeholders})
            """,
            (int(chat_id), *sorted(configured_keys)),
        )
        deleted = int(getattr(cur, "rowcount", 0) or 0)
        con.commit()

    if deleted > 0:
        logger.info("Removed stale system rules: chat_id=%s deleted=%s", chat_id, deleted)


def _get_system_rule_state(chat_id: int, system_key: str) -> tuple[bool, int | None, bool | None]:
    with _conn() as con:
        row = con.execute(
            "SELECT id, enabled FROM rules WHERE chat_id = ? AND system_key = ?",
            (chat_id, system_key),
        ).fetchone()
        if not row:
            return False, None, None
        return True, int(row["id"]), int(row["enabled"]) == 1


def _set_rule_enabled(chat_id: int, system_key: str, *, enabled: int) -> None:
    with _conn() as con:
        con.execute(
            "UPDATE rules SET enabled = ? WHERE chat_id = ? AND system_key = ?",
            (1 if enabled else 0, chat_id, system_key),
        )
        con.commit()

