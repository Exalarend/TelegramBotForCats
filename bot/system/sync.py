from dataclasses import dataclass

from bot.db.schema import _conn
from bot.db.repo import ensure_system_rule_interval, ensure_system_rule_weekly
from bot.system.config_loader import SystemRule


@dataclass
class SyncResult:
    """Result of sync_system_rules_for_chat: lists of rule titles that were added or removed."""

    added: list[str]
    removed: list[str]


def sync_system_rules_for_chat(*, chat_id: int, rules: list[SystemRule], logger) -> SyncResult:
    """
    Ensures that all configured system rules exist in DB for this chat.
    - Removed rules: deleted for all users.
    - New rules: added with enabled_by_default.
    - Existing rules: days always from config; time_hhmm and enabled from config unless user customized.
    Returns SyncResult with added/removed rule titles for startup notifications.
    """
    configured_keys = {str(r.system_key) for r in rules if str(r.system_key)}
    removed_titles = _cleanup_stale_system_rules(chat_id=chat_id, configured_keys=configured_keys, logger=logger)

    added_titles: list[str] = []
    for idx, r in enumerate(rules):
        existed, rule_id, _ = _get_system_rule_state(chat_id, r.system_key)

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
                enabled_by_default=r.enabled_by_default,
                sort_order=idx,
            )
        else:
            ensure_system_rule_interval(
                chat_id=chat_id,
                system_key=r.system_key,
                title=r.title,
                interval_minutes=int(r.schedule["interval_minutes"]),
                images=images_payload,
                enabled_by_default=r.enabled_by_default,
                sort_order=idx,
            )

        if not existed:
            added_titles.append(r.title)
            logger.info("System rule created: chat_id=%s system_key=%s", chat_id, r.system_key)

    return SyncResult(added=added_titles, removed=removed_titles)


def _cleanup_stale_system_rules(*, chat_id: int, configured_keys: set[str], logger) -> list[str]:
    """
    Remove system rules that are no longer in YAML.
    Returns list of titles of removed rules (for startup notifications).
    """
    removed_titles: list[str] = []
    if not configured_keys:
        return removed_titles

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
        # Get titles before deleting (for notifications)
        rows = con.execute(
            f"""
            SELECT title FROM rules
            WHERE chat_id = ?
              AND is_system = 1
              AND system_key IS NOT NULL
              AND system_key NOT IN ({placeholders})
            """,
            (int(chat_id), *sorted(configured_keys)),
        ).fetchall()
        removed_titles = [str(r["title"] or "").strip() or "Уведомление" for r in rows]

        # Delete
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

    return removed_titles


def _get_system_rule_state(chat_id: int, system_key: str) -> tuple[bool, int | None, bool | None]:
    with _conn() as con:
        row = con.execute(
            "SELECT id, enabled FROM rules WHERE chat_id = ? AND system_key = ?",
            (chat_id, system_key),
        ).fetchone()
        if not row:
            return False, None, None
        return True, int(row["id"]), int(row["enabled"]) == 1

