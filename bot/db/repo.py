import sqlite3
import time

from bot.db.schema import _conn


def get_all_chats() -> list[dict]:
    with _conn() as con:
        rows = con.execute("SELECT chat_id FROM chats").fetchall()
        return [{"chat_id": int(r["chat_id"])} for r in rows]


def upsert_chat(chat_id: int) -> None:
    with _conn() as con:
        tz = _get_default_timezone(con)
        con.execute("INSERT OR IGNORE INTO chats(chat_id, enabled, timezone, include_meta) VALUES(?, 1, ?, 1)", (chat_id, tz))
        con.commit()


def _get_default_timezone(con: sqlite3.Connection) -> str:
    r = con.execute("SELECT timezone FROM chats LIMIT 1").fetchone()
    if r and r["timezone"]:
        return str(r["timezone"])
    return "Europe/Moscow"


def get_chat_settings(chat_id: int) -> dict:
    upsert_chat(chat_id)
    with _conn() as con:
        r = con.execute(
            "SELECT enabled, timezone, image_file_id, include_meta FROM chats WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
        return {
            "chat_id": chat_id,
            "enabled": int(r["enabled"]) == 1,
            "timezone": str(r["timezone"]),
            "image_file_id": (str(r["image_file_id"]) if r["image_file_id"] else None),
            "include_meta": int(r["include_meta"]) == 1,
        }


def set_chat_enabled(chat_id: int, enabled: int) -> None:
    upsert_chat(chat_id)
    with _conn() as con:
        con.execute("UPDATE chats SET enabled = ? WHERE chat_id = ?", (1 if enabled else 0, chat_id))
        con.commit()


def set_chat_include_meta(chat_id: int, include_meta: int) -> None:
    upsert_chat(chat_id)
    with _conn() as con:
        con.execute("UPDATE chats SET include_meta = ? WHERE chat_id = ?", (1 if include_meta else 0, chat_id))
        con.commit()


def migrate_chat_id(*, old_chat_id: int, new_chat_id: int) -> None:
    """
    Telegram group -> supergroup migration changes chat_id.
    This keeps chat settings and moves all rules to the new chat_id.
    """
    old_id = int(old_chat_id)
    new_id = int(new_chat_id)
    if old_id == new_id:
        return

    with _conn() as con:
        old_row = con.execute(
            "SELECT enabled, timezone, image_file_id, include_meta FROM chats WHERE chat_id = ?",
            (old_id,),
        ).fetchone()
        if not old_row:
            return

        # Ensure target chat row exists and preserve settings from the old chat.
        new_row = con.execute("SELECT chat_id FROM chats WHERE chat_id = ?", (new_id,)).fetchone()
        if new_row:
            con.execute(
                "UPDATE chats SET enabled = ?, timezone = ?, image_file_id = ?, include_meta = ? WHERE chat_id = ?",
                (
                    int(old_row["enabled"]),
                    str(old_row["timezone"]),
                    old_row["image_file_id"],
                    int(old_row["include_meta"]),
                    new_id,
                ),
            )
        else:
            con.execute(
                "INSERT INTO chats(chat_id, enabled, timezone, image_file_id, include_meta) VALUES(?, ?, ?, ?, ?)",
                (
                    new_id,
                    int(old_row["enabled"]),
                    str(old_row["timezone"]),
                    old_row["image_file_id"],
                    int(old_row["include_meta"]),
                ),
            )

        # Avoid conflicts for system rules if the new chat already has them (they are reproducible from YAML).
        keys = [
            str(r["system_key"])
            for r in con.execute(
                "SELECT system_key FROM rules WHERE chat_id = ? AND system_key IS NOT NULL",
                (old_id,),
            ).fetchall()
        ]
        if keys:
            placeholders = ",".join("?" for _ in keys)
            con.execute(
                f"DELETE FROM rules WHERE chat_id = ? AND system_key IN ({placeholders})",
                (new_id, *keys),
            )

        con.execute("UPDATE rules SET chat_id = ? WHERE chat_id = ?", (new_id, old_id))
        con.execute("DELETE FROM chats WHERE chat_id = ?", (old_id,))
        con.commit()


def _parse_days(days: str | None) -> list[int]:
    if not days:
        return []
    out: list[int] = []
    for part in days.split(","):
        part = part.strip()
        if part == "":
            continue
        out.append(int(part))
    return out


def get_rules(chat_id: int) -> list[dict]:
    upsert_chat(chat_id)
    with _conn() as con:
        rows = con.execute(
            """
            SELECT id, title, kind, days, time_hhmm, interval_minutes, created_at_ts, last_sent_at_ts, message_text, image_file_id,
                   is_system, system_key, text_probability, image_probability, enabled
            FROM rules
            WHERE chat_id = ?
            ORDER BY COALESCE(sort_order, 999999) ASC, id ASC
            """,
            (chat_id,),
        ).fetchall()
        rules: list[dict] = []
        for r in rows:
            rules.append(
                {
                    "id": int(r["id"]),
                    "chat_id": chat_id,
                    "title": str(r["title"] or ""),
                    "kind": str(r["kind"]),
                    "days": _parse_days(r["days"]),
                    "time_hhmm": (str(r["time_hhmm"]) if r["time_hhmm"] else None),
                    "interval_minutes": (int(r["interval_minutes"]) if r["interval_minutes"] is not None else None),
                    "created_at_ts": int(r["created_at_ts"]) if r["created_at_ts"] is not None else 0,
                    "last_sent_at_ts": (int(r["last_sent_at_ts"]) if r["last_sent_at_ts"] is not None else None),
                    "message_text": str(r["message_text"] or ""),
                    "image_file_id": (str(r["image_file_id"]) if r["image_file_id"] else None),
                    "is_system": int(r["is_system"]) == 1,
                    "system_key": (str(r["system_key"]) if r["system_key"] else None),
                    "text_probability": float(r["text_probability"]) if r["text_probability"] is not None else 1.0,
                    "image_probability": float(r["image_probability"]) if r["image_probability"] is not None else 0.0,
                    "enabled": int(r["enabled"]) == 1,
                }
            )
        return rules


def get_rule(chat_id: int, rule_id: int) -> dict | None:
    """
    Fast lookup for a single rule.
    """
    upsert_chat(chat_id)
    with _conn() as con:
        r = con.execute(
            """
            SELECT id, title, kind, days, time_hhmm, interval_minutes, created_at_ts, last_sent_at_ts, message_text, image_file_id,
                   is_system, system_key, text_probability, image_probability, enabled
            FROM rules
            WHERE chat_id = ? AND id = ?
            """,
            (chat_id, int(rule_id)),
        ).fetchone()
        if not r:
            return None
        return {
            "id": int(r["id"]),
            "chat_id": chat_id,
            "title": str(r["title"] or ""),
            "kind": str(r["kind"]),
            "days": _parse_days(r["days"]),
            "time_hhmm": (str(r["time_hhmm"]) if r["time_hhmm"] else None),
            "interval_minutes": (int(r["interval_minutes"]) if r["interval_minutes"] is not None else None),
            "created_at_ts": int(r["created_at_ts"]) if r["created_at_ts"] is not None else 0,
            "last_sent_at_ts": (int(r["last_sent_at_ts"]) if r["last_sent_at_ts"] is not None else None),
            "message_text": str(r["message_text"] or ""),
            "image_file_id": (str(r["image_file_id"]) if r["image_file_id"] else None),
            "is_system": int(r["is_system"]) == 1,
            "system_key": (str(r["system_key"]) if r["system_key"] else None),
            "text_probability": float(r["text_probability"]) if r["text_probability"] is not None else 1.0,
            "image_probability": float(r["image_probability"]) if r["image_probability"] is not None else 0.0,
            "enabled": int(r["enabled"]) == 1,
        }


def get_rule_text_options(rule_id: int) -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT id, image_option_id, text, weight FROM rule_text_options WHERE rule_id = ? ORDER BY id ASC",
            (rule_id,),
        ).fetchall()
        return [
            {
                "id": int(r["id"]),
                "image_option_id": (int(r["image_option_id"]) if r["image_option_id"] is not None else None),
                "text": str(r["text"]),
                "weight": float(r["weight"]),
            }
            for r in rows
        ]


def get_rule_image_options(rule_id: int) -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT id, ref, ref_type, weight FROM rule_image_options WHERE rule_id = ? ORDER BY id ASC",
            (rule_id,),
        ).fetchall()
        return [
            {"id": int(r["id"]), "ref": str(r["ref"]), "ref_type": str(r["ref_type"]), "weight": float(r["weight"])}
            for r in rows
        ]


def ensure_system_rule_weekly(
    *,
    chat_id: int,
    system_key: str,
    title: str,
    days: list[int],
    time_hhmm: str,
    images: list[dict],
    enabled_by_default: bool = True,
    sort_order: int = 0,
) -> int:
    """
    Creates a system (default) weekly rule if it doesn't exist and syncs its pools.
    - days: always from config
    - time_hhmm, enabled: from config unless user_customized (user changed via UI)

    images: [
      {"ref": str, "ref_type": "file_id"|"path"|"url", "weight": float, "texts": [(text, weight), ...]},
      ...
    ]
    """
    upsert_chat(chat_id)
    days_s = ",".join(str(d) for d in sorted(set(days)))
    now_ts = int(time.time())
    with _conn() as con:
        row = con.execute(
            "SELECT id, kind, days, time_hhmm, user_customized FROM rules WHERE chat_id = ? AND system_key = ?",
            (chat_id, system_key),
        ).fetchone()
        if row:
            rule_id = int(row["id"])
            user_customized = int(row["user_customized"] or 0) == 1
            # System rules: days always from config; time/enabled only if user didn't customize
            con.execute(
                """
                UPDATE rules
                SET title = ?, text_probability = 1.0, image_probability = 1.0, is_system = 1, sort_order = ?
                WHERE id = ? AND chat_id = ?
                """,
                (str(title), sort_order, rule_id, chat_id),
            )
            con.execute("UPDATE rules SET days = ? WHERE id = ? AND chat_id = ?", (days_s, rule_id, chat_id))
            if not user_customized:
                con.execute(
                    "UPDATE rules SET time_hhmm = ?, enabled = ? WHERE id = ? AND chat_id = ?",
                    (time_hhmm, 1 if enabled_by_default else 0, rule_id, chat_id),
                )
            # Replace pools to match the current defaults.
            con.execute("DELETE FROM rule_text_options WHERE rule_id = ?", (rule_id,))
            con.execute("DELETE FROM rule_image_options WHERE rule_id = ?", (rule_id,))
            for img in images:
                ref = str(img["ref"])
                ref_type = str(img["ref_type"])
                weight = float(img.get("weight", 1.0))
                cur_img = con.execute(
                    "INSERT INTO rule_image_options(rule_id, ref, ref_type, weight) VALUES(?, ?, ?, ?)",
                    (rule_id, ref, ref_type, weight),
                )
                image_option_id = int(cur_img.lastrowid)
                for text, w in (img.get("texts") or []):
                    con.execute(
                        "INSERT INTO rule_text_options(rule_id, image_option_id, text, weight) VALUES(?, ?, ?, ?)",
                        (rule_id, image_option_id, str(text), float(w)),
                    )
            con.commit()
            return rule_id

        cur = con.execute(
            """
            INSERT INTO rules(
              chat_id, title, kind, days, time_hhmm, interval_minutes,
              created_at_ts, last_sent_at_ts,
              message_text, image_file_id,
              is_system, system_key, text_probability, image_probability, enabled, sort_order
            )
            VALUES(?, ?, 'weekly', ?, ?, NULL, ?, NULL, '', NULL, 1, ?, 1.0, 1.0, ?, ?)
            """,
            (chat_id, str(title), days_s, time_hhmm, now_ts, system_key, 1 if enabled_by_default else 0, sort_order),
        )
        rule_id = int(cur.lastrowid)
        for img in images:
            ref = str(img["ref"])
            ref_type = str(img["ref_type"])
            weight = float(img.get("weight", 1.0))
            cur_img = con.execute(
                "INSERT INTO rule_image_options(rule_id, ref, ref_type, weight) VALUES(?, ?, ?, ?)",
                (rule_id, ref, ref_type, weight),
            )
            image_option_id = int(cur_img.lastrowid)
            for text, w in (img.get("texts") or []):
                con.execute(
                    "INSERT INTO rule_text_options(rule_id, image_option_id, text, weight) VALUES(?, ?, ?, ?)",
                    (rule_id, image_option_id, str(text), float(w)),
                )
        con.commit()
        return rule_id


def ensure_system_rule_interval(
    *,
    chat_id: int,
    system_key: str,
    title: str,
    interval_minutes: int,
    images: list[dict],
    enabled_by_default: bool = True,
    sort_order: int = 0,
) -> int:
    """
    Creates a system interval rule if it doesn't exist and syncs its pools.
    - interval_minutes, enabled: from config unless user_customized.
    """
    upsert_chat(chat_id)
    now_ts = int(time.time())
    with _conn() as con:
        row = con.execute(
            "SELECT id, kind, interval_minutes, user_customized FROM rules WHERE chat_id = ? AND system_key = ?",
            (chat_id, system_key),
        ).fetchone()
        if row:
            rule_id = int(row["id"])
            user_customized = int(row["user_customized"] or 0) == 1
            con.execute(
                """
                UPDATE rules
                SET title = ?, text_probability = 1.0, image_probability = 1.0, is_system = 1, sort_order = ?
                WHERE id = ? AND chat_id = ?
                """,
                (str(title), sort_order, rule_id, chat_id),
            )
            if not user_customized:
                con.execute(
                    "UPDATE rules SET interval_minutes = ?, enabled = ? WHERE id = ? AND chat_id = ?",
                    (int(interval_minutes), 1 if enabled_by_default else 0, rule_id, chat_id),
                )
            con.execute("DELETE FROM rule_text_options WHERE rule_id = ?", (rule_id,))
            con.execute("DELETE FROM rule_image_options WHERE rule_id = ?", (rule_id,))
            for img in images:
                ref = str(img["ref"])
                ref_type = str(img["ref_type"])
                weight = float(img.get("weight", 1.0))
                cur_img = con.execute(
                    "INSERT INTO rule_image_options(rule_id, ref, ref_type, weight) VALUES(?, ?, ?, ?)",
                    (rule_id, ref, ref_type, weight),
                )
                image_option_id = int(cur_img.lastrowid)
                for text, w in (img.get("texts") or []):
                    con.execute(
                        "INSERT INTO rule_text_options(rule_id, image_option_id, text, weight) VALUES(?, ?, ?, ?)",
                        (rule_id, image_option_id, str(text), float(w)),
                    )
            con.commit()
            return rule_id

        cur = con.execute(
            """
            INSERT INTO rules(
              chat_id, title, kind, days, time_hhmm, interval_minutes,
              created_at_ts, last_sent_at_ts,
              message_text, image_file_id,
              is_system, system_key, text_probability, image_probability, enabled, sort_order
            )
            VALUES(?, ?, 'interval', NULL, NULL, ?, ?, NULL, '', NULL, 1, ?, 1.0, 1.0, ?, ?)
            """,
            (chat_id, str(title), int(interval_minutes), now_ts, system_key, 1 if enabled_by_default else 0, sort_order),
        )
        rule_id = int(cur.lastrowid)
        for img in images:
            ref = str(img["ref"])
            ref_type = str(img["ref_type"])
            weight = float(img.get("weight", 1.0))
            cur_img = con.execute(
                "INSERT INTO rule_image_options(rule_id, ref, ref_type, weight) VALUES(?, ?, ?, ?)",
                (rule_id, ref, ref_type, weight),
            )
            image_option_id = int(cur_img.lastrowid)
            for text, w in (img.get("texts") or []):
                con.execute(
                    "INSERT INTO rule_text_options(rule_id, image_option_id, text, weight) VALUES(?, ?, ?, ?)",
                    (rule_id, image_option_id, str(text), float(w)),
                )
        con.commit()
        return rule_id


def create_rule_weekly(
    chat_id: int,
    title: str,
    days: list[int],
    time_hhmm: str,
    message_text: str,
    image_file_id: str | None,
) -> int:
    upsert_chat(chat_id)
    days_s = ",".join(str(d) for d in sorted(set(days)))
    now_ts = int(time.time())
    with _conn() as con:
        cur = con.execute(
            """
            INSERT INTO rules(chat_id, title, kind, days, time_hhmm, interval_minutes, created_at_ts, last_sent_at_ts, message_text, image_file_id, enabled)
            VALUES(?, ?, 'weekly', ?, ?, NULL, ?, NULL, ?, ?, 1)
            """,
            (chat_id, str(title), days_s, time_hhmm, now_ts, message_text, image_file_id),
        )
        con.commit()
        return int(cur.lastrowid)


def create_rule_interval(
    chat_id: int,
    title: str,
    interval_minutes: int,
    message_text: str,
    image_file_id: str | None,
) -> int:
    upsert_chat(chat_id)
    now_ts = int(time.time())
    with _conn() as con:
        cur = con.execute(
            """
            INSERT INTO rules(chat_id, title, kind, days, time_hhmm, interval_minutes, created_at_ts, last_sent_at_ts, message_text, image_file_id, enabled)
            VALUES(?, ?, 'interval', NULL, NULL, ?, ?, NULL, ?, ?, 1)
            """,
            (chat_id, str(title), int(interval_minutes), now_ts, message_text, image_file_id),
        )
        con.commit()
        return int(cur.lastrowid)


def set_rule_text(chat_id: int, rule_id: int, message_text: str) -> None:
    upsert_chat(chat_id)
    with _conn() as con:
        con.execute(
            "UPDATE rules SET message_text = ? WHERE chat_id = ? AND id = ?",
            (message_text, chat_id, rule_id),
        )
        con.commit()


def set_rule_title(chat_id: int, rule_id: int, title: str) -> None:
    """
    Users can rename only non-system rules.
    """
    upsert_chat(chat_id)
    with _conn() as con:
        con.execute(
            "UPDATE rules SET title = ? WHERE chat_id = ? AND id = ? AND COALESCE(is_system, 0) = 0",
            (str(title), chat_id, rule_id),
        )
        con.commit()


def set_rule_image_file_id(chat_id: int, rule_id: int, file_id: str | None) -> None:
    upsert_chat(chat_id)
    with _conn() as con:
        con.execute(
            "UPDATE rules SET image_file_id = ? WHERE chat_id = ? AND id = ?",
            (file_id, chat_id, rule_id),
        )
        con.commit()


def set_rule_time_hhmm(chat_id: int, rule_id: int, time_hhmm: str) -> None:
    upsert_chat(chat_id)
    with _conn() as con:
        con.execute(
            """
            UPDATE rules SET time_hhmm = ?, user_customized = CASE WHEN is_system = 1 THEN 1 ELSE user_customized END
            WHERE chat_id = ? AND id = ? AND kind = 'weekly'
            """,
            (time_hhmm, chat_id, rule_id),
        )
        con.commit()


def set_rule_interval_minutes(chat_id: int, rule_id: int, interval_minutes: int) -> None:
    upsert_chat(chat_id)
    with _conn() as con:
        con.execute(
            """
            UPDATE rules SET interval_minutes = ?, user_customized = CASE WHEN is_system = 1 THEN 1 ELSE user_customized END
            WHERE chat_id = ? AND id = ? AND kind = 'interval'
            """,
            (int(interval_minutes), chat_id, rule_id),
        )
        con.commit()


def toggle_rule_enabled(chat_id: int, rule_id: int) -> None:
    upsert_chat(chat_id)
    with _conn() as con:
        r = con.execute("SELECT enabled FROM rules WHERE chat_id = ? AND id = ?", (chat_id, rule_id)).fetchone()
        if not r:
            return
        enabled = int(r["enabled"])
        con.execute(
            """
            UPDATE rules SET enabled = ?, user_customized = CASE WHEN is_system = 1 THEN 1 ELSE user_customized END
            WHERE chat_id = ? AND id = ?
            """,
            (0 if enabled else 1, chat_id, rule_id),
        )
        con.commit()


def set_rule_last_sent_at_ts(chat_id: int, rule_id: int, ts: int) -> None:
    upsert_chat(chat_id)
    with _conn() as con:
        con.execute(
            "UPDATE rules SET last_sent_at_ts = ? WHERE chat_id = ? AND id = ?",
            (int(ts), chat_id, rule_id),
        )
        con.commit()


def delete_rule(chat_id: int, rule_id: int) -> None:
    upsert_chat(chat_id)
    with _conn() as con:
        con.execute("DELETE FROM rules WHERE chat_id = ? AND id = ?", (chat_id, rule_id))
        con.commit()

