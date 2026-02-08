import os
import sqlite3

_DB_PATH: str | None = None


def ensure_schema(*, db_path: str, default_timezone: str) -> None:
    """
    Initializes DB connection target and applies lightweight migrations.
    """
    global _DB_PATH
    _DB_PATH = db_path
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

    with _conn() as con:
        # SQLite performance pragmas (best-effort).
        try:
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute("PRAGMA synchronous=NORMAL;")
        except Exception:
            # Some environments/filesystems may not support WAL; ignore.
            pass

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS chats (
              chat_id INTEGER PRIMARY KEY,
              enabled INTEGER NOT NULL DEFAULT 1,
              timezone TEXT NOT NULL,
              image_file_id TEXT,
              include_meta INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS rules (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              chat_id INTEGER NOT NULL,
              title TEXT NOT NULL DEFAULT '',
              kind TEXT NOT NULL, -- weekly | interval
              days TEXT,          -- for weekly: "0,2,4" (python weekday: Mon=0..Sun=6)
              time_hhmm TEXT,     -- for weekly: "09:30"
              interval_minutes INTEGER, -- for interval
              created_at_ts INTEGER NOT NULL DEFAULT 0,
              last_sent_at_ts INTEGER,
              message_text TEXT NOT NULL DEFAULT '',
              image_file_id TEXT,
              is_system INTEGER NOT NULL DEFAULT 0,
              system_key TEXT,
              text_probability REAL NOT NULL DEFAULT 1.0,
              image_probability REAL NOT NULL DEFAULT 0.0,
              enabled INTEGER NOT NULL DEFAULT 1,
              FOREIGN KEY(chat_id) REFERENCES chats(chat_id) ON DELETE CASCADE
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS rule_text_options (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              rule_id INTEGER NOT NULL,
              image_option_id INTEGER,
              text TEXT NOT NULL,
              weight REAL NOT NULL DEFAULT 1.0,
              FOREIGN KEY(rule_id) REFERENCES rules(id) ON DELETE CASCADE
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS rule_image_options (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              rule_id INTEGER NOT NULL,
              ref TEXT NOT NULL,
              ref_type TEXT NOT NULL, -- file_id | path | url
              weight REAL NOT NULL DEFAULT 1.0,
              FOREIGN KEY(rule_id) REFERENCES rules(id) ON DELETE CASCADE
            )
            """
        )
        con.execute("PRAGMA foreign_keys = ON;")

        # Lightweight migrations for existing DBs
        chat_cols = {row["name"] for row in con.execute("PRAGMA table_info(chats)").fetchall()}
        if "include_meta" not in chat_cols:
            con.execute("ALTER TABLE chats ADD COLUMN include_meta INTEGER NOT NULL DEFAULT 1")

        rule_cols = {row["name"] for row in con.execute("PRAGMA table_info(rules)").fetchall()}
        if "title" not in rule_cols:
            con.execute("ALTER TABLE rules ADD COLUMN title TEXT NOT NULL DEFAULT ''")
        if "created_at_ts" not in rule_cols:
            con.execute("ALTER TABLE rules ADD COLUMN created_at_ts INTEGER NOT NULL DEFAULT 0")
        if "last_sent_at_ts" not in rule_cols:
            con.execute("ALTER TABLE rules ADD COLUMN last_sent_at_ts INTEGER")
        if "message_text" not in rule_cols:
            con.execute("ALTER TABLE rules ADD COLUMN message_text TEXT NOT NULL DEFAULT ''")
        if "image_file_id" not in rule_cols:
            con.execute("ALTER TABLE rules ADD COLUMN image_file_id TEXT")
        if "is_system" not in rule_cols:
            con.execute("ALTER TABLE rules ADD COLUMN is_system INTEGER NOT NULL DEFAULT 0")
        if "system_key" not in rule_cols:
            con.execute("ALTER TABLE rules ADD COLUMN system_key TEXT")
        if "text_probability" not in rule_cols:
            con.execute("ALTER TABLE rules ADD COLUMN text_probability REAL NOT NULL DEFAULT 1.0")
        if "image_probability" not in rule_cols:
            con.execute("ALTER TABLE rules ADD COLUMN image_probability REAL NOT NULL DEFAULT 0.0")
        if "user_customized" not in rule_cols:
            con.execute("ALTER TABLE rules ADD COLUMN user_customized INTEGER NOT NULL DEFAULT 0")
        if "sort_order" not in rule_cols:
            con.execute("ALTER TABLE rules ADD COLUMN sort_order INTEGER")

        con.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_rules_chat_system_key ON rules(chat_id, system_key) WHERE system_key IS NOT NULL"
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_rules_chat_id ON rules(chat_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_rules_chat_id_id ON rules(chat_id, id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_rules_chat_enabled ON rules(chat_id, enabled)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_rules_last_sent_at_ts ON rules(last_sent_at_ts)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_rule_text_options_rule_id ON rule_text_options(rule_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_rule_image_options_rule_id ON rule_image_options(rule_id)")

        text_cols = {row["name"] for row in con.execute("PRAGMA table_info(rule_text_options)").fetchall()}
        if "image_option_id" not in text_cols:
            con.execute("ALTER TABLE rule_text_options ADD COLUMN image_option_id INTEGER")

        # Normalize existing values
        con.execute(
            "UPDATE chats SET timezone = COALESCE(NULLIF(timezone, ''), ?) WHERE timezone IS NULL OR timezone = ''",
            (default_timezone,),
        )
        con.execute("UPDATE chats SET include_meta = COALESCE(include_meta, 1) WHERE include_meta IS NULL")
        con.execute("UPDATE rules SET title = COALESCE(title, '') WHERE title IS NULL")
        con.execute("UPDATE rules SET message_text = COALESCE(message_text, '') WHERE message_text IS NULL")
        con.execute("UPDATE rules SET is_system = COALESCE(is_system, 0) WHERE is_system IS NULL")
        # Fill created_at for legacy rows
        con.execute(
            "UPDATE rules SET created_at_ts = CAST(strftime('%s','now') AS INTEGER) WHERE created_at_ts IS NULL OR created_at_ts = 0"
        )
        con.commit()


def _conn() -> sqlite3.Connection:
    if _DB_PATH is None:
        raise RuntimeError("DB is not initialized. Call ensure_schema() first.")
    con = sqlite3.connect(_DB_PATH, timeout=2.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")
    try:
        con.execute("PRAGMA busy_timeout=2000;")
    except Exception:
        pass
    return con

