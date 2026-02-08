import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BotConfig:
    token: str
    db_path: str
    default_timezone: str

    system_yaml_path: str
    big_red_button_yaml_path: str

    api_timeout_seconds: float
    api_retry_attempts: int
    pool_timeout_seconds: float

    log_level: str
    log_dir: str
    log_retention_days: int

    notify_startup_changes: bool
    startup_changes_file: str  # Path to file with manual changelog; if exists and non-empty, sent instead of auto-detected

    @staticmethod
    def from_env() -> "BotConfig":
        token = _env("BOT_TOKEN")
        db_path = _env("BOT_DB_PATH", "data/bot.db")
        default_timezone = _env("DEFAULT_TIMEZONE", "Europe/Moscow")
        system_yaml_path = _env("SYSTEM_NOTIFICATIONS_YAML", "config/system_notifications.yaml")
        big_red_button_yaml_path = _env("BIG_RED_BUTTON_YAML", "config/big_red_button.yaml")
        api_timeout_seconds = float(os.getenv("BOT_API_TIMEOUT_SECONDS", "20"))
        api_retry_attempts = int(os.getenv("BOT_API_RETRY_ATTEMPTS", "4"))
        pool_timeout_seconds = float(os.getenv("BOT_API_POOL_TIMEOUT_SECONDS", "10"))
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        log_dir = os.getenv("BOT_LOG_DIR", "logs").strip()
        log_retention_days = int(os.getenv("BOT_LOG_RETENTION_DAYS", "30"))
        notify_startup_changes = os.getenv("BOT_NOTIFY_STARTUP_CHANGES", "true").lower() in ("1", "true", "yes")
        startup_changes_file = (os.getenv("BOT_STARTUP_CHANGES_FILE") or "").strip() or "config/startup_changes.txt"

        return BotConfig(
            token=token,
            db_path=db_path,
            default_timezone=default_timezone,
            system_yaml_path=system_yaml_path,
            big_red_button_yaml_path=big_red_button_yaml_path,
            api_timeout_seconds=api_timeout_seconds,
            api_retry_attempts=api_retry_attempts,
            pool_timeout_seconds=pool_timeout_seconds,
            log_level=log_level,
            log_dir=log_dir,
            log_retention_days=log_retention_days,
            notify_startup_changes=notify_startup_changes,
            startup_changes_file=startup_changes_file,
        )


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        if default is None:
            raise RuntimeError(f"Missing env var: {name}")
        return default
    return v

