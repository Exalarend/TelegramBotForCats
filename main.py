import argparse
import asyncio
import logging
import sys
import time
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from dotenv import load_dotenv

from bot.config import BotConfig
from bot.run import run_bot

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def _cleanup_old_logs(log_dir: Path, retention_days: int) -> None:
    """Удаляет в log_dir файлы старше retention_days дней (по mtime)."""
    if retention_days <= 0:
        return
    cutoff = time.time() - retention_days * 86400
    logger = logging.getLogger("ministry-bot")
    for path in log_dir.iterdir():
        if not path.is_file():
            continue
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
                logger.debug("Removed old log file: %s", path)
        except OSError as e:
            logger.warning("Failed to remove old log %s: %s", path, e)


def setup_logging(
    level: str,
    *,
    log_dir: str = "",
    log_retention_days: int = 30,
) -> logging.Logger:
    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
    )
    root = logging.getLogger()
    if log_dir:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            log_path / "bot.log",
            when="midnight",
            interval=1,
            encoding="utf-8",
        )
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        root.addHandler(file_handler)
        _cleanup_old_logs(log_path, log_retention_days)
    return logging.getLogger("ministry-bot")


async def main(*, stop_event: asyncio.Event | None = None) -> None:
    load_dotenv()
    config = BotConfig.from_env()
    logger = setup_logging(
        config.log_level,
        log_dir=config.log_dir,
        log_retention_days=config.log_retention_days,
    )
    await run_bot(config, logger=logger, stop_event=stop_event)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telegram-бот для котиков")
    parser.add_argument(
        "--tray",
        action="store_true",
        help="Запустить в свернутом виде: иконка в системном трее (область уведомлений). Выход — через меню по иконке.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        if args.tray:
            from bot.tray import run_tray_in_thread

            stop_event = asyncio.Event()
            run_tray_in_thread(stop_event)
            asyncio.run(main(stop_event=stop_event))
        else:
            asyncio.run(main())
    except KeyboardInterrupt:
        pass
    sys.exit(0)
