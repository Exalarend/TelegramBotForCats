import asyncio
import logging
import os
from time import perf_counter

from telegram import Update
from telegram.error import Conflict, NetworkError, TimedOut

from bot.app import build_app
from bot.config import BotConfig
from bot.db.schema import ensure_schema
from bot.db import repo
from bot.scheduler import reschedule_chat_jobs
from bot.system.sync import sync_system_rules_for_chat


async def run_bot(
    config: BotConfig,
    *,
    logger: logging.Logger,
    stop_event: asyncio.Event | None = None,
) -> None:
    t0 = perf_counter()
    ensure_schema(db_path=config.db_path, default_timezone=config.default_timezone)
    logger.info("Startup: DB ready in %.3fs", perf_counter() - t0)

    t1 = perf_counter()
    app = build_app(config, logger=logger)
    logger.info("Startup: app built in %.3fs", perf_counter() - t1)

    logger.info("Bot is starting...")
    t2 = perf_counter()
    max_connect_retries = int(os.getenv("BOT_STARTUP_CONNECT_RETRIES", "3"))
    retry_delay = float(os.getenv("BOT_STARTUP_RETRY_DELAY_SECONDS", "5"))
    connect_error = None
    for attempt in range(1, max_connect_retries + 1):
        try:
            await app.initialize()
            connect_error = None
            break
        except (TimedOut, NetworkError) as e:
            connect_error = e
            if attempt < max_connect_retries:
                logger.warning(
                    "Connection to Telegram API failed (attempt %s/%s): %s. Retrying in %.1fs...",
                    attempt,
                    max_connect_retries,
                    type(e).__name__,
                    retry_delay,
                )
                await asyncio.sleep(retry_delay)
            else:
                logger.error(
                    "Connection to Telegram API failed after %s attempts. Check internet, firewall, proxy.",
                    max_connect_retries,
                )
    if connect_error is not None:
        raise connect_error
    logger.info("Startup: app initialized in %.3fs", perf_counter() - t2)

    # Ошибка 409 Conflict при polling — другой экземпляр бота уже держит getUpdates.
    def _asyncio_exc_handler(loop: asyncio.AbstractEventLoop, context: dict) -> None:
        exc = context.get("exception")
        if isinstance(exc, Conflict):
            logger.error(
                "Conflict (409): уже запущен другой экземпляр бота с этим токеном. "
                "Закройте все остальные окна/процессы бота (и этот же бот на других ПК или серверах) и перезапустите один раз."
            )
            return
        loop.default_exception_handler(context)

    asyncio.get_running_loop().set_exception_handler(_asyncio_exc_handler)

    # Start responding ASAP; heavy sync/schedule happens in background.
    t3 = perf_counter()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    logger.info("Startup: polling started in %.3fs", perf_counter() - t3)

    app.bot_data["startup_scheduling_done"] = False

    async def _startup_sync_and_schedule() -> None:
        t = perf_counter()
        try:
            chats = repo.get_all_chats()
            system_rules = app.bot_data.get("system_rules") or []
            total = len(chats)
            if total == 0:
                logger.info("Startup: no known chats to schedule.")
                return

            semaphore = asyncio.Semaphore(4)
            done = 0

            async def _process_chat(chat_id: int) -> None:
                nonlocal done
                async with semaphore:
                    if system_rules:
                        try:
                            await asyncio.to_thread(sync_system_rules_for_chat, chat_id=chat_id, rules=system_rules, logger=logger)
                        except Exception:
                            logger.exception("Failed to sync system rules for chat_id=%s", chat_id)
                    try:
                        await reschedule_chat_jobs(app, chat_id, logger=logger)
                    except Exception:
                        logger.exception("Failed to schedule chat_id=%s", chat_id)
                    done += 1
                    if done % 10 == 0 or done == total:
                        logger.info("Startup: scheduled %s/%s chats", done, total)

            await asyncio.gather(*(_process_chat(int(c["chat_id"])) for c in chats))
            logger.info("Startup: sync+schedule done for chats=%s in %.3fs", total, perf_counter() - t)
        finally:
            app.bot_data["startup_scheduling_done"] = True

    asyncio.create_task(_startup_sync_and_schedule())

    if stop_event is not None:
        logger.info("Bot is running (tray mode). Use tray menu to stop.")
        await stop_event.wait()
        logger.info("Stopping bot...")
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        return

    logger.info("Bot is running. Press Ctrl+C to stop.")
    await asyncio.Event().wait()

