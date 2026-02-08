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
from bot.notify.sender import SendOptions, TelegramSender
from bot.scheduler import reschedule_chat_jobs
from bot.system.sync import SyncResult, sync_system_rules_for_chat


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

    def _read_manual_startup_changes() -> str | None:
        """Read manual changelog from file. Returns None if file missing/empty. Clears file after read."""
        path = config.startup_changes_file
        if not path or not os.path.isfile(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                text = f.read().strip()
        except OSError:
            logger.warning("Failed to read startup changes file: %s", path)
            return None
        if not text:
            return None
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write("")
        except OSError:
            logger.warning("Failed to clear startup changes file after read: %s", path)
        return text

    def _format_auto_startup_changes(result: SyncResult) -> str:
        lines = ["🔄 Бот перезапущен. Изменения в уведомлениях:"]
        if result.added:
            lines.append("• Добавлено: " + ", ".join(result.added))
        if result.removed:
            lines.append("• Удалено: " + ", ".join(result.removed))
        return "\n".join(lines)

    async def _startup_sync_and_schedule() -> None:
        t = perf_counter()
        try:
            chats = repo.get_all_chats()
            system_rules = app.bot_data.get("system_rules") or []
            total = len(chats)
            if total == 0:
                logger.info("Startup: no known chats to schedule.")
                return

            # Manual changelog takes precedence over auto-detected
            manual_msg = _read_manual_startup_changes()

            semaphore = asyncio.Semaphore(4)
            done = 0

            async def _process_chat(chat_id: int, sync_result: SyncResult | None) -> None:
                nonlocal done
                async with semaphore:
                    try:
                        await reschedule_chat_jobs(app, chat_id, logger=logger)
                    except Exception:
                        logger.exception("Failed to schedule chat_id=%s", chat_id)
                    done += 1
                    if done % 10 == 0 or done == total:
                        logger.info("Startup: scheduled %s/%s chats", done, total)

                    # Notify about changes (manual file or auto-detected)
                    if not config.notify_startup_changes:
                        return
                    msg = manual_msg
                    if not msg and sync_result and (sync_result.added or sync_result.removed):
                        msg = _format_auto_startup_changes(sync_result)
                    if msg:
                        try:
                            send_opts = app.bot_data.get("send_options")
                            if isinstance(send_opts, SendOptions):
                                sender = TelegramSender(bot=app.bot, options=send_opts, logger=logger)
                                await sender.send_message(chat_id=chat_id, text=msg)
                        except Exception:
                            logger.exception("Failed to send startup changes notification to chat_id=%s", chat_id)

            # Sync all chats in parallel (thread pool)
            async def _sync_one(chat_id: int) -> tuple[int, SyncResult | None]:
                if not system_rules:
                    return chat_id, None
                try:
                    res = await asyncio.to_thread(
                        sync_system_rules_for_chat, chat_id=chat_id, rules=system_rules, logger=logger
                    )
                    return chat_id, res
                except Exception:
                    logger.exception("Failed to sync system rules for chat_id=%s", chat_id)
                    return chat_id, None

            sync_tasks = [_sync_one(int(c["chat_id"])) for c in chats]
            sync_pairs = await asyncio.gather(*sync_tasks)
            sync_results = dict(sync_pairs)

            await asyncio.gather(
                *(
                    _process_chat(chat_id, sync_results.get(chat_id))
                    for chat_id in (int(c["chat_id"]) for c in chats)
                )
            )
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

