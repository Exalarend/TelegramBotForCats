import logging

from telegram import Update
from telegram.error import Conflict, NetworkError, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from bot.config import BotConfig
from bot.handlers import chat_member as chat_member_handlers
from bot.handlers import menu as menu_handlers
from bot.handlers import messages as message_handlers
from bot.notify.sender import SendOptions


def build_app(config: BotConfig, *, logger: logging.Logger) -> Application:
    app = (
        Application.builder()
        .token(config.token)
        .connect_timeout(config.api_timeout_seconds)
        .read_timeout(config.api_timeout_seconds)
        .write_timeout(config.api_timeout_seconds)
        .pool_timeout(config.pool_timeout_seconds)
        .get_updates_connect_timeout(config.api_timeout_seconds)
        .get_updates_read_timeout(config.api_timeout_seconds)
        .get_updates_write_timeout(config.api_timeout_seconds)
        .get_updates_pool_timeout(config.pool_timeout_seconds)
        .build()
    )

    # shared runtime objects
    app.bot_data["logger"] = logger
    app.bot_data["send_options"] = SendOptions(timeout_seconds=config.api_timeout_seconds, retry_attempts=config.api_retry_attempts)
    app.bot_data["finalize_rule_create"] = message_handlers.finalize_rule_create

    # load YAML system notifications
    try:
        from bot.system.config_loader import load_system_rules

        app.bot_data["system_rules"] = load_system_rules(config.system_yaml_path)
        logger.info("Loaded system notifications YAML: %s", config.system_yaml_path)
    except Exception:
        app.bot_data["system_rules"] = []
        logger.exception("Failed to load system notifications YAML: %s", config.system_yaml_path)

    # handlers
    app.add_handler(CommandHandler("start", message_handlers.cmd_start))
    app.add_handler(CommandHandler("menu", message_handlers.cmd_menu))
    app.add_handler(MessageHandler(filters.StatusUpdate.MIGRATE, message_handlers.on_migrate))
    app.add_handler(ChatMemberHandler(chat_member_handlers.on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(CallbackQueryHandler(menu_handlers.on_callback))
    app.add_handler(MessageHandler(filters.PHOTO, message_handlers.on_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handlers.on_text))
    app.add_error_handler(error_handler)

    return app


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger = context.application.bot_data.get("logger")
    err = context.error

    # Best-effort UX: if a button was pressed and we hit a network error,
    # tell the user to retry.
    try:
        if isinstance(update, Update) and update.callback_query is not None and isinstance(err, (NetworkError, TimedOut)):
            try:
                await update.callback_query.answer("Не удалось сделать действие, попробуйте ещё раз.", show_alert=True)
            except Exception:
                pass
    except Exception:
        pass

    # Conflict = другой экземпляр бота уже держит getUpdates (один токен — один long-polling).
    if isinstance(err, Conflict):
        log = logger if isinstance(logger, logging.Logger) else logging.getLogger("ministry-bot")
        log.error(
            "Conflict: уже запущен другой экземпляр бота с этим токеном. "
            "Закройте все остальные окна/процессы бота (и этот же бот на других ПК или серверах) и перезапустите один раз."
        )
        return

    # Network errors are expected on flaky connections; don't spam stack traces.
    if isinstance(err, (NetworkError, TimedOut)):
        if isinstance(logger, logging.Logger):
            logger.warning("Network error while handling update=%r: %s", update, err.__class__.__name__)
        else:
            logging.getLogger("ministry-bot").warning("Network error while handling update=%r: %s", update, err.__class__.__name__)
        return

    if isinstance(logger, logging.Logger):
        logger.exception("Unhandled exception while handling update=%r", update, exc_info=err)
    else:
        logging.getLogger("ministry-bot").exception("Unhandled exception while handling update=%r", update, exc_info=err)

