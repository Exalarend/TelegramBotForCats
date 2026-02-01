import logging

from telegram import Update
from telegram.ext import ContextTypes

from bot.db import repo
from bot.notify.sender import TelegramSender
from bot.scheduler import reschedule_chat_jobs
from bot.system.sync import sync_system_rules_for_chat


async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    When the bot is added to a chat, create/sync system notifications right away.
    """
    if update.my_chat_member is None or update.effective_chat is None:
        return
    new_status = update.my_chat_member.new_chat_member.status
    old_status = update.my_chat_member.old_chat_member.status
    if new_status in {"member", "administrator"} and old_status in {"left", "kicked"}:
        chat_id = update.effective_chat.id
        repo.upsert_chat(chat_id)

        logger = _logger(context)
        rules = context.application.bot_data.get("system_rules") or []
        if rules:
            sync_system_rules_for_chat(chat_id=chat_id, rules=rules, logger=logger)
        await reschedule_chat_jobs(context.application, chat_id, logger=logger)

        try:
            sender = TelegramSender(bot=context.bot, options=context.application.bot_data["send_options"], logger=logger)
            await sender.send_message(
                chat_id=chat_id,
                text="Я добавлен в чат и уже настроил системные уведомления по умолчанию. Откройте /start для управления.",
            )
        except Exception:
            logger.exception("Failed to send welcome message chat_id=%s", chat_id)


def _logger(context: ContextTypes.DEFAULT_TYPE) -> logging.Logger:
    log = context.application.bot_data.get("logger")
    return log if isinstance(log, logging.Logger) else logging.getLogger("ministry-bot")

