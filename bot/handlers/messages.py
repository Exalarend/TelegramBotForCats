import logging

import asyncio
from telegram import ForceReply, Message, Update
from telegram.ext import ContextTypes

from bot.db import repo
from bot.handlers.menu import kb_draft_image, kb_main
from bot.handlers.utils import check_admin_in_groups, prompt_user_input, tg_call_with_retries
from bot.scheduler import reschedule_chat_jobs, reschedule_rule_job, send_rule_notification
from bot.system.sync import sync_system_rules_for_chat
from bot.handlers import state as flow_state


async def on_migrate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handles Telegram group -> supergroup migration service messages.
    Ensures DB and jobs move to the new chat_id.
    """
    if update.effective_message is None:
        return
    old_id = update.effective_message.migrate_from_chat_id
    new_id = update.effective_message.migrate_to_chat_id
    if not old_id or not new_id:
        return
    logger = _logger(context)
    try:
        repo.migrate_chat_id(old_chat_id=int(old_id), new_chat_id=int(new_id))
    except Exception:
        logger.exception("Failed to migrate chat_id in DB (service msg): %s -> %s", old_id, new_id)
        return
    try:
        await reschedule_chat_jobs(context.application, int(new_id), logger=logger)
    except Exception:
        logger.exception("Failed to reschedule migrated chat_id=%s", new_id)
        return
    logger.info("Migrated chat_id in DB: %s -> %s", old_id, new_id)


async def _safe_reply(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, *, reply_markup=None) -> None:
    """
    Best-effort reply helper: retries transient Telegram/network errors and never raises.
    """
    if update.effective_message is None:
        return
    try:
        await tg_call_with_retries(
            lambda: update.effective_message.reply_text(text, reply_markup=reply_markup),
            what="messages.reply_text",
            logger=_logger(context),
        )
    except Exception:
        _logger(context).warning("Failed to reply to user (network/Telegram issue).")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None:
        return

    chat_id = update.effective_chat.id
    repo.upsert_chat(chat_id)

    _sync_system(context, chat_id)

    allowed, ok = await check_admin_in_groups(update, context)
    if not ok:
        await _safe_reply(update, context, "Не удалось открыть меню из-за проблем с сетью. Попробуйте ещё раз.")
        return
    if not allowed:
        await _safe_reply(update, context, "Настройки в группе доступны только администраторам.")
        return

    if update.effective_message:
        # Keep /start responsive: minimal retries and no long waits.
        try:
            await tg_call_with_retries(
                lambda: update.effective_message.reply_text(
                    "Привет. Это «Министерство не твоих собачьих дел».\n\nОткрой меню ниже, чтобы настроить уведомления.",
                    reply_markup=kb_main(chat_id),
                ),
                what="cmd_start.reply_text",
                logger=_logger(context),
                max_attempts=2,
                base_delay_s=0.1,
            )
        except Exception:
            _logger(context).warning("Failed to reply to /start (network/Telegram issue).")
            return

    # Do scheduling in background so /start doesn't block.
    if not bool(context.application.bot_data.get("startup_scheduling_done", True)):
        return

    async def _reschedule() -> None:
        try:
            await reschedule_chat_jobs(context.application, chat_id, logger=_logger(context))
        except Exception:
            _logger(context).exception("Failed to schedule after /start chat_id=%s", chat_id)

    asyncio.create_task(_reschedule())


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None:
        return
    chat_id = update.effective_chat.id
    repo.upsert_chat(chat_id)

    _sync_system(context, chat_id)

    allowed, ok = await check_admin_in_groups(update, context)
    if not ok:
        await _safe_reply(update, context, "Не удалось открыть меню из-за проблем с сетью. Попробуйте ещё раз.")
        return
    if not allowed:
        await _safe_reply(update, context, "Настройки в группе доступны только администраторам.")
        return
    if update.effective_message:
        await tg_call_with_retries(
            lambda: update.effective_message.reply_text("Меню.", reply_markup=kb_main(chat_id)),
            what="cmd_menu.reply_text",
            logger=_logger(context),
        )


async def on_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None or update.effective_message is None:
        return

    msg: Message = update.effective_message
    if not msg.photo:
        return
    file_id = msg.photo[-1].file_id
    chat_id = update.effective_chat.id

    awaiting = flow_state.get_awaiting_photo(context, chat_id=chat_id, actor_user_id=(update.effective_user.id if update.effective_user else None))
    if not awaiting:
        return

    # If we prompted the user to reply with a photo, accept only replies to that prompt.
    prompt_id = awaiting.get("prompt_message_id")
    if prompt_id:
        rt = msg.reply_to_message
        if rt is None or rt.message_id != int(prompt_id):
            await _reprompt_photo(update, context, awaiting=awaiting)
            return

    allowed, ok = await check_admin_in_groups(update, context)
    if not ok:
        await _safe_reply(update, context, "Не удалось обработать действие из-за проблем с сетью. Попробуйте ещё раз.")
        await _reprompt_photo(update, context, awaiting=awaiting)
        return
    if not allowed:
        await _safe_reply(update, context, "Настройки в группе доступны только администраторам.")
        return

    mode = awaiting.get("mode")
    logger = _logger(context)

    if mode == "draft_rule":
        draft = flow_state.get_draft(
            context, chat_id=chat_id, actor_user_id=(update.effective_user.id if update.effective_user else None)
        )
        if not draft or draft.get("stage") != "await_rule_photo":
            return
        draft["image_file_id"] = file_id
        rid = finalize_rule_create(chat_id, draft)

        # Preview right after creation
        try:
            settings = repo.get_chat_settings(chat_id)
            rule = repo.get_rule(chat_id, rid)
            if rule:
                await send_rule_notification(
                    bot=context.bot,
                    chat_id=chat_id,
                    settings=settings,
                    rule=rule,
                    is_test=True,
                    send_options=context.application.bot_data["send_options"],
                    logger=logger,
                )
        except Exception:
            logger.exception("Failed to send preview for chat_id=%s rule_id=%s", chat_id, rid)

        flow_state.clear_flow(context)

        await _safe_reply(update, context, f"Уведомление создано (id={rid}).", reply_markup=kb_main(chat_id))
        await reschedule_rule_job(context.application, chat_id=chat_id, rule_id=rid, logger=logger)
        return

    if mode == "rule_image":
        rule_id = int(awaiting.get("rule_id", 0))
        if rule_id <= 0:
            return
        repo.set_rule_image_file_id(chat_id=chat_id, rule_id=rule_id, file_id=file_id)
        flow_state.clear_awaiting_photo(context)
        await _safe_reply(update, context, "Картинка сохранена для этого уведомления.")
        return


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat is None or update.effective_message is None:
        return

    chat_id = update.effective_chat.id
    draft = flow_state.get_draft(context, chat_id=chat_id, actor_user_id=(update.effective_user.id if update.effective_user else None))
    if not draft:
        return

    # If we prompted the user to reply to a bot message, accept only replies to that prompt.
    prompt_id = draft.get("prompt_message_id")
    if prompt_id:
        rt = update.effective_message.reply_to_message
        if rt is None or rt.message_id != int(prompt_id):
            await _safe_reply(update, context, "Пожалуйста, ответьте на сообщение бота (reply) — так надёжнее.")
            await _reprompt_draft(update, context, draft=draft)
            return

    allowed, ok = await check_admin_in_groups(update, context)
    if not ok:
        await _safe_reply(update, context, "Не удалось обработать действие из-за проблем с сетью. Попробуйте ещё раз.")
        await _reprompt_draft(update, context, draft=draft)
        return
    if not allowed:
        await _safe_reply(update, context, "Настройки в группе доступны только администраторам.")
        return

    stage = draft.get("stage")
    text = (update.effective_message.text or "").strip()
    logger = _logger(context)

    if stage == "await_time":
        try:
            hh, mm = text.split(":")
            hh_i = int(hh)
            mm_i = int(mm)
            if not (0 <= hh_i <= 23 and 0 <= mm_i <= 59):
                raise ValueError()
        except Exception:
            await _safe_reply(update, context, "Неверный формат. Введите время как HH:MM, например 09:30.")
            return
        days = sorted(set(draft.get("days", [])))
        if not days:
            await _safe_reply(update, context, "Вы не выбрали дни. Вернитесь в меню добавления правила.")
            flow_state.clear_flow(context)
            return
        draft_next = {**draft, "time_hhmm": f"{hh_i:02d}:{mm_i:02d}", "days": days}
        prompt_id = await prompt_user_input(
            update=update,
            context=context,
            prompt="Введите название уведомления (например «Покормить кота»).",
        )
        if prompt_id:
            draft_next = flow_state.touch_or_init_draft(
                draft_next, chat_id=chat_id, actor_user_id=(update.effective_user.id if update.effective_user else None)
            )
            draft_next = flow_state.set_stage_after_prompt(draft_next, stage="await_rule_title", prompt_message_id=prompt_id)
            flow_state.set_draft(context, draft_next)
        return

    if stage == "await_interval_custom":
        try:
            minutes = int(text)
            if minutes < 1 or minutes > 60 * 24 * 7:
                raise ValueError()
        except Exception:
            await _safe_reply(update, context, "Введите число минут (например 120).")
            return
        draft_next = {**draft, "interval_minutes": minutes}
        prompt_id = await prompt_user_input(
            update=update,
            context=context,
            prompt="Введите название уведомления (например «Проверить миску»).",
        )
        if prompt_id:
            draft_next = flow_state.touch_or_init_draft(
                draft_next, chat_id=chat_id, actor_user_id=(update.effective_user.id if update.effective_user else None)
            )
            draft_next = flow_state.set_stage_after_prompt(draft_next, stage="await_rule_title", prompt_message_id=prompt_id)
            flow_state.set_draft(context, draft_next)
        return

    if stage == "await_rule_title":
        if not text:
            await _safe_reply(update, context, "Название не должно быть пустым. Введите название уведомления.")
            return
        draft_next = {**draft, "title": text}
        prompt_id = await prompt_user_input(
            update=update,
            context=context,
            prompt="Введите текст уведомления (он будет приходить вместе с временем).",
        )
        if prompt_id:
            draft_next = flow_state.touch_or_init_draft(
                draft_next, chat_id=chat_id, actor_user_id=(update.effective_user.id if update.effective_user else None)
            )
            draft_next = flow_state.set_stage_after_prompt(draft_next, stage="await_rule_text", prompt_message_id=prompt_id)
            flow_state.set_draft(context, draft_next)
        return

    if stage == "await_rule_text":
        if not text:
            await _safe_reply(update, context, "Текст не должен быть пустым. Введите текст уведомления.")
            return
        draft["message_text"] = text
        draft["stage"] = "await_rule_image_choice"
        await _safe_reply(update, context, "Добавим картинку к этому уведомлению? (необязательно)", reply_markup=kb_draft_image(chat_id))
        return

    if stage == "await_edit_rule_text":
        rule_id = int(draft.get("rule_id", 0))
        if rule_id <= 0:
            flow_state.clear_flow(context)
            await _safe_reply(update, context, "Сессия редактирования устарела.", reply_markup=kb_main(chat_id))
            return
        if not text:
            await _safe_reply(update, context, "Текст не должен быть пустым. Введите новый текст уведомления.")
            return
        repo.set_rule_text(chat_id=chat_id, rule_id=rule_id, message_text=text)
        flow_state.clear_flow(context)
        await _safe_reply(update, context, "Текст обновлён.")
        return

    if stage == "await_edit_rule_title":
        rule_id = int(draft.get("rule_id", 0))
        if rule_id <= 0:
            flow_state.clear_flow(context)
            await _safe_reply(update, context, "Сессия редактирования устарела.", reply_markup=kb_main(chat_id))
            return
        if not text:
            await _safe_reply(update, context, "Название не должно быть пустым. Введите новое название.")
            return
        repo.set_rule_title(chat_id=chat_id, rule_id=rule_id, title=text)
        flow_state.clear_flow(context)
        await _safe_reply(update, context, "Название обновлено.")
        return

    if stage == "await_edit_rule_time":
        rule_id = int(draft.get("rule_id", 0))
        kind = str(draft.get("kind") or "")
        if rule_id <= 0 or kind not in {"weekly", "interval"}:
            flow_state.clear_flow(context)
            await _safe_reply(update, context, "Сессия редактирования устарела.", reply_markup=kb_main(chat_id))
            return
        if kind == "weekly":
            try:
                hh, mm = text.split(":")
                hh_i = int(hh)
                mm_i = int(mm)
                if not (0 <= hh_i <= 23 and 0 <= mm_i <= 59):
                    raise ValueError()
            except Exception:
                await _safe_reply(update, context, "Неверный формат. Введите время как HH:MM, например 09:30.")
                return
            repo.set_rule_time_hhmm(chat_id=chat_id, rule_id=rule_id, time_hhmm=f"{hh_i:02d}:{mm_i:02d}")
            flow_state.clear_flow(context)
            await _safe_reply(update, context, "Время обновлено.")
            await reschedule_rule_job(context.application, chat_id=chat_id, rule_id=rule_id, logger=logger)
            return
        else:
            try:
                minutes = int(text)
                if minutes < 1 or minutes > 60 * 24 * 7:
                    raise ValueError()
            except Exception:
                await _safe_reply(update, context, "Введите число минут (например 120).")
                return
            repo.set_rule_interval_minutes(chat_id=chat_id, rule_id=rule_id, interval_minutes=minutes)
            flow_state.clear_flow(context)
            await _safe_reply(update, context, "Интервал обновлён.")
            await reschedule_rule_job(context.application, chat_id=chat_id, rule_id=rule_id, logger=logger)
            return


def finalize_rule_create(chat_id: int, draft: dict) -> int:
    kind = draft.get("kind")
    title = str(draft.get("title") or "").strip()
    if not title:
        title = "Уведомление"
    message_text = str(draft.get("message_text") or "")
    image_file_id = draft.get("image_file_id")
    if kind == "weekly":
        return repo.create_rule_weekly(
            chat_id=chat_id,
            title=title,
            days=list(draft.get("days") or []),
            time_hhmm=str(draft.get("time_hhmm")),
            message_text=message_text,
            image_file_id=image_file_id,
        )
    if kind == "interval":
        return repo.create_rule_interval(
            chat_id=chat_id,
            title=title,
            interval_minutes=int(draft.get("interval_minutes")),
            message_text=message_text,
            image_file_id=image_file_id,
        )
    raise ValueError("Invalid draft kind")


def _sync_system(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    rules = context.application.bot_data.get("system_rules") or []
    if rules:
        sync_system_rules_for_chat(chat_id=chat_id, rules=rules, logger=_logger(context))


def _logger(context: ContextTypes.DEFAULT_TYPE) -> logging.Logger:
    log = context.application.bot_data.get("logger")
    return log if isinstance(log, logging.Logger) else logging.getLogger("ministry-bot")


async def _reprompt_photo(update: Update, context: ContextTypes.DEFAULT_TYPE, *, awaiting: dict) -> None:
    """
    Best-effort: re-ask the user for the photo when we couldn't process due to network issues.
    """
    if update.effective_chat is None or update.effective_message is None:
        return
    mode = str(awaiting.get("mode") or "")
    chat_id = update.effective_chat.id
    if mode == "draft_rule":
        draft = flow_state.get_draft(
            context, chat_id=chat_id, actor_user_id=(update.effective_user.id if update.effective_user else None)
        )
        if not isinstance(draft, dict):
            return
        # Keep waiting for a photo.
        try:
            text = "Отправьте фото ответом на это сообщение — я привяжу его к уведомлению."
            reply_markup = ForceReply(selective=True)
            if is_group(update.effective_chat.type) and update.effective_user is not None:
                username = getattr(update.effective_user, "username", None)
                if username:
                    text = f"@{username}, {text}"
                else:
                    reply_markup = ForceReply(selective=False)
            prompt_msg = await tg_call_with_retries(
                lambda: update.effective_message.reply_text(text, reply_markup=reply_markup),
                what="photo_prompt.reply_text",
                logger=_logger(context),
            )
        except Exception:
            return
        draft_next = flow_state.set_stage_after_prompt(draft, stage="await_rule_photo", prompt_message_id=prompt_msg.message_id)
        flow_state.set_draft(context, draft_next)
        awaiting_next = flow_state.touch_or_init_awaiting(
            {"chat_id": chat_id, "mode": "draft_rule", "prompt_message_id": prompt_msg.message_id},
            chat_id=chat_id,
            actor_user_id=(update.effective_user.id if update.effective_user else None),
        )
        flow_state.set_awaiting_photo(context, awaiting_next)
    elif mode == "rule_image":
        rule_id = int(awaiting.get("rule_id", 0))
        try:
            text = "Отправьте фото ответом на это сообщение — я сохраню его для уведомления."
            reply_markup = ForceReply(selective=True)
            if is_group(update.effective_chat.type) and update.effective_user is not None:
                username = getattr(update.effective_user, "username", None)
                if username:
                    text = f"@{username}, {text}"
                else:
                    reply_markup = ForceReply(selective=False)
            prompt_msg = await tg_call_with_retries(
                lambda: update.effective_message.reply_text(text, reply_markup=reply_markup),
                what="photo_prompt.reply_text",
                logger=_logger(context),
            )
        except Exception:
            return
        awaiting_next = flow_state.touch_or_init_awaiting(
            {"chat_id": chat_id, "mode": "rule_image", "rule_id": rule_id, "prompt_message_id": prompt_msg.message_id},
            chat_id=chat_id,
            actor_user_id=(update.effective_user.id if update.effective_user else None),
        )
        flow_state.set_awaiting_photo(context, awaiting_next)


async def _reprompt_draft(update: Update, context: ContextTypes.DEFAULT_TYPE, *, draft: dict) -> None:
    stage = str(draft.get("stage") or "")
    kind = str(draft.get("kind") or "")
    prompt: str | None = None

    if stage == "await_time":
        prompt = "Введите время в формате HH:MM (например 09:30)."
    elif stage == "await_interval_custom":
        prompt = "Введите интервал в минутах (например 120)."
    elif stage == "await_rule_title":
        prompt = "Введите название уведомления (например «Покормить кота»)."
    elif stage == "await_rule_text":
        prompt = "Введите текст уведомления (он будет приходить вместе с временем)."
    elif stage == "await_edit_rule_text":
        prompt = "Введите новый текст уведомления."
    elif stage == "await_edit_rule_title":
        prompt = "Введите новое название уведомления."
    elif stage == "await_edit_rule_time":
        prompt = "Введите новое время в формате HH:MM (например 09:30)." if kind == "weekly" else "Введите новый интервал в минутах (например 120)."

    if prompt:
        prompt_id = await prompt_user_input(update=update, context=context, prompt=prompt)
        if prompt_id:
            chat_id = update.effective_chat.id if update.effective_chat else int(draft.get("chat_id") or 0)
            draft_next = flow_state.touch_or_init_draft(
                draft, chat_id=chat_id, actor_user_id=(update.effective_user.id if update.effective_user else None)
            )
            draft_next = flow_state.set_stage_after_prompt(draft_next, stage=str(draft_next.get("stage") or ""), prompt_message_id=prompt_id)
            flow_state.set_draft(context, draft_next)

