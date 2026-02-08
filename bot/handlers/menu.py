import logging

from telegram import ForceReply, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, NetworkError, TimedOut
from telegram.ext import ContextTypes
from telegram.helpers import escape
from time import perf_counter

from bot.db import repo
from bot.handlers import state as flow_state
from bot.handlers.utils import check_admin_in_groups, is_group, tg_call_with_retries
from bot.notify.picker import pick_big_red_content
from bot.notify.sender import SendOptions, TelegramSender
from bot.system.big_red_loader import find_node_by_path, get_nodes_at_path
from bot.scheduler import reschedule_chat_jobs, reschedule_rule_job, send_rule_notification
from bot.utils.rules_format import fmt_rule_name, fmt_rule_schedule


def weekday_labels() -> list[str]:
    # Python weekday: Mon=0..Sun=6
    return ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


def kb_main(chat_id: int) -> InlineKeyboardMarkup:
    settings = repo.get_chat_settings(chat_id)
    meta_label = "✅ Инфо (дата/время)" if settings.get("include_meta", True) else "⬜ Только текст"
    enabled_label = "🟢 Вкл/Выкл" if settings.get("enabled", True) else "🔴 Вкл/Выкл"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📅 Уведомления", callback_data=f"rules:{chat_id}")],
            [InlineKeyboardButton(meta_label, callback_data=f"toggle_meta:{chat_id}")],
            [InlineKeyboardButton(enabled_label, callback_data=f"toggle_chat:{chat_id}")],
            [InlineKeyboardButton("🔴 Большая красная кнопка", callback_data=f"big_red:{chat_id}")],
            [InlineKeyboardButton("ℹ️ Помощь", callback_data="help")],
        ]
    )


def kb_big_red_button(chat_id: int, root_nodes: list, path: str) -> InlineKeyboardMarkup:
    """Keyboard for Big Red Button tree. path='' = root, path='key1.key2' = nested."""
    from bot.system.big_red_loader import get_nodes_at_path

    nodes = get_nodes_at_path(root_nodes, path)
    rows: list[list[InlineKeyboardButton]] = []
    for node in nodes:
        full_path = f"{path}.{node.key}" if path else node.key
        if node.is_folder():
            rows.append([InlineKeyboardButton(f"📁 {node.title}", callback_data=f"big_red:{chat_id}:{full_path}")])
        else:
            rows.append([InlineKeyboardButton(node.title, callback_data=f"big_red_press:{chat_id}:{full_path}")])
    parent_path = ".".join(path.split(".")[:-1]) if path else ""
    if path:
        back_data = f"big_red:{chat_id}:{parent_path}" if parent_path else f"big_red:{chat_id}"
    else:
        back_data = f"menu:{chat_id}"
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=back_data)])
    return InlineKeyboardMarkup(rows)


def kb_rules(chat_id: int, rules: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton("➕ Добавить правило", callback_data=f"rule_add:{chat_id}")])
    for r in rules:
        rid = r["id"]
        enabled = "✅" if r["enabled"] else "⛔"
        name = _rule_display_name(r)
        if r.get("is_system") and not name.startswith("⭐"):
            name = f"⭐ {name}"
        rows.append([InlineKeyboardButton(f"{enabled} {name}", callback_data=f"rule_view:{chat_id}:{rid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"menu:{chat_id}")])
    return InlineKeyboardMarkup(rows)


def kb_rule_view(chat_id: int, rule: dict) -> InlineKeyboardMarkup:
    rid = rule["id"]
    if rule.get("is_system"):
        time_label = "⏱ Время" if rule.get("kind") == "weekly" else "🔁 Интервал"
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ Вкл" if not rule["enabled"] else "⛔ Выкл",
                        callback_data=f"rule_toggle:{chat_id}:{rid}",
                    ),
                    InlineKeyboardButton(time_label, callback_data=f"rule_time_edit:{chat_id}:{rid}"),
                ],
                [InlineKeyboardButton("⬅️ К списку", callback_data=f"rules:{chat_id}")],
            ]
        )

    has_image = bool(rule.get("image_file_id"))
    image_row = [InlineKeyboardButton("🖼 Картинка", callback_data=f"rule_image_set:{chat_id}:{rid}")]
    if has_image:
        image_row.append(InlineKeyboardButton("🧹 Убрать", callback_data=f"rule_image_clear:{chat_id}:{rid}"))

    time_label = "⏱ Время" if rule.get("kind") == "weekly" else "🔁 Интервал"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Вкл" if not rule["enabled"] else "⛔ Выкл",
                    callback_data=f"rule_toggle:{chat_id}:{rid}",
                ),
                InlineKeyboardButton("🏷 Название", callback_data=f"rule_title_edit:{chat_id}:{rid}"),
                InlineKeyboardButton("✍️ Текст", callback_data=f"rule_text_edit:{chat_id}:{rid}"),
            ],
            [InlineKeyboardButton(time_label, callback_data=f"rule_time_edit:{chat_id}:{rid}")],
            image_row,
            [InlineKeyboardButton("🗑 Удалить", callback_data=f"rule_del:{chat_id}:{rid}")],
            [InlineKeyboardButton("⬅️ К списку", callback_data=f"rules:{chat_id}")],
        ]
    )


def kb_add_kind(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📆 По дням недели + время", callback_data=f"add_kind_weekly:{chat_id}")],
            [InlineKeyboardButton("🔁 Интервал (минуты)", callback_data=f"add_kind_interval:{chat_id}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"rules:{chat_id}")],
        ]
    )


def kb_pick_days(chat_id: int, selected: set[int]) -> InlineKeyboardMarkup:
    labels = weekday_labels()
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for i, lab in enumerate(labels):
        mark = "✅" if i in selected else "⬜"
        row.append(InlineKeyboardButton(f"{mark} {lab}", callback_data=f"day_toggle:{chat_id}:{i}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Готово", callback_data=f"day_done:{chat_id}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"rule_add:{chat_id}")])
    return InlineKeyboardMarkup(rows)


def kb_pick_interval(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("30 мин", callback_data=f"interval:{chat_id}:30"),
                InlineKeyboardButton("60 мин", callback_data=f"interval:{chat_id}:60"),
                InlineKeyboardButton("120 мин", callback_data=f"interval:{chat_id}:120"),
            ],
            [InlineKeyboardButton("✍️ Ввести своё", callback_data=f"interval_custom:{chat_id}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"rule_add:{chat_id}")],
        ]
    )


def kb_draft_image(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🖼 Добавить картинку", callback_data=f"draft_image_add:{chat_id}")],
            [InlineKeyboardButton("➡️ Без картинки (сохранить)", callback_data=f"draft_image_skip:{chat_id}")],
            [InlineKeyboardButton("❌ Отменить", callback_data=f"draft_cancel:{chat_id}")],
        ]
    )


def rule_to_view(rule: dict) -> dict:
    return {**rule, "schedule": fmt_rule_schedule(rule), "display_name": _rule_display_name(rule)}


def _rule_display_name(rule: dict) -> str:
    return fmt_rule_name(rule)


def rule_view_text(rule: dict, timezone: str) -> str:
    name = _rule_display_name(rule)
    schedule = fmt_rule_schedule(rule)
    enabled = "ВКЛ" if rule["enabled"] else "ВЫКЛ"
    if rule.get("is_system"):
        texts = repo.get_rule_text_options(int(rule["id"]))
        imgs = repo.get_rule_image_options(int(rule["id"]))
        texts_n = len(texts)
        imgs_n = len(imgs)
        return (
            "<b>⭐ Системное уведомление</b>\n"
            f"🏷 <b>{escape(name)}</b>\n"
            f"📌 {escape(schedule)}\n"
            f"🟢 {enabled}\n"
            f"🕒 TZ: {escape(timezone)}\n"
            f"🖼 Картинки: вариантов={imgs_n}\n"
            f"📝 Тексты: всего={texts_n}\n\n"
            "✋ Текст и картинку менять нельзя. Можно менять время и вкл/выкл."
        )

    has_image = "есть" if rule.get("image_file_id") else "нет"
    txt = (rule.get("message_text") or "").strip()
    txt_escaped = escape(txt) if txt else "—"
    return (
        "<b>Уведомление</b>\n"
        f"🏷 <b>{escape(name)}</b>\n"
        f"📌 {escape(schedule)}\n"
        f"🟢 {enabled}\n"
        f"🕒 TZ: {escape(timezone)}\n"
        f"🖼 Картинка: {has_image}\n\n"
        f"📝 <b>Текст:</b>\n{txt_escaped}"
    )


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.callback_query is None or update.effective_chat is None:
        return
    q = update.callback_query
    t0 = perf_counter()

    logger = context.application.bot_data.get("logger")
    if not isinstance(logger, logging.Logger):
        logger = logging.getLogger("ministry-bot")

    async def edit_text(text: str, *, reply_markup=None, parse_mode=None) -> None:
        try:
            await tg_call_with_retries(
                lambda: q.edit_message_text(text, reply_markup=reply_markup, parse_mode=parse_mode),
                what="menu.edit_message_text",
                logger=logger,
            )
        except BadRequest as e:
            msg = str(e)
            if "Message is not modified" in msg:
                return
            logger.warning("edit_message_text failed: %s", msg)
            return
        except (NetworkError, TimedOut) as e:
            logger.warning("edit_message_text network error: %s", e.__class__.__name__)
            try:
                await q.answer("Не удалось сделать действие, попробуйте ещё раз.", show_alert=True)
            except Exception:
                pass
            return

    async def edit_markup(*, reply_markup) -> None:
        try:
            await tg_call_with_retries(
                lambda: q.edit_message_reply_markup(reply_markup=reply_markup),
                what="menu.edit_message_reply_markup",
                logger=logger,
            )
        except BadRequest as e:
            msg = str(e)
            if "Message is not modified" in msg:
                return
            logger.warning("edit_message_reply_markup failed: %s", msg)
            return
        except (NetworkError, TimedOut) as e:
            logger.warning("edit_message_reply_markup network error: %s", e.__class__.__name__)
            try:
                await q.answer("Не удалось сделать действие, попробуйте ещё раз.", show_alert=True)
            except Exception:
                pass
            return

    async def reply_text(text: str, *, reply_markup=None):
        # Used for ForceReply prompts (input steps).
        try:
            # ForceReply(selective=True) works reliably in groups only when the user is explicitly targeted.
            # In callback flows the bot message is usually a reply to a bot message, so "selective" may not match.
            # We target via @username when possible; otherwise fall back to non-selective.
            out_text = text
            out_reply_markup = reply_markup
            out_parse_mode = None
            if isinstance(reply_markup, ForceReply):
                if is_group(update.effective_chat.type) and bool(getattr(reply_markup, "selective", False)):
                    u = q.from_user
                    username = getattr(u, "username", None)
                    if username:
                        out_text = f"@{username}, {text}"
                    else:
                        out_reply_markup = ForceReply(selective=False)

            if q.message:
                return await tg_call_with_retries(
                    lambda: q.message.reply_text(out_text, reply_markup=out_reply_markup, parse_mode=out_parse_mode),
                    what="menu.reply_text",
                    logger=logger,
                )
            if update.effective_chat:
                return await tg_call_with_retries(
                    lambda: context.bot.send_message(
                        chat_id=update.effective_chat.id, text=out_text, reply_markup=out_reply_markup, parse_mode=out_parse_mode
                    ),
                    what="menu.send_message",
                    logger=logger,
                )
        except Exception:
            try:
                await q.answer("Не удалось сделать действие, попробуйте ещё раз.", show_alert=True)
            except Exception:
                pass
        return None

    # answerCallbackQuery is time-sensitive and may fail on flaky networks.
    # It's not required for logic, so we treat failures as best-effort.
    try:
        await q.answer()
    except BadRequest as e:
        msg = str(e)
        if "Query is too old" in msg or "query id is invalid" in msg:
            logger.debug("CallbackQuery answer skipped: %s", msg)
        else:
            logger.warning("CallbackQuery answer failed: %s", msg)
    except (NetworkError, TimedOut) as e:
        logger.warning("CallbackQuery answer network error: %s", e.__class__.__name__)

    allowed, ok = await check_admin_in_groups(update, context)
    if not ok:
        # Can't reliably check permissions due to Telegram/API issues.
        try:
            await q.answer("Не удалось сделать действие, попробуйте ещё раз.", show_alert=True)
        except Exception:
            pass
        return
    if not allowed:
        await edit_text("Настройки в группе доступны только администраторам.")
        return

    data = q.data or ""

    if data == "help":
        await edit_text(
            "Команды:\n"
            "- /start — открыть меню\n"
            "- /menu — открыть меню\n\n"
            "Меню:\n"
            "- 📅 «Уведомления» — список напоминаний, добавление/редактирование\n"
            "- ✅/⬜ «Инфо (дата/время)» — показывать/скрывать «шапку» (дата, время, TZ, название и расписание) уведомлений\n"
            "- 🟢/🔴 «Вкл/Выкл» — включить/выключить отправку уведомлений целиком для чата\n"
            "- 🔴 «Большая красная кнопка» — меню с кнопками (случайная картинка + текст)\n\n"
            "Правила:\n"
            "- Обычные уведомлени: можно менять 🏷 название, ✍️ текст, 🖼 картинку, ⏱ время/🔁 интервал, вкл/выкл, удалять\n"
            "- ⭐ Системные уведомления: можно только ⏱/🔁 и вкл/выкл (название/текст/картинка задаются самим министерством)\n\n"
            "Рекомендации:\n"
            "- Если бот просит ввести значение — отвечайте через команду «Reply» на сообщение бота (это надёжнее, особенно в группах)\n\n"
            "- Если команда не работает — попробуйте действие ещё раз.",
            reply_markup=kb_main(update.effective_chat.id),
        )
        return

    parts = data.split(":")
    action = parts[0]

    if action == "menu":
        chat_id = int(parts[1])
        await edit_text("Меню.", reply_markup=kb_main(chat_id))
        return

    if action == "toggle_chat":
        chat_id = int(parts[1])
        settings = repo.get_chat_settings(chat_id)
        repo.set_chat_enabled(chat_id, 0 if settings["enabled"] else 1)
        await edit_text(
            f"Уведомления теперь: {'ВКЛ' if not settings['enabled'] else 'ВЫКЛ'}",
            reply_markup=kb_main(chat_id),
        )
        await reschedule_chat_jobs(context.application, chat_id, logger=logger)
        return

    if action == "toggle_meta":
        chat_id = int(parts[1])
        settings = repo.get_chat_settings(chat_id)
        repo.set_chat_include_meta(chat_id, 0 if settings.get("include_meta", True) else 1)
        # No second DB read; we know the intended new value.
        new_include_meta = not settings.get("include_meta", True)
        await edit_text(
            f"Режим сообщений: {'с инфо (дата/время)' if new_include_meta else 'только текст'}",
            reply_markup=kb_main(chat_id),
        )
        return

    if action == "rules":
        chat_id = int(parts[1])
        repo.upsert_chat(chat_id)
        rules = [rule_to_view(r) for r in repo.get_rules(chat_id)]
        await edit_text("Правила уведомлений:", reply_markup=kb_rules(chat_id, rules))
        return

    if action == "big_red":
        chat_id = int(parts[1])
        path = parts[2] if len(parts) > 2 else ""
        root_nodes = context.application.bot_data.get("big_red_buttons") or []
        if not root_nodes:
            await edit_text("Большая красная кнопка пока не настроена.", reply_markup=kb_main(chat_id))
            return
        await edit_text("🔴 Большая красная кнопка\n\nВыберите кнопку — получите случайную картинку и текст:", reply_markup=kb_big_red_button(chat_id, root_nodes, path))
        return

    if action == "big_red_press":
        chat_id = int(parts[1])
        node_path = parts[2] if len(parts) > 2 else ""
        root_nodes = context.application.bot_data.get("big_red_buttons") or []
        btn = find_node_by_path(root_nodes, node_path)
        if not btn or not btn.is_leaf():
            root_nodes = context.application.bot_data.get("big_red_buttons") or []
            parent_path = ".".join(node_path.rsplit(".", 1)[:-1]) if "." in node_path else ""
            await edit_text("Кнопка не найдена.", reply_markup=kb_big_red_button(chat_id, root_nodes, parent_path))
            return
        picked = pick_big_red_content(btn)
        send_opts = context.application.bot_data.get("send_options")
        if not isinstance(send_opts, SendOptions):
            send_opts = SendOptions(timeout_seconds=20, retry_attempts=4)
        sender = TelegramSender(bot=context.bot, options=send_opts, logger=logger)
        text = (picked.text or "").strip()
        try:
            if picked.image_ref:
                await sender.send_photo(
                    chat_id=chat_id,
                    ref=str(picked.image_ref),
                    ref_type=str(picked.image_ref_type or "file_id"),
                    caption=text if text else None,
                    parse_mode=ParseMode.HTML if text else None,
                )
            elif text:
                await sender.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
        except Exception:
            logger.exception("Failed to send big_red content chat_id=%s path=%s", chat_id, node_path)
            try:
                await q.answer("Не удалось отправить сообщение.", show_alert=True)
            except Exception:
                pass
            return
        # Delete menu message after sending content
        try:
            if q.message:
                await q.message.delete()
        except Exception as e:
            logger.debug("Could not delete big_red menu message: %s", e)
        return

    if action == "rule_add":
        chat_id = int(parts[1])
        flow_state.clear_flow(context)
        await edit_text("Какое правило добавить?", reply_markup=kb_add_kind(chat_id))
        return

    if action == "add_kind_weekly":
        chat_id = int(parts[1])
        draft = flow_state.touch_or_init_draft(
            {"kind": "weekly", "days": set(), "stage": "pick_days"},
            chat_id=chat_id,
            actor_user_id=q.from_user.id,
        )
        flow_state.set_draft(context, draft)
        await edit_text("Выберите дни недели:", reply_markup=kb_pick_days(chat_id, set()))
        return

    if action == "day_toggle":
        chat_id = int(parts[1])
        day = int(parts[2])
        draft = flow_state.get_draft(context, chat_id=chat_id, actor_user_id=q.from_user.id)
        if not draft or draft.get("kind") != "weekly":
            await q.answer("Сессия добавления правила устарела.", show_alert=True)
            return
        selected: set[int] = draft.get("days", set())
        if day in selected:
            selected.remove(day)
        else:
            selected.add(day)
        draft = {**draft, "days": selected}
        flow_state.set_draft(context, draft)
        await edit_markup(reply_markup=kb_pick_days(chat_id, selected))
        return

    if action == "day_done":
        chat_id = int(parts[1])
        draft = flow_state.get_draft(context, chat_id=chat_id, actor_user_id=q.from_user.id)
        if not draft:
            await q.answer("Сессия добавления правила устарела.", show_alert=True)
            return
        selected: set[int] = draft.get("days", set())
        if not selected:
            await q.answer("Выберите хотя бы один день.", show_alert=True)
            return
        await edit_text("Ок. Теперь ответьте на сообщение ниже временем в формате HH:MM (например 09:30).")
        msg = await reply_text("Введите время в формате HH:MM (например 09:30).", reply_markup=ForceReply(selective=True))
        if msg:
            draft_next = {**draft, "days": list(sorted(selected))}
            draft_next = flow_state.set_stage_after_prompt(draft_next, stage="await_time", prompt_message_id=msg.message_id)
            flow_state.set_draft(context, draft_next)
        else:
            await q.answer("Не удалось сделать действие, попробуйте ещё раз.", show_alert=True)
        return

    if action == "add_kind_interval":
        chat_id = int(parts[1])
        draft = flow_state.touch_or_init_draft(
            {"kind": "interval", "stage": "pick_interval"},
            chat_id=chat_id,
            actor_user_id=q.from_user.id,
        )
        flow_state.set_draft(context, draft)
        await edit_text("Выберите интервал:", reply_markup=kb_pick_interval(chat_id))
        return

    if action == "interval":
        chat_id = int(parts[1])
        minutes = int(parts[2])
        draft = flow_state.touch_or_init_draft(
            {"kind": "interval", "interval_minutes": minutes},
            chat_id=chat_id,
            actor_user_id=q.from_user.id,
        )
        await edit_text("Ок. Теперь ответьте на сообщение ниже названием уведомления.")
        msg = await reply_text(
            "Введите название уведомления (например «Покормить кота»).",
            reply_markup=ForceReply(selective=True),
        )
        if msg:
            draft = flow_state.set_stage_after_prompt(draft, stage="await_rule_title", prompt_message_id=msg.message_id)
            flow_state.set_draft(context, draft)
        else:
            await q.answer("Не удалось сделать действие, попробуйте ещё раз.", show_alert=True)
        return

    if action == "interval_custom":
        chat_id = int(parts[1])
        draft0 = flow_state.get_draft(context, chat_id=chat_id, actor_user_id=q.from_user.id) or {}
        draft = flow_state.touch_or_init_draft(
            {**draft0, "kind": "interval"},
            chat_id=chat_id,
            actor_user_id=q.from_user.id,
        )
        await edit_text("Ок. Теперь ответьте на сообщение ниже числом минут (например 120).")
        msg = await reply_text("Введите интервал в минутах (например 120).", reply_markup=ForceReply(selective=True))
        if msg:
            draft = flow_state.set_stage_after_prompt(draft, stage="await_interval_custom", prompt_message_id=msg.message_id)
            flow_state.set_draft(context, draft)
        else:
            await q.answer("Не удалось сделать действие, попробуйте ещё раз.", show_alert=True)
        return

    if action == "rule_view":
        chat_id = int(parts[1])
        rid = int(parts[2])
        rule = repo.get_rule(chat_id, rid)
        if not rule:
            await edit_text("Правило не найдено.", reply_markup=kb_rules(chat_id, [rule_to_view(r) for r in repo.get_rules(chat_id)]))
            return
        settings = repo.get_chat_settings(chat_id)
        await edit_text(rule_view_text(rule, settings["timezone"]), reply_markup=kb_rule_view(chat_id, rule), parse_mode=ParseMode.HTML)
        return

    if action == "rule_toggle":
        chat_id = int(parts[1])
        rid = int(parts[2])
        repo.toggle_rule_enabled(chat_id=chat_id, rule_id=rid)
        await reschedule_rule_job(context.application, chat_id=chat_id, rule_id=rid, logger=logger)
        rule = repo.get_rule(chat_id, rid)
        if not rule:
            await edit_text("Правило не найдено.", reply_markup=kb_rules(chat_id, [rule_to_view(r) for r in repo.get_rules(chat_id)]))
            return
        settings = repo.get_chat_settings(chat_id)
        await edit_text(rule_view_text(rule, settings["timezone"]), reply_markup=kb_rule_view(chat_id, rule), parse_mode=ParseMode.HTML)
        return

    if action == "rule_text_edit":
        chat_id = int(parts[1])
        rid = int(parts[2])
        rule = repo.get_rule(chat_id, rid)
        if not rule:
            await edit_text("Уведомление не найдено.", reply_markup=kb_rules(chat_id, [rule_to_view(r) for r in repo.get_rules(chat_id)]))
            return
        if rule.get("is_system"):
            settings = repo.get_chat_settings(chat_id)
            await edit_text(rule_view_text(rule, settings["timezone"]), reply_markup=kb_rule_view(chat_id, rule), parse_mode=ParseMode.HTML)
            return
        draft = flow_state.touch_or_init_draft(
            {"stage": "await_edit_rule_text", "rule_id": rid},
            chat_id=chat_id,
            actor_user_id=q.from_user.id,
        )
        await edit_text("Ок. Теперь ответьте на сообщение ниже новым текстом уведомления.")
        msg = await reply_text("Введите новый текст уведомления.", reply_markup=ForceReply(selective=True))
        if msg:
            draft = flow_state.set_stage_after_prompt(draft, stage="await_edit_rule_text", prompt_message_id=msg.message_id)
            flow_state.set_draft(context, draft)
        else:
            await q.answer("Не удалось сделать действие, попробуйте ещё раз.", show_alert=True)
        return

    if action == "rule_title_edit":
        chat_id = int(parts[1])
        rid = int(parts[2])
        rule = repo.get_rule(chat_id, rid)
        if not rule:
            await edit_text("Уведомление не найдено.", reply_markup=kb_rules(chat_id, [rule_to_view(r) for r in repo.get_rules(chat_id)]))
            return
        if rule.get("is_system"):
            settings = repo.get_chat_settings(chat_id)
            await edit_text(rule_view_text(rule, settings["timezone"]), reply_markup=kb_rule_view(chat_id, rule), parse_mode=ParseMode.HTML)
            return
        draft = flow_state.touch_or_init_draft(
            {"stage": "await_edit_rule_title", "rule_id": rid},
            chat_id=chat_id,
            actor_user_id=q.from_user.id,
        )
        await edit_text("Ок. Теперь ответьте на сообщение ниже новым названием уведомления.")
        msg = await reply_text("Введите новое название уведомления.", reply_markup=ForceReply(selective=True))
        if msg:
            draft = flow_state.set_stage_after_prompt(draft, stage="await_edit_rule_title", prompt_message_id=msg.message_id)
            flow_state.set_draft(context, draft)
        else:
            await q.answer("Не удалось сделать действие, попробуйте ещё раз.", show_alert=True)
        return

    if action == "rule_time_edit":
        chat_id = int(parts[1])
        rid = int(parts[2])
        rule = repo.get_rule(chat_id, rid)
        if not rule:
            await edit_text("Уведомление не найдено.", reply_markup=kb_rules(chat_id, [rule_to_view(r) for r in repo.get_rules(chat_id)]))
            return
        kind = str(rule.get("kind"))
        draft = flow_state.touch_or_init_draft(
            {"stage": "await_edit_rule_time", "rule_id": rid, "kind": kind},
            chat_id=chat_id,
            actor_user_id=q.from_user.id,
        )
        prompt = "Введите новое время в формате HH:MM (например 09:30)." if kind == "weekly" else "Введите новый интервал в минутах (например 120)."
        await edit_text("Ок. Теперь ответьте на сообщение ниже новым значением.")
        msg = await reply_text(prompt, reply_markup=ForceReply(selective=True))
        if msg:
            draft = flow_state.set_stage_after_prompt(draft, stage="await_edit_rule_time", prompt_message_id=msg.message_id)
            flow_state.set_draft(context, draft)
        else:
            await q.answer("Не удалось сделать действие, попробуйте ещё раз.", show_alert=True)
        return

    if action == "rule_image_set":
        chat_id = int(parts[1])
        rid = int(parts[2])
        rule = repo.get_rule(chat_id, rid)
        if not rule:
            await edit_text("Уведомление не найдено.", reply_markup=kb_rules(chat_id, [rule_to_view(r) for r in repo.get_rules(chat_id)]))
            return
        if rule.get("is_system"):
            settings = repo.get_chat_settings(chat_id)
            await edit_text(rule_view_text(rule, settings["timezone"]), reply_markup=kb_rule_view(chat_id, rule), parse_mode=ParseMode.HTML)
            return
        await edit_text("Ок. Отправьте фото ответом на сообщение ниже — я сохраню его для этого уведомления.")
        msg = await reply_text("Отправьте фото ответом на это сообщение.", reply_markup=ForceReply(selective=True))
        if msg:
            awaiting = flow_state.touch_or_init_awaiting(
                {"mode": "rule_image", "rule_id": rid, "prompt_message_id": msg.message_id},
                chat_id=chat_id,
                actor_user_id=q.from_user.id,
            )
            flow_state.set_awaiting_photo(context, awaiting)
        else:
            await q.answer("Не удалось сделать действие, попробуйте ещё раз.", show_alert=True)
        return

    if action == "rule_image_clear":
        chat_id = int(parts[1])
        rid = int(parts[2])
        rule0 = repo.get_rule(chat_id, rid)
        if rule0 and rule0.get("is_system"):
            settings = repo.get_chat_settings(chat_id)
            await edit_text(rule_view_text(rule0, settings["timezone"]), reply_markup=kb_rule_view(chat_id, rule0), parse_mode=ParseMode.HTML)
            return
        repo.set_rule_image_file_id(chat_id=chat_id, rule_id=rid, file_id=None)
        rule = repo.get_rule(chat_id, rid)
        if not rule:
            await edit_text("Уведомление не найдено.", reply_markup=kb_rules(chat_id, [rule_to_view(r) for r in repo.get_rules(chat_id)]))
            return
        settings = repo.get_chat_settings(chat_id)
        await edit_text(rule_view_text(rule, settings["timezone"]), reply_markup=kb_rule_view(chat_id, rule), parse_mode=ParseMode.HTML)
        return

    if action == "draft_image_add":
        chat_id = int(parts[1])
        draft = flow_state.get_draft(context, chat_id=chat_id, actor_user_id=q.from_user.id)
        if not draft or draft.get("stage") != "await_rule_image_choice":
            await q.answer("Сессия создания устарела.", show_alert=True)
            return
        await edit_text("Ок. Теперь отправьте фото ответом на сообщение ниже.")
        msg = await reply_text(
            "Отправьте фото ответом на это сообщение — я привяжу его к этому уведомлению.",
            reply_markup=ForceReply(selective=True),
        )
        if msg:
            draft_next = flow_state.set_stage_after_prompt(draft, stage="await_rule_photo", prompt_message_id=msg.message_id)
            flow_state.set_draft(context, draft_next)
            awaiting = flow_state.touch_or_init_awaiting(
                {"mode": "draft_rule", "prompt_message_id": msg.message_id},
                chat_id=chat_id,
                actor_user_id=q.from_user.id,
            )
            flow_state.set_awaiting_photo(context, awaiting)
        else:
            await q.answer("Не удалось сделать действие, попробуйте ещё раз.", show_alert=True)
        return

    if action == "draft_image_skip":
        chat_id = int(parts[1])
        draft = context.user_data.get("draft_rule")
        if not draft or draft.get("chat_id") != chat_id or draft.get("stage") != "await_rule_image_choice":
            await q.answer("Сессия создания устарела.", show_alert=True)
            return
        draft["image_file_id"] = None
        rid = context.application.bot_data["finalize_rule_create"](chat_id, draft)
        flow_state.clear_flow(context)
        # Preview right after creation (best-effort)
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

        await edit_text(f"Уведомление создано (id={rid}).", reply_markup=kb_main(chat_id))
        await reschedule_rule_job(context.application, chat_id=chat_id, rule_id=rid, logger=logger)
        return

    if action == "draft_cancel":
        chat_id = int(parts[1])
        flow_state.clear_flow(context)
        rules = [rule_to_view(r) for r in repo.get_rules(chat_id)]
        await edit_text("Отменено. Правила уведомлений:", reply_markup=kb_rules(chat_id, rules))
        return

    if action == "rule_del":
        chat_id = int(parts[1])
        rid = int(parts[2])
        rule0 = repo.get_rule(chat_id, rid)
        if rule0 and rule0.get("is_system"):
            settings = repo.get_chat_settings(chat_id)
            await edit_text(rule_view_text(rule0, settings["timezone"]), reply_markup=kb_rule_view(chat_id, rule0), parse_mode=ParseMode.HTML)
            return
        repo.delete_rule(chat_id=chat_id, rule_id=rid)
        flow_state.clear_flow(context)
        # Remove job for this rule only to avoid resetting interval jobs.
        await reschedule_rule_job(context.application, chat_id=chat_id, rule_id=rid, logger=logger)
        rules = [rule_to_view(r) for r in repo.get_rules(chat_id)]
        await edit_text("Правила уведомлений:", reply_markup=kb_rules(chat_id, rules))
        return

    # Fallback: unknown action
    # (kept silent; only perf log below)

    # Log slow callbacks for debugging performance.
    dt = perf_counter() - t0
    if dt >= 0.5:
        logger.info("Menu callback action=%s took %.3fs", action, dt)

