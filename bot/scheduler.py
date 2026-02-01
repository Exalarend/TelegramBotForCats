import asyncio
import logging
import time as time_mod
from datetime import datetime, time
from datetime import timezone
from time import perf_counter
from zoneinfo import ZoneInfo

from telegram.constants import ParseMode
from telegram.helpers import escape
from telegram.error import ChatMigrated
from telegram.ext import Application, ContextTypes

from bot.db import repo
from bot.notify.picker import pick_system_content
from bot.notify.sender import SendOptions, TelegramSender
from bot.utils.retry import compute_retry_delay_s
from bot.utils.rules_format import fmt_rule_name, fmt_rule_schedule
from bot.utils.schedule import python_weekday_to_jobqueue


MAX_SEND_RETRY_ATTEMPTS = 3


JOB_KIND_RULE = "rule"
JOB_KIND_RETRY = "rule_retry"


def _rule_generation_store(app: Application) -> dict[tuple[int, int, str], int]:
    """
    Generation tokens for scheduled jobs.
    Key: (chat_id, rule_id, job_kind) where job_kind in {JOB_KIND_RULE, JOB_KIND_RETRY}.

    If old jobs linger due to async schedule_removal(), generation gating prevents duplicate sends.
    """
    store: dict[tuple[int, int, str], int] = app.bot_data.setdefault("rule_generation", {})
    return store


def _current_rule_generation(app: Application, *, chat_id: int, rule_id: int, job_kind: str) -> int | None:
    return _rule_generation_store(app).get((int(chat_id), int(rule_id), str(job_kind)))


def _bump_rule_generation(app: Application, *, chat_id: int, rule_id: int, job_kind: str) -> int:
    key = (int(chat_id), int(rule_id), str(job_kind))
    store = _rule_generation_store(app)
    store[key] = int(store.get(key, 0)) + 1
    return int(store[key])


def _job_kind_from_name(job_name: str) -> str:
    return JOB_KIND_RETRY if str(job_name).startswith("rule_retry:") else JOB_KIND_RULE


def _job_is_stale(app: Application, *, chat_id: int, rule_id: int, job_kind: str, job_generation: int) -> bool:
    """
    Returns True if the job is stale (should be ignored).
    If there is no known generation for this (chat_id, rule_id, kind), the job is treated as current.
    """
    expected = _current_rule_generation(app, chat_id=chat_id, rule_id=rule_id, job_kind=job_kind)
    if expected is None:
        return False
    return int(job_generation) != int(expected)


def _jobs_by_name(app: Application, name: str):
    try:
        return app.job_queue.get_jobs_by_name(name)
    except Exception:
        return tuple(j for j in app.job_queue.jobs() if j.name == name)


def _remove_rule_job(app: Application, *, rule_id: int) -> None:
    for job in _jobs_by_name(app, f"rule:{rule_id}"):
        job.schedule_removal()
    for job in _jobs_by_name(app, f"rule_retry:{rule_id}"):
        job.schedule_removal()


def _remove_chat_jobs(app: Application, *, chat_id: int) -> None:
    # Deprecated (O(total_jobs)). Kept for emergency cleanups.
    for job in app.job_queue.jobs():
        if job.data and job.data.get("chat_id") == chat_id and job.name and (job.name.startswith("rule:") or job.name.startswith("rule_retry:")):
            job.schedule_removal()


def _remove_chat_jobs_by_rule_ids(app: Application, *, chat_id: int, rule_ids: list[int]) -> None:
    # Fast path: remove by names (no full scan).
    for rid in rule_ids:
        _remove_rule_job(app, rule_id=rid)


def _chat_lock(app: Application, chat_id: int) -> asyncio.Lock:
    locks: dict[int, asyncio.Lock] = app.bot_data.setdefault("reschedule_locks", {})
    lock = locks.get(int(chat_id))
    if lock is None:
        lock = asyncio.Lock()
        locks[int(chat_id)] = lock
    return lock


async def reschedule_chat_jobs(app: Application, chat_id: int, *, logger: logging.Logger) -> None:
    # Full reschedule: used when chat enabled toggles or on startup.
    # Fast removal by rule ids (avoid scanning all jobs).
    async with _chat_lock(app, chat_id):
        t0 = perf_counter()
        rules = repo.get_rules(chat_id)
        rule_ids = [int(r["id"]) for r in rules]
        _remove_chat_jobs_by_rule_ids(app, chat_id=chat_id, rule_ids=rule_ids)

        settings = repo.get_chat_settings(chat_id)
        if not settings["enabled"]:
            return

        tz = ZoneInfo(settings["timezone"])

        for r in rules:
            if not r["enabled"]:
                continue

            _schedule_rule_job(app, chat_id=chat_id, tz=tz, rule=r)

        dt = perf_counter() - t0
        if dt >= 0.5:
            logger.info("Scheduled chat_id=%s rules=%s in %.3fs", chat_id, len(rules), dt)
        else:
            logger.info("Scheduled chat_id=%s rules=%s", chat_id, len(rules))


async def reschedule_rule_job(app: Application, *, chat_id: int, rule_id: int, logger: logging.Logger) -> None:
    """
    Partial reschedule: only (re)create the job for a specific rule.
    This avoids resetting interval jobs when editing unrelated rules.
    """
    async with _chat_lock(app, chat_id):
        _remove_rule_job(app, rule_id=rule_id)

        settings = repo.get_chat_settings(chat_id)
        if not settings["enabled"]:
            return

        tz = ZoneInfo(settings["timezone"])
        rule = repo.get_rule(chat_id, rule_id)
        if not rule or not rule["enabled"]:
            return

        _schedule_rule_job(app, chat_id=chat_id, tz=tz, rule=rule)
        logger.info("Rescheduled chat_id=%s rule_id=%s", chat_id, rule_id)


def _schedule_rule_job(app: Application, *, chat_id: int, tz: ZoneInfo, rule: dict) -> None:
    if rule["kind"] == "weekly":
        gen = _bump_rule_generation(app, chat_id=chat_id, rule_id=int(rule["id"]), job_kind=JOB_KIND_RULE)
        hh, mm = map(int, str(rule["time_hhmm"]).split(":"))
        t = time(hour=hh, minute=mm, tzinfo=tz)
        days = tuple(python_weekday_to_jobqueue(d) for d in rule["days"])
        app.job_queue.run_daily(
            send_notification_job,
            time=t,
            days=days,
            name=f"rule:{rule['id']}",
            data={"chat_id": chat_id, "rule_id": rule["id"], "gen": int(gen)},
        )
    elif rule["kind"] == "interval":
        gen = _bump_rule_generation(app, chat_id=chat_id, rule_id=int(rule["id"]), job_kind=JOB_KIND_RULE)
        _schedule_interval_run(app, chat_id=chat_id, rule=rule, retry_attempt=0, gen=gen)


def _schedule_interval_run(app: Application, *, chat_id: int, rule: dict, retry_attempt: int, gen: int) -> None:
    # For interval rules we always use a single run_once job (name=rule:<id>).
    # This prevents interval rules from "stopping" after a failure.
    run_at = _compute_next_interval_run_dt(rule) if retry_attempt <= 0 else _retry_dt(retry_attempt)
    _remove_interval_job_only(app, rule_id=int(rule["id"]))
    app.job_queue.run_once(
        send_notification_job,
        when=run_at,
        name=f"rule:{rule['id']}",
        data={"chat_id": chat_id, "rule_id": int(rule["id"]), "retry_attempt": int(retry_attempt), "gen": int(gen)},
    )


def _remove_interval_job_only(app: Application, *, rule_id: int) -> None:
    for job in _jobs_by_name(app, f"rule:{rule_id}"):
        job.schedule_removal()


def _schedule_weekly_retry(app: Application, *, chat_id: int, rule_id: int, retry_attempt: int) -> None:
    """
    Weekly rules remain scheduled via run_daily. Retry is a separate run_once job.
    """
    gen = _bump_rule_generation(app, chat_id=chat_id, rule_id=int(rule_id), job_kind=JOB_KIND_RETRY)
    _remove_weekly_retry_job(app, rule_id=rule_id)
    app.job_queue.run_once(
        send_notification_job,
        when=_retry_dt(retry_attempt),
        name=f"rule_retry:{rule_id}",
        data={"chat_id": chat_id, "rule_id": int(rule_id), "retry_attempt": int(retry_attempt), "gen": int(gen)},
    )


def _remove_weekly_retry_job(app: Application, *, rule_id: int) -> None:
    for job in _jobs_by_name(app, f"rule_retry:{rule_id}"):
        job.schedule_removal()


def _retry_delay_seconds(retry_attempt: int) -> int:
    return compute_retry_delay_s(retry_attempt)


def _retry_dt(retry_attempt: int) -> datetime:
    now_ts = int(time_mod.time())
    return datetime.fromtimestamp(now_ts + _retry_delay_seconds(retry_attempt), tz=timezone.utc)


def _compute_next_interval_run_dt(rule: dict) -> datetime:
    """
    Next run for interval rules is computed from the last sent time.
    If never sent, we use created_at_ts as the anchor.

    This prevents interval notifications from firing immediately after bot restart.
    """
    interval_sec = int(rule["interval_minutes"]) * 60
    now_ts = int(time_mod.time())
    anchor_ts = int(rule.get("last_sent_at_ts") or 0) or int(rule.get("created_at_ts") or 0) or now_ts
    if interval_sec <= 0:
        return datetime.fromtimestamp(now_ts + 60, tz=timezone.utc)

    if now_ts < anchor_ts:
        next_ts = anchor_ts + interval_sec
    else:
        delta = now_ts - anchor_ts
        n = (delta // interval_sec) + 1
        next_ts = anchor_ts + n * interval_sec

    # schedule in UTC
    return datetime.fromtimestamp(int(next_ts), tz=timezone.utc)


async def send_notification_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = context.job.data if context.job else None
    if not data:
        return
    chat_id = int(data["chat_id"])
    rule_id = int(data["rule_id"])
    retry_attempt = int(data.get("retry_attempt") or 0)
    job_name = str(getattr(context.job, "name", "") or "")
    job_kind = _job_kind_from_name(job_name)
    job_gen = int(data.get("gen") or 0)
    is_weekly_retry_job = job_kind == JOB_KIND_RETRY

    settings = repo.get_chat_settings(chat_id)
    if not settings["enabled"]:
        return

    rule = repo.get_rule(chat_id, rule_id)
    if not rule or not rule["enabled"]:
        return

    logger = context.application.bot_data.get("logger")
    if not isinstance(logger, logging.Logger):
        logger = logging.getLogger("ministry-bot")

    if _job_is_stale(context.application, chat_id=chat_id, rule_id=rule_id, job_kind=job_kind, job_generation=job_gen):
        logger.debug("Ignoring stale job kind=%s chat_id=%s rule_id=%s gen=%s", job_kind, chat_id, rule_id, job_gen)
        return

    try:
        await send_rule_notification(
            bot=context.bot,
            chat_id=chat_id,
            settings=settings,
            rule=rule,
            is_test=False,
            send_options=context.application.bot_data["send_options"],
            logger=logger,
        )

        # On success: clear any pending weekly retry job and advance interval anchor.
        if is_weekly_retry_job:
            _remove_weekly_retry_job(context.application, rule_id=rule_id)

        if rule.get("kind") == "interval":
            now_ts = int(time_mod.time())
            repo.set_rule_last_sent_at_ts(chat_id=chat_id, rule_id=rule_id, ts=now_ts)
            rule["last_sent_at_ts"] = now_ts
            # Interval jobs are self-rescheduling: schedule next run from the updated anchor.
            async with _chat_lock(context.application, chat_id):
                gen = _bump_rule_generation(context.application, chat_id=chat_id, rule_id=rule_id, job_kind=JOB_KIND_RULE)
                _schedule_interval_run(context.application, chat_id=chat_id, rule=rule, retry_attempt=0, gen=gen)
        return
    except ChatMigrated as e:
        new_chat_id = int(getattr(e, "new_chat_id", 0) or 0)
        if new_chat_id:
            logger.warning("Chat migrated: %s -> %s (rule_id=%s)", chat_id, new_chat_id, rule_id)
            # Capture current rules for old chat so we can remove old jobs reliably.
            try:
                old_rule_ids = [int(r["id"]) for r in repo.get_rules(chat_id)]
            except Exception:
                old_rule_ids = [rule_id]
            try:
                repo.migrate_chat_id(old_chat_id=chat_id, new_chat_id=new_chat_id)
            except Exception:
                logger.exception("Failed to migrate chat_id in DB: %s -> %s", chat_id, new_chat_id)
                return

            # Remove all old jobs that still target the old chat_id.
            for rid in old_rule_ids:
                _remove_rule_job(context.application, rule_id=rid)

            # Reschedule the rule under the new chat id and try soon.
            try:
                await reschedule_chat_jobs(context.application, new_chat_id, logger=logger)
            except Exception:
                logger.exception("Failed to reschedule migrated chat_id=%s", new_chat_id)
                return

            migrated_rule = repo.get_rule(new_chat_id, rule_id)
            if migrated_rule and migrated_rule.get("enabled"):
                # Ensure a quick retry for the rule that triggered migration.
                async with _chat_lock(context.application, new_chat_id):
                    if migrated_rule.get("kind") == "interval":
                        # Replace the normal next-run with a quick retry (generation-gated).
                        gen = _bump_rule_generation(context.application, chat_id=new_chat_id, rule_id=rule_id, job_kind=JOB_KIND_RULE)
                        _schedule_interval_run(context.application, chat_id=new_chat_id, rule=migrated_rule, retry_attempt=1, gen=gen)
                    else:
                        # Retry job is separate from the main daily schedule (doesn't invalidate it).
                        _schedule_weekly_retry(context.application, chat_id=new_chat_id, rule_id=rule_id, retry_attempt=1)
            return
        logger.exception("ChatMigrated without new_chat_id chat_id=%s rule_id=%s", chat_id, rule_id)
        return
    except Exception:
        logger.exception("Failed to deliver notification chat_id=%s rule_id=%s", chat_id, rule_id)

    # Failure handling: schedule short retries.
    if rule.get("kind") == "interval":
        next_attempt = retry_attempt + 1
        async with _chat_lock(context.application, chat_id):
            gen = _bump_rule_generation(context.application, chat_id=chat_id, rule_id=rule_id, job_kind=JOB_KIND_RULE)
            if next_attempt <= MAX_SEND_RETRY_ATTEMPTS:
                _schedule_interval_run(context.application, chat_id=chat_id, rule=rule, retry_attempt=next_attempt, gen=gen)
                logger.warning("Scheduled interval retry chat_id=%s rule_id=%s attempt=%s", chat_id, rule_id, next_attempt)
            else:
                # Give up on short retries; schedule next regular interval run (from anchor).
                _schedule_interval_run(context.application, chat_id=chat_id, rule=rule, retry_attempt=0, gen=gen)
                logger.warning("Interval retries exceeded, back to regular schedule chat_id=%s rule_id=%s", chat_id, rule_id)
        return

    # Weekly: schedule a separate retry job.
    next_attempt = retry_attempt + 1
    async with _chat_lock(context.application, chat_id):
        if next_attempt <= MAX_SEND_RETRY_ATTEMPTS:
            _schedule_weekly_retry(context.application, chat_id=chat_id, rule_id=rule_id, retry_attempt=next_attempt)
            logger.warning("Scheduled weekly retry chat_id=%s rule_id=%s attempt=%s", chat_id, rule_id, next_attempt)
        else:
            logger.warning("Weekly retries exceeded chat_id=%s rule_id=%s", chat_id, rule_id)


async def send_rule_notification(
    *,
    bot,
    chat_id: int,
    settings: dict,
    rule: dict,
    is_test: bool,
    send_options: SendOptions,
    logger: logging.Logger,
) -> None:
    sender = TelegramSender(bot=bot, options=send_options, logger=logger)

    tz = ZoneInfo(settings["timezone"])
    now = datetime.now(tz)
    include_meta = bool(settings.get("include_meta", True))

    header = ""
    if include_meta:
        weekday = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"][now.weekday()]
        date_s = now.strftime("%d.%m.%Y")
        time_s = now.strftime("%H:%M")
        name = fmt_rule_name(rule)
        schedule = fmt_rule_schedule(rule)
        lines = [
            "<b>Министерство не твоих собачьих дел</b>",
            f"📅 <b>{date_s}</b> ({weekday})",
            f"⏰ <b>{time_s}</b> ({escape(settings['timezone'])})",
            f"🏷 <b>{escape(name)}</b>",
            f"📌 {escape(schedule)}",
        ]
        if is_test:
            lines.insert(0, "🧪 <b>Тестовое уведомление</b>")
        header = "\n".join(lines)

    # Content selection
    text = (rule.get("message_text") or "").strip()
    image_ref = rule.get("image_file_id")
    image_ref_type = "file_id" if image_ref else None

    if rule.get("is_system"):
        text_options = repo.get_rule_text_options(int(rule["id"]))
        image_options = repo.get_rule_image_options(int(rule["id"]))
        picked = pick_system_content(
            rule=rule,
            text_options=text_options,
            image_options=image_options,
        )
        text = (picked.text or "").strip()
        image_ref = picked.image_ref
        image_ref_type = picked.image_ref_type

        if not include_meta and not text and not image_ref and text_options:
            # ensure at least some output when meta is off
            text = str(text_options[0].get("text") or "").strip()

    body_html = escape(text) if text else ""

    if image_ref:
        caption = (header + (("\n\n" + body_html) if body_html else "")) if include_meta and header else body_html
        if 0 < len(caption) <= 1024:
            await sender.send_photo(chat_id=chat_id, ref=str(image_ref), ref_type=str(image_ref_type or "file_id"), caption=caption, parse_mode=ParseMode.HTML)
        else:
            if include_meta and header and len(header) <= 1024:
                await sender.send_photo(chat_id=chat_id, ref=str(image_ref), ref_type=str(image_ref_type or "file_id"), caption=header, parse_mode=ParseMode.HTML)
            else:
                await sender.send_photo(chat_id=chat_id, ref=str(image_ref), ref_type=str(image_ref_type or "file_id"))
            if body_html:
                await sender.send_message(chat_id=chat_id, text=body_html, parse_mode=ParseMode.HTML)
    else:
        text_out = (header + (("\n\n" + body_html) if body_html else "")) if include_meta and header else (body_html if body_html else header)
        await sender.send_message(chat_id=chat_id, text=text_out, parse_mode=ParseMode.HTML)


