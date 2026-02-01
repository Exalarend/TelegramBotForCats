import asyncio
import logging
import time

from telegram import ForceReply, Update
from telegram.constants import ChatType
from telegram.error import BadRequest, NetworkError, RetryAfter, TimedOut
from telegram.ext import ContextTypes


def is_group(chat_type: str | None) -> bool:
    return chat_type in {ChatType.GROUP, ChatType.SUPERGROUP}


ADMIN_CHECK_CACHE_TTL_S = 120
ADMIN_CHECK_CACHE_MAX_SIZE = 1000


def _cleanup_admin_check_cache(cache: dict, *, now_ts: int) -> None:
    # Remove expired entries first.
    expired_keys: list[object] = []
    for k, v in list(cache.items()):
        try:
            _allowed, expires_at = v
        except Exception:
            expired_keys.append(k)
            continue
        if int(expires_at) <= int(now_ts):
            expired_keys.append(k)
    for k in expired_keys:
        cache.pop(k, None)

    # If still too big, drop oldest entries (dict preserves insertion order).
    while len(cache) > ADMIN_CHECK_CACHE_MAX_SIZE:
        try:
            cache.pop(next(iter(cache)))
        except Exception:
            break


async def require_admin_in_groups(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    allowed, ok = await check_admin_in_groups(update, context)
    return allowed if ok else False


async def check_admin_in_groups(update: Update, context: ContextTypes.DEFAULT_TYPE) -> tuple[bool, bool]:
    """
    Returns (allowed, ok).
    - ok=True means the check was performed reliably (allowed can be True/False).
    - ok=False means the check couldn't be performed (network/Telegram API issue).
    """
    if update.effective_chat is None or update.effective_user is None:
        return False, True
    if not is_group(update.effective_chat.type):
        return True, True

    logger = logging.getLogger("ministry-bot")
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    # Short TTL cache to avoid calling Telegram API on every click.
    cache: dict = context.application.bot_data.setdefault("admin_check_cache", {})
    cache_key = (int(chat_id), int(user_id))
    now_ts = int(time.time())
    if len(cache) > ADMIN_CHECK_CACHE_MAX_SIZE:
        _cleanup_admin_check_cache(cache, now_ts=now_ts)
    cached = cache.get(cache_key)
    if cached:
        allowed, expires_at = cached
        if int(expires_at) > now_ts:
            return bool(allowed), True
        cache.pop(cache_key, None)

    # Best-effort retries for transient network issues.
    attempt = 0
    max_attempts = 3
    delay_s = 0.4
    while True:
        attempt += 1
        try:
            member = await context.bot.get_chat_member(chat_id, user_id)
            allowed = member.status in {"administrator", "creator"}
            cache[cache_key] = (bool(allowed), now_ts + ADMIN_CHECK_CACHE_TTL_S)
            if len(cache) > ADMIN_CHECK_CACHE_MAX_SIZE:
                _cleanup_admin_check_cache(cache, now_ts=now_ts)
            return bool(allowed), True
        except RetryAfter as e:
            retry_after = float(getattr(e, "retry_after", 1.0) or 1.0)
            sleep_s = min(3.0, max(0.2, retry_after))
            logger.warning("Admin check rate-limited (RetryAfter=%ss)", retry_after)
            if attempt >= max_attempts:
                return False, False
            await asyncio.sleep(sleep_s)
        except (NetworkError, TimedOut) as e:
            logger.warning("Admin check network error (%s), attempt %s/%s", e.__class__.__name__, attempt, max_attempts)
            if attempt >= max_attempts:
                return False, False
            await asyncio.sleep(delay_s)
            delay_s = min(delay_s * 2.0, 2.0)
        except Exception as e:
            logger.warning("Admin check failed (%s), attempt %s/%s", e.__class__.__name__, attempt, max_attempts)
            if attempt >= max_attempts:
                return False, False
            await asyncio.sleep(delay_s)
            delay_s = min(delay_s * 2.0, 2.0)


async def prompt_user_input(*, update: Update, context: ContextTypes.DEFAULT_TYPE, prompt: str) -> int | None:
    """
    In groups, bots may not receive arbitrary text due to privacy mode.
    Asking the user to reply to a bot message (ForceReply) improves reliability.
    """
    if update.effective_chat is None:
        return None
    # Always ask with ForceReply so the user can reply directly to the bot prompt.
    # In groups, ForceReply(selective=True) is unreliable without an explicit @username mention,
    # so if the user has no username we fall back to non-selective ForceReply and validate by actor_user_id + reply_to_message.
    try:
        prompt_text = prompt
        reply_markup = ForceReply(selective=True)

        if is_group(update.effective_chat.type) and update.effective_user is not None:
            username = getattr(update.effective_user, "username", None)
            if username:
                prompt_text = f"@{username}, {prompt}"
            else:
                reply_markup = ForceReply(selective=False)

        if update.effective_message:
            msg = await tg_call_with_retries(
                lambda: update.effective_message.reply_text(prompt_text, reply_markup=reply_markup),
                what="prompt_user_input.reply_text",
                logger=logging.getLogger("ministry-bot"),
            )
        else:
            msg = await tg_call_with_retries(
                lambda: update.effective_chat.send_message(prompt_text, reply_markup=reply_markup),
                what="prompt_user_input.send_message",
                logger=logging.getLogger("ministry-bot"),
            )
    except Exception:
        logging.getLogger("ministry-bot").warning("Failed to send ForceReply prompt (network issue).")
        return None
    return int(msg.message_id)


async def tg_call_with_retries(
    coro_factory,
    *,
    what: str,
    logger: logging.Logger | None = None,
    max_attempts: int = 3,
    base_delay_s: float = 0.4,
):
    """
    Small retry helper for Telegram API calls in interactive flows (menus, prompts).
    Retries transient network errors and RetryAfter with a short backoff.
    """
    log = logger or logging.getLogger("ministry-bot")
    delay_s = base_delay_s
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await coro_factory()
        except BadRequest:
            # 400 errors are not transient; callers often want to handle them specially
            # (e.g. "Message is not modified"). Do not retry here.
            raise
        except RetryAfter as e:
            last_exc = e
            retry_after = float(getattr(e, "retry_after", 1.0) or 1.0)
            sleep_s = min(3.0, max(0.2, retry_after))
            log.warning("%s rate-limited (RetryAfter=%ss) attempt %s/%s", what, retry_after, attempt, max_attempts)
            if attempt == max_attempts:
                raise
            await asyncio.sleep(sleep_s)
        except (NetworkError, TimedOut) as e:
            last_exc = e
            log.warning("%s network error (%s) attempt %s/%s", what, e.__class__.__name__, attempt, max_attempts)
            if attempt == max_attempts:
                raise
            await asyncio.sleep(delay_s)
            delay_s = min(delay_s * 2.0, 2.0)
        except Exception as e:
            # Do not retry arbitrary errors too aggressively, but a short retry helps for rare transient cases.
            last_exc = e
            log.warning("%s failed (%s) attempt %s/%s", what, e.__class__.__name__, attempt, max_attempts)
            if attempt == max_attempts:
                raise
            await asyncio.sleep(delay_s)
            delay_s = min(delay_s * 2.0, 2.0)

    # Should be unreachable due to raises above.
    if last_exc:
        raise last_exc

