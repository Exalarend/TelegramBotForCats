import asyncio
import os
from dataclasses import dataclass

from telegram.error import NetworkError, RetryAfter, TimedOut


@dataclass(frozen=True)
class SendOptions:
    timeout_seconds: float
    retry_attempts: int


class TelegramSender:
    def __init__(self, *, bot, options: SendOptions, logger):
        self._bot = bot
        self._options = options
        self._logger = logger

    async def send_message(self, *, chat_id: int, text: str, parse_mode=None) -> None:
        await self._call_with_retries(
            lambda: self._bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode,
                connect_timeout=self._options.timeout_seconds,
                read_timeout=self._options.timeout_seconds,
                write_timeout=self._options.timeout_seconds,
            ),
            what=f"send_message(chat_id={chat_id})",
        )

    async def send_photo(
        self,
        *,
        chat_id: int,
        ref: str,
        ref_type: str,
        caption: str | None = None,
        parse_mode=None,
    ) -> None:
        if ref_type in {"file_id", "url"}:
            await self._call_with_retries(
                lambda: self._bot.send_photo(
                    chat_id=chat_id,
                    photo=ref,
                    caption=caption,
                    parse_mode=parse_mode,
                    connect_timeout=self._options.timeout_seconds,
                    read_timeout=self._options.timeout_seconds,
                    write_timeout=self._options.timeout_seconds,
                ),
                what=f"send_photo(chat_id={chat_id})",
            )
            return

        if ref_type == "path":
            abs_path = self._abs_ref_path(ref)

            async def _do():
                with open(abs_path, "rb") as f:
                    return await self._bot.send_photo(
                        chat_id=chat_id,
                        photo=f,
                        caption=caption,
                        parse_mode=parse_mode,
                        connect_timeout=self._options.timeout_seconds,
                        read_timeout=self._options.timeout_seconds,
                        write_timeout=self._options.timeout_seconds,
                    )

            await self._call_with_retries(lambda: _do(), what=f"send_photo(chat_id={chat_id})")
            return

        # fallback: treat as file_id
        await self.send_photo(chat_id=chat_id, ref=ref, ref_type="file_id", caption=caption, parse_mode=parse_mode)

    def _abs_ref_path(self, ref: str) -> str:
        # repo root is two levels up from this file: bot/notify/sender.py
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        return os.path.abspath(os.path.join(repo_root, ref))

    async def _call_with_retries(self, coro_factory, *, what: str) -> None:
        delay = 1.0
        last_exc: Exception | None = None

        for attempt in range(1, self._options.retry_attempts + 1):
            try:
                await coro_factory()
                return
            except RetryAfter as e:
                last_exc = e
                wait_s = float(getattr(e, "retry_after", 1)) + 1.0
                self._logger.warning("%s: RetryAfter=%ss (attempt %s/%s)", what, wait_s, attempt, self._options.retry_attempts)
                await asyncio.sleep(wait_s)
            except (TimedOut, NetworkError) as e:
                last_exc = e
                if attempt >= self._options.retry_attempts:
                    break
                self._logger.warning(
                    "%s: %s (attempt %s/%s), retrying in %.1fs",
                    what,
                    e.__class__.__name__,
                    attempt,
                    self._options.retry_attempts,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2.0, 15.0)

        if last_exc:
            raise last_exc

