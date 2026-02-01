"""Системный трей: иконка в области уведомлений Windows, показ/скрытие консоли, меню ПКМ."""

import logging
import sys
import threading
from pathlib import Path
from typing import Callable

import pystray
from PIL import Image, ImageDraw

logger = logging.getLogger("ministry-bot.tray")

# Размер иконки для трея (Windows обычно 16x16, масштабирует до 32)
TRAY_ICON_SIZE = 64

# Путь к иконке относительно корня проекта
_DEFAULT_ICON_PATH = (
    Path(__file__).resolve().parent.parent / "assets" / "system" / "Garfield_2.png"
)

# Windows API для работы с консольным окном (только Windows)
if sys.platform == "win32":
    import ctypes

    _kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
    _user32 = ctypes.windll.user32  # type: ignore[attr-defined]
    SW_HIDE = 0
    SW_SHOW = 5
    SW_RESTORE = 9
else:
    _kernel32 = _user32 = None
    SW_HIDE = SW_SHOW = SW_RESTORE = 0


def _get_console_hwnd() -> int:
    """Возвращает HWND консольного окна процесса (0 если нет консоли, например pythonw)."""
    if _kernel32 is None:
        return 0
    hwnd = _kernel32.GetConsoleWindow()
    return hwnd if hwnd else 0


def _show_console() -> None:
    """Показывает и выводит консоль на передний план."""
    hwnd = _get_console_hwnd()
    if hwnd == 0:
        return
    try:
        _user32.ShowWindow(hwnd, SW_RESTORE)
        _user32.SetForegroundWindow(hwnd)
    except Exception:
        logger.debug("ShowWindow/SetForegroundWindow failed", exc_info=True)


def _hide_console() -> None:
    """Скрывает консольное окно (в трей)."""
    hwnd = _get_console_hwnd()
    if hwnd == 0:
        return
    try:
        _user32.ShowWindow(hwnd, SW_HIDE)
    except Exception:
        logger.debug("ShowWindow SW_HIDE failed", exc_info=True)


def _is_console_visible() -> bool:
    """True, если консольное окно видимо (не скрыто в трей)."""
    if _user32 is None:
        return False
    hwnd = _get_console_hwnd()
    if hwnd == 0:
        return False
    try:
        return bool(_user32.IsWindowVisible(hwnd))
    except Exception:
        return False


def _is_console_minimized() -> bool:
    """True, если консольное окно свёрнуто (иконка на панели задач)."""
    if _user32 is None:
        return False
    hwnd = _get_console_hwnd()
    if hwnd == 0:
        return False
    try:
        return bool(_user32.IsIconic(hwnd))
    except Exception:
        return False


def _run_minimize_to_tray_poller(stop_event: threading.Event, poll_interval: float = 0.5) -> None:
    """
    В фоне проверяет: если консоль свёрнута — скрывает её в трей.
    Останавливается при stop_event.set().
    """
    if _user32 is None:
        return
    while not stop_event.wait(timeout=poll_interval):
        if _get_console_hwnd() == 0:
            continue
        if _is_console_minimized():
            _hide_console()


def _load_tray_icon(path: Path | None = None) -> Image.Image:
    """Загружает и уменьшает изображение для иконки трея."""
    p = path or _DEFAULT_ICON_PATH
    if not p.exists():
        # Fallback: простая иконка-заглушка (оранжевый круг — котик)
        img = Image.new("RGBA", (TRAY_ICON_SIZE, TRAY_ICON_SIZE), (0, 0, 0, 0))
        dc = ImageDraw.Draw(img)
        margin = 4
        dc.ellipse(
            [margin, margin, TRAY_ICON_SIZE - margin, TRAY_ICON_SIZE - margin],
            fill=(255, 165, 0),
            outline=(200, 100, 0),
        )
        return img
    img = Image.open(p).convert("RGBA")
    img = img.resize((TRAY_ICON_SIZE, TRAY_ICON_SIZE), Image.Resampling.LANCZOS)
    return img


def run_tray_in_thread(
    stop_event: object,
    *,
    on_quit: Callable[[], None] | None = None,
    icon_path: Path | None = None,
    minimize_to_tray: bool = True,
) -> tuple[pystray.Icon, threading.Thread]:
    """
    Запускает иконку бота в системном трее в фоновом потоке.

    - ЛКМ по иконке — переключение: показать консоль или свернуть в трей.
    - ПКМ — меню: «Развернуть / Свернуть в трей», «Выход».
    - На Windows: при сворачивании консоли окно скрывается в трей (если minimize_to_tray=True).

    :param stop_event: asyncio.Event или threading.Event; при «Выход» вызывается .set().
    :param on_quit: опциональный callback без аргументов (вызывается после set() на stop_event).
    :param icon_path: путь к картинке для иконки (по умолчанию assets/system/Garfield_2.png).
    :param minimize_to_tray: на Windows — при сворачивании консоли скрывать её в трей.
    :return: (icon, thread) — иконка и поток, в котором крутится tray.
    """
    icon_image = _load_tray_icon(icon_path)

    def _quit_clicked() -> None:
        try:
            if hasattr(stop_event, "set"):
                stop_event.set()
            if on_quit:
                on_quit()
        except Exception:
            logger.exception("Tray on_quit error")
        try:
            icon.stop()
        except Exception:
            logger.exception("Tray icon.stop error")

    def _toggle_console_clicked() -> None:
        """По клику: если консоль видна — свернуть в трей, иначе — развернуть."""
        try:
            if _is_console_visible():
                _hide_console()
            else:
                _show_console()
        except Exception:
            logger.exception("Tray toggle console error")

    # ЛКМ (default=True) и «Развернуть» в меню — переключают показ/скрытие консоли
    menu = pystray.Menu(
        pystray.MenuItem("Развернуть / Свернуть в трей", _toggle_console_clicked, default=True),
        pystray.MenuItem("Выход", _quit_clicked),
    )
    icon = pystray.Icon(
        "telegram_bot_cats",
        icon_image,
        "Бот для котиков",
        menu=menu,
    )

    poller_stop = threading.Event()
    poller_thread: threading.Thread | None = None
    if minimize_to_tray and sys.platform == "win32" and _get_console_hwnd() != 0:
        poller_thread = threading.Thread(
            target=_run_minimize_to_tray_poller,
            args=(poller_stop,),
            daemon=True,
            name="tray-minimize-poller",
        )
        poller_thread.start()

    def _run() -> None:
        try:
            icon.run()
        except Exception:
            logger.exception("Tray icon.run error")
        finally:
            poller_stop.set()

    thread = threading.Thread(target=_run, daemon=True, name="tray")
    thread.start()
    logger.info("Tray icon started (LKM: toggle console, PKM: menu).")
    return icon, thread
