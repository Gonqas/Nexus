from __future__ import annotations

from datetime import datetime
from pathlib import Path
from time import monotonic
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from core.config.settings import (
    CASAFARI_DEBUG_BASE_DIR,
    CASAFARI_HISTORY_BASE_URL,
    CASAFARI_PROFILE_DIR,
    CASAFARI_STORAGE_STATE_PATH,
    CASAFARI_VERIFIED_HISTORY_URL_PATH,
)


def load_start_url() -> str:
    if CASAFARI_VERIFIED_HISTORY_URL_PATH.exists():
        verified = CASAFARI_VERIFIED_HISTORY_URL_PATH.read_text(encoding="utf-8").strip()
        if verified:
            return verified
    return CASAFARI_HISTORY_BASE_URL


def is_login_url(url: str | None) -> bool:
    text = (url or "").strip().lower()
    return bool(text) and ("login" in text or "sign-in" in text or "signin" in text)


def is_history_ready_url(url: str | None) -> bool:
    text = (url or "").strip()
    if not text:
        return False

    parsed = urlparse(text)
    path = (parsed.path or "").lower()
    query = parse_qs(parsed.query)
    query_keys = {str(key).lower() for key in query}

    if "history" in path:
        return True

    return "historytype" in query_keys and "from" in query_keys and "to" in query_keys


def looks_like_history_body(body_text: str | None) -> bool:
    text = (body_text or "").strip().lower()
    if len(text) < 120:
        return False

    required_signals = ("nuevo", "bajada de precio", "reservado", "vendido")
    listing_signals = ("anuncios", "en venta", "idealista", "fotocasa", "pisos", "milanuncios")
    return any(token in text for token in required_signals) and any(
        token in text for token in listing_signals
    )


def _file_mtime_iso(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")


def get_casafari_session_status() -> dict:
    session_exists = CASAFARI_STORAGE_STATE_PATH.exists()
    profile_ready = CASAFARI_PROFILE_DIR.exists() and any(CASAFARI_PROFILE_DIR.iterdir())
    verified_url = None
    if CASAFARI_VERIFIED_HISTORY_URL_PATH.exists():
        value = CASAFARI_VERIFIED_HISTORY_URL_PATH.read_text(encoding="utf-8").strip()
        verified_url = value or None

    return {
        "session_exists": session_exists,
        "profile_ready": profile_ready,
        "session_ready": (session_exists or profile_ready) and bool(verified_url),
        "session_file": str(CASAFARI_STORAGE_STATE_PATH),
        "profile_dir": str(CASAFARI_PROFILE_DIR),
        "session_saved_at": _file_mtime_iso(CASAFARI_STORAGE_STATE_PATH),
        "verified_history_url": verified_url,
        "verified_history_saved_at": _file_mtime_iso(CASAFARI_VERIFIED_HISTORY_URL_PATH),
    }


def prepare_casafari_session(
    progress_callback=None,
    *,
    max_wait_seconds: int = 900,
    stable_checks_required: int = 5,
) -> dict:
    start_url = load_start_url()
    screenshot_path = CASAFARI_DEBUG_BASE_DIR / "last_verified_page.png"
    screenshot_path.parent.mkdir(parents=True, exist_ok=True)

    def emit(message: str, current: int = 0, total: int = 0) -> None:
        if progress_callback:
            progress_callback(message, current, total)

    deadline = monotonic() + max_wait_seconds
    last_url = start_url
    stable_hits = 0

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            str(CASAFARI_PROFILE_DIR),
            headless=False,
            locale="es-ES",
        )
        page = context.pages[0] if context.pages else context.new_page()

        try:
            emit("Abriendo Casafari para preparar sesion...", 0, stable_checks_required)
            try:
                page.goto(start_url, wait_until="domcontentloaded", timeout=45000)
            except PlaywrightTimeoutError:
                page.goto(start_url, wait_until="commit", timeout=45000)

            page.wait_for_timeout(2500)
            emit(
                "Haz login y abre la vista de historial de Casafari. "
                "Cuando esa pantalla quede estable, la sesion se guardara sola.",
                0,
                stable_checks_required,
            )

            while monotonic() < deadline:
                if page.is_closed():
                    raise RuntimeError(
                        "La ventana de Casafari se cerro antes de guardar la sesion."
                    )

                try:
                    current_url = page.url
                except Exception:
                    current_url = last_url

                if current_url:
                    last_url = current_url
                current_url = last_url

                try:
                    body_text = page.locator("body").inner_text(timeout=2500)
                except Exception:
                    body_text = ""

                useful_body = looks_like_history_body(body_text)
                ready_url = is_history_ready_url(current_url)

                if is_login_url(current_url):
                    stable_hits = 0
                    emit(
                        "Esperando login manual en Casafari...",
                        0,
                        stable_checks_required,
                    )
                elif ready_url and useful_body:
                    stable_hits += 1
                    emit(
                        "Pantalla de historial detectada. "
                        f"Mantenla abierta un poco mas ({stable_hits}/{stable_checks_required})...",
                        stable_hits,
                        stable_checks_required,
                    )
                    if stable_hits >= stable_checks_required:
                        context.storage_state(path=str(CASAFARI_STORAGE_STATE_PATH))
                        CASAFARI_VERIFIED_HISTORY_URL_PATH.write_text(
                            current_url,
                            encoding="utf-8",
                        )
                        page.screenshot(path=str(screenshot_path), full_page=True)
                        emit("Sesion Casafari guardada correctamente.", 1, 1)
                        return {
                            "status": "success",
                            "session_file": str(CASAFARI_STORAGE_STATE_PATH),
                            "verified_history_url": current_url,
                            "screenshot_path": str(screenshot_path),
                        }
                else:
                    stable_hits = 0
                    emit(
                        "Login detectado. Ahora abre el historial de Casafari "
                        "y dejalo estable unos segundos.",
                        0,
                        stable_checks_required,
                    )

                page.wait_for_timeout(1000)

            raise RuntimeError(
                "No se pudo guardar la sesion a tiempo. "
                "Haz login, abre la pantalla de historial y mantenla abierta unos segundos."
            )
        finally:
            try:
                context.close()
            except Exception:
                pass
