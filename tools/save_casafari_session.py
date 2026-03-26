from core.config.settings import (
    CASAFARI_DEBUG_BASE_DIR,
    CASAFARI_HISTORY_BASE_URL,
    CASAFARI_STORAGE_STATE_PATH,
    CASAFARI_VERIFIED_HISTORY_URL_PATH,
)
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


def load_start_url() -> str:
    if CASAFARI_VERIFIED_HISTORY_URL_PATH.exists():
        verified = CASAFARI_VERIFIED_HISTORY_URL_PATH.read_text(encoding="utf-8").strip()
        if verified:
            return verified
    return CASAFARI_HISTORY_BASE_URL


def main() -> None:
    start_url = load_start_url()
    screenshot_path = CASAFARI_DEBUG_BASE_DIR / "last_verified_page.png"
    screenshot_path.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(locale="es-ES")
        page = context.new_page()

        try:
            page.goto(start_url, wait_until="domcontentloaded", timeout=45000)
        except PlaywrightTimeoutError:
            page.goto(start_url, wait_until="commit", timeout=45000)

        page.wait_for_timeout(3000)

        input(
            "\nHaz login en Casafari, navega manualmente a la pantalla exacta "
            "de historial/listado que quieres usar como fuente principal "
            "y, cuando la estés viendo correctamente, pulsa ENTER aquí..."
        )

        context.storage_state(path=str(CASAFARI_STORAGE_STATE_PATH))
        CASAFARI_VERIFIED_HISTORY_URL_PATH.write_text(page.url, encoding="utf-8")
        page.screenshot(path=str(screenshot_path), full_page=True)

        browser.close()

    print(f"SESIÓN GUARDADA EN: {CASAFARI_STORAGE_STATE_PATH}")
    print(f"URL VERIFICADA GUARDADA EN: {CASAFARI_VERIFIED_HISTORY_URL_PATH}")
    print(f"SCREENSHOT GUARDADO EN: {screenshot_path}")


if __name__ == "__main__":
    main()