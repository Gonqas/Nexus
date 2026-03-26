from core.services.casafari_session_service import prepare_casafari_session


def main() -> None:
    def emit(message: str, current: int, total: int) -> None:
        if total and total > 0:
            print(f"[{current}/{total}] {message}")
        else:
            print(message)

    result = prepare_casafari_session(progress_callback=emit, max_wait_seconds=1800)
    print(f"SESION GUARDADA EN: {result['session_file']}")
    print(f"URL VERIFICADA GUARDADA EN: {result['verified_history_url']}")
    print(f"SCREENSHOT GUARDADO EN: {result['screenshot_path']}")


if __name__ == "__main__":
    main()
