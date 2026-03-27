from pathlib import Path

import core.services.casafari_session_service as casafari_session_service


def test_get_casafari_session_status_reports_missing_session(monkeypatch, tmp_path: Path) -> None:
    storage_path = tmp_path / "casafari_storage_state.json"
    verified_path = tmp_path / "casafari_verified_history_url.txt"

    monkeypatch.setattr(casafari_session_service, "CASAFARI_STORAGE_STATE_PATH", storage_path)
    monkeypatch.setattr(
        casafari_session_service,
        "CASAFARI_VERIFIED_HISTORY_URL_PATH",
        verified_path,
    )

    status = casafari_session_service.get_casafari_session_status()

    assert status["session_exists"] is False
    assert status["session_ready"] is False
    assert status["verified_history_url"] is None


def test_get_casafari_session_status_reports_ready_session(monkeypatch, tmp_path: Path) -> None:
    storage_path = tmp_path / "casafari_storage_state.json"
    verified_path = tmp_path / "casafari_verified_history_url.txt"
    profile_dir = tmp_path / "casafari_profile"
    profile_dir.mkdir()
    (profile_dir / "Default").mkdir()
    storage_path.write_text("{}", encoding="utf-8")
    verified_path.write_text(
        "https://es.casafari.com/account/history?id=abc&historyType=new&from=2026-01-01&to=2026-01-02",
        encoding="utf-8",
    )

    monkeypatch.setattr(casafari_session_service, "CASAFARI_STORAGE_STATE_PATH", storage_path)
    monkeypatch.setattr(
        casafari_session_service,
        "CASAFARI_VERIFIED_HISTORY_URL_PATH",
        verified_path,
    )
    monkeypatch.setattr(casafari_session_service, "CASAFARI_PROFILE_DIR", profile_dir)

    status = casafari_session_service.get_casafari_session_status()

    assert status["session_exists"] is True
    assert status["profile_ready"] is True
    assert status["session_ready"] is True
    assert "historyType" in status["verified_history_url"]


def test_is_history_ready_url_accepts_history_urls() -> None:
    assert (
        casafari_session_service.is_history_ready_url(
            "https://es.casafari.com/account/history?id=abc&historyType=new&from=1&to=2"
        )
        is True
    )
    assert casafari_session_service.is_history_ready_url("https://es.casafari.com/account") is False


def test_looks_like_history_body_detects_real_history_copy() -> None:
    body = (
        "Nuevo 595 Bajada de precio 280 Reservado 0 Vendido 0 "
        "595 anuncios Nuevo piso en venta en calle de Juan Pradillo "
        "Idealista Laura"
    )
    assert casafari_session_service.looks_like_history_body(body) is True
    assert casafari_session_service.looks_like_history_body("login") is False
