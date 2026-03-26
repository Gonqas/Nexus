from pathlib import Path

import core.services.import_inbox_service as import_inbox_service


def test_list_pending_baseline_files_only_returns_supported_files(tmp_path, monkeypatch) -> None:
    inbox_dir = tmp_path / "baseline_uploads"
    inbox_dir.mkdir(parents=True, exist_ok=True)

    file_csv = inbox_dir / "a.csv"
    file_xlsx = inbox_dir / "b.xlsx"
    file_txt = inbox_dir / "c.txt"
    file_csv.write_text("a", encoding="utf-8")
    file_xlsx.write_text("b", encoding="utf-8")
    file_txt.write_text("c", encoding="utf-8")

    monkeypatch.setattr(import_inbox_service, "BASELINE_INBOX_DIR", inbox_dir)

    files = import_inbox_service.list_pending_baseline_files()

    assert str(file_csv) in files
    assert str(file_xlsx) in files
    assert str(file_txt) not in files


def test_ensure_baseline_inbox_dir_creates_folder(tmp_path, monkeypatch) -> None:
    inbox_dir = tmp_path / "new_inbox"
    monkeypatch.setattr(import_inbox_service, "BASELINE_INBOX_DIR", inbox_dir)

    created = import_inbox_service.ensure_baseline_inbox_dir()

    assert isinstance(created, Path)
    assert created.exists()
