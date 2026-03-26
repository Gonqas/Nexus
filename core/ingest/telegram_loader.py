from collections.abc import Iterator
from pathlib import Path
from zipfile import ZipFile


def iter_telegram_html_documents(export_path: str | Path) -> Iterator[tuple[str, str]]:
    path = Path(export_path)

    if not path.exists():
        raise FileNotFoundError(f"No existe el export de Telegram: {path}")

    if path.is_file() and path.suffix.lower() == ".zip":
        with ZipFile(path, "r") as zip_file:
            for name in zip_file.namelist():
                if name.lower().endswith(".html"):
                    html = zip_file.read(name).decode("utf-8", errors="ignore")
                    yield name, html
        return

    if path.is_dir():
        for html_file in sorted(path.rglob("*.html")):
            yield str(html_file.relative_to(path)), html_file.read_text(encoding="utf-8", errors="ignore")
        return

    raise ValueError(f"Formato no soportado para Telegram: {path}")