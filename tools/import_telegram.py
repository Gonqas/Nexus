from pathlib import Path

from core.parsers.telegram_parser import parse_telegram_export
from core.services.telegram_import_service import import_telegram_alerts
from db.init_db import init_database
from db.session import SessionLocal


def main() -> None:
    init_database()

    zip_path = Path("data/raw/telegram_export.zip")
    folder_path = Path("data/raw/telegram_export")

    if zip_path.exists():
        source = zip_path
    elif folder_path.exists():
        source = folder_path
    else:
        raise FileNotFoundError(
            "No encuentro Telegram. Usa data/raw/telegram_export.zip o data/raw/telegram_export/"
        )

    parsed_alerts = parse_telegram_export(str(source))

    with SessionLocal() as session:
        stats = import_telegram_alerts(session, parsed_alerts)

    print("TELEGRAM IMPORT OK")
    for key, value in stats.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()