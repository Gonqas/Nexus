from pathlib import Path

from core.ingest.csv_loader import load_leads_csv
from core.services.import_service import import_leads_rows
from db.session import SessionLocal


def main() -> None:
    csv_path = Path("data/raw/leads.csv")

    rows = load_leads_csv(csv_path)

    with SessionLocal() as session:
        stats = import_leads_rows(session, rows)

    print("IMPORT OK")
    for key, value in stats.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()