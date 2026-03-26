from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from db.base import Base
from db.session import engine
from db.models.match_review import MatchReview


def main() -> None:
    Base.metadata.create_all(bind=engine, tables=[MatchReview.__table__])
    print("Tabla match_review creada o ya existente")


if __name__ == "__main__":
    main()