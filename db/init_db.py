from db.base import Base
from db.session import engine
import db.models  # noqa: F401


def init_database() -> None:
    Base.metadata.create_all(bind=engine)