from PySide6.QtCore import QThread, Signal

from core.services.casafari_links_service import rerun_pending_casafari_reconciliation
from db.session import SessionLocal


class CasafariReconcileWorker(QThread):
    finished_ok = Signal(dict)
    failed = Signal(str)

    def __init__(self, limit: int = 5000) -> None:
        super().__init__()
        self.limit = limit

    def run(self) -> None:
        try:
            with SessionLocal() as session:
                result = rerun_pending_casafari_reconciliation(
                    session=session,
                    limit=self.limit,
                )
            self.finished_ok.emit(result)
        except Exception as exc:
            self.failed.emit(str(exc))