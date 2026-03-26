from PySide6.QtCore import QThread, Signal

from core.services.casafari_sync_service import sync_casafari_history
from db.session import SessionLocal


class CasafariSyncWorker(QThread):
    progress_changed = Signal(str, int, int)
    finished_ok = Signal(dict)
    failed = Signal(str)

    def run(self) -> None:
        try:
            with SessionLocal() as session:
                result = sync_casafari_history(
                    session,
                    progress_callback=self._emit_progress,
                )
            self.finished_ok.emit(result)
        except Exception as exc:
            self.failed.emit(str(exc))

    def _emit_progress(self, message: str, current: int, total: int) -> None:
        self.progress_changed.emit(message, current, total)