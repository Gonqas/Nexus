from PySide6.QtCore import QThread, Signal

from core.services.casafari_session_service import prepare_casafari_session


class CasafariSessionWorker(QThread):
    progress_changed = Signal(str, int, int)
    finished_ok = Signal(dict)
    failed = Signal(str)

    def run(self) -> None:
        try:
            result = prepare_casafari_session(progress_callback=self._emit_progress)
            self.finished_ok.emit(result)
        except Exception as exc:
            self.failed.emit(str(exc))

    def _emit_progress(self, message: str, current: int, total: int) -> None:
        self.progress_changed.emit(message, current, total)
