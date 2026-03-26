from PySide6.QtCore import QThread, Signal

from core.services.csv_import_service import import_csv_files
from db.session import SessionLocal


class CsvImportWorker(QThread):
    progress_changed = Signal(str, int, int)
    finished_ok = Signal(dict)
    failed = Signal(str)

    def __init__(self, file_paths: list[str], delete_after_success: bool) -> None:
        super().__init__()
        self.file_paths = file_paths
        self.delete_after_success = delete_after_success

    def run(self) -> None:
        try:
            with SessionLocal() as session:
                result = import_csv_files(
                    session=session,
                    file_paths=self.file_paths,
                    delete_after_success=self.delete_after_success,
                    progress_callback=self._emit_progress,
                )
            self.finished_ok.emit(result)
        except Exception as exc:
            self.failed.emit(str(exc))

    def _emit_progress(self, message: str, current: int, total: int) -> None:
        self.progress_changed.emit(message, current, total)