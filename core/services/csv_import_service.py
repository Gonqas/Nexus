import hashlib
from pathlib import Path

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from core.ingest.csv_loader import load_leads_csv
from core.services.import_service import import_leads_rows
from db.base import utc_now
from db.models.ingestion_run import IngestionRun


def _emit(progress_callback, message: str, current: int, total: int) -> None:
    if progress_callback:
        progress_callback(message, current, total)


def compute_file_hash(file_path: str | Path) -> str:
    path = Path(file_path)
    hasher = hashlib.sha256()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)

    return hasher.hexdigest()


def list_csv_ingestion_runs(session: Session, limit: int = 100) -> list[dict]:
    runs = list(
        session.scalars(
            select(IngestionRun)
            .where(IngestionRun.source_type == "csv")
            .order_by(desc(IngestionRun.started_at), desc(IngestionRun.id))
            .limit(limit)
        ).all()
    )

    result = []
    for run in runs:
        result.append(
            {
                "id": run.id,
                "file_name": run.file_name,
                "file_hash": run.file_hash,
                "status": run.status,
                "message": run.message,
                "rows_read": run.rows_read,
                "listings_created": run.listings_created,
                "snapshots_created": run.snapshots_created,
                "casafari_raw_items_resolved": run.casafari_raw_items_resolved,
                "casafari_market_events_created": run.casafari_market_events_created,
                "file_deleted": run.file_deleted,
                "started_at": run.started_at,
                "finished_at": run.finished_at,
            }
        )
    return result


def _delete_file_safely(path: Path) -> bool:
    try:
        if path.exists():
            path.unlink()
        return True
    except Exception:
        return False


def import_csv_file(
    session: Session,
    file_path: str | Path,
    delete_after_success: bool = True,
    progress_callback=None,
) -> dict:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"No existe el CSV: {path}")

    file_hash = compute_file_hash(path)

    existing_success = session.scalar(
        select(IngestionRun).where(
            IngestionRun.source_type == "csv",
            IngestionRun.file_hash == file_hash,
            IngestionRun.status.in_(["success", "skipped_duplicate"]),
        )
    )

    run = IngestionRun(
        source_type="csv",
        source_name="csv_leads",
        file_name=path.name,
        original_path=str(path),
        file_hash=file_hash,
        status="running",
        started_at=utc_now(),
        file_deleted=False,
    )
    session.add(run)
    session.commit()

    if existing_success is not None:
        run.status = "skipped_duplicate"
        run.finished_at = utc_now()
        run.message = "CSV ya importado anteriormente (mismo hash)"
        if delete_after_success:
            run.file_deleted = _delete_file_safely(path)
        session.commit()

        return {
            "status": "skipped_duplicate",
            "file_name": path.name,
            "file_hash": file_hash,
            "rows_read": 0,
            "listings_created": 0,
            "snapshots_created": 0,
            "casafari_raw_items_resolved": 0,
            "casafari_market_events_created": 0,
            "file_deleted": run.file_deleted,
            "message": run.message,
        }

    try:
        _emit(progress_callback, f"Leyendo CSV: {path.name}", 0, 0)
        rows = load_leads_csv(path)

        _emit(progress_callback, f"Importando filas: {path.name}", 0, len(rows))
        stats = import_leads_rows(session, rows)

        run.status = "success"
        run.finished_at = utc_now()
        run.rows_read = stats.get("rows_read")
        run.contacts_processed = stats.get("contacts_created_or_updated")
        run.assets_processed = stats.get("assets_created_or_matched")
        run.listings_created = stats.get("listings_created")
        run.snapshots_created = stats.get("snapshots_created")
        run.casafari_raw_items_processed = stats.get("casafari_raw_items_processed")
        run.casafari_raw_items_resolved = stats.get("casafari_raw_items_resolved")
        run.casafari_raw_items_ambiguous = stats.get("casafari_raw_items_ambiguous")
        run.casafari_raw_items_unresolved = stats.get("casafari_raw_items_unresolved")
        run.casafari_market_events_created = stats.get("casafari_market_events_created")
        run.message = (
            f"Import OK | filas={stats.get('rows_read', 0)} | "
            f"listings_nuevos={stats.get('listings_created', 0)} | "
            f"snapshots={stats.get('snapshots_created', 0)} | "
            f"casafari_resueltos={stats.get('casafari_raw_items_resolved', 0)}"
        )
        session.commit()

        if delete_after_success:
            run.file_deleted = _delete_file_safely(path)
            if not run.file_deleted:
                run.message = (run.message or "") + " | no se pudo borrar el fichero"
            session.commit()

        return {
            "status": "success",
            "file_name": path.name,
            "file_hash": file_hash,
            "rows_read": run.rows_read or 0,
            "listings_created": run.listings_created or 0,
            "snapshots_created": run.snapshots_created or 0,
            "casafari_raw_items_resolved": run.casafari_raw_items_resolved or 0,
            "casafari_market_events_created": run.casafari_market_events_created or 0,
            "file_deleted": run.file_deleted,
            "message": run.message,
        }

    except Exception as exc:
        session.rollback()

        run = session.get(IngestionRun, run.id)
        if run is not None:
            run.status = "error"
            run.finished_at = utc_now()
            run.error_text = str(exc)[:5000]
            run.message = "Error en importación CSV"
            session.commit()

        raise


def import_csv_files(
    session: Session,
    file_paths: list[str],
    delete_after_success: bool = True,
    progress_callback=None,
) -> dict:
    total = len(file_paths)
    results: list[dict] = []

    aggregate = {
        "files_total": total,
        "files_success": 0,
        "files_skipped_duplicate": 0,
        "files_error": 0,
        "rows_read": 0,
        "listings_created": 0,
        "snapshots_created": 0,
        "casafari_raw_items_resolved": 0,
        "casafari_market_events_created": 0,
    }

    for idx, file_path in enumerate(file_paths, start=1):
        path = Path(file_path)
        _emit(progress_callback, f"Procesando CSV {idx}/{total}: {path.name}", idx - 1, total)

        try:
            result = import_csv_file(
                session=session,
                file_path=path,
                delete_after_success=delete_after_success,
                progress_callback=progress_callback,
            )
            results.append(result)

            if result["status"] == "success":
                aggregate["files_success"] += 1
            elif result["status"] == "skipped_duplicate":
                aggregate["files_skipped_duplicate"] += 1

            aggregate["rows_read"] += result.get("rows_read", 0)
            aggregate["listings_created"] += result.get("listings_created", 0)
            aggregate["snapshots_created"] += result.get("snapshots_created", 0)
            aggregate["casafari_raw_items_resolved"] += result.get("casafari_raw_items_resolved", 0)
            aggregate["casafari_market_events_created"] += result.get("casafari_market_events_created", 0)

        except Exception as exc:
            aggregate["files_error"] += 1
            results.append(
                {
                    "status": "error",
                    "file_name": path.name,
                    "message": str(exc),
                }
            )

    _emit(progress_callback, "Importación CSV finalizada", total, total)

    return {
        "files": results,
        "summary": aggregate,
    }