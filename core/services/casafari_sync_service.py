from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.config.settings import CASAFARI_SOURCE_NAME
from core.connectors.casafari_history_connector import (
    CasafariHistoryConnector,
    derive_sync_range,
)
from core.services.casafari_debug_service import get_latest_casafari_debug_summary
from core.services.casafari_reconciliation_service import reconcile_casafari_raw_items
from db.models.raw_history_item import RawHistoryItem
from db.repositories.sync_repo import get_or_create_sync_state


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def upsert_raw_item(session: Session, data: dict) -> tuple[RawHistoryItem, bool]:
    item = session.scalar(
        select(RawHistoryItem).where(RawHistoryItem.source_uid == data["source_uid"])
    )

    if item is None:
        item = RawHistoryItem(
            source_name=CASAFARI_SOURCE_NAME,
            source_uid=data["source_uid"],
            history_type=data.get("history_type"),
            event_type_guess=data.get("event_type_guess"),
            event_datetime=data.get("event_datetime"),
            title=data.get("title"),
            address_raw=data.get("address_raw"),
            listing_url=data.get("listing_url"),
            portal=data.get("portal"),
            contact_name=data.get("contact_name"),
            contact_phone=data.get("contact_phone"),
            current_price_eur=data.get("current_price_eur"),
            previous_price_eur=data.get("previous_price_eur"),
            page_number=data.get("page_number"),
            raw_text=data.get("raw_text"),
            raw_payload_json=data.get("raw_payload_json"),
        )
        session.add(item)
        session.flush()
        return item, True

    item.history_type = data.get("history_type") or item.history_type
    item.event_type_guess = data.get("event_type_guess") or item.event_type_guess
    item.event_datetime = data.get("event_datetime") or item.event_datetime
    item.title = data.get("title") or item.title
    item.address_raw = data.get("address_raw") or item.address_raw
    item.listing_url = data.get("listing_url") or item.listing_url
    item.portal = data.get("portal") or item.portal
    item.contact_name = data.get("contact_name") or item.contact_name
    item.contact_phone = data.get("contact_phone") or item.contact_phone
    item.current_price_eur = data.get("current_price_eur") or item.current_price_eur
    item.previous_price_eur = data.get("previous_price_eur") or item.previous_price_eur
    item.page_number = data.get("page_number") or item.page_number
    item.raw_text = data.get("raw_text") or item.raw_text
    item.raw_payload_json = data.get("raw_payload_json") or item.raw_payload_json
    session.flush()
    return item, False


def get_sync_status(session: Session) -> dict:
    state = get_or_create_sync_state(session, CASAFARI_SOURCE_NAME)
    debug_summary = get_latest_casafari_debug_summary()

    return {
        "source_name": state.source_name,
        "last_started_at": state.last_started_at,
        "last_finished_at": state.last_finished_at,
        "last_success_from": state.last_success_from,
        "last_success_to": state.last_success_to,
        "last_status": state.last_status,
        "last_message": state.last_message,
        "last_item_count": state.last_item_count,
        "last_debug_dir": debug_summary.get("debug_dir"),
        "last_extractor_used": debug_summary.get("extractor_used"),
        "last_pages_seen": debug_summary.get("pages_seen"),
        "last_total_expected": debug_summary.get("total_expected"),
        "last_coverage_gap": debug_summary.get("coverage_gap"),
        "last_candidate_payload_count": debug_summary.get("candidate_payload_count"),
        "last_captured_payload_count": debug_summary.get("captured_payload_count"),
        "last_warning_count": debug_summary.get("warning_count"),
        "last_warnings": debug_summary.get("warnings") or [],
        "last_sync_mode": debug_summary.get("sync_mode"),
        "last_final_url": debug_summary.get("final_url"),
        "last_target_url": debug_summary.get("target_url"),
    }


def sync_casafari_history(
    session: Session,
    progress_callback=None,
    *,
    sync_mode: str = "balanced",
) -> dict:
    state = get_or_create_sync_state(session, CASAFARI_SOURCE_NAME)

    from_dt, to_dt = derive_sync_range(state.last_success_to)

    state.last_started_at = utc_now()
    state.last_status = "running"
    state.last_message = "Sincronización en curso"
    session.commit()

    def emit(message: str, current: int, total: int) -> None:
        if progress_callback:
            progress_callback(message, current, total)

    try:
        connector = CasafariHistoryConnector(progress_callback=emit)
        payload = connector.fetch_history(
            from_dt=from_dt,
            to_dt=to_dt,
            sync_mode=sync_mode,
        )

        if not payload["items"]:
            raise RuntimeError(
                "Casafari devolvió 0 items. "
                f"URL final: {payload['final_url']} | "
                f"Extractor: {payload['extractor_used']} | "
                f"Payloads candidatos: {payload['candidate_payload_count']} | "
                f"Debug: {payload['debug_dir']}"
            )

        created = 0
        updated = 0
        source_uids: list[str] = []

        for item_data in payload["items"]:
            raw_item, is_created = upsert_raw_item(session, item_data)
            source_uids.append(raw_item.source_uid)
            if is_created:
                created += 1
            else:
                updated += 1

        reconcile_stats = reconcile_casafari_raw_items(
            session,
            source_uids=source_uids,
            only_unresolved=False,
            limit=None,
        )

        warning_suffix = ""
        if payload.get("warnings"):
            warning_suffix = f" | warnings={len(payload['warnings'])}"

        state.last_finished_at = utc_now()
        state.last_success_from = from_dt.replace(tzinfo=timezone.utc)
        state.last_success_to = to_dt.replace(tzinfo=timezone.utc)
        state.last_status = "success"
        state.last_message = (
            f"Sync OK: {created} nuevos, {updated} actualizados, "
            f"modo={payload.get('sync_mode', sync_mode)}, "
            f"extractor={payload['extractor_used']}, "
            f"resueltos={reconcile_stats['raw_items_resolved']}, "
            f"eventos={reconcile_stats['market_events_created']}"
            f"{warning_suffix}"
        )
        state.last_item_count = len(payload["items"])
        session.commit()

        return {
            "status": "success",
            "from_dt": from_dt,
            "to_dt": to_dt,
            "sync_mode": payload.get("sync_mode", sync_mode),
            "raw_items_seen": len(payload["items"]),
            "raw_items_created": created,
            "raw_items_updated": updated,
            "raw_items_resolved": reconcile_stats["raw_items_resolved"],
            "raw_items_ambiguous": reconcile_stats["raw_items_ambiguous"],
            "raw_items_unresolved": reconcile_stats["raw_items_unresolved"],
            "market_events_created": reconcile_stats["market_events_created"],
            "target_url": payload["target_url"],
            "final_url": payload["final_url"],
            "extractor_used": payload["extractor_used"],
            "pages_seen": payload["pages_seen"],
            "candidate_payload_count": payload["candidate_payload_count"],
            "captured_payload_count": payload["captured_payload_count"],
            "total_expected": payload.get("total_expected"),
            "warnings": payload.get("warnings", []),
            "debug_dir": payload["debug_dir"],
        }

    except Exception as exc:
        session.rollback()

        state = get_or_create_sync_state(session, CASAFARI_SOURCE_NAME)
        state.last_finished_at = utc_now()
        state.last_status = "error"
        state.last_message = str(exc)[:500]
        session.commit()

        raise
