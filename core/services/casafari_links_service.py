from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session, joinedload

from core.services.ai_explanations_service import explain_casafari_case
from core.services.casafari_reconciliation_service import reconcile_casafari_raw_items
from core.services.casafari_semantics_service import (
    classify_address_semantics,
    classify_match_confidence_band,
    classify_phone_profile,
    classify_price_semantics,
    infer_match_reason_taxonomy,
)
from core.normalization.text import normalize_text_key
from core.services.matching_metrics_service import (
    add_match_review,
    get_latest_match_review_map,
    get_matching_metrics,
    suggest_threshold_diagnostics,
)
from db.models.casafari_event_link import CasafariEventLink
from db.models.listing import Listing
from db.models.market_event import MarketEvent
from db.models.raw_history_item import RawHistoryItem


def get_casafari_link_stats(session: Session) -> dict[str, int]:
    total_raw = int(
        session.scalar(
            select(func.count(RawHistoryItem.id)).where(
                RawHistoryItem.source_name == "casafari_history"
            )
        )
        or 0
    )

    total_links = int(session.scalar(select(func.count(CasafariEventLink.id))) or 0)

    resolved = int(
        session.scalar(
            select(func.count(CasafariEventLink.id)).where(
                CasafariEventLink.match_status == "resolved"
            )
        )
        or 0
    )

    ambiguous = int(
        session.scalar(
            select(func.count(CasafariEventLink.id)).where(
                CasafariEventLink.match_status == "ambiguous"
            )
        )
        or 0
    )

    unresolved = int(
        session.scalar(
            select(func.count(CasafariEventLink.id)).where(
                CasafariEventLink.match_status == "unresolved"
            )
        )
        or 0
    )

    pending = int(
        session.scalar(
            select(func.count(CasafariEventLink.id)).where(
                CasafariEventLink.match_status == "pending"
            )
        )
        or 0
    )

    linked_to_listing = int(
        session.scalar(
            select(func.count(CasafariEventLink.id)).where(
                CasafariEventLink.listing_id.is_not(None)
            )
        )
        or 0
    )

    market_events_created = int(
        session.scalar(
            select(func.count(MarketEvent.id)).where(
                MarketEvent.source_channel == "casafari"
            )
        )
        or 0
    )

    return {
        "total_raw": total_raw,
        "total_links": total_links,
        "resolved": resolved,
        "ambiguous": ambiguous,
        "unresolved": unresolved,
        "pending": pending,
        "linked_to_listing": linked_to_listing,
        "market_events_created": market_events_created,
    }


def list_casafari_links(
    session: Session,
    status_filter: str = "all",
    focus_filter: str = "all",
    query_text: str | None = None,
    limit: int = 300,
) -> list[dict]:
    stmt = (
        select(CasafariEventLink)
        .join(RawHistoryItem, CasafariEventLink.raw_history_item_id == RawHistoryItem.id)
        .options(
            joinedload(CasafariEventLink.raw_history_item),
            joinedload(CasafariEventLink.listing).joinedload(Listing.asset),
            joinedload(CasafariEventLink.market_event),
            joinedload(CasafariEventLink.contact),
        )
        .order_by(desc(RawHistoryItem.event_datetime), desc(RawHistoryItem.id))
        .limit(limit)
    )

    if status_filter and status_filter != "all":
        stmt = stmt.where(CasafariEventLink.match_status == status_filter)

    links = list(session.scalars(stmt).all())
    latest_review_map = get_latest_match_review_map(
        session,
        [link.raw_history_item_id for link in links if link.raw_history_item_id is not None],
    )

    phone_cache: dict[str, dict] = {}
    rows: list[dict] = []

    query_key = normalize_text_key(query_text)

    for link in links:
        item = link.raw_history_item
        listing = link.listing
        asset = listing.asset if listing else None
        market_event = link.market_event

        listing_label = "-"
        if listing is not None:
            listing_label = (
                asset.address_raw
                if asset and asset.address_raw
                else listing.listing_url
                if listing.listing_url
                else f"listing_id={listing.id}"
            )

        phone_key = item.contact_phone or ""
        if phone_key not in phone_cache:
            phone_cache[phone_key] = classify_phone_profile(session, item.contact_phone)

        address_meta = classify_address_semantics(item.address_raw)
        price_meta = classify_price_semantics(
            item.current_price_eur,
            item.previous_price_eur,
            item.raw_text,
        )
        match_band = classify_match_confidence_band(link.match_status, link.match_score)
        reason_taxonomy = infer_match_reason_taxonomy(session, item, link)
        latest_review = latest_review_map.get(item.id if item else None, {})

        row = {
            "raw_history_item_id": item.id if item else None,
            "event_datetime": item.event_datetime if item else None,
            "event_type_guess": item.event_type_guess if item else None,
            "address_raw": item.address_raw if item else None,
            "address_precision": address_meta["address_precision"],
            "zone_like_label": address_meta["zone_like_label"],
            "contact_phone": item.contact_phone if item else None,
            "contact_name": item.contact_name if item else None,
            "portal": item.portal if item else None,
            "current_price_eur": item.current_price_eur if item else None,
            "previous_price_eur": item.previous_price_eur if item else None,
            "price_confidence": price_meta["price_confidence"],
            "price_source": price_meta["price_source"],
            "phone_profile": phone_cache[phone_key]["phone_profile"],
            "phone_listing_count": phone_cache[phone_key]["phone_listing_count"],
            "phone_asset_count": phone_cache[phone_key]["phone_asset_count"],
            "phone_portal_count": phone_cache[phone_key]["phone_portal_count"],
            "match_status": link.match_status,
            "match_strategy": link.match_strategy,
            "match_score": link.match_score,
            "match_confidence_band": match_band,
            "reason_taxonomy": reason_taxonomy,
            "match_note": link.match_note,
            "listing_id": link.listing_id,
            "asset_id": link.asset_id,
            "market_event_id": link.market_event_id,
            "listing_label": listing_label,
            "market_event_type": market_event.event_type if market_event else None,
            "latest_review_label": latest_review.get("review_label"),
            "latest_review_reason": latest_review.get("review_reason"),
            "latest_review_reviewer": latest_review.get("reviewer"),
            "latest_review_created_at": latest_review.get("created_at"),
        }
        row.update(explain_casafari_case(row))

        if focus_filter == "review_needed":
            if row["match_status"] not in {"ambiguous", "unresolved", "pending"}:
                continue
            if row["latest_review_label"]:
                continue
        elif focus_filter == "poor_address":
            if row["address_precision"] not in {"zone_like", "unknown"}:
                continue
        elif focus_filter == "repeated_phone":
            if row["phone_profile"] != "broker_like":
                continue
        elif focus_filter == "weak_identity":
            if row["reason_taxonomy"] not in {
                "weak_identity",
                "zone_only_address",
                "repeated_phone_conflict",
                "no_candidates",
                "not_in_csv_yet",
            }:
                continue
        elif focus_filter == "price_conflict":
            if row["reason_taxonomy"] != "price_conflict":
                continue

        if query_key:
            haystack = normalize_text_key(
                " ".join(
                    str(value)
                    for value in (
                        row["address_raw"],
                        row["contact_name"],
                        row["contact_phone"],
                        row["portal"],
                        row["match_status"],
                        row["reason_taxonomy"],
                        row["listing_label"],
                        row["match_note"],
                    )
                    if value
                )
            )
            if not haystack or query_key not in haystack:
                continue

        rows.append(row)

        if len(rows) >= limit:
            break

    return rows


def get_casafari_matching_review_summary(session: Session) -> dict:
    metrics = get_matching_metrics(session)
    diagnostics = suggest_threshold_diagnostics(session)
    return {
        "metrics": metrics,
        "threshold_diagnostics": diagnostics,
    }


def save_casafari_match_review(
    session: Session,
    *,
    raw_history_item_id: int | None,
    listing_id: int | None,
    asset_id: int | None,
    review_label: str,
    review_reason: str | None = None,
    reviewer: str | None = None,
    predicted_status: str | None = None,
    predicted_score: float | None = None,
) -> dict:
    candidate_type = "listing" if listing_id else "raw_item"

    row = add_match_review(
        session,
        review_label=review_label,
        source_channel="casafari_history",
        candidate_type=candidate_type,
        raw_history_item_id=raw_history_item_id,
        listing_id=listing_id,
        asset_id=asset_id,
        candidate_listing_id=listing_id,
        candidate_asset_id=asset_id,
        predicted_score=predicted_score,
        predicted_status=predicted_status,
        review_reason=review_reason,
        reviewer=reviewer,
    )
    session.commit()

    return {
        "id": row.id,
        "review_label": row.review_label,
        "review_reason": row.review_reason,
        "reviewer": row.reviewer,
        "created_at": row.created_at,
    }


def rerun_pending_casafari_reconciliation(
    session: Session,
    limit: int = 5000,
) -> dict[str, int]:
    stats = reconcile_casafari_raw_items(
        session,
        source_uids=None,
        only_unresolved=True,
        limit=limit,
    )
    session.commit()
    return stats
