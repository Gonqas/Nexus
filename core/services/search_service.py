from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session, joinedload

from core.services.casafari_semantics_service import infer_match_reason_taxonomy
from db.models.asset import Asset
from db.models.casafari_event_link import CasafariEventLink
from db.models.contact import Contact
from db.models.listing import Listing
from db.models.market_event import MarketEvent
from db.models.raw_history_item import RawHistoryItem


SEARCH_INDEX_TABLE = "search_index_fts"
SEARCH_META_TABLE = "search_index_meta"


def _safe_text(value: object | None) -> str:
    if value is None:
        return ""
    return str(value)


def _digits_only(value: object | None) -> str:
    return re.sub(r"\D", "", _safe_text(value))


def _normalize_fts_query(query: str) -> str:
    cleaned = re.sub(r"[^\w\s]", " ", query or "", flags=re.UNICODE)
    tokens = [token for token in cleaned.split() if token]
    parts: list[str] = []

    for token in tokens[:10]:
        safe = token.replace('"', "").strip()
        if not safe:
            continue
        if safe.isdigit() or len(safe) >= 2:
            parts.append(f"{safe}*")
        else:
            parts.append(safe)

    return " ".join(parts)


def _ensure_search_tables(session: Session) -> None:
    conn = session.connection()
    conn.exec_driver_sql(
        f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS {SEARCH_INDEX_TABLE}
        USING fts5(
            entity_type UNINDEXED,
            entity_id UNINDEXED,
            section UNINDEXED,
            title,
            content,
            phone_digits,
            sort_key UNINDEXED,
            tokenize = 'unicode61 remove_diacritics 2'
        )
        """
    )
    conn.exec_driver_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SEARCH_META_TABLE} (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )


def _get_meta(session: Session, key: str) -> str | None:
    row = session.connection().exec_driver_sql(
        f"SELECT value FROM {SEARCH_META_TABLE} WHERE key = ?",
        (key,),
    ).first()
    return row[0] if row else None


def _set_meta(session: Session, key: str, value: str) -> None:
    session.connection().exec_driver_sql(
        f"""
        INSERT INTO {SEARCH_META_TABLE}(key, value)
        VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def _dt_key(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.isoformat()


def _compute_signature(session: Session) -> str:
    parts = [
        str(int(session.scalar(select(func.count(Asset.id))) or 0)),
        str(int(session.scalar(select(func.max(Asset.id))) or 0)),
        _dt_key(session.scalar(select(func.max(Asset.updated_at)))),
        str(int(session.scalar(select(func.count(Listing.id))) or 0)),
        str(int(session.scalar(select(func.max(Listing.id))) or 0)),
        _dt_key(session.scalar(select(func.max(Listing.updated_at)))),
        str(int(session.scalar(select(func.count(RawHistoryItem.id))) or 0)),
        str(int(session.scalar(select(func.max(RawHistoryItem.id))) or 0)),
        _dt_key(session.scalar(select(func.max(RawHistoryItem.created_at)))),
        str(int(session.scalar(select(func.count(CasafariEventLink.id))) or 0)),
        str(int(session.scalar(select(func.max(CasafariEventLink.id))) or 0)),
        _dt_key(session.scalar(select(func.max(CasafariEventLink.updated_at)))),
        str(int(session.scalar(select(func.count(MarketEvent.id))) or 0)),
        str(int(session.scalar(select(func.max(MarketEvent.id))) or 0)),
        _dt_key(session.scalar(select(func.max(MarketEvent.created_at)))),
    ]
    return "|".join(parts)


def _build_asset_docs(session: Session) -> list[tuple]:
    docs: list[tuple] = []
    assets = list(
        session.scalars(
            select(Asset)
            .options(joinedload(Asset.building), joinedload(Asset.listings))
            .order_by(Asset.id.desc())
        ).unique().all()
    )

    for asset in assets:
        listings_count = len(asset.listings or [])
        title = " | ".join(
            part
            for part in (
                asset.address_raw or asset.address_norm,
                asset.asset_type_detail or asset.asset_type_family,
            )
            if part
        )
        content = " ".join(
            part
            for part in (
                f"asset {asset.id}",
                asset.address_raw,
                asset.address_norm,
                asset.asset_type_family,
                asset.asset_type_detail,
                asset.neighborhood,
                asset.district,
                asset.building.address_base if asset.building else None,
                f"listings {listings_count}",
            )
            if part
        )
        docs.append(
            ("asset", asset.id, "assets", title, content, "", _dt_key(asset.updated_at))
        )

    return docs


def _build_listing_docs(session: Session) -> list[tuple]:
    docs: list[tuple] = []
    listings = list(
        session.scalars(
            select(Listing)
            .options(joinedload(Listing.asset), joinedload(Listing.contact))
            .order_by(Listing.id.desc())
        ).all()
    )

    for listing in listings:
        asset = listing.asset
        contact = listing.contact
        title = " | ".join(
            part
            for part in (
                listing.source_portal,
                asset.address_raw if asset else None,
                _safe_text(listing.price_eur) if listing.price_eur is not None else None,
            )
            if part
        )
        content = " ".join(
            part
            for part in (
                f"listing {listing.id}",
                f"asset {listing.asset_id}" if listing.asset_id else None,
                listing.source_portal,
                listing.external_id,
                listing.status,
                _safe_text(listing.price_eur),
                listing.listing_url,
                listing.property_url,
                asset.address_raw if asset else None,
                asset.neighborhood if asset else None,
                asset.district if asset else None,
                contact.name_raw if contact else None,
                contact.phone_raw if contact else None,
            )
            if part
        )
        docs.append(
            (
                "listing",
                listing.id,
                "listings",
                title,
                content,
                _digits_only(contact.phone_raw if contact else None),
                _dt_key(listing.updated_at or listing.last_seen_at),
            )
        )

    return docs


def _build_raw_docs(session: Session) -> list[tuple]:
    docs: list[tuple] = []
    links = list(
        session.scalars(
            select(CasafariEventLink)
            .options(
                joinedload(CasafariEventLink.raw_history_item),
                joinedload(CasafariEventLink.listing).joinedload(Listing.asset),
                joinedload(CasafariEventLink.contact),
            )
            .order_by(CasafariEventLink.id.desc())
        ).all()
    )

    seen_raw_ids: set[int] = set()
    for link in links:
        item = link.raw_history_item
        if item is None:
            continue
        seen_raw_ids.add(item.id)
        listing = link.listing
        asset = listing.asset if listing else None
        reason_taxonomy = infer_match_reason_taxonomy(session, item, link)

        title = " | ".join(
            part
            for part in (
                item.address_raw,
                item.portal,
                item.event_type_guess,
                link.match_status,
            )
            if part
        )
        content = " ".join(
            part
            for part in (
                f"raw {item.id}",
                item.title,
                item.address_raw,
                item.contact_name,
                item.contact_phone,
                item.portal,
                item.event_type_guess,
                item.raw_text,
                link.match_status,
                link.match_strategy,
                link.match_note,
                reason_taxonomy,
                asset.address_raw if asset else None,
            )
            if part
        )
        docs.append(
            (
                "raw",
                item.id,
                "raws",
                title,
                content,
                _digits_only(item.contact_phone),
                _dt_key(item.event_datetime or item.created_at),
            )
        )

    raw_items = list(
        session.scalars(
            select(RawHistoryItem).order_by(RawHistoryItem.id.desc())
        ).all()
    )
    for item in raw_items:
        if item.id in seen_raw_ids:
            continue
        title = " | ".join(
            part for part in (item.address_raw, item.portal, item.event_type_guess) if part
        )
        content = " ".join(
            part
            for part in (
                f"raw {item.id}",
                item.title,
                item.address_raw,
                item.contact_name,
                item.contact_phone,
                item.portal,
                item.event_type_guess,
                item.raw_text,
                "pending_review",
            )
            if part
        )
        docs.append(
            (
                "raw",
                item.id,
                "raws",
                title,
                content,
                _digits_only(item.contact_phone),
                _dt_key(item.event_datetime or item.created_at),
            )
        )

    return docs


def _build_event_docs(session: Session) -> list[tuple]:
    docs: list[tuple] = []
    events = list(
        session.scalars(
            select(MarketEvent)
            .options(joinedload(MarketEvent.asset), joinedload(MarketEvent.listing))
            .order_by(MarketEvent.event_datetime.desc(), MarketEvent.id.desc())
        ).all()
    )

    for event in events:
        asset = event.asset
        listing = event.listing
        title = " | ".join(
            part
            for part in (
                event.event_type,
                asset.address_raw if asset else None,
                listing.source_portal if listing else None,
            )
            if part
        )
        content = " ".join(
            part
            for part in (
                f"event {event.id}",
                event.event_type,
                event.source_channel,
                _safe_text(event.price_new),
                _safe_text(event.price_old),
                event.status_new,
                event.status_old,
                event.raw_text,
                asset.address_raw if asset else None,
                listing.source_portal if listing else None,
            )
            if part
        )
        docs.append(
            (
                "event",
                event.id,
                "events",
                title,
                content,
                "",
                _dt_key(event.event_datetime or event.created_at),
            )
        )

    return docs


def rebuild_search_index(session: Session) -> dict[str, object]:
    _ensure_search_tables(session)
    conn = session.connection()
    conn.exec_driver_sql(f"DELETE FROM {SEARCH_INDEX_TABLE}")

    docs = []
    docs.extend(_build_asset_docs(session))
    docs.extend(_build_listing_docs(session))
    docs.extend(_build_raw_docs(session))
    docs.extend(_build_event_docs(session))

    if docs:
        conn.exec_driver_sql(
            f"""
            INSERT INTO {SEARCH_INDEX_TABLE}
            (entity_type, entity_id, section, title, content, phone_digits, sort_key)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            docs,
        )

    signature = _compute_signature(session)
    _set_meta(session, "signature", signature)
    _set_meta(session, "doc_count", str(len(docs)))
    _set_meta(session, "backend", "fts5")
    session.flush()

    return {
        "backend": "fts5",
        "doc_count": len(docs),
        "signature": signature,
    }


def get_search_index_status(session: Session) -> dict[str, object]:
    _ensure_search_tables(session)
    doc_count = int(_get_meta(session, "doc_count") or 0)
    stored_signature = _get_meta(session, "signature")
    current_signature = _compute_signature(session)
    return {
        "backend": _get_meta(session, "backend") or "fts5",
        "doc_count": doc_count,
        "is_stale": stored_signature != current_signature,
        "signature": stored_signature,
    }


def ensure_search_index(session: Session, *, force_rebuild: bool = False) -> dict[str, object]:
    status = get_search_index_status(session)
    if force_rebuild or status["doc_count"] == 0 or status["is_stale"]:
        return rebuild_search_index(session)
    return status


def _fetch_assets(session: Session, ordered_ids: list[int], snippets: dict[int, str], ranks: dict[int, float]) -> list[dict]:
    if not ordered_ids:
        return []
    rows = list(
        session.scalars(
            select(Asset)
            .options(joinedload(Asset.building), joinedload(Asset.listings))
            .where(Asset.id.in_(ordered_ids))
        ).unique().all()
    )
    by_id = {row.id: row for row in rows}
    result: list[dict] = []
    for asset_id in ordered_ids:
        asset = by_id.get(asset_id)
        if asset is None:
            continue
        result.append(
            {
                "asset_id": asset.id,
                "asset_type": asset.asset_type_detail or asset.asset_type_family,
                "address": asset.address_raw or asset.address_norm,
                "neighborhood": asset.neighborhood,
                "district": asset.district,
                "listings_count": len(asset.listings or []),
                "snippet": snippets.get(asset.id),
                "rank": ranks.get(asset.id),
            }
        )
    return result


def _fetch_listings(session: Session, ordered_ids: list[int], snippets: dict[int, str], ranks: dict[int, float]) -> list[dict]:
    if not ordered_ids:
        return []
    rows = list(
        session.scalars(
            select(Listing)
            .options(joinedload(Listing.asset), joinedload(Listing.contact))
            .where(Listing.id.in_(ordered_ids))
        ).all()
    )
    by_id = {row.id: row for row in rows}
    result: list[dict] = []
    for listing_id in ordered_ids:
        listing = by_id.get(listing_id)
        if listing is None:
            continue
        asset = listing.asset
        contact = listing.contact
        result.append(
            {
                "listing_id": listing.id,
                "asset_id": listing.asset_id,
                "portal": listing.source_portal,
                "status": listing.status,
                "price_eur": listing.price_eur,
                "address": asset.address_raw if asset else None,
                "contact_name": contact.name_raw if contact else None,
                "contact_phone": contact.phone_raw if contact else None,
                "snippet": snippets.get(listing.id),
                "rank": ranks.get(listing.id),
            }
        )
    return result


def _fetch_raws(session: Session, ordered_ids: list[int], snippets: dict[int, str], ranks: dict[int, float]) -> list[dict]:
    if not ordered_ids:
        return []
    items = list(
        session.scalars(
            select(RawHistoryItem)
            .where(RawHistoryItem.id.in_(ordered_ids))
            .order_by(RawHistoryItem.id.desc())
        ).all()
    )
    links = list(
        session.scalars(
            select(CasafariEventLink)
            .options(joinedload(CasafariEventLink.raw_history_item))
            .where(CasafariEventLink.raw_history_item_id.in_(ordered_ids))
        ).all()
    )
    item_by_id = {row.id: row for row in items}
    link_by_raw_id = {row.raw_history_item_id: row for row in links}

    result: list[dict] = []
    for raw_id in ordered_ids:
        item = item_by_id.get(raw_id)
        if item is None:
            continue
        link = link_by_raw_id.get(raw_id)
        reason_taxonomy = (
            infer_match_reason_taxonomy(session, item, link)
            if link is not None
            else "pending_review"
        )
        result.append(
            {
                "raw_history_item_id": item.id,
                "event_datetime": item.event_datetime,
                "event_type_guess": item.event_type_guess,
                "match_status": link.match_status if link else "pending",
                "reason_taxonomy": reason_taxonomy,
                "address": item.address_raw,
                "contact_name": item.contact_name,
                "contact_phone": item.contact_phone,
                "portal": item.portal,
                "match_note": link.match_note if link else None,
                "snippet": snippets.get(item.id),
                "rank": ranks.get(item.id),
            }
        )
    return result


def _fetch_events(session: Session, ordered_ids: list[int], snippets: dict[int, str], ranks: dict[int, float]) -> list[dict]:
    if not ordered_ids:
        return []
    rows = list(
        session.scalars(
            select(MarketEvent)
            .options(joinedload(MarketEvent.asset), joinedload(MarketEvent.listing))
            .where(MarketEvent.id.in_(ordered_ids))
        ).all()
    )
    by_id = {row.id: row for row in rows}
    result: list[dict] = []
    for event_id in ordered_ids:
        event = by_id.get(event_id)
        if event is None:
            continue
        asset = event.asset
        listing = event.listing
        result.append(
            {
                "market_event_id": event.id,
                "event_datetime": event.event_datetime,
                "event_type": event.event_type,
                "source_channel": event.source_channel,
                "price_new": event.price_new,
                "address": asset.address_raw if asset else None,
                "portal": listing.source_portal if listing else None,
                "snippet": snippets.get(event.id),
                "rank": ranks.get(event.id),
            }
        )
    return result


def search_payload(
    session: Session,
    query: str,
    section_filter: str = "all",
    limit_per_section: int = 25,
    *,
    force_rebuild_index: bool = False,
) -> dict[str, object]:
    raw_query = (query or "").strip()
    empty = {
        "query": raw_query,
        "section_filter": section_filter,
        "assets": [],
        "listings": [],
        "raws": [],
        "events": [],
        "summary": {
            "assets": 0,
            "listings": 0,
            "raws": 0,
            "events": 0,
            "total": 0,
        },
        "index_status": ensure_search_index(session, force_rebuild=force_rebuild_index),
    }
    if not raw_query:
        return empty

    index_status = ensure_search_index(session, force_rebuild=force_rebuild_index)
    match_query = _normalize_fts_query(raw_query)
    if not match_query:
        return empty | {"index_status": index_status}

    params = [match_query]
    where_bits = [f"{SEARCH_INDEX_TABLE} MATCH ?"]
    if section_filter != "all":
        where_bits.append("section = ?")
        params.append(section_filter)
    params.append(limit_per_section * 4)

    hits = session.connection().exec_driver_sql(
        f"""
        SELECT
            entity_type,
            entity_id,
            section,
            snippet({SEARCH_INDEX_TABLE}, 4, '[', ']', ' … ', 18) AS snippet_text,
            bm25({SEARCH_INDEX_TABLE}, 8.0, 1.0, 6.0) AS rank
        FROM {SEARCH_INDEX_TABLE}
        WHERE {' AND '.join(where_bits)}
        ORDER BY rank ASC, sort_key DESC
        LIMIT ?
        """,
        tuple(params),
    ).all()

    ids_by_section: dict[str, list[int]] = defaultdict(list)
    snippets_by_section: dict[str, dict[int, str]] = defaultdict(dict)
    ranks_by_section: dict[str, dict[int, float]] = defaultdict(dict)

    for entity_type, entity_id, section, snippet_text, rank in hits:
        if entity_id in snippets_by_section[section]:
            continue
        ids_by_section[section].append(int(entity_id))
        snippets_by_section[section][int(entity_id)] = snippet_text
        ranks_by_section[section][int(entity_id)] = float(rank)

    assets = _fetch_assets(
        session,
        ids_by_section.get("assets", [])[:limit_per_section],
        snippets_by_section.get("assets", {}),
        ranks_by_section.get("assets", {}),
    )
    listings = _fetch_listings(
        session,
        ids_by_section.get("listings", [])[:limit_per_section],
        snippets_by_section.get("listings", {}),
        ranks_by_section.get("listings", {}),
    )
    raws = _fetch_raws(
        session,
        ids_by_section.get("raws", [])[:limit_per_section],
        snippets_by_section.get("raws", {}),
        ranks_by_section.get("raws", {}),
    )
    events = _fetch_events(
        session,
        ids_by_section.get("events", [])[:limit_per_section],
        snippets_by_section.get("events", {}),
        ranks_by_section.get("events", {}),
    )

    summary = {
        "assets": len(assets),
        "listings": len(listings),
        "raws": len(raws),
        "events": len(events),
    }
    summary["total"] = sum(summary.values())

    return {
        "query": raw_query,
        "section_filter": section_filter,
        "assets": assets,
        "listings": listings,
        "raws": raws,
        "events": events,
        "summary": summary,
        "index_status": index_status,
    }
