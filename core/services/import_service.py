from collections.abc import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from core.identity.asset_matcher import find_existing_asset
from core.identity.listing_resolver import extract_external_id, find_existing_listing
from core.normalization.addresses import normalize_address_key, normalize_address_raw
from core.normalization.phones import normalize_phone
from core.normalization.portals import canonicalize_portal_label
from core.normalization.property_types import normalize_property_type
from core.normalization.text import normalize_text_key
from core.normalization.urls import normalize_url
from core.parsers.price_parser import parse_area_m2, parse_lead_date, parse_price_eur
from db.models.asset import Asset
from db.models.building import Building
from db.models.contact import Contact
from db.models.listing import Listing
from db.models.listing_snapshot import ListingSnapshot
from core.services.geography_enrichment_service import (
    enrich_asset_geography,
    enrich_building_geography,
)


def _safe_str(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def get_or_create_contact(
    session: Session,
    phone_raw: str | None,
    name_raw: str | None,
) -> Contact | None:
    phone_norm = normalize_phone(phone_raw)
    name_norm = normalize_text_key(name_raw)

    if not phone_norm and not name_norm:
        return None

    contact = None

    if phone_norm:
        contact = session.scalar(
            select(Contact).where(Contact.phone_norm == phone_norm)
        )

    if contact is None and name_norm:
        contact = session.scalar(
            select(Contact).where(
                Contact.phone_norm.is_(None),
                Contact.name_norm == name_norm,
            )
        )

    if contact is None:
        contact = Contact(
            phone_raw=phone_raw,
            phone_norm=phone_norm,
            name_raw=name_raw,
            name_norm=name_norm,
            contact_type_guess="unknown",
        )
        session.add(contact)
        session.flush()
        return contact

    contact.phone_raw = phone_raw or contact.phone_raw
    contact.phone_norm = phone_norm or contact.phone_norm
    contact.name_raw = name_raw or contact.name_raw
    contact.name_norm = name_norm or contact.name_norm

    return contact


def get_or_create_building(session: Session, address_raw: str | None) -> Building | None:
    address_base = normalize_address_raw(address_raw)
    address_key = normalize_address_key(address_raw)

    if not address_key:
        return None

    building = session.scalar(
        select(Building).where(Building.address_base == address_base)
    )
    if building is not None:
        enrich_building_geography(building)
        return building

    building = Building(address_base=address_base)
    session.add(building)
    session.flush()

    enrich_building_geography(building)
    return building


def get_or_create_asset(
    session: Session,
    building: Building | None,
    property_type_raw: str | None,
    address_raw: str | None,
    area_m2: float | None,
) -> Asset:
    address_norm = normalize_address_key(address_raw)
    address_clean = normalize_address_raw(address_raw)
    asset_type_family, asset_type_detail = normalize_property_type(property_type_raw)

    existing = find_existing_asset(
        session=session,
        address_norm=address_norm,
        asset_type_detail=asset_type_detail,
        area_m2=area_m2,
        building_id=building.id if building else None,
    )

    if existing is not None:
        if building and existing.building_id is None:
            existing.building_id = building.id

        if existing.address_raw is None:
            existing.address_raw = address_clean

        if existing.area_m2 is None and area_m2 is not None:
            existing.area_m2 = area_m2

        enrich_asset_geography(existing)
        return existing

    asset = Asset(
        building_id=building.id if building else None,
        asset_type_family=asset_type_family,
        asset_type_detail=asset_type_detail,
        address_raw=address_clean,
        address_norm=address_norm,
        area_m2=area_m2,
        data_confidence=0.6,
    )
    session.add(asset)
    session.flush()

    enrich_asset_geography(asset)
    return asset


def get_or_create_listing(
    session: Session,
    asset: Asset,
    contact: Contact | None,
    row: dict,
) -> tuple[Listing, bool]:
    listing_url = normalize_url(_safe_str(row.get("Página del anuncio")))
    property_url = normalize_url(_safe_str(row.get("Página de la propiedad")))
    source_portal = canonicalize_portal_label(_safe_str(row.get("Fuente")))
    status = _safe_str(row.get("Estado del anuncio"))

    price_eur = parse_price_eur(_safe_str(row.get("Precio")))
    area_m2 = parse_area_m2(_safe_str(row.get("Área construida")))
    first_seen = parse_lead_date(_safe_str(row.get("Fecha de creación del lead")))

    external_id = extract_external_id(listing_url)

    listing = find_existing_listing(
        session=session,
        listing_url=listing_url,
        property_url=property_url,
        external_id=external_id,
        source_portal=source_portal,
        asset_id=asset.id,
        contact_id=contact.id if contact else None,
    )

    created = False

    if listing is None:
        listing = Listing(
            asset_id=asset.id,
            contact_id=contact.id if contact else None,
            source_portal=source_portal,
            listing_url=listing_url,
            property_url=property_url,
            external_id=external_id,
            first_seen_at=first_seen,
            last_seen_at=first_seen,
            status=status,
            price_eur=price_eur,
            price_per_m2=(price_eur / area_m2) if price_eur and area_m2 else None,
            area_m2=area_m2,
            origin_channel="csv",
        )
        session.add(listing)
        session.flush()
        created = True
        return listing, created

    listing.asset_id = asset.id
    listing.contact_id = contact.id if contact else listing.contact_id
    listing.source_portal = source_portal or listing.source_portal
    listing.listing_url = listing_url or listing.listing_url
    listing.property_url = property_url or listing.property_url
    listing.external_id = external_id or listing.external_id
    listing.status = status or listing.status
    listing.price_eur = price_eur or listing.price_eur
    listing.area_m2 = area_m2 or listing.area_m2
    listing.price_per_m2 = (
        (listing.price_eur / listing.area_m2)
        if listing.price_eur and listing.area_m2
        else listing.price_per_m2
    )
    listing.last_seen_at = first_seen or listing.last_seen_at
    listing.origin_channel = listing.origin_channel or "csv"

    return listing, created


def create_listing_snapshot(
    session: Session,
    listing: Listing,
    contact: Contact | None,
    row: dict,
) -> bool:
    snapshot_dt = parse_lead_date(_safe_str(row.get("Fecha de creación del lead")))
    if snapshot_dt is None:
        return False

    existing = session.scalar(
        select(ListingSnapshot).where(
            ListingSnapshot.listing_id == listing.id,
            ListingSnapshot.source_channel == "csv",
            ListingSnapshot.snapshot_datetime == snapshot_dt,
        )
    )
    if existing is not None:
        return False

    snapshot = ListingSnapshot(
        listing_id=listing.id,
        contact_id=contact.id if contact else None,
        snapshot_datetime=snapshot_dt,
        source_channel="csv",
        price_eur=listing.price_eur,
        status=listing.status,
        raw_payload=str(row),
    )
    session.add(snapshot)
    return True


def import_leads_rows(session: Session, rows: Iterable[dict]) -> dict[str, int]:
    stats = {
        "rows_read": 0,
        "contacts_created_or_updated": 0,
        "assets_created_or_matched": 0,
        "listings_created": 0,
        "snapshots_created": 0,
        "casafari_raw_items_processed": 0,
        "casafari_raw_items_resolved": 0,
        "casafari_raw_items_ambiguous": 0,
        "casafari_raw_items_unresolved": 0,
        "casafari_market_events_created": 0,
    }

    for row in rows:
        stats["rows_read"] += 1

        phone_raw = _safe_str(row.get("Número de teléfono"))
        name_raw = _safe_str(row.get("Nombre"))
        property_type_raw = _safe_str(row.get("Tipo de propiedad"))
        address_raw = _safe_str(row.get("Dirección"))
        area_m2 = parse_area_m2(_safe_str(row.get("Área construida")))

        contact = get_or_create_contact(session, phone_raw=phone_raw, name_raw=name_raw)
        stats["contacts_created_or_updated"] += 1

        building = get_or_create_building(session, address_raw=address_raw)

        asset = get_or_create_asset(
            session=session,
            building=building,
            property_type_raw=property_type_raw,
            address_raw=address_raw,
            area_m2=area_m2,
        )
        stats["assets_created_or_matched"] += 1

        listing, created = get_or_create_listing(session, asset=asset, contact=contact, row=row)
        if created:
            stats["listings_created"] += 1

        snapshot_created = create_listing_snapshot(
            session=session,
            listing=listing,
            contact=contact,
            row=row,
        )
        if snapshot_created:
            stats["snapshots_created"] += 1

    session.flush()

    from core.services.casafari_reconciliation_service import reconcile_casafari_raw_items

    reconcile_stats = reconcile_casafari_raw_items(
        session,
        source_uids=None,
        only_unresolved=True,
        limit=5000,
    )

    stats["casafari_raw_items_processed"] = reconcile_stats["raw_items_processed"]
    stats["casafari_raw_items_resolved"] = reconcile_stats["raw_items_resolved"]
    stats["casafari_raw_items_ambiguous"] = reconcile_stats["raw_items_ambiguous"]
    stats["casafari_raw_items_unresolved"] = reconcile_stats["raw_items_unresolved"]
    stats["casafari_market_events_created"] = reconcile_stats["market_events_created"]

    session.commit()
    return stats
