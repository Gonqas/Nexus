from collections.abc import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session
from datetime import datetime, timezone

from core.identity.telegram_window_matcher import find_unique_listing_in_csv_window
from core.identity.listing_resolver import extract_external_id, find_existing_listing
from core.identity.telegram_asset_resolver import find_strict_existing_asset
from core.identity.telegram_listing_matcher import find_listing_by_contact_portal_and_shape
from core.normalization.urls import normalize_url
from core.services.import_service import get_or_create_contact
from db.models.listing import Listing
from db.models.market_event import MarketEvent
from db.models.telegram_alert import TelegramAlert


ALLOW_TELEGRAM_NEW_ASSETS = False
ALLOW_TELEGRAM_NEW_LISTINGS_ONLY_IF_ASSET_MATCHES = True


def upsert_telegram_alert(session: Session, data: dict) -> tuple[TelegramAlert, bool]:
    alert = session.scalar(
        select(TelegramAlert).where(TelegramAlert.canonical_key == data["canonical_key"])
    )

    created = False

    incoming_dt = normalize_dt(data.get("message_datetime"))

    if alert is None:
        alert = TelegramAlert(
            message_key=data["message_key"],
            canonical_key=data["canonical_key"],
            source_file=data.get("source_file"),
            external_message_id=data.get("external_message_id"),
            message_datetime=incoming_dt,
            latest_message_datetime=incoming_dt,
            occurrence_count=1,
            event_type_guess=data.get("event_type_guess"),
            property_type_raw=data.get("property_type_raw"),
            address_raw=data.get("address_raw"),
            price_eur=data.get("price_eur"),
            price_per_m2=data.get("price_per_m2"),
            area_m2=data.get("area_m2"),
            bedrooms=data.get("bedrooms"),
            bathrooms=data.get("bathrooms"),
            listing_url=normalize_url(data.get("listing_url")),
            source_portal=data.get("source_portal"),
            contact_phone_raw=data.get("contact_phone_raw"),
            contact_name_raw=data.get("contact_name_raw"),
            owner_listing_count=data.get("owner_listing_count"),
            alert_name_raw=data.get("alert_name_raw"),
            raw_text=data.get("raw_text"),
        )
        session.add(alert)
        session.flush()
        created = True
        return alert, created

    alert.occurrence_count += 1
    incoming_dt = normalize_dt(data.get("message_datetime"))
    current_first = normalize_dt(alert.message_datetime)
    current_last = normalize_dt(alert.latest_message_datetime)

    if incoming_dt:
        if current_first is None or incoming_dt < current_first:
            alert.message_datetime = incoming_dt
        if current_last is None or incoming_dt > current_last:
            alert.latest_message_datetime = incoming_dt

    alert.event_type_guess = data.get("event_type_guess") or alert.event_type_guess
    alert.property_type_raw = data.get("property_type_raw") or alert.property_type_raw
    alert.address_raw = data.get("address_raw") or alert.address_raw
    alert.price_eur = data.get("price_eur") or alert.price_eur
    alert.price_per_m2 = data.get("price_per_m2") or alert.price_per_m2
    alert.area_m2 = data.get("area_m2") or alert.area_m2
    alert.bedrooms = data.get("bedrooms") or alert.bedrooms
    alert.bathrooms = data.get("bathrooms") or alert.bathrooms
    alert.listing_url = normalize_url(data.get("listing_url")) or alert.listing_url
    alert.source_portal = data.get("source_portal") or alert.source_portal
    alert.contact_phone_raw = data.get("contact_phone_raw") or alert.contact_phone_raw
    alert.contact_name_raw = data.get("contact_name_raw") or alert.contact_name_raw
    alert.owner_listing_count = data.get("owner_listing_count") or alert.owner_listing_count
    alert.alert_name_raw = alert.alert_name_raw or data.get("alert_name_raw")
    alert.raw_text = alert.raw_text or data.get("raw_text")

    return alert, created

def normalize_dt(value: datetime | None) -> datetime | None:
    if value is None:
        return None

    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    return value

def get_or_create_listing_from_alert(
    session: Session,
    alert: TelegramAlert,
    asset_id: int,
    contact_id: int | None,
) -> tuple[Listing, bool]:
    external_id = extract_external_id(alert.listing_url)

    listing = find_existing_listing(
        session=session,
        listing_url=alert.listing_url,
        property_url=None,
        external_id=external_id,
        source_portal=alert.source_portal,
        asset_id=asset_id,
        contact_id=contact_id,
    )

    if listing is not None:
        listing.asset_id = asset_id
        listing.contact_id = contact_id or listing.contact_id
        listing.source_portal = alert.source_portal or listing.source_portal
        listing.price_eur = alert.price_eur or listing.price_eur
        listing.price_per_m2 = alert.price_per_m2 or listing.price_per_m2
        listing.area_m2 = alert.area_m2 or listing.area_m2
        listing.bedrooms = alert.bedrooms or listing.bedrooms
        listing.bathrooms = alert.bathrooms or listing.bathrooms
        listing.last_seen_at = alert.latest_message_datetime or alert.message_datetime or listing.last_seen_at
        if alert.event_type_guess == "listing_detected":
            listing.status = "active"
        return listing, False

    if not ALLOW_TELEGRAM_NEW_LISTINGS_ONLY_IF_ASSET_MATCHES:
        raise RuntimeError("No está permitido crear listings desde Telegram")

    listing = Listing(
        asset_id=asset_id,
        contact_id=contact_id,
        source_portal=alert.source_portal,
        listing_url=alert.listing_url,
        property_url=None,
        external_id=external_id,
        first_seen_at=alert.message_datetime,
        last_seen_at=alert.latest_message_datetime or alert.message_datetime,
        status="telegram_seen",
        price_eur=alert.price_eur,
        price_per_m2=alert.price_per_m2,
        area_m2=alert.area_m2,
        bedrooms=alert.bedrooms,
        bathrooms=alert.bathrooms,
        origin_channel="telegram",
    )
    session.add(listing)
    session.flush()
    return listing, True


def _is_duplicate_event(existing_event: MarketEvent, alert: TelegramAlert) -> bool:
    if alert.message_datetime is None:
        return False

    if existing_event.event_type != (alert.event_type_guess or "telegram_alert"):
        return False

    if existing_event.event_datetime.date() != alert.message_datetime.date():
        return False

    if existing_event.price_new != alert.price_eur:
        return False

    return True


def create_market_event_from_alert(
    session: Session,
    alert: TelegramAlert,
    asset_id: int | None,
    listing_id: int | None,
) -> bool:
    if alert.message_datetime is None:
        return False

    existing_events = list(
        session.scalars(
            select(MarketEvent).where(
                MarketEvent.listing_id == listing_id,
                MarketEvent.asset_id == asset_id,
                MarketEvent.event_type == (alert.event_type_guess or "telegram_alert"),
            )
        ).all()
    )

    for existing in existing_events:
        if _is_duplicate_event(existing, alert):
            return False

    event = MarketEvent(
        asset_id=asset_id,
        listing_id=listing_id,
        event_type=alert.event_type_guess or "telegram_alert",
        event_datetime=alert.message_datetime,
        price_new=alert.price_eur,
        source_channel="telegram",
        raw_text=alert.raw_text,
    )
    session.add(event)
    return True


def resolve_alert(session: Session, alert: TelegramAlert) -> tuple[bool, bool]:
    alert.matched_existing_listing = False
    alert.matched_existing_asset = False
    alert.created_new_listing = False
    alert.created_new_asset = False
    alert.resolution_strategy = None

    listing = None
    asset = None
    contact = None

    external_id = extract_external_id(alert.listing_url)

    listing = find_existing_listing(
        session=session,
        listing_url=alert.listing_url,
        property_url=None,
        external_id=external_id,
        source_portal=alert.source_portal,
        asset_id=None,
        contact_id=None,
    )
    if listing is not None:
        asset = listing.asset
        alert.matched_existing_listing = True
        alert.resolution_strategy = "existing_listing"
        alert.resolution_note = "Match por listing_url/external_id"

    if alert.contact_phone_raw or alert.contact_name_raw:
        contact = get_or_create_contact(
            session,
            phone_raw=alert.contact_phone_raw,
            name_raw=alert.contact_name_raw,
        )

    if listing is None and contact is not None:
        listing = find_listing_by_contact_portal_and_shape(
            session=session,
            source_portal=alert.source_portal,
            contact_id=contact.id,
            address_raw=alert.address_raw,
            area_m2=alert.area_m2,
        )
        if listing is not None:
            asset = listing.asset
            alert.matched_existing_listing = True
            alert.resolution_strategy = "contact_portal_shape"
            alert.resolution_note = "Match por portal + contacto + dirección core + m²"

    if listing is None:
        listing = find_unique_listing_in_csv_window(
            session=session,
            alert_datetime=alert.message_datetime,
            source_portal=alert.source_portal,
            address_raw=alert.address_raw,
            property_type_raw=alert.property_type_raw,
            area_m2=alert.area_m2,
            price_eur=alert.price_eur,
            phone_raw=alert.contact_phone_raw,
        )
        if listing is not None:
            asset = listing.asset
            alert.matched_existing_listing = True
            alert.resolution_strategy = "window_portal_address_area_type"
            alert.resolution_note = (
                "Match conservador dentro de ventana CSV por portal + dirección core + tipo + m²"
            )

    if asset is None:
        asset = find_strict_existing_asset(
            session=session,
            address_raw=alert.address_raw,
            property_type_raw=alert.property_type_raw,
            area_m2=alert.area_m2,
        )
        if asset is not None:
            alert.matched_existing_asset = True
            alert.resolution_strategy = "existing_asset_strict"
            alert.resolution_note = "Match estricto por dirección/tipo/área"

    if asset is None and not ALLOW_TELEGRAM_NEW_ASSETS:
        alert.resolved = False
        alert.resolution_strategy = "unresolved_no_strict_match"
        alert.resolution_note = "Sin match estricto con listing/asset existente"
        return False, False

    if listing is None:
        listing, created_new_listing = get_or_create_listing_from_alert(
            session=session,
            alert=alert,
            asset_id=asset.id,
            contact_id=contact.id if contact else None,
        )
        alert.created_new_listing = created_new_listing
        if created_new_listing and alert.resolution_strategy == "existing_asset_strict":
            alert.resolution_strategy = "existing_asset_new_listing"
            alert.resolution_note = "Asset existente; listing nuevo desde Telegram"

    event_created = create_market_event_from_alert(
        session=session,
        alert=alert,
        asset_id=asset.id if asset else None,
        listing_id=listing.id if listing else None,
    )

    alert.asset_id = asset.id if asset else None
    alert.listing_id = listing.id if listing else None
    alert.resolved = True

    return True, event_created


def import_telegram_alerts(session: Session, parsed_alerts: Iterable[dict]) -> dict[str, int]:
    stats = {
        "alerts_read_raw": 0,
        "alerts_consolidated_created": 0,
        "alerts_consolidated_updated": 0,
        "alerts_resolved": 0,
        "market_events_created": 0,
        "alerts_created_new_listing": 0,
        "alerts_matched_existing_listing": 0,
        "alerts_matched_existing_asset": 0,
    }

    for data in parsed_alerts:
        stats["alerts_read_raw"] += 1

        alert, created = upsert_telegram_alert(session, data)
        if created:
            stats["alerts_consolidated_created"] += 1
        else:
            stats["alerts_consolidated_updated"] += 1

    session.flush()

    alerts = list(session.scalars(select(TelegramAlert)).all())
    for alert in alerts:
        resolved, event_created = resolve_alert(session, alert)

        if alert.matched_existing_listing:
            stats["alerts_matched_existing_listing"] += 1
        if alert.matched_existing_asset:
            stats["alerts_matched_existing_asset"] += 1
        if alert.created_new_listing:
            stats["alerts_created_new_listing"] += 1

        if resolved:
            stats["alerts_resolved"] += 1
        if event_created:
            stats["market_events_created"] += 1

    session.commit()
    return stats