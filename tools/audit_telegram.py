from collections import Counter

from sqlalchemy import select
from db.models.telegram_alert import TelegramAlert
from db.session import SessionLocal


def trunc(text: str | None, max_len: int = 140) -> str:
    if not text:
        return ""
    text = text.replace("\n", " ").strip()
    return text if len(text) <= max_len else text[:max_len] + "..."


def main() -> None:
    with SessionLocal() as session:
        alerts = list(session.scalars(select(TelegramAlert)).all())

    total = len(alerts)
    total_occurrences = sum(a.occurrence_count for a in alerts)
    resolved = sum(1 for a in alerts if a.resolved)
    unresolved = total - resolved
    with_url = sum(1 for a in alerts if a.listing_url)
    with_address = sum(1 for a in alerts if a.address_raw)
    with_price = sum(1 for a in alerts if a.price_eur is not None)
    with_phone = sum(1 for a in alerts if a.contact_phone_raw)

    event_counter = Counter(a.event_type_guess or "unknown" for a in alerts)
    portal_counter = Counter(a.source_portal or "unknown" for a in alerts)

    print("TELEGRAM AUDIT")
    print(f"total_consolidated_alerts: {total}")
    print(f"total_raw_occurrences: {total_occurrences}")
    print(f"resolved: {resolved}")
    print(f"unresolved: {unresolved}")
    print(f"with_url: {with_url}")
    print(f"with_address: {with_address}")
    print(f"with_price: {with_price}")
    print(f"with_phone: {with_phone}")

    print("\nEVENT TYPES:")
    for key, value in event_counter.most_common():
        print(f"  {key}: {value}")

    print("\nSOURCE PORTALS:")
    for key, value in portal_counter.most_common():
        print(f"  {key}: {value}")

    print("\nTOP OCCURRENCE COUNTS:")
    for alert in sorted(alerts, key=lambda a: a.occurrence_count, reverse=True)[:20]:
        if alert.occurrence_count <= 1:
            continue
        print(
            f"{alert.occurrence_count}x | {alert.message_datetime.date() if alert.message_datetime else 'no-date'}"
            f" | {alert.price_eur} | {alert.listing_url}"
        )

    print("\nSAMPLE UNRESOLVED ALERTS:")
    sample = [a for a in alerts if not a.resolved][:20]
    for idx, alert in enumerate(sample, 1):
        print(f"\n--- Unresolved #{idx} ---")
        print(f"datetime: {alert.message_datetime}")
        print(f"type: {alert.event_type_guess}")
        print(f"url: {alert.listing_url}")
        print(f"address: {alert.address_raw}")
        print(f"price: {alert.price_eur}")
        print(f"portal: {alert.source_portal}")
        print(f"occurrence_count: {alert.occurrence_count}")
        print(f"text: {trunc(alert.raw_text, 250)}")


if __name__ == "__main__":
    main()