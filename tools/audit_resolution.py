from collections import Counter

from sqlalchemy import select

from db.models.telegram_alert import TelegramAlert
from db.session import SessionLocal


def main() -> None:
    with SessionLocal() as session:
        alerts = list(session.scalars(select(TelegramAlert)).all())

    total = len(alerts)
    resolved = sum(1 for a in alerts if a.resolved)
    unresolved = total - resolved
    matched_existing_listing = sum(1 for a in alerts if a.matched_existing_listing)
    matched_existing_asset = sum(1 for a in alerts if a.matched_existing_asset)
    created_new_listing = sum(1 for a in alerts if a.created_new_listing)
    created_new_asset = sum(1 for a in alerts if a.created_new_asset)

    strategy_counter = Counter(a.resolution_strategy or "none" for a in alerts)

    print("RESOLUTION AUDIT")
    print(f"total_alerts: {total}")
    print(f"resolved: {resolved}")
    print(f"unresolved: {unresolved}")
    print(f"matched_existing_listing: {matched_existing_listing}")
    print(f"matched_existing_asset: {matched_existing_asset}")
    print(f"created_new_listing: {created_new_listing}")
    print(f"created_new_asset: {created_new_asset}")

    print("\nRESOLUTION STRATEGIES:")
    for key, value in strategy_counter.most_common():
        print(f"  {key}: {value}")

    print("\nSAMPLE UNRESOLVED:")
    sample = [a for a in alerts if not a.resolved][:15]
    for idx, alert in enumerate(sample, 1):
        print(f"\n--- #{idx} ---")
        print(f"type: {alert.event_type_guess}")
        print(f"url: {alert.listing_url}")
        print(f"address: {alert.address_raw}")
        print(f"area: {alert.area_m2}")
        print(f"portal: {alert.source_portal}")
        print(f"note: {alert.resolution_note}")
        print(f"text: {(alert.raw_text or '')[:220].replace(chr(10), ' ')}")


if __name__ == "__main__":
    main()