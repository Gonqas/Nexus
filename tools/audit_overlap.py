from sqlalchemy import func, select

from db.models.listing_snapshot import ListingSnapshot
from db.models.telegram_alert import TelegramAlert
from db.session import SessionLocal


def day_or_none(dt):
    if dt is None:
        return None
    return dt.date()


def main() -> None:
    with SessionLocal() as session:
        min_snapshot_dt = session.scalar(select(func.min(ListingSnapshot.snapshot_datetime)))
        max_snapshot_dt = session.scalar(select(func.max(ListingSnapshot.snapshot_datetime)))
        alerts = list(session.scalars(select(TelegramAlert)).all())

    min_day = day_or_none(min_snapshot_dt)
    max_day = day_or_none(max_snapshot_dt)

    before_csv = 0
    inside_csv = 0
    after_csv = 0
    no_date = 0

    unresolved_before_csv = 0
    unresolved_inside_csv = 0
    unresolved_after_csv = 0

    unique_urls = set()
    unresolved_unique_urls = set()

    for alert in alerts:
        alert_day = day_or_none(alert.message_datetime)

        if alert.listing_url:
            unique_urls.add(alert.listing_url)

        if not alert.resolved and alert.listing_url:
            unresolved_unique_urls.add(alert.listing_url)

        if alert_day is None or min_day is None or max_day is None:
            no_date += 1
            continue

        if alert_day < min_day:
            before_csv += 1
            if not alert.resolved:
                unresolved_before_csv += 1
        elif alert_day > max_day:
            after_csv += 1
            if not alert.resolved:
                unresolved_after_csv += 1
        else:
            inside_csv += 1
            if not alert.resolved:
                unresolved_inside_csv += 1

    print("OVERLAP AUDIT")
    print(f"csv_snapshot_min: {min_snapshot_dt}")
    print(f"csv_snapshot_max: {max_snapshot_dt}")
    print(f"telegram_consolidated_alerts: {len(alerts)}")
    print(f"telegram_total_occurrences: {sum(a.occurrence_count for a in alerts)}")
    print(f"telegram_unique_urls: {len(unique_urls)}")
    print(f"telegram_unresolved_unique_urls: {len(unresolved_unique_urls)}")
    print(f"telegram_before_csv_window: {before_csv}")
    print(f"telegram_inside_csv_window: {inside_csv}")
    print(f"telegram_after_csv_window: {after_csv}")
    print(f"telegram_no_date: {no_date}")
    print(f"unresolved_before_csv_window: {unresolved_before_csv}")
    print(f"unresolved_inside_csv_window: {unresolved_inside_csv}")
    print(f"unresolved_after_csv_window: {unresolved_after_csv}")


if __name__ == "__main__":
    main()