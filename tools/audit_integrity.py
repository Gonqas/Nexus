
from sqlalchemy import func, select

from db.models.asset import Asset
from db.models.listing import Listing
from db.models.listing_snapshot import ListingSnapshot
from db.models.market_event import MarketEvent
from db.models.telegram_alert import TelegramAlert
from db.session import SessionLocal


def main() -> None:
    with SessionLocal() as session:
        total_assets = session.scalar(select(func.count(Asset.id))) or 0
        total_listings = session.scalar(select(func.count(Listing.id))) or 0
        total_snapshots = session.scalar(select(func.count(ListingSnapshot.id))) or 0
        total_events = session.scalar(select(func.count(MarketEvent.id))) or 0
        unresolved_alerts = session.scalar(
            select(func.count(TelegramAlert.id)).where(TelegramAlert.resolved == False)  # noqa: E712
        ) or 0

        duplicate_listing_urls = session.execute(
            select(Listing.listing_url, func.count(Listing.id))
            .where(Listing.listing_url.is_not(None))
            .group_by(Listing.listing_url)
            .having(func.count(Listing.id) > 1)
        ).all()

        suspicious_assets = session.execute(
            select(
                Asset.address_norm,
                Asset.asset_type_detail,
                func.round(Asset.area_m2, 0),
                func.count(Asset.id),
            )
            .where(Asset.address_norm.is_not(None))
            .group_by(Asset.address_norm, Asset.asset_type_detail, func.round(Asset.area_m2, 0))
            .having(func.count(Asset.id) > 1)
        ).all()

        csv_events = session.scalar(
            select(func.count(MarketEvent.id)).where(MarketEvent.source_channel == "csv")
        ) or 0

    print("AUDIT OK")
    print(f"assets: {int(total_assets)}")
    print(f"listings: {int(total_listings)}")
    print(f"snapshots: {int(total_snapshots)}")
    print(f"market_events: {int(total_events)}")
    print(f"telegram_unresolved: {int(unresolved_alerts)}")
    print(f"duplicate_listing_urls: {len(duplicate_listing_urls)}")
    print(f"suspicious_asset_groups: {len(suspicious_assets)}")
    print(f"csv_events_should_be_zero: {int(csv_events)}")

    if duplicate_listing_urls:
        print("\nDUPLICATE LISTING URLS:")
        for url, count in duplicate_listing_urls[:20]:
            print(f"  {count}x {url}")

    if suspicious_assets:
        print("\nSUSPICIOUS ASSET GROUPS:")
        for address_norm, asset_type_detail, area_rounded, count in suspicious_assets[:20]:
            print(f"  {count}x | {asset_type_detail} | {area_rounded}m² | {address_norm}")


if __name__ == "__main__":
    main()