import re
from datetime import datetime, timezone


def parse_price_eur(value: str | None) -> float | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    text = text.replace("€", "").replace(".", "").replace(",", "")
    match = re.search(r"\d+", text)
    if not match:
        return None

    return float(match.group(0))


def parse_area_m2(value: str | None) -> float | None:
    if value is None:
        return None

    text = str(value).strip().lower()
    if not text:
        return None

    text = text.replace("m²", "").replace("m2", "").replace(",", ".")
    match = re.search(r"\d+(\.\d+)?", text)
    if not match:
        return None

    return float(match.group(0))


def parse_lead_date(value: str | None) -> datetime | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        dt = datetime.strptime(text, "%d.%m.%Y")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None