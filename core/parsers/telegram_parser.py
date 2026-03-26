import hashlib
import re
from datetime import timezone

from bs4 import BeautifulSoup
from dateutil import parser as dt_parser

from core.ingest.telegram_loader import iter_telegram_html_documents
from core.normalization.text import normalize_text, normalize_text_key
from core.normalization.urls import normalize_url


PROPERTY_KEYWORDS = {
    "penthouse": "atico",
    "atico": "atico",
    "apartment": "apartamento",
    "apartamento": "apartamento",
    "piso": "piso",
    "studio": "estudio",
    "estudio": "estudio",
    "retail": "local",
    "local": "local",
    "office": "oficina",
    "oficina": "oficina",
    "garage": "garaje",
    "garaje": "garaje",
    "chalet": "chalet",
    "house": "casa",
    "casa": "casa",
}


def _parse_int_like(text: str | None) -> float | None:
    if not text:
        return None

    digits = re.sub(r"[^\d]", "", text)
    if not digits:
        return None

    return float(digits)


def _extract_price(text: str) -> float | None:
    match = re.search(r"Venta:\s*€\s*([\d\.,]+)", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"Sale:\s*€\s*([\d\.,]+)", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"€\s*([\d\.,]+)", text, flags=re.IGNORECASE)

    if not match:
        return None

    return _parse_int_like(match.group(1))


def _extract_price_per_m2(text: str) -> float | None:
    match = re.search(r"€\s*([\d\.,]+)\s*/\s*m²", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"\((?:€)?([\d\.,]+)\s*/\s*m²\)", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"([\d\.,]+)\s*€/m²", text, flags=re.IGNORECASE)
    if not match:
        return None

    return _parse_int_like(match.group(1))


def _extract_area(text: str) -> float | None:
    match = re.search(r"Área total:\s*([\d\.,]+)", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"Area total:\s*([\d\.,]+)", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"([\d\.,]+)\s*(m²|m2|sqm)", text, flags=re.IGNORECASE)

    if not match:
        return None

    return _parse_int_like(match.group(1))


def _extract_bedrooms(text: str) -> int | None:
    match = re.search(r"Dormitorios:\s*(\d+)", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"Bedrooms:\s*(\d+)", text, flags=re.IGNORECASE)
    return int(match.group(1)) if match else None


def _extract_bathrooms(text: str) -> int | None:
    match = re.search(r"Baños:\s*(\d+)", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"Bathrooms:\s*(\d+)", text, flags=re.IGNORECASE)
    return int(match.group(1)) if match else None


def _extract_phone(text: str) -> str | None:
    match = re.search(r"(\+34[\s-]?\d{3}[\s-]?\d{3}[\s-]?\d{3}|\b\d{9}\b)", text)
    if not match:
        return None
    return match.group(1)


def _extract_owner_listing_count(text: str) -> int | None:
    patterns = [
        r"Número de anuncios de este propietario:\s*(\d+)",
        r"Owner listings?\s*[:\-]?\s*(\d+)",
        r"(\d+)\s*owner listings?",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def _extract_contact_name(text: str) -> str | None:
    patterns = [
        r"(Idealista|Fotocasa for Sale|Milanuncios FSBO)\s*:\s*([^\n]+?)(?:\s+Contactos:|\s+Número de anuncios|\s+Nombre de la alerta|$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            value = normalize_text(match.group(2))
            return value
    return None


def _extract_source_portal_from_text(text: str) -> str | None:
    patterns = [
        r"(Idealista|Fotocasa for Sale|Milanuncios FSBO)\s*:",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return normalize_text(match.group(1))
    return None


def _extract_alert_name(text: str) -> str | None:
    match = re.search(r"Nombre de la alerta:\s*([^\n]+)", text, flags=re.IGNORECASE)
    if not match:
        return None
    return normalize_text(match.group(1))


def _extract_property_type(text: str) -> str | None:
    match = re.match(r"^\s*([A-Za-zÀ-ÿ]+)\s*:", text)
    if match:
        first_token = normalize_text_key(match.group(1))
        if first_token in PROPERTY_KEYWORDS:
            return PROPERTY_KEYWORDS[first_token]

    key = normalize_text_key(text) or ""
    for keyword, value in PROPERTY_KEYWORDS.items():
        if keyword in key:
            return value
    return None


def _extract_address(text: str) -> str | None:
    patterns = [
        r"^[A-Za-zÀ-ÿ]+\s*:\s*(.+?)\s+Venta:",
        r"^[A-Za-zÀ-ÿ]+\s*:\s*(.+?)\s+Sale:",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return normalize_text(match.group(1))

    return None


def _guess_event_type(text: str) -> str:
    key = normalize_text_key(text) or ""

    if any(marker in key for marker in ["bajada de precio", "price drop", "price reduced"]):
        return "price_drop"
    if any(marker in key for marker in ["subida de precio", "price raise", "price increased"]):
        return "price_raise"
    if any(marker in key for marker in ["reaparece", "reactivado", "republicado", "listing reappeared"]):
        return "listing_reappeared"
    if any(marker in key for marker in ["anuncio eliminado", "listing removed", "retirado", "off market"]):
        return "listing_removed"

    if re.search(r"^[A-Za-zÀ-ÿ]+\s*:\s*.+?\s+Venta:", text, flags=re.IGNORECASE):
        return "listing_detected"

    return "telegram_alert"


def _extract_external_links(message_div) -> list[str]:
    links: list[str] = []

    for a_tag in message_div.find_all("a", href=True):
        href = a_tag.get("href", "").strip()
        if href.startswith("http://") or href.startswith("https://"):
            links.append(normalize_url(href))

    seen = set()
    result = []
    for link in links:
        if link and link not in seen:
            seen.add(link)
            result.append(link)

    return result


def _parse_message_datetime(message_div):
    date_div = None
    for div in message_div.find_all("div"):
        classes = div.get("class", [])
        if "date" in classes:
            date_div = div
            break

    if date_div is None:
        return None

    raw_date = date_div.get("title") or date_div.get_text(" ", strip=True)
    raw_date = normalize_text(raw_date)
    if not raw_date:
        return None

    try:
        dt = dt_parser.parse(raw_date, dayfirst=True)

        # Normalizamos siempre a UTC naive para evitar problemas con SQLite
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

        return dt
    except Exception:
        return None


def _build_message_key(source_file: str, external_message_id: str | None, raw_text: str) -> str:
    base = f"{source_file}|{external_message_id or ''}|{raw_text}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()


def _build_canonical_key(
    listing_url: str | None,
    message_datetime,
    event_type_guess: str | None,
    price_eur: float | None,
) -> str:
    day_key = message_datetime.date().isoformat() if message_datetime else "no-date"
    url_key = listing_url or "no-url"
    event_key = event_type_guess or "unknown"
    price_key = str(int(price_eur)) if price_eur is not None else "no-price"

    base = f"{url_key}|{day_key}|{event_key}|{price_key}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()


def _looks_like_outside_madrid_capital(address: str | None) -> bool:
    if not address:
        return False

    key = normalize_text_key(address) or ""

    outside_markers = [
        "el burgo",
        "mirabal",
        "avenida europa",
        "pozuelo",
        "boadilla",
        "majadahonda",
        "las rozas",
        "alcobendas",
        "san sebastian de los reyes",
        "alcorcon",
        "getafe",
        "leganes",
        "mostoles",
        "fuenlabrada",
        "parla",
    ]

    return any(marker in key for marker in outside_markers)


def _is_relevant_real_estate_alert(raw_text: str, listing_url: str | None) -> bool:
    key = normalize_text_key(raw_text) or ""

    if "/start" in key:
        return False
    if "gracias por registrarse" in key:
        return False
    if "casafari ai" in key and "alertas de busqueda" in key:
        return False

    if listing_url and ("venta:" in key or "sale:" in key):
        return True

    if re.search(r"^[A-Za-zÀ-ÿ]+\s*:\s*.+?\s+Venta:", raw_text, flags=re.IGNORECASE):
        return True

    return False


def parse_telegram_export(export_path: str) -> list[dict]:
    parsed_alerts: list[dict] = []

    for source_file, html in iter_telegram_html_documents(export_path):
        soup = BeautifulSoup(html, "html.parser")

        for div in soup.find_all("div"):
            classes = div.get("class", [])
            if "message" not in classes:
                continue

            external_message_id = div.get("id")

            text_div = div.find("div", class_="text")
            if text_div is None:
                continue

            raw_text = text_div.get_text("\n", strip=True)
            raw_text = normalize_text(raw_text)
            if not raw_text:
                continue

            links = _extract_external_links(div)
            listing_url = links[0] if links else None

            if not _is_relevant_real_estate_alert(raw_text, listing_url):
                continue

            address_raw = _extract_address(raw_text)
            if _looks_like_outside_madrid_capital(address_raw):
                continue

            message_datetime = _parse_message_datetime(div)
            event_type_guess = _guess_event_type(raw_text)
            price_eur = _extract_price(raw_text)

            parsed_alerts.append(
                {
                    "message_key": _build_message_key(source_file, external_message_id, raw_text),
                    "canonical_key": _build_canonical_key(
                        listing_url=listing_url,
                        message_datetime=message_datetime,
                        event_type_guess=event_type_guess,
                        price_eur=price_eur,
                    ),
                    "source_file": source_file,
                    "external_message_id": external_message_id,
                    "message_datetime": message_datetime,
                    "event_type_guess": event_type_guess,
                    "property_type_raw": _extract_property_type(raw_text),
                    "address_raw": address_raw,
                    "price_eur": price_eur,
                    "price_per_m2": _extract_price_per_m2(raw_text),
                    "area_m2": _extract_area(raw_text),
                    "bedrooms": _extract_bedrooms(raw_text),
                    "bathrooms": _extract_bathrooms(raw_text),
                    "listing_url": listing_url,
                    "source_portal": _extract_source_portal_from_text(raw_text) or ("Casafari" if listing_url else None),
                    "contact_phone_raw": _extract_phone(raw_text),
                    "contact_name_raw": _extract_contact_name(raw_text),
                    "owner_listing_count": _extract_owner_listing_count(raw_text),
                    "alert_name_raw": _extract_alert_name(raw_text),
                    "raw_text": raw_text,
                }
            )

    return parsed_alerts