from urllib.parse import urlparse, urlunparse


def normalize_url(url: str | None) -> str | None:
    if not url:
        return None

    url = url.strip()
    if not url:
        return None

    parsed = urlparse(url)

    scheme = "https"
    netloc = parsed.netloc.lower().strip()

    if netloc.startswith("www."):
        netloc = netloc[4:]
    if netloc.startswith("es."):
        netloc = netloc[3:]

    path = parsed.path.rstrip("/")

    normalized = urlunparse((scheme, netloc, path, "", "", ""))
    return normalized or None