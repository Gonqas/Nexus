import re
import unicodedata


def normalize_text(value: str | None) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_text_key(value: str | None) -> str | None:
    text = normalize_text(value)
    if not text:
        return None

    text = text.lower()
    text = "".join(
        c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn"
    )
    text = re.sub(r"[^a-z0-9\s,.-]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text