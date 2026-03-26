from __future__ import annotations

from core.normalization.text import normalize_text, normalize_text_key


PORTAL_PATTERNS: list[tuple[str, tuple[str, ...], str]] = [
    ("idealista", ("idealista",), "Idealista"),
    ("fotocasa", ("fotocasa",), "Fotocasa"),
    ("habitaclia", ("habitaclia",), "Habitaclia"),
    ("milanuncios", ("milanuncios",), "Milanuncios"),
    ("pisos", ("pisos.com", "pisos com", "pisos"), "Pisos"),
    ("yaencontre", ("yaencontre",), "Yaencontre"),
]


def normalize_portal_key(value: str | None) -> str | None:
    text = normalize_text_key(value)
    if not text:
        return None

    for key, tokens, _label in PORTAL_PATTERNS:
        if any(token in text for token in tokens):
            return key

    head = text.split(":")[0].strip()
    return head or text


def canonicalize_portal_label(value: str | None) -> str | None:
    text = normalize_text(value)
    if not text:
        return None

    key = normalize_portal_key(text)
    if not key:
        return text

    for candidate_key, _tokens, label in PORTAL_PATTERNS:
        if candidate_key == key:
            return label

    return text.split(":")[0].strip() or text
