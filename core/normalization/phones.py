import re


def normalize_phone(value: str | None) -> str | None:
    if value is None:
        return None

    raw = str(value).strip()
    if not raw:
        return None

    raw = raw.replace(" ", "").replace("-", "")
    raw = re.sub(r"[^\d+]", "", raw)

    if raw.startswith("00"):
        raw = "+" + raw[2:]

    if raw.startswith("+"):
        digits = "+" + re.sub(r"\D", "", raw[1:])
        return digits if len(digits) >= 8 else None

    digits = re.sub(r"\D", "", raw)
    if not digits:
        return None

    if len(digits) == 9:
        return f"+34{digits}"

    if len(digits) >= 8:
        return digits

    return None 