from core.normalization.text import normalize_text_key


PROPERTY_TYPE_MAP: dict[str, tuple[str, str]] = {
    "piso": ("residential_standard", "piso"),
    "apartamento": ("residential_standard", "apartamento"),
    "estudio": ("residential_standard", "estudio"),
    "atico": ("residential_standard", "atico"),
    "duplex": ("residential_standard", "duplex"),
    "chalet": ("residential_standard", "chalet"),
    "casa": ("residential_standard", "casa"),
    "local": ("retail_office", "local"),
    "oficina": ("retail_office", "oficina"),
    "garaje": ("other", "garaje"),
    "trastero": ("other", "trastero"),
}


def normalize_property_type(raw_value: str | None) -> tuple[str | None, str | None]:
    key = normalize_text_key(raw_value)
    if not key:
        return None, None

    return PROPERTY_TYPE_MAP.get(key, ("other", key))