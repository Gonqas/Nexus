from __future__ import annotations

from math import cos, floor, radians

from core.features.zone_features import infer_zone_label_for_asset


MADRID_REF_LAT = 40.4168
MICROZONE_CELL_SIZE_M = 350.0
METERS_PER_DEG_LAT = 111_320.0
METERS_PER_DEG_LON = 111_320.0 * cos(radians(MADRID_REF_LAT))
_BASE36 = "0123456789abcdefghijklmnopqrstuvwxyz"


def _to_base36(value: int) -> str:
    if value == 0:
        return "0"

    digits: list[str] = []
    remaining = abs(int(value))
    while remaining:
        remaining, remainder = divmod(remaining, 36)
        digits.append(_BASE36[remainder])
    return "".join(reversed(digits))


def _signed_code(value: int) -> str:
    prefix = "p" if value >= 0 else "m"
    return f"{prefix}{_to_base36(value)}"


def microzone_indices(
    lat: float | None,
    lon: float | None,
    *,
    cell_size_m: float = MICROZONE_CELL_SIZE_M,
) -> tuple[int, int] | None:
    if lat is None or lon is None:
        return None
    if cell_size_m <= 0:
        return None

    x_idx = int(floor((float(lon) * METERS_PER_DEG_LON) / cell_size_m))
    y_idx = int(floor((float(lat) * METERS_PER_DEG_LAT) / cell_size_m))
    return x_idx, y_idx


def microzone_cell_code(
    lat: float | None,
    lon: float | None,
    *,
    cell_size_m: float = MICROZONE_CELL_SIZE_M,
) -> str | None:
    indices = microzone_indices(lat, lon, cell_size_m=cell_size_m)
    if indices is None:
        return None
    x_idx, y_idx = indices
    return f"{_signed_code(x_idx)}-{_signed_code(y_idx)}"


def microzone_centroid(
    lat: float | None,
    lon: float | None,
    *,
    cell_size_m: float = MICROZONE_CELL_SIZE_M,
) -> tuple[float, float] | None:
    indices = microzone_indices(lat, lon, cell_size_m=cell_size_m)
    if indices is None:
        return None

    x_idx, y_idx = indices
    center_lon = ((x_idx + 0.5) * cell_size_m) / METERS_PER_DEG_LON
    center_lat = ((y_idx + 0.5) * cell_size_m) / METERS_PER_DEG_LAT
    return round(center_lat, 6), round(center_lon, 6)


def microzone_label(
    parent_zone_label: str | None,
    lat: float | None,
    lon: float | None,
    *,
    cell_size_m: float = MICROZONE_CELL_SIZE_M,
) -> str | None:
    cell_code = microzone_cell_code(lat, lon, cell_size_m=cell_size_m)
    if not cell_code:
        return None

    parent_zone = parent_zone_label or "Sin zona"
    return f"{parent_zone} / MZ {cell_code}"


def infer_microzone_for_asset(asset) -> str | None:
    if asset is None:
        return None

    if getattr(asset, "lat", None) is None or getattr(asset, "lon", None) is None:
        return None

    parent_zone = infer_zone_label_for_asset(asset)
    return microzone_label(parent_zone, asset.lat, asset.lon)
