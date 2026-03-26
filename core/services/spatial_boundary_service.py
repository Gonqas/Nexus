from __future__ import annotations

import json
import urllib.request
from functools import lru_cache
from pathlib import Path
from typing import Any


BOUNDARY_SOURCES = {
    "districts": {
        "url": "https://datos.madrid.es/dataset/900012-0-limites-administrativos-mapas/resource/900012-3-limites-administrativos-mapas/download/900012-3-limites-administrativos-mapas.json",
        "cache_name": "madrid_districts.topojson",
        "object_name": "DISTRITOS",
        "label_key": "NOMBRE",
        "parent_key": None,
    },
    "neighborhoods": {
        "url": "https://datos.madrid.es/dataset/900012-0-limites-administrativos-mapas/resource/900012-4-limites-administrativos-mapas/download/900012-4-limites-administrativos-mapas.json",
        "cache_name": "madrid_neighborhoods.topojson",
        "object_name": "BARRIOS",
        "label_key": "NOMBRE",
        "parent_key": "NOMDIS",
    },
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _cache_dir() -> Path:
    return _repo_root() / "data" / "processed" / "spatial_boundaries"


def _cache_path(level: str) -> Path:
    meta = BOUNDARY_SOURCES[level]
    return _cache_dir() / meta["cache_name"]


def _download_boundary(level: str) -> dict[str, Any]:
    meta = BOUNDARY_SOURCES[level]
    with urllib.request.urlopen(meta["url"], timeout=45) as response:
        return json.load(response)


@lru_cache(maxsize=4)
def load_official_boundary_topology(level: str) -> dict[str, Any] | None:
    if level not in BOUNDARY_SOURCES:
        raise ValueError(f"Unsupported boundary level: {level}")

    cache_path = _cache_path(level)
    try:
        if cache_path.exists():
            return json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        pass

    try:
        payload = _download_boundary(level)
    except Exception:
        return None

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )
    return payload


def get_boundary_layer_meta(level: str) -> dict[str, Any]:
    meta = BOUNDARY_SOURCES[level]
    return {
        "level": level,
        "object_name": meta["object_name"],
        "label_key": meta["label_key"],
        "parent_key": meta["parent_key"],
    }
