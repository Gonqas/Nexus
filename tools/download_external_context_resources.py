from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import Request, urlopen

import orjson

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from core.discovery.external_context_catalog import (
    DOWNLOADABLE_FORMATS,
    REQUEST_HEADERS,
    safe_slug,
    select_download_candidates,
)
from core.services.external_context_service import load_external_context_focus_catalog


RAW_DOWNLOAD_DIR = BASE_DIR / "data" / "raw" / "external_context" / "resources"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Descarga una tanda de recursos del catalogo externo.")
    parser.add_argument("--theme", default=None)
    parser.add_argument("--portal-id", default=None)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--max-size-mb", type=float, default=25.0)
    parser.add_argument("--formats", default=",".join(sorted(DOWNLOADABLE_FORMATS)))
    return parser.parse_args()


def ensure_dirs() -> None:
    RAW_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


def _head_size(url: str) -> int | None:
    try:
        request = Request(url, method="HEAD", headers=REQUEST_HEADERS)
        with urlopen(request, timeout=30) as response:
            value = response.headers.get("Content-Length")
        return int(value) if value else None
    except Exception:
        return None


def _extension(resource: dict) -> str:
    fmt = str(resource.get("format") or "").strip().lower()
    if fmt:
        return fmt
    url = str(resource.get("url") or "")
    suffix = Path(url.split("?")[0]).suffix.lower().strip(".")
    return suffix or "bin"


def _download(url: str, destination: Path) -> int:
    last_error = None
    for attempt in range(3):
        try:
            request = Request(url, headers=REQUEST_HEADERS)
            with urlopen(request, timeout=180) as response:
                payload = response.read()
            destination.write_bytes(payload)
            return len(payload)
        except Exception as exc:
            last_error = exc
    raise last_error or RuntimeError(f"Download failed: {url}")


def main() -> None:
    args = parse_args()
    ensure_dirs()

    allowed_formats = {
        chunk.strip().lower()
        for chunk in str(args.formats or "").split(",")
        if chunk.strip()
    }
    focus_catalog = load_external_context_focus_catalog()
    candidates = select_download_candidates(
        focus_catalog,
        theme=args.theme,
        portal_id=args.portal_id,
        limit=args.limit,
        allowed_formats=allowed_formats,
    )

    manifest = {
        "generated_at": None,
        "theme": args.theme,
        "portal_id": args.portal_id,
        "limit": args.limit,
        "max_size_mb": args.max_size_mb,
        "downloads": [],
        "skipped": [],
    }

    for item in candidates:
        resource = item["resource"]
        url = str(resource.get("url") or "")
        size_bytes = resource.get("size_bytes") or _head_size(url)
        if size_bytes is not None and size_bytes > int(args.max_size_mb * 1024 * 1024):
            manifest["skipped"].append(
                {
                    "reason": "size_limit",
                    "dataset_title": item["dataset_title"],
                    "url": url,
                    "size_bytes": size_bytes,
                }
            )
            continue

        theme = item["primary_theme"] or "other"
        portal_id = item["portal_id"] or "portal"
        folder = RAW_DOWNLOAD_DIR / theme / portal_id
        folder.mkdir(parents=True, exist_ok=True)

        extension = _extension(resource)
        filename = f"{safe_slug(item['slug'])}.{extension}"
        destination = folder / filename
        try:
            bytes_written = _download(url, destination)
        except Exception as exc:
            if destination.exists():
                destination.unlink(missing_ok=True)
            manifest["skipped"].append(
                {
                    "reason": "download_error",
                    "dataset_title": item["dataset_title"],
                    "url": url,
                    "error": str(exc),
                }
            )
            continue

        manifest["downloads"].append(
            {
                "dataset_title": item["dataset_title"],
                "dataset_url": item["dataset_url"],
                "portal_id": item["portal_id"],
                "primary_theme": theme,
                "resource_name": resource.get("name"),
                "resource_url": url,
                "saved_path": str(destination),
                "bytes_written": bytes_written,
            }
        )

    generated_at = datetime.now(UTC)
    manifest["generated_at"] = generated_at.isoformat()
    manifest_path = RAW_DOWNLOAD_DIR / f"download_manifest_{generated_at.strftime('%Y%m%dT%H%M%SZ')}.json"
    latest_manifest_path = RAW_DOWNLOAD_DIR / "download_manifest_latest.json"
    payload = orjson.dumps(manifest, option=orjson.OPT_INDENT_2)
    manifest_path.write_bytes(payload)
    latest_manifest_path.write_bytes(payload)

    print(f"downloads={len(manifest['downloads'])}")
    print(f"skipped={len(manifest['skipped'])}")
    print(f"manifest={manifest_path}")


if __name__ == "__main__":
    main()
