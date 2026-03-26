from __future__ import annotations

import orjson

import core.services.external_zone_context_service as external_zone_context_service


def test_get_zone_external_context_resolves_neighborhood_and_district(tmp_path, monkeypatch) -> None:
    path = tmp_path / "zone_context.json"
    path.write_bytes(
        orjson.dumps(
            {
                "districts": {
                    "centro": {
                        "zone_label": "Centro",
                        "zone_level": "district",
                        "population": 145411,
                    }
                },
                "neighborhoods": {
                    "sol": {
                        "zone_label": "Sol",
                        "zone_level": "neighborhood",
                        "district_label": "Centro",
                        "population": 7093,
                    }
                },
            }
        )
    )

    monkeypatch.setattr(external_zone_context_service, "ZONE_CONTEXT_PATH", path)
    monkeypatch.setattr(external_zone_context_service, "_CACHE_SIGNATURE", None)
    monkeypatch.setattr(external_zone_context_service, "_CACHE_PAYLOAD", None)

    sol = external_zone_context_service.get_zone_external_context("Sol")
    centro = external_zone_context_service.get_zone_external_context("Centro")

    assert sol["zone_level"] == "neighborhood"
    assert sol["population"] == 7093
    assert centro["zone_level"] == "district"
    assert centro["population"] == 145411
