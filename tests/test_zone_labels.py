from core.features.location_labels import canonical_zone_label, is_official_zone_label
from core.features.zone_features import infer_zone_label


def test_street_name_is_not_accepted_as_zone() -> None:
    assert canonical_zone_label("calle Alcala") is None
    assert infer_zone_label("calle Alcala") == "Sin zona"


def test_known_neighborhood_is_preserved_as_zone() -> None:
    assert is_official_zone_label("Guindalera") is True
    assert canonical_zone_label("Guindalera") == "Guindalera"
    assert infer_zone_label("Guindalera") == "Guindalera"
