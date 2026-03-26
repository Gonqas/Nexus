from core.geography.madrid_street_catalog import parse_address_text
from core.normalization.addresses import (
    extract_address_core,
    normalize_address_key,
    normalize_address_raw,
)


def test_normalize_address_raw_canonicalizes_common_street_abbreviations() -> None:
    assert (
        normalize_address_raw("C/ Alcala 123, 3o B, 28009 Madrid")
        == "calle alcala, 123"
    )


def test_normalize_address_raw_drops_secondary_unit_noise() -> None:
    assert (
        normalize_address_raw("Avda. de America 45 portal 2, Madrid")
        == "avenida america, 45"
    )


def test_normalize_address_key_converges_equivalent_address_spellings() -> None:
    assert normalize_address_key("C/ Alcala 123, 3o B, 28009 Madrid") == normalize_address_key(
        "Calle de Alcala numero 123"
    )


def test_parse_address_text_keeps_street_name_without_house_number_noise() -> None:
    parsed = parse_address_text("C/ Alcala 123, 3o B, 28009 Madrid")

    assert parsed.street_type == "calle"
    assert parsed.street_name == "alcala"
    assert parsed.house_number == "123"


def test_extract_address_core_keeps_clean_neighborhood_when_no_street_exists() -> None:
    assert extract_address_core("Lavapies - Madrid") == "lavapies"
