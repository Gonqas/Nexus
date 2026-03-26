from core.normalization.portals import canonicalize_portal_label, normalize_portal_key


def test_portal_labels_are_canonicalized_from_csv_and_casafari_variants() -> None:
    assert canonicalize_portal_label("Fotocasa for Sale") == "Fotocasa"
    assert canonicalize_portal_label("Idealista: Hector") == "Idealista"
    assert canonicalize_portal_label("Pisos.com") == "Pisos"
    assert canonicalize_portal_label("Yaencontre") == "Yaencontre"


def test_portal_keys_match_across_noisy_variants() -> None:
    assert normalize_portal_key("Fotocasa : Anuncio Particular") == "fotocasa"
    assert normalize_portal_key("idealista: maria") == "idealista"
    assert normalize_portal_key("Pisos.com") == "pisos"
