from core.connectors.casafari_history_connector import (
    normalize_network_item,
    parse_history_page,
)


def test_normalize_network_item_ignores_onboarding_noise() -> None:
    record = {
        "url": "https://www.casafari.com/account/starting-page",
        "title": "What would you like to do first?",
        "description": "modal userguiding faq___ property sourcing",
    }

    item = normalize_network_item(
        record,
        page_url="https://es.casafari.com/account/history",
        payload_url="https://es.casafari.com/graphql",
        page_number=1,
    )

    assert item is None


def test_parse_history_page_extracts_real_cards() -> None:
    html = """
    <html><body>
      <a href="https://www.idealista.com/inmueble/123/">
        Nuevo piso en venta en calle de Bravo Murillo, 295, Valdeacederas - Barrio
      </a>
      <div>
        26 mar 2026
        800.000 €
        Idealista: Leticia
        +34 646 55 90 92
      </div>
    </body></html>
    """

    items = parse_history_page(html, "https://es.casafari.com/account/history", 1)

    assert len(items) == 1
    assert items[0]["portal"] == "Idealista"
    assert items[0]["event_type_guess"] == "listing_detected"
