from pathlib import Path

from core.ingest.csv_loader import REQUIRED_LEADS_COLUMNS, load_leads_csv


def test_csv_loader_detects_semicolon_separator_and_header_aliases(tmp_path: Path) -> None:
    csv_path = tmp_path / "baseline_semicolon.csv"
    csv_path.write_text(
        "\n".join(
            [
                "telefono;nombre;tipo de propiedad;fuente;direccion;precio;estado del anuncio;superficie construida;url propiedad;url anuncio;fecha lead",
                "699111222;Ana;Piso;Fotocasa for Sale;Calle Mayor 10;350000;Activo;85;https://portal.test/propiedad;https://portal.test/anuncio;2026-03-20",
            ]
        ),
        encoding="utf-8",
    )

    rows = load_leads_csv(csv_path)

    assert len(rows) == 1
    row = rows[0]
    assert row[REQUIRED_LEADS_COLUMNS[0]] == 699111222
    assert row[REQUIRED_LEADS_COLUMNS[3]] == "Fotocasa for Sale"
    assert row[REQUIRED_LEADS_COLUMNS[4]] == "Calle Mayor 10"
