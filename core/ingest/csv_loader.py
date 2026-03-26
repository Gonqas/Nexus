from pathlib import Path

import polars as pl

REQUIRED_LEADS_COLUMNS = [
    "Número de teléfono",
    "Nombre",
    "Tipo de propiedad",
    "Fuente",
    "Dirección",
    "Precio",
    "Estado del anuncio",
    "Área construida",
    "Página de la propiedad",
    "Página del anuncio",
    "Fecha de creación del lead",
]


def load_leads_csv(csv_path: str | Path) -> list[dict]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"No existe el CSV: {path}")

    df = pl.read_csv(
        path,
        infer_schema_length=1000,
        encoding="utf8-lossy",
        ignore_errors=True,
    )

    missing = [col for col in REQUIRED_LEADS_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            "El CSV no tiene las columnas esperadas. "
            f"Faltan: {', '.join(missing)}"
        )

    return df.to_dicts()