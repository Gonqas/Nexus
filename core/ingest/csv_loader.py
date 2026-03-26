from pathlib import Path

import polars as pl
from core.normalization.text import normalize_text_key

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

HEADER_ALIASES = {
    "numero de telefono": "Número de teléfono",
    "telefono": "Número de teléfono",
    "telefono de contacto": "Número de teléfono",
    "nombre": "Nombre",
    "tipo de propiedad": "Tipo de propiedad",
    "fuente": "Fuente",
    "direccion": "Dirección",
    "precio": "Precio",
    "estado del anuncio": "Estado del anuncio",
    "area construida": "Área construida",
    "superficie construida": "Área construida",
    "pagina de la propiedad": "Página de la propiedad",
    "url propiedad": "Página de la propiedad",
    "pagina del anuncio": "Página del anuncio",
    "url anuncio": "Página del anuncio",
    "fecha de creacion del lead": "Fecha de creación del lead",
    "fecha lead": "Fecha de creación del lead",
}


def _rename_columns(df: pl.DataFrame) -> pl.DataFrame:
    rename_map: dict[str, str] = {}

    for column in df.columns:
        key = normalize_text_key(column)
        if not key:
            continue
        target = HEADER_ALIASES.get(key)
        if target and column != target:
            rename_map[column] = target

    if rename_map:
        df = df.rename(rename_map)

    return df


def _read_csv_with_best_separator(path: Path) -> pl.DataFrame:
    separators = [",", ";", "\t", "|"]
    best_df: pl.DataFrame | None = None
    best_score = -1

    for separator in separators:
        try:
            df = pl.read_csv(
                path,
                separator=separator,
                infer_schema_length=1000,
                encoding="utf8-lossy",
                ignore_errors=True,
            )
        except Exception:
            continue

        df = _rename_columns(df)
        score = sum(1 for col in REQUIRED_LEADS_COLUMNS if col in df.columns)
        if score > best_score:
            best_df = df
            best_score = score

        if score == len(REQUIRED_LEADS_COLUMNS):
            return df

    if best_df is None:
        raise ValueError(f"No se pudo leer el fichero tabular: {path.name}")

    return best_df


def _read_tabular_file(path: Path) -> pl.DataFrame:
    suffix = path.suffix.lower()

    if suffix == ".csv":
        return _read_csv_with_best_separator(path)

    if suffix in {".xlsx", ".xls"}:
        try:
            df = pl.read_excel(path)
        except Exception as exc:
            raise ValueError(
                "No se pudo leer el Excel. Instala soporte de Excel para polars "
                "(por ejemplo `fastexcel`) o convierte el fichero a CSV."
            ) from exc
        return _rename_columns(df)

    raise ValueError(f"Formato no soportado: {path.suffix}")


def load_leads_csv(csv_path: str | Path) -> list[dict]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"No existe el CSV: {path}")

    df = _read_tabular_file(path)

    missing = [col for col in REQUIRED_LEADS_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            "El fichero no tiene las columnas esperadas. "
            f"Faltan: {', '.join(missing)}"
        )

    return df.to_dicts()
