from __future__ import annotations

import re
from typing import Any

from sqlalchemy.orm import Session

from core.normalization.text import normalize_text_key
from core.services.casafari_links_service import list_casafari_links
from core.services.opportunity_queue_service_v2 import get_opportunity_queue_v2
from core.services.search_service import search_payload
from core.services.zone_intelligence_service_v2 import get_zone_intelligence_v2


STOPWORDS = {
    "que",
    "cual",
    "cuales",
    "donde",
    "hay",
    "las",
    "los",
    "del",
    "para",
    "con",
    "sin",
    "una",
    "uno",
    "unas",
    "unos",
    "quiero",
    "ver",
    "muestrame",
    "ensename",
    "top",
    "barrios",
    "barrios?",
    "zonas",
    "zona",
    "casos",
    "casafari",
    "oportunidades",
}


def _clean_query(query: str | None) -> str:
    return normalize_text_key(query) or ""


def _extract_limit(query_key: str, default: int = 5, max_limit: int = 10) -> int:
    match = re.search(r"\b(\d{1,2})\b", query_key)
    if not match:
        return default
    return max(1, min(int(match.group(1)), max_limit))


def _detect_intent(query_key: str) -> tuple[str, dict[str, Any]]:
    if not query_key:
        return "empty", {}

    if any(term in query_key for term in ("weak identity", "sin resolver", "matching", "review", "revisar")):
        focus = "all"
        if "weak identity" in query_key:
            focus = "weak_identity"
        elif "precio" in query_key and "conflict" in query_key:
            focus = "price_conflict"
        elif "telefono" in query_key:
            focus = "repeated_phone"
        elif "direccion" in query_key:
            focus = "poor_address"
        else:
            focus = "review_needed"
        return "casafari_review", {"focus_filter": focus}

    if any(term in query_key for term in ("transformacion", "cambio de uso", "locales cerrados", "vut")):
        return "zone_transformation", {}

    if any(term in query_key for term in ("confianza baja", "sin zona", "poco fiable", "poca confianza")):
        return "zone_confidence", {}

    if any(term in query_key for term in ("prediccion", "30d", "absorcion", "proximas semanas")):
        return "zone_predictive", {}

    if any(term in query_key for term in ("captacion", "captar", "captable")):
        return "zone_capture", {}

    if any(term in query_key for term in ("calor", "caliente", "actividad", "presion")):
        return "zone_heat", {}

    if any(term in query_key for term in ("oportunidad", "oportunidades", "bajada de precio", "entrada nueva", "price drop")):
        event_type = "all"
        if "bajada de precio" in query_key or "price drop" in query_key:
            event_type = "price_drop"
        elif "entrada nueva" in query_key:
            event_type = "listing_detected"
        return "opportunities", {"event_type": event_type}

    return "search_fallback", {}


def _extract_subject_hint(query_key: str) -> str | None:
    tokens = [token for token in re.split(r"\s+", query_key) if token and token not in STOPWORDS]
    if not tokens:
        return None
    return " ".join(tokens[:4])


def _filter_rows_by_hint(rows: list[dict], hint: str | None, *keys: str) -> list[dict]:
    if not hint:
        return rows
    filtered: list[dict] = []
    for row in rows:
        haystack = " ".join(str(row.get(key) or "") for key in keys)
        haystack_key = normalize_text_key(haystack) or ""
        if hint in haystack_key:
            filtered.append(row)
    return filtered or rows


def _build_zone_suggestions(rows: list[dict], limit: int) -> list[dict[str, str]]:
    suggestions: list[dict[str, str]] = []
    for row in rows[:limit]:
        suggestions.append(
            {
                "tipo": "Zona",
                "item": str(row.get("zone_label") or "-"),
                "por_que": str(row.get("ai_brief") or row.get("executive_summary") or "-"),
                "accion": str(row.get("ai_next_step") or row.get("recommended_action") or "-"),
                "target_view": "radar",
                "zone_label": str(row.get("zone_label") or ""),
            }
        )
    return suggestions


def _build_opportunity_suggestions(rows: list[dict], limit: int) -> list[dict[str, str]]:
    suggestions: list[dict[str, str]] = []
    for row in rows[:limit]:
        label = row.get("asset_address") or row.get("zone_label") or f"Evento {row.get('event_id')}"
        suggestions.append(
            {
                "tipo": "Oportunidad",
                "item": str(label),
                "por_que": str(row.get("ai_brief") or row.get("reason") or "-"),
                "accion": str(row.get("ai_next_step") or "-"),
                "target_view": "queue",
                "event_id": row.get("event_id"),
                "zone_label": str(row.get("zone_label") or ""),
                "microzone_label": str(row.get("microzone_label") or ""),
            }
        )
    return suggestions


def _build_casafari_suggestions(rows: list[dict], limit: int) -> list[dict[str, str]]:
    suggestions: list[dict[str, str]] = []
    for row in rows[:limit]:
        label = row.get("address_raw") or row.get("portal") or f"Raw {row.get('raw_history_item_id')}"
        suggestions.append(
            {
                "tipo": "Casafari",
                "item": str(label),
                "por_que": str(row.get("ai_brief") or row.get("reason_taxonomy") or "-"),
                "accion": str(row.get("ai_next_step") or "-"),
                "target_view": "casafari",
                "query_text": str(row.get("address_raw") or row.get("contact_phone") or ""),
                "focus_filter": str(row.get("reason_taxonomy") or ""),
            }
        )
    return suggestions


def run_copilot_query(session: Session, query: str, *, default_limit: int = 5) -> dict[str, Any]:
    query_raw = (query or "").strip()
    query_key = _clean_query(query_raw)
    limit = _extract_limit(query_key, default=default_limit)
    intent, options = _detect_intent(query_key)
    hint = _extract_subject_hint(query_key)

    empty_answer = {
        "query": query_raw,
        "intent": "empty",
        "title": "Haz una pregunta corta",
        "answer": "Puedes preguntar por barrios calientes, zonas con transformacion, oportunidades o casos Casafari sin resolver.",
        "next_step": "Prueba algo como: 'barrios con transformacion', 'oportunidades con bajada de precio' o 'casafari weak identity'.",
        "suggestions": [],
        "search_payload": None,
    }
    if not query_key:
        return empty_answer

    if intent.startswith("zone_"):
        rows = get_zone_intelligence_v2(session, window_days=14)
        rows = _filter_rows_by_hint(rows, hint, "zone_label", "executive_summary", "ai_summary")

        if intent == "zone_transformation":
            rows = sorted(rows, key=lambda row: float(row.get("zone_transformation_signal_score") or 0.0), reverse=True)
            title = "Zonas con transformacion"
            answer = "Estas zonas concentran mas senal urbana y de cambio de uso ahora mismo."
        elif intent == "zone_confidence":
            rows = sorted(rows, key=lambda row: float(row.get("zone_confidence_score") or 0.0))
            title = "Zonas con lectura fragil"
            answer = "Estas zonas necesitan limpieza de dato o mejor cobertura geografica antes de confiar del todo."
        elif intent == "zone_predictive":
            rows = sorted(rows, key=lambda row: float(row.get("predicted_absorption_30d_score") or 0.0), reverse=True)
            title = "Zonas con mejor lectura 30d"
            answer = "Estas zonas tienen mejor combinacion actual de liquidez, actividad y confianza para las proximas semanas."
        elif intent == "zone_capture":
            rows = sorted(rows, key=lambda row: float(row.get("zone_capture_score") or 0.0), reverse=True)
            title = "Zonas para captar"
            answer = "Estas zonas salen mejor paradas para captacion con la lectura actual del sistema."
        else:
            rows = sorted(
                rows,
                key=lambda row: (
                    float(row.get("zone_heat_score") or 0.0),
                    float(row.get("zone_relative_heat_score") or 0.0),
                ),
                reverse=True,
            )
            title = "Zonas con mas actividad"
            answer = "Estas zonas tienen mas movimiento reciente y merecen comparacion directa."

        return {
            "query": query_raw,
            "intent": intent,
            "title": title,
            "answer": answer,
            "next_step": "Abre la zona que mas te encaje y bajala luego al mapa o a la cola operativa.",
            "suggestions": _build_zone_suggestions(rows, limit),
            "search_payload": None,
        }

    if intent == "opportunities":
        rows = get_opportunity_queue_v2(session, window_days=14, limit=200)
        event_type = options.get("event_type") or "all"
        if event_type != "all":
            rows = [row for row in rows if row.get("event_type") == event_type]
        rows = _filter_rows_by_hint(rows, hint, "asset_address", "zone_label", "microzone_label", "ai_summary")

        return {
            "query": query_raw,
            "intent": intent,
            "title": "Oportunidades recomendadas",
            "answer": "He priorizado las oportunidades que mejor encajan con tu pregunta usando evento, zona, microzona y ventana 30d.",
            "next_step": "Valida primero las de prioridad alta y abre en mapa las que dependan mucho de zona o microzona.",
            "suggestions": _build_opportunity_suggestions(rows, limit),
            "search_payload": None,
        }

    if intent == "casafari_review":
        rows = list_casafari_links(
            session,
            status_filter="all",
            focus_filter=str(options.get("focus_filter") or "review_needed"),
            query_text=hint or query_raw,
            limit=max(limit * 4, 20),
        )
        return {
            "query": query_raw,
            "intent": intent,
            "title": "Casos Casafari a revisar",
            "answer": "He filtrado los casos que mas probablemente necesitan una decision manual o una validacion extra.",
            "next_step": "Empieza por los casos con identidad debil o conflicto de precio antes de relanzar matching.",
            "suggestions": _build_casafari_suggestions(rows, limit),
            "search_payload": None,
        }

    payload = search_payload(session, query=query_raw, section_filter="all", limit_per_section=limit)
    total = int((payload.get("summary") or {}).get("total") or 0)
    return {
        "query": query_raw,
        "intent": "search_fallback",
        "title": "Busqueda transversal",
        "answer": f"No he detectado una intencion operativa clara, asi que he lanzado una busqueda general y he encontrado {total} resultados.",
        "next_step": "Si quieres una respuesta mas guiada, pregunta por zonas, oportunidades o Casafari con una frase mas concreta.",
        "suggestions": [],
        "search_payload": payload,
    }
