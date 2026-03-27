from __future__ import annotations

import re
from typing import Any

from rapidfuzz import fuzz, process
from sqlalchemy.orm import Session

from core.normalization.text import normalize_text_key
from core.services.casafari_links_service import list_casafari_links
from core.services.copilot_llm_service import maybe_enhance_copilot_payload
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

ACTION_LEXICONS = {
    "action_reconcile": (
        "reconciliar pendientes",
        "reintenta matching",
        "resolver pendientes",
        "revisa y enlaza",
        "vuelve a enlazar",
    ),
    "action_prepare_session": (
        "preparar sesion",
        "prepara sesion",
        "login casafari",
        "iniciar sesion casafari",
        "dejar lista la sesion",
    ),
    "action_sync": (
        "sincroniza casafari",
        "sincronizar casafari",
        "actualiza casafari",
        "scrapea casafari",
        "trae el delta",
        "lee casafari",
    ),
    "action_reindex": (
        "reindexa",
        "reindexar",
        "actualiza indice",
        "reconstruye indice",
        "rehace el indice",
    ),
}

CASAFARI_FOCUS_LEXICONS = {
    "weak_identity": ("weak identity", "identidad debil", "identidad floja", "sin identidad"),
    "price_conflict": ("price conflict", "conflicto de precio", "precio conflictivo", "precio no cuadra"),
    "repeated_phone": ("telefono repetido", "telefonos repetidos", "mismo telefono", "telefono reciclado"),
    "poor_address": ("direccion pobre", "direccion floja", "direccion incompleta", "direccion mala"),
    "review_needed": ("sin resolver", "matching", "review", "revisar", "dudoso", "incierto"),
}

ZONE_MODE_LEXICONS = {
    "zone_transformation": ("transformacion", "cambio de uso", "locales cerrados", "vut", "transformadora"),
    "zone_confidence": ("confianza baja", "sin zona", "poco fiable", "poca confianza", "lectura fragil"),
    "zone_predictive": ("prediccion", "30d", "absorcion", "proximas semanas", "las proximas semanas"),
    "zone_capture": ("captacion", "captar", "captable", "merece captar", "entrar a captar"),
    "zone_heat": ("calor", "caliente", "actividad", "presion", "movimiento"),
}

ZONE_COMPARE_FOCUS_LEXICONS = {
    "capture": ("captacion", "captar", "captable", "comercial", "para captar"),
    "confidence": ("confianza", "fiable", "solidez", "calidad del dato", "seguridad", "lectura"),
    "transformation": ("transformacion", "cambio de uso", "locales cerrados", "vut", "transformadora"),
    "heat": ("calor", "actividad", "movimiento", "dinamica", "actividad relativa"),
    "pressure": ("presion", "saturacion", "competencia", "mercado tensionado"),
    "liquidity": ("liquidez", "absorcion", "salida", "rotacion"),
    "predictive": ("prediccion", "30d", "proximas semanas", "futuro cercano"),
}

OPPORTUNITY_EVENT_LEXICONS = {
    "price_drop": ("bajada de precio", "price drop", "rebaja", "descuento"),
    "listing_detected": ("entrada nueva", "nuevo anuncio", "nueva entrada", "listing detectado"),
}

KNOWN_PORTALS = ("idealista", "fotocasa", "pisos", "milanuncios")
STREET_TERMS = ("calle", "cl", "avenida", "av", "paseo", "plaza", "camino", "ronda")


def _tokens(query_key: str) -> set[str]:
    return {token for token in re.split(r"\s+", query_key) if token}


def _contains_any_phrase(query_key: str, phrases: tuple[str, ...]) -> bool:
    return any(phrase in query_key for phrase in phrases)


def _contains_all_tokens(query_key: str, *terms: str) -> bool:
    token_set = _tokens(query_key)
    for term in terms:
        if not term:
            continue
        term_tokens = [token for token in normalize_text_key(term).split() if token]
        if term_tokens and all(token in token_set for token in term_tokens):
            return True
    return False


def _score_lexicon(query_key: str, phrases: tuple[str, ...]) -> int:
    score = 0
    for phrase in phrases:
        if phrase in query_key:
            score += 3
            continue
        phrase_tokens = [token for token in normalize_text_key(phrase).split() if token]
        if phrase_tokens and all(token in _tokens(query_key) for token in phrase_tokens):
            score += 1
    return score


def _clean_query(query: str | None) -> str:
    return normalize_text_key(query) or ""


def _extract_limit(query_key: str, default: int = 5, max_limit: int = 10) -> int:
    match = re.search(r"\b(\d{1,2})\b", query_key)
    if not match:
        return default
    return max(1, min(int(match.group(1)), max_limit))


def _extract_phone(query_key: str) -> str | None:
    compact = re.sub(r"[^0-9+]", "", query_key)
    match = re.search(r"(?:\+34)?(\d{9})", compact)
    if not match:
        return None
    return match.group(1)


def _extract_address_fragment(query_key: str) -> str | None:
    if not any(term in query_key for term in STREET_TERMS):
        return None
    match = re.search(r"(calle|cl|avenida|av|paseo|plaza|camino|ronda)[a-z0-9\s.-]{3,}", query_key)
    if not match:
        return None
    return match.group(0).strip()


def _extract_portals(query_key: str) -> list[str]:
    return [portal for portal in KNOWN_PORTALS if portal in query_key]


def _match_zone_entities(
    session: Session,
    query_key: str,
    *,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = get_zone_intelligence_v2(session, window_days=14)
    labels = [str(row.get("zone_label") or "").strip() for row in rows if row.get("zone_label")]
    labels = list(dict.fromkeys(label for label in labels if label))
    if not labels:
        return {"resolved": [], "ambiguous": []}

    exact = [label for label in labels if (normalize_text_key(label) or "") in query_key]
    if exact:
        return {"resolved": exact[:2], "ambiguous": []}

    query_tokens = [token for token in _tokens(query_key) if len(token) >= 4]
    prefix_matches = []
    for label in labels:
        label_key = normalize_text_key(label) or ""
        label_tokens = [token for token in label_key.split() if token]
        if any(any(label_token.startswith(query_token) for label_token in label_tokens) for query_token in query_tokens):
            prefix_matches.append(label)
    prefix_matches = list(dict.fromkeys(prefix_matches))
    if len(prefix_matches) > 1:
        return {"resolved": [], "ambiguous": prefix_matches[:3]}
    if len(prefix_matches) == 1:
        return {"resolved": prefix_matches[:1], "ambiguous": []}

    selected_zone = str(((context or {}).get("selected_row") or {}).get("zone_label") or "").strip()
    choices = labels
    fuzzy = process.extract(
        query_key,
        choices,
        scorer=fuzz.WRatio,
        processor=lambda value: normalize_text_key(value) or "",
        limit=4,
    )
    fuzzy = [(label, score) for label, score, _ in fuzzy if score >= 72]
    if not fuzzy:
        return {"resolved": [selected_zone] if selected_zone and any(token in query_key for token in ("esta", "esta zona", "esta microzona")) else [], "ambiguous": []}

    best_score = fuzzy[0][1]
    close = [label for label, score in fuzzy if (best_score - score) <= 3]
    if len(close) > 1:
        return {"resolved": [], "ambiguous": close[:3]}
    return {"resolved": [fuzzy[0][0]], "ambiguous": []}


def _build_understanding(
    session: Session,
    query_raw: str,
    query_key: str,
    *,
    intent: str,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    zone_match = _match_zone_entities(session, query_key, context=context)
    phone = _extract_phone(query_key)
    address = _extract_address_fragment(query_key)
    portals = _extract_portals(query_key)

    entities: dict[str, Any] = {
        "zones": zone_match["resolved"],
        "ambiguous_zones": zone_match["ambiguous"],
        "phone": phone,
        "address": address,
        "portals": portals,
    }

    pieces: list[str] = []
    if intent and intent != "search_fallback":
        pieces.append(f"intencion {intent}")
    if zone_match["resolved"]:
        pieces.append(f"zona {zone_match['resolved'][0]}")
    if portals:
        pieces.append(f"portal {', '.join(portals)}")
    if phone:
        pieces.append(f"telefono {phone}")
    if address:
        pieces.append(f"direccion {address}")
    if intent == "zone_compare":
        pieces.append(f"foco {_zone_focus_label(_extract_zone_compare_focus(query_key))}")

    understanding_text = " | ".join(pieces) if pieces else "sin entidades claras"
    clarification_needed = bool(zone_match["ambiguous"])
    clarification_options = zone_match["ambiguous"][:3]
    confidence = "high"
    if clarification_needed:
        confidence = "low"
    elif not pieces:
        confidence = "medium"

    return {
        "entities": entities,
        "understanding_text": understanding_text,
        "clarification_needed": clarification_needed,
        "clarification_options": clarification_options,
        "confidence": confidence,
        "resolved_zone": zone_match["resolved"][0] if zone_match["resolved"] else None,
        "reformulated_query": query_raw,
    }


def _should_multi_intent(query_key: str, intents: list[tuple[str, dict[str, Any]]]) -> bool:
    if len(intents) < 2:
        return False
    return any(marker in query_key for marker in (" y ", " ademas ", " tambien ", " luego "))


def _detect_intent(query_key: str) -> tuple[str, dict[str, Any]]:
    if not query_key:
        return "empty", {}

    for action_id, phrases in ACTION_LEXICONS.items():
        if _score_lexicon(query_key, phrases) >= 3:
            return action_id, {}

    casafari_related = (
        "casafari" in query_key
        or _contains_any_phrase(query_key, ("weak identity", "price conflict"))
        or _contains_all_tokens(query_key, "sin resolver", "telefono repetido", "direccion pobre")
    )
    if casafari_related:
        focus = "all"
        best_score = 0
        for focus_key, phrases in CASAFARI_FOCUS_LEXICONS.items():
            score = _score_lexicon(query_key, phrases)
            if score > best_score:
                best_score = score
                focus = focus_key
        if best_score == 0:
            focus = "review_needed"
        return "casafari_review", {"focus_filter": focus}

    zone_best_intent = ""
    zone_best_score = 0
    for zone_intent, phrases in ZONE_MODE_LEXICONS.items():
        score = _score_lexicon(query_key, phrases)
        if score > zone_best_score:
            zone_best_score = score
            zone_best_intent = zone_intent
    if zone_best_intent:
        return zone_best_intent, {}

    if any(term in query_key for term in ("oportunidad", "oportunidades", "entrada", "anuncio", "precio")):
        event_type = "all"
        best_event_score = 0
        for candidate_event_type, phrases in OPPORTUNITY_EVENT_LEXICONS.items():
            score = _score_lexicon(query_key, phrases)
            if score > best_event_score:
                best_event_score = score
                event_type = candidate_event_type
        return "opportunities", {"event_type": event_type}

    return "search_fallback", {}


def _detect_all_intents(query_key: str) -> list[tuple[str, dict[str, Any]]]:
    found: list[tuple[str, dict[str, Any]]] = []

    for action_id, phrases in ACTION_LEXICONS.items():
        if _score_lexicon(query_key, phrases) >= 3:
            found.append((action_id, {}))

    casafari_related = (
        "casafari" in query_key
        or _contains_any_phrase(query_key, ("weak identity", "price conflict"))
        or _contains_all_tokens(query_key, "sin resolver", "telefono repetido", "direccion pobre")
    )
    if casafari_related:
        focus = "review_needed"
        best_score = 0
        for focus_key, phrases in CASAFARI_FOCUS_LEXICONS.items():
            score = _score_lexicon(query_key, phrases)
            if score > best_score:
                best_score = score
                focus = focus_key
        found.append(("casafari_review", {"focus_filter": focus}))

    zone_best_intent = ""
    zone_best_score = 0
    for zone_intent, phrases in ZONE_MODE_LEXICONS.items():
        score = _score_lexicon(query_key, phrases)
        if score > zone_best_score:
            zone_best_score = score
            zone_best_intent = zone_intent
    if zone_best_intent:
        found.append((zone_best_intent, {}))

    if any(term in query_key for term in ("oportunidad", "oportunidades", "entrada", "anuncio", "precio")):
        event_type = "all"
        best_event_score = 0
        for candidate_event_type, phrases in OPPORTUNITY_EVENT_LEXICONS.items():
            score = _score_lexicon(query_key, phrases)
            if score > best_event_score:
                best_event_score = score
                event_type = candidate_event_type
        found.append(("opportunities", {"event_type": event_type}))

    if not found:
        found.append(("search_fallback", {}))

    deduped: list[tuple[str, dict[str, Any]]] = []
    seen: set[str] = set()
    for intent, options in found:
        if intent in seen:
            continue
        seen.add(intent)
        deduped.append((intent, options))
    return deduped


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


def _build_action_suggestion(
    *,
    item: str,
    why: str,
    action: str,
    action_id: str,
    target_view: str,
) -> list[dict[str, str]]:
    return [
        {
            "tipo": "Accion",
            "item": item,
            "por_que": why,
            "accion": action,
            "action_id": action_id,
            "target_view": target_view,
        }
    ]


def safe_price_compare(value: object | None) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):,.0f} EUR".replace(",", ".")
    except (TypeError, ValueError):
        return str(value)


def _extract_zone_compare_focus(query_key: str) -> str:
    best_focus = "overall"
    best_score = 0
    for focus, phrases in ZONE_COMPARE_FOCUS_LEXICONS.items():
        score = _score_lexicon(query_key, phrases)
        if score > best_score:
            best_focus = focus
            best_score = score
    return best_focus


def _zone_focus_label(focus: str) -> str:
    return {
        "capture": "captacion",
        "confidence": "confianza",
        "transformation": "transformacion",
        "heat": "actividad",
        "pressure": "presion",
        "liquidity": "liquidez",
        "predictive": "prediccion 30d",
        "overall": "lectura global",
    }.get(focus, "lectura global")


def _zone_focus_score(row: dict[str, Any], focus: str) -> float:
    if focus == "capture":
        return float(row.get("zone_capture_score") or 0.0)
    if focus == "confidence":
        return float(row.get("zone_confidence_score") or 0.0)
    if focus == "transformation":
        return float(row.get("zone_transformation_signal_score") or 0.0)
    if focus == "heat":
        return float(row.get("zone_relative_heat_score") or row.get("zone_heat_score") or 0.0)
    if focus == "pressure":
        return float(row.get("zone_pressure_score") or 0.0)
    if focus == "liquidity":
        return float(row.get("zone_liquidity_score") or 0.0)
    if focus == "predictive":
        return float(row.get("predicted_absorption_30d_score") or 0.0)

    capture = float(row.get("zone_capture_score") or 0.0)
    confidence = float(row.get("zone_confidence_score") or 0.0)
    relative_heat = float(row.get("zone_relative_heat_score") or row.get("zone_heat_score") or 0.0)
    transformation = float(row.get("zone_transformation_signal_score") or 0.0)
    liquidity = float(row.get("zone_liquidity_score") or 0.0)
    return round(
        (0.36 * capture)
        + (0.22 * confidence)
        + (0.16 * relative_heat)
        + (0.14 * transformation)
        + (0.12 * liquidity),
        1,
    )


def _safe_percent(value: object | None) -> str:
    try:
        numeric = float(value or 0.0)
        if numeric > 1.0:
            numeric = 1.0
        if numeric < 0.0:
            numeric = 0.0
        return f"{numeric * 100:.0f}%"
    except (TypeError, ValueError):
        return "-"


def _safe_number(value: object | None, decimals: int = 1) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.{decimals}f}"
    except (TypeError, ValueError):
        return str(value)


def _zone_reason_bits(row: dict[str, Any], focus: str) -> list[str]:
    bits: list[str] = []

    if focus == "capture":
        bits.append(f"capture {float(row.get('zone_capture_score') or 0.0):.1f}")
        bits.append(f"heat relativo {float(row.get('zone_relative_heat_score') or row.get('zone_heat_score') or 0.0):.1f}")
        bits.append(f"confianza {float(row.get('zone_confidence_score') or 0.0):.1f}")
        if row.get("events_14d_per_10k_population") is not None:
            bits.append(f"{_safe_number(row.get('events_14d_per_10k_population'))} evt/10k hab")
        return bits

    if focus == "confidence":
        bits.append(f"confianza {float(row.get('zone_confidence_score') or 0.0):.1f}")
        bits.append(f"geo precisa {_safe_percent(row.get('geo_point_ratio'))}")
        bits.append(f"Casafari resuelto {_safe_percent(row.get('resolved_ratio'))}")
        if row.get("official_population"):
            bits.append("contexto oficial disponible")
        return bits

    if focus == "transformation":
        bits.append(f"transformacion {float(row.get('zone_transformation_signal_score') or 0.0):.1f}")
        bits.append(f"cambios de uso 24m {int(row.get('official_change_of_use_24m') or 0)}")
        bits.append(f"locales cerrados {int(row.get('official_closed_locales') or 0)}")
        bits.append(f"VUT {int(row.get('official_vut_units') or 0)}")
        return bits

    if focus == "heat":
        bits.append(f"heat {float(row.get('zone_heat_score') or 0.0):.1f}")
        bits.append(f"heat relativo {float(row.get('zone_relative_heat_score') or 0.0):.1f}")
        if row.get("events_14d_per_10k_population") is not None:
            bits.append(f"{_safe_number(row.get('events_14d_per_10k_population'))} evt/10k hab")
        bits.append(f"raw Casafari {int(row.get('casafari_raw_in_zone') or 0)}")
        return bits

    if focus == "pressure":
        bits.append(f"presion {float(row.get('zone_pressure_score') or 0.0):.1f}")
        bits.append(f"price drops {int(row.get('price_drop_count') or 0)}")
        bits.append(f"broker share {_safe_percent(row.get('broker_phone_share'))}")
        bits.append(f"listings activos {int(row.get('active_listings_count') or 0)}")
        return bits

    if focus == "liquidity":
        bits.append(f"liquidez {float(row.get('zone_liquidity_score') or 0.0):.1f}")
        bits.append(f"absorciones {int(row.get('absorption_count') or 0)}")
        if row.get("absorption_per_10k_population") is not None:
            bits.append(f"{_safe_number(row.get('absorption_per_10k_population'))} abs/10k hab")
        bits.append(f"nueva oferta {int(row.get('listing_detected_count') or 0)}")
        return bits

    if focus == "predictive":
        bits.append(f"prediccion 30d {float(row.get('predicted_absorption_30d_score') or 0.0):.1f}")
        bits.append(f"liquidez {float(row.get('zone_liquidity_score') or 0.0):.1f}")
        bits.append(f"confianza {float(row.get('zone_confidence_score') or 0.0):.1f}")
        bits.append(f"heat relativo {float(row.get('zone_relative_heat_score') or 0.0):.1f}")
        return bits

    bits.append(f"capture {float(row.get('zone_capture_score') or 0.0):.1f}")
    bits.append(f"confianza {float(row.get('zone_confidence_score') or 0.0):.1f}")
    bits.append(f"heat relativo {float(row.get('zone_relative_heat_score') or 0.0):.1f}")
    bits.append(f"transformacion {float(row.get('zone_transformation_signal_score') or 0.0):.1f}")
    if row.get("events_14d_per_10k_population") is not None:
        bits.append(f"{_safe_number(row.get('events_14d_per_10k_population'))} evt/10k hab")
    return bits


def _zone_source_summary(row: dict[str, Any]) -> str:
    sources: list[str] = []
    if any(row.get(key) for key in ("events_14d", "listing_detected_count", "absorption_count")):
        sources.append("mercado interno")
    if row.get("casafari_raw_in_zone") or row.get("resolved_ratio"):
        sources.append("Casafari")
    if row.get("geo_point_ratio") is not None:
        sources.append("geografia")
    if row.get("official_population") or row.get("official_change_of_use_24m") or row.get("official_vulnerability_index") is not None:
        sources.append("contexto oficial")
    sources = list(dict.fromkeys(sources))
    return ", ".join(sources) if sources else "senal disponible"


def _find_zone_rows_by_labels(rows: list[dict], labels: list[str]) -> list[dict]:
    label_keys = {normalize_text_key(label) or "" for label in labels if label}
    if not label_keys:
        return []
    return [
        row
        for row in rows
        if (normalize_text_key(str(row.get("zone_label") or "")) or "") in label_keys
    ]


def _find_opportunity_row_by_event(rows: list[dict], event_id: object | None) -> dict[str, Any] | None:
    for row in rows:
        if row.get("event_id") == event_id:
            return row
    return None


def _extract_compare_opportunity_candidates(
    rows: list[dict],
    query_key: str,
    context: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    selected_row = dict((context or {}).get("selected_row") or {})
    recent_rows = list((context or {}).get("recent_rows") or [])
    candidates: list[dict[str, Any]] = []

    if selected_row.get("target_view") == "queue":
        selected_event = selected_row.get("event_id")
        selected_match = _find_opportunity_row_by_event(rows, selected_event)
        if selected_match is not None:
            candidates.append(selected_match)

    for row in rows:
        if len(candidates) >= 2:
            break
        address_key = normalize_text_key(str(row.get("asset_address") or "")) or ""
        zone_key = normalize_text_key(str(row.get("zone_label") or "")) or ""
        portal_key = normalize_text_key(str(row.get("portal") or "")) or ""
        if (
            (address_key and address_key in query_key)
            or (zone_key and zone_key in query_key)
            or (portal_key and portal_key in query_key)
        ):
            if all(existing.get("event_id") != row.get("event_id") for existing in candidates):
                candidates.append(row)

    if "la otra" in query_key or "la de antes" in query_key or "la anterior" in query_key:
        for recent in recent_rows:
            event_id = recent.get("event_id")
            row = _find_opportunity_row_by_event(rows, event_id)
            if row and all(existing.get("event_id") != row.get("event_id") for existing in candidates):
                candidates.append(row)
            if len(candidates) >= 2:
                break

    if len(candidates) < 2:
        recent_matches: list[dict[str, Any]] = []
        for recent in recent_rows:
            event_id = recent.get("event_id")
            row = _find_opportunity_row_by_event(rows, event_id)
            if row and all(existing.get("event_id") != row.get("event_id") for existing in recent_matches):
                recent_matches.append(row)
            if len(recent_matches) >= 2:
                break
        for row in recent_matches:
            if all(existing.get("event_id") != row.get("event_id") for existing in candidates):
                candidates.append(row)
            if len(candidates) >= 2:
                break

    return candidates[:2]


def _extract_compare_labels(
    query_key: str,
    rows: list[dict],
    context: dict[str, Any] | None,
) -> list[str]:
    found: list[str] = []
    for row in rows:
        label = str(row.get("zone_label") or "").strip()
        label_key = normalize_text_key(label) or ""
        if label and label_key and label_key in query_key:
            found.append(label)

    if len(found) >= 2:
        return found[:2]

    selected_row = (context or {}).get("selected_row") or {}
    selected_zone = str(selected_row.get("zone_label") or "").strip()
    if selected_zone and all(normalize_text_key(selected_zone) != normalize_text_key(label) for label in found):
        found.insert(0, selected_zone)

    recent_labels = [str(label).strip() for label in ((context or {}).get("recent_zone_labels") or []) if label]
    for label in recent_labels:
        if len(found) >= 2:
            break
        if all(normalize_text_key(label) != normalize_text_key(existing) for existing in found):
            found.append(label)

    return found[:2]


def _build_zone_compare_payload(
    row_a: dict[str, Any],
    row_b: dict[str, Any],
    *,
    focus: str = "overall",
) -> dict[str, Any]:
    label_a = str(row_a.get("zone_label") or "Zona A")
    label_b = str(row_b.get("zone_label") or "Zona B")
    score_a = _zone_focus_score(row_a, focus)
    score_b = _zone_focus_score(row_b, focus)
    focus_label = _zone_focus_label(focus)

    if score_a >= score_b:
        stronger_label, stronger_row, stronger_score = label_a, row_a, score_a
        weaker_label, weaker_row, weaker_score = label_b, row_b, score_b
    else:
        stronger_label, stronger_row, stronger_score = label_b, row_b, score_b
        weaker_label, weaker_row, weaker_score = label_a, row_a, score_a

    stronger_bits = ", ".join(_zone_reason_bits(stronger_row, focus))
    weaker_bits = ", ".join(_zone_reason_bits(weaker_row, focus))
    stronger_sources = _zone_source_summary(stronger_row)
    weaker_sources = _zone_source_summary(weaker_row)
    if stronger_sources == weaker_sources:
        source_line = f"Lo baso en {stronger_sources}."
    else:
        source_line = f"Lo baso en {stronger_sources} y {weaker_sources} cuando hay dato disponible."

    answer = (
        f"{stronger_label} sale mejor que {weaker_label} en {focus_label} "
        f"({stronger_score:.1f} frente a {weaker_score:.1f}). "
        f"En {stronger_label} pesan {stronger_bits}. "
        f"En {weaker_label} la lectura queda por detras porque hoy marca {weaker_bits}. "
        f"{source_line}"
    )

    if focus == "confidence":
        next_step = (
            f"Si vas a decidir pronto, confia antes en {stronger_label}. "
            f"Para {weaker_label}, limpiaria geo y matching antes de apoyarme demasiado en la lectura."
        )
    elif focus == "transformation":
        next_step = (
            f"Si buscas senal urbana y nuevas oportunidades, empieza por {stronger_label} "
            f"y baja luego al mapa para validar microzonas y calles concretas."
        )
    else:
        next_step = (
            f"Si tu foco ahora es {focus_label}, empieza por {stronger_label}. "
            f"Despues, compara en mapa si la diferencia viene de actividad, confianza o contexto oficial."
        )

    return {
        "title": f"Comparacion por {focus_label}: {label_a} vs {label_b}",
        "answer": answer,
        "next_step": next_step,
        "suggestions": _build_zone_suggestions([row_a, row_b], 2),
        "comparison_focus": focus,
    }


def _build_opportunity_compare_payload(
    row_a: dict[str, Any],
    row_b: dict[str, Any],
) -> dict[str, Any]:
    label_a = str(row_a.get("asset_address") or f"Evento {row_a.get('event_id')}")
    label_b = str(row_b.get("asset_address") or f"Evento {row_b.get('event_id')}")
    score_a = float(row_a.get("score") or 0.0)
    score_b = float(row_b.get("score") or 0.0)
    price_a = row_a.get("price_eur")
    price_b = row_b.get("price_eur")
    zone_a = str(row_a.get("zone_label") or "-")
    zone_b = str(row_b.get("zone_label") or "-")

    winner = label_a if score_a >= score_b else label_b
    answer = (
        f"{winner} sale mejor priorizada ahora mismo. "
        f"Puntuacion: {label_a} {score_a:.1f} frente a {label_b} {score_b:.1f}. "
        f"Zona: {zone_a} vs {zone_b}. "
        f"Precio: {safe_price_compare(price_a)} frente a {safe_price_compare(price_b)}."
    )
    next_step = f"Abre en mapa o en cola la que gane por score y valida si la diferencia viene de zona, microzona o recencia."
    return {
        "title": f"Comparacion de oportunidades: {label_a} vs {label_b}",
        "answer": answer,
        "next_step": next_step,
        "suggestions": _build_opportunity_suggestions([row_a, row_b], 2),
    }


def _build_context_action_payload(
    *,
    query_raw: str,
    selected_row: dict[str, Any],
    title: str,
    answer: str,
    next_step: str,
    auto_action: str,
) -> dict[str, Any]:
    return {
        "query": query_raw,
        "intent": "context_action",
        "title": title,
        "answer": answer,
        "next_step": next_step,
        "suggestions": [selected_row],
        "followups": _build_followups(intent="context_explain", selected_row=selected_row),
        "auto_action": auto_action,
        "search_payload": None,
    }


def _build_followups(
    *,
    intent: str,
    selected_row: dict[str, Any] | None = None,
    recent_zone_labels: list[str] | None = None,
) -> list[str]:
    selected_row = selected_row or {}
    recent_zone_labels = [label for label in (recent_zone_labels or []) if label]
    selected_zone = str(selected_row.get("zone_label") or "").strip()
    selected_item = str(selected_row.get("item") or "").strip()

    if intent == "zone_capture":
        followups = ["explicamela", "abrirla en mapa", "comparala con Guindalera"]
        if selected_zone:
            followups[2] = f"comparala con {recent_zone_labels[1] if len(recent_zone_labels) > 1 else 'Guindalera'}"
        return followups

    if intent in {"zone_transformation", "zone_heat", "zone_predictive", "zone_confidence", "zone_compare"}:
        return [
            "explicamela",
            "comparala en confianza",
            "abre la seleccion en mapa",
        ]

    if intent == "opportunities":
        label = selected_item or "esta oportunidad"
        return [
            "explicamela",
            f"abre {label} en mapa",
            "que deberia revisar primero",
        ]

    if intent == "casafari_review":
        return [
            "explicamelo",
            "abre el caso",
            "reconciliar pendientes casafari",
        ]

    if intent.startswith("action_"):
        return [
            "ejecuta la accion",
            "que pasa despues",
            "abre el contexto",
        ]

    if intent == "context_explain":
        return [
            "comparala con otra zona",
            "abre el contexto",
            "abre en mapa",
        ]

    return [
        "zonas para captar",
        "oportunidades con entrada nueva",
        "casafari weak identity",
    ]


def _build_selected_context_payload(
    session: Session,
    query_raw: str,
    selected_row: dict[str, Any],
) -> dict[str, Any] | None:
    target_view = str(selected_row.get("target_view") or "").strip().lower()

    if target_view == "radar" and selected_row.get("zone_label"):
        rows = get_zone_intelligence_v2(session, window_days=14)
        matched = _find_zone_rows_by_labels(rows, [str(selected_row.get("zone_label") or "")])
        if not matched:
            return None
        row = matched[0]
        return {
            "query": query_raw,
            "intent": "context_explain",
            "title": f"Explicacion de {row.get('zone_label')}",
            "answer": str(row.get("ai_summary") or row.get("executive_summary") or row.get("ai_brief") or "-"),
            "next_step": str(row.get("ai_next_step") or row.get("recommended_action") or "-"),
            "suggestions": _build_zone_suggestions([row], 1),
            "followups": _build_followups(
                intent="context_explain",
                selected_row=selected_row,
                recent_zone_labels=[str(row.get("zone_label") or "")],
            ),
            "search_payload": None,
        }

    if target_view == "queue" and selected_row.get("event_id") is not None:
        rows = get_opportunity_queue_v2(session, window_days=14, limit=250)
        row = _find_opportunity_row_by_event(rows, selected_row.get("event_id"))
        if row is None:
            return None
        event_label = row.get("asset_address") or f"Evento {row.get('event_id')}"
        return {
            "query": query_raw,
            "intent": "context_explain",
            "title": f"Explicacion de {event_label}",
            "answer": str(row.get("ai_summary") or row.get("reason") or row.get("ai_brief") or "-"),
            "next_step": str(row.get("ai_next_step") or "-"),
            "suggestions": _build_opportunity_suggestions([row], 1),
            "followups": _build_followups(intent="context_explain", selected_row=selected_row),
            "search_payload": None,
        }

    if target_view == "casafari":
        label = selected_row.get("item") or selected_row.get("query_text") or "Caso Casafari"
        return {
            "query": query_raw,
            "intent": "context_explain",
            "title": f"Explicacion de {label}",
            "answer": str(selected_row.get("por_que") or "-"),
            "next_step": str(selected_row.get("accion") or "-"),
            "suggestions": [selected_row],
            "followups": _build_followups(intent="context_explain", selected_row=selected_row),
            "search_payload": None,
        }

    return None


def _build_multi_intent_payload(
    session: Session,
    query_raw: str,
    query_key: str,
    intents: list[tuple[str, dict[str, Any]]],
    *,
    limit: int,
    understanding: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    titles: list[str] = []
    answers: list[str] = []
    combined_suggestions: list[dict[str, Any]] = []

    for intent, options in intents[:2]:
        if intent.startswith("zone_"):
            rows = get_zone_intelligence_v2(session, window_days=14)
            if intent == "zone_capture":
                rows = sorted(rows, key=lambda row: float(row.get("zone_capture_score") or 0.0), reverse=True)
                titles.append("captacion")
                answers.append("he priorizado zonas para captar")
            elif intent == "zone_transformation":
                rows = sorted(rows, key=lambda row: float(row.get("zone_transformation_signal_score") or 0.0), reverse=True)
                titles.append("transformacion")
                answers.append("he añadido zonas con señal urbana fuerte")
            elif intent == "zone_confidence":
                rows = sorted(rows, key=lambda row: float(row.get("zone_confidence_score") or 0.0))
                titles.append("confianza")
                answers.append("he separado zonas con lectura mas fragil")
            else:
                rows = sorted(rows, key=lambda row: float(row.get("zone_heat_score") or 0.0), reverse=True)
                titles.append("actividad")
                answers.append("he recogido zonas con mas movimiento")
            combined_suggestions.extend(_build_zone_suggestions(rows, min(2, limit)))
            continue

        if intent == "casafari_review":
            rows = list_casafari_links(
                session,
                status_filter="all",
                focus_filter=str(options.get("focus_filter") or "review_needed"),
                query_text=query_raw,
                limit=max(limit * 2, 10),
            )
            titles.append("Casafari")
            answers.append("he incluido los casos Casafari mas relevantes")
            combined_suggestions.extend(_build_casafari_suggestions(rows, min(2, limit)))
            continue

        if intent == "opportunities":
            rows = get_opportunity_queue_v2(session, window_days=14, limit=120)
            event_type = options.get("event_type") or "all"
            if event_type != "all":
                rows = [row for row in rows if row.get("event_type") == event_type]
            titles.append("oportunidades")
            answers.append("he añadido oportunidades ya priorizadas")
            combined_suggestions.extend(_build_opportunity_suggestions(rows, min(2, limit)))
            continue

        if intent.startswith("action_"):
            titles.append("accion")
            answers.append("he detectado una accion operativa dentro de la misma consulta")

    deduped_suggestions: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for row in combined_suggestions:
        row_key = "|".join(
            [
                str(row.get("tipo") or ""),
                str(row.get("item") or ""),
                str(row.get("action_id") or ""),
                str(row.get("event_id") or ""),
                str(row.get("zone_label") or ""),
            ]
        )
        if row_key in seen_keys:
            continue
        seen_keys.add(row_key)
        deduped_suggestions.append(row)
        if len(deduped_suggestions) >= limit:
            break

    payload = {
        "query": query_raw,
        "intent": "multi_intent",
        "title": f"Consulta compuesta: {' + '.join(titles) if titles else 'mixta'}",
        "answer": " y ".join(answers) if answers else "He dividido tu consulta en varios focos operativos.",
        "next_step": "Elige primero el bloque que mas te interese y luego sigue con las preguntas de contexto.",
        "suggestions": deduped_suggestions,
        "followups": ["explicamela", "abre el contexto", "abre la seleccion en mapa"],
        "understanding": understanding,
        "search_payload": None,
    }
    return maybe_enhance_copilot_payload(query_raw, payload, context=context)


def run_copilot_query(
    session: Session,
    query: str,
    *,
    default_limit: int = 5,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    query_raw = (query or "").strip()
    query_key = _clean_query(query_raw)
    limit = _extract_limit(query_key, default=default_limit)
    detected_intents = _detect_all_intents(query_key)
    intent, options = detected_intents[0]
    hint = _extract_subject_hint(query_key)
    selected_row = dict((context or {}).get("selected_row") or {})
    understanding = _build_understanding(
        session,
        query_raw,
        query_key,
        intent=intent,
        context=context,
    )
    if understanding.get("resolved_zone") and not hint:
        hint = str(understanding["resolved_zone"])

    empty_answer = {
        "query": query_raw,
        "intent": "empty",
        "title": "Haz una pregunta corta",
        "answer": "Puedes preguntar por barrios calientes, zonas con transformacion, oportunidades o casos Casafari sin resolver.",
        "next_step": "Prueba algo como: 'barrios con transformacion', 'oportunidades con bajada de precio' o 'casafari weak identity'.",
        "suggestions": [],
        "followups": _build_followups(intent="empty"),
        "understanding": understanding,
        "search_payload": None,
    }
    if not query_key:
        return empty_answer

    if _should_multi_intent(query_key, detected_intents):
        return _build_multi_intent_payload(
            session,
            query_raw,
            query_key,
            detected_intents,
            limit=limit,
            understanding=understanding,
            context=context,
        )

    if understanding["clarification_needed"]:
        rows = get_zone_intelligence_v2(session, window_days=14)
        matched = _find_zone_rows_by_labels(rows, list(understanding["clarification_options"]))
        ambiguity_payload = {
            "query": query_raw,
            "intent": "clarification_needed",
            "title": "Necesito afinar a que zona te refieres",
            "answer": "He detectado varias zonas posibles y no quiero inventarme una interpretacion equivocada.",
            "next_step": f"Confirma una de estas opciones: {', '.join(understanding['clarification_options'])}.",
            "suggestions": _build_zone_suggestions(matched, min(limit, len(matched))),
            "followups": [str(option) for option in understanding["clarification_options"]],
            "understanding": understanding,
            "search_payload": None,
        }
        return maybe_enhance_copilot_payload(query_raw, ambiguity_payload, context=context)

    if selected_row:
        if any(
            term in query_key
            for term in ("abre la seleccion en mapa", "abre en mapa", "abrela en mapa", "llevame al mapa")
        ):
            if selected_row.get("zone_label") or selected_row.get("microzone_label") or selected_row.get("event_id"):
                return _build_context_action_payload(
                    query_raw=query_raw,
                    selected_row=selected_row,
                    title="Abrir en mapa",
                    answer="Abro el mapa con el foco del contexto activo para que valides la zona o la oportunidad sobre el terreno.",
                    next_step="Revisa la microzona y decide si conviene seguir o actuar.",
                    auto_action="open_map",
                )

        if any(
            term in query_key
            for term in ("abre el caso", "abre contexto", "abre el contexto", "abrelo", "llevame al caso")
        ):
            return _build_context_action_payload(
                query_raw=query_raw,
                selected_row=selected_row,
                title="Abrir contexto",
                answer="Abro el modulo que corresponde al contexto activo para seguir trabajando desde ahi.",
                next_step="Desde esa vista podras validar mejor el caso o profundizar en el detalle.",
                auto_action="open_context",
            )

        if any(term in query_key for term in ("ejecuta la accion", "ejecutala", "hazlo", "lanzala", "ejecutalo")):
            if selected_row.get("action_id"):
                return _build_context_action_payload(
                    query_raw=query_raw,
                    selected_row=selected_row,
                    title="Ejecutar accion",
                    answer="Lanzo la accion del contexto actual sin que tengas que buscar el boton correspondiente.",
                    next_step="Cuando termine, revisa el estado o el modulo al que te lleve la accion.",
                    auto_action="execute_action",
                )

    compare_markers = ("compara", "comparame", "vs", "frente a", "mejor que", "peor que")
    if any(term in query_key for term in compare_markers):
        if selected_row.get("target_view") == "queue" or any(token in query_key for token in ("oportunidad", "evento", "anuncio", "portal")):
            rows = get_opportunity_queue_v2(session, window_days=14, limit=250)
            opportunity_candidates = _extract_compare_opportunity_candidates(rows, query_key, context)
            if len(opportunity_candidates) >= 2:
                compare_payload = _build_opportunity_compare_payload(
                    opportunity_candidates[0],
                    opportunity_candidates[1],
                )
                compare_payload["query"] = query_raw
                compare_payload["intent"] = "opportunity_compare"
                compare_payload["followups"] = [
                    "abre la seleccion en mapa",
                    "abre el contexto",
                    "explicamela",
                ]
                compare_payload["understanding"] = understanding
                compare_payload["search_payload"] = None
                return maybe_enhance_copilot_payload(query_raw, compare_payload, context=context)

        rows = get_zone_intelligence_v2(session, window_days=14)
        labels = _extract_compare_labels(query_key, rows, context)
        matched_rows = _find_zone_rows_by_labels(rows, labels)
        if len(matched_rows) >= 2:
            focus = _extract_zone_compare_focus(query_key)
            compare_payload = _build_zone_compare_payload(
                matched_rows[0],
                matched_rows[1],
                focus=focus,
            )
            compare_payload["query"] = query_raw
            compare_payload["intent"] = "zone_compare"
            compare_payload["followups"] = _build_followups(
                intent="zone_compare",
                selected_row=selected_row,
                recent_zone_labels=labels,
            )
            compare_payload["understanding"] = understanding
            compare_payload["search_payload"] = None
            return maybe_enhance_copilot_payload(query_raw, compare_payload, context=context)

    if selected_row and any(term in query_key for term in ("por que", "explica", "explicame", "esta arriba", "que pasa")):
        context_payload = _build_selected_context_payload(session, query_raw, selected_row)
        if context_payload:
            context_payload["understanding"] = understanding
            return maybe_enhance_copilot_payload(query_raw, context_payload, context=context)

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

        payload = {
            "query": query_raw,
            "intent": intent,
            "title": title,
            "answer": answer,
            "next_step": "Abre la zona que mas te encaje y bajala luego al mapa o a la cola operativa.",
            "suggestions": _build_zone_suggestions(rows, limit),
            "followups": _build_followups(
                intent=intent,
                selected_row=selected_row,
                recent_zone_labels=[str(row.get("zone_label") or "") for row in rows[:2]],
            ),
            "understanding": understanding,
            "search_payload": None,
        }
        return maybe_enhance_copilot_payload(query_raw, payload, context=context)

    if intent == "action_reconcile":
        payload = {
            "query": query_raw,
            "intent": intent,
            "title": "Reconciliar Casafari",
            "answer": "Puedo relanzar el matching de pendientes para intentar resolver los casos que se quedaron a medias.",
            "next_step": "Ejecuta la accion y luego revisa los casos que sigan con identidad debil o conflicto de precio.",
            "suggestions": _build_action_suggestion(
                item="Reconciliar pendientes Casafari",
                why="Sirve para reintentar el matching con el estado actual de la base.",
                action="Ejecutar reconciliacion",
                action_id="casafari_reconcile",
                target_view="casafari",
            ),
            "followups": _build_followups(intent=intent),
            "understanding": understanding,
            "search_payload": None,
        }
        return maybe_enhance_copilot_payload(query_raw, payload, context=context)

    if intent == "action_prepare_session":
        payload = {
            "query": query_raw,
            "intent": intent,
            "title": "Preparar sesion Casafari",
            "answer": "Puedo abrir el flujo para dejar la sesion lista antes de sincronizar.",
            "next_step": "Haz login y deja visible la pantalla de historial para que la sesion quede validada.",
            "suggestions": _build_action_suggestion(
                item="Preparar sesion Casafari",
                why="Es el paso previo necesario antes de sincronizar si la sesion ha caducado.",
                action="Abrir preparacion de sesion",
                action_id="casafari_prepare_session",
                target_view="sync",
            ),
            "followups": _build_followups(intent=intent),
            "understanding": understanding,
            "search_payload": None,
        }
        return maybe_enhance_copilot_payload(query_raw, payload, context=context)

    if intent == "action_sync":
        payload = {
            "query": query_raw,
            "intent": intent,
            "title": "Sincronizar Casafari",
            "answer": "Puedo lanzar una nueva sincronizacion del delta de Casafari con la sesion guardada.",
            "next_step": "Si la sesion no esta lista, prepara sesion antes de sincronizar.",
            "suggestions": _build_action_suggestion(
                item="Sincronizar Casafari",
                why="Traera raws nuevos y reintentara enlazarlos con la base.",
                action="Ejecutar sync",
                action_id="casafari_sync",
                target_view="sync",
            ),
            "followups": _build_followups(intent=intent),
            "understanding": understanding,
            "search_payload": None,
        }
        return maybe_enhance_copilot_payload(query_raw, payload, context=context)

    if intent == "action_reindex":
        payload = {
            "query": query_raw,
            "intent": intent,
            "title": "Reindexar busqueda",
            "answer": "Puedo reconstruir el indice para que la busqueda y el copiloto tengan el dato mas fresco.",
            "next_step": "Hazlo cuando notes resultados raros o despues de cambios grandes en la base.",
            "suggestions": _build_action_suggestion(
                item="Reindexar FTS",
                why="Actualiza el indice de busqueda global del sistema.",
                action="Reindexar ahora",
                action_id="search_reindex",
                target_view="search",
            ),
            "followups": _build_followups(intent=intent),
            "understanding": understanding,
            "search_payload": None,
        }
        return maybe_enhance_copilot_payload(query_raw, payload, context=context)

    if intent == "opportunities":
        rows = get_opportunity_queue_v2(session, window_days=14, limit=200)
        event_type = options.get("event_type") or "all"
        if event_type != "all":
            rows = [row for row in rows if row.get("event_type") == event_type]
        rows = _filter_rows_by_hint(
            rows,
            hint,
            "asset_address",
            "zone_label",
            "microzone_label",
            "ai_summary",
        )

        payload = {
            "query": query_raw,
            "intent": intent,
            "title": "Oportunidades recomendadas",
            "answer": "He priorizado las oportunidades que mejor encajan con tu pregunta usando evento, zona, microzona y ventana 30d.",
            "next_step": "Valida primero las de prioridad alta y abre en mapa las que dependan mucho de zona o microzona.",
            "suggestions": _build_opportunity_suggestions(rows, limit),
            "followups": _build_followups(intent=intent, selected_row=selected_row),
            "understanding": understanding,
            "search_payload": None,
        }
        return maybe_enhance_copilot_payload(query_raw, payload, context=context)

    if intent == "casafari_review":
        rows = list_casafari_links(
            session,
            status_filter="all",
            focus_filter=str(options.get("focus_filter") or "review_needed"),
            query_text=hint or query_raw,
            limit=max(limit * 4, 20),
        )
        payload = {
            "query": query_raw,
            "intent": intent,
            "title": "Casos Casafari a revisar",
            "answer": "He filtrado los casos que mas probablemente necesitan una decision manual o una validacion extra.",
            "next_step": "Empieza por los casos con identidad debil o conflicto de precio antes de relanzar matching.",
            "suggestions": _build_casafari_suggestions(rows, limit),
            "followups": _build_followups(intent=intent, selected_row=selected_row),
            "understanding": understanding,
            "search_payload": None,
        }
        return maybe_enhance_copilot_payload(query_raw, payload, context=context)

    payload = search_payload(session, query=query_raw, section_filter="all", limit_per_section=limit)
    total = int((payload.get("summary") or {}).get("total") or 0)
    result = {
        "query": query_raw,
        "intent": "search_fallback",
        "title": "Busqueda transversal",
        "answer": f"No he detectado una intencion operativa clara, asi que he lanzado una busqueda general y he encontrado {total} resultados.",
        "next_step": "Si quieres una respuesta mas guiada, pregunta por zonas, oportunidades o Casafari con una frase mas concreta.",
        "suggestions": [],
        "followups": _build_followups(intent="search_fallback"),
        "understanding": understanding,
        "search_payload": payload,
    }
    return maybe_enhance_copilot_payload(query_raw, result, context=context)
