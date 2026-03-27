from __future__ import annotations


def _safe_float(value) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value) -> int:
    try:
        return int(round(float(value or 0.0)))
    except (TypeError, ValueError):
        return 0


def _join_sentence(parts: list[str]) -> str:
    cleaned = [part.strip() for part in parts if part and part.strip()]
    return ". ".join(cleaned).strip()


def _zone_phase_label(capture: float, confidence: float) -> str:
    if confidence < 40:
        return "lectura todavia fragil"
    if capture >= 65:
        return "zona bastante accionable"
    if capture >= 50:
        return "zona interesante para seguimiento"
    return "zona en observacion"


def _event_label(event_type: str | None) -> str:
    mapping = {
        "listing_detected": "entrada nueva",
        "price_drop": "bajada de precio",
        "price_raise": "subida de precio",
        "reserved": "reserva",
        "sold": "venta",
        "expired": "caducidad",
        "not_available": "retirada",
    }
    return mapping.get(event_type or "", event_type or "senal")


def _match_reason_human(reason_taxonomy: str | None) -> str:
    mapping = {
        "resolved_strong": "encaje muy solido",
        "resolved_soft": "encaje razonable",
        "ambiguous_multiple_candidates": "hay varios candidatos plausibles",
        "pending_review": "queda pendiente de revisar",
        "not_in_csv_yet": "puede que aun no este en el baseline",
        "no_candidates": "no aparecen candidatos claros",
        "zone_only_address": "la direccion es demasiado generica",
        "repeated_phone_conflict": "el telefono se repite demasiado",
        "price_conflict": "el precio no termina de cuadrar",
        "weak_identity": "faltan senales de identidad",
    }
    return mapping.get(reason_taxonomy or "", reason_taxonomy or "caso sin clasificar")


def explain_zone_row(row: dict) -> dict:
    zone_label = str(row.get("zone_label") or "Sin zona")
    capture = _safe_float(row.get("zone_capture_score"))
    heat = _safe_float(row.get("zone_heat_score"))
    relative_heat = _safe_float(row.get("zone_relative_heat_score"))
    transformation = _safe_float(row.get("zone_transformation_signal_score"))
    liquidity = _safe_float(row.get("zone_liquidity_score"))
    confidence = _safe_float(row.get("zone_confidence_score"))
    pressure = _safe_float(row.get("zone_pressure_score"))
    prediction = _safe_float(row.get("predicted_absorption_30d_score"))
    events_per_10k = _safe_float(row.get("events_14d_per_10k_population"))
    population = _safe_int(row.get("official_population"))
    vulnerability = row.get("official_vulnerability_index")
    action = str(row.get("recommended_action") or "Seguir y vigilar")

    strengths: list[str] = []
    if capture >= 65:
        strengths.append("buena opcion para captar")
    elif capture >= 50:
        strengths.append("senal suficiente para seguirla")

    if relative_heat >= 65 or heat >= 65:
        strengths.append("mercado con movimiento fuerte")
    elif relative_heat >= 45 or heat >= 45:
        strengths.append("movimiento de mercado razonable")

    if liquidity >= 60:
        strengths.append("salida de producto sana")
    elif liquidity < 35:
        strengths.append("salida de producto floja")

    if transformation >= 65:
        strengths.append("transformacion urbana clara")
    elif transformation >= 45:
        strengths.append("cambios urbanos en marcha")

    risks: list[str] = []
    if confidence < 40:
        risks.append("todavia falta confianza en el dato")
    if pressure >= 65 and liquidity < 45:
        risks.append("hay presion alta sin liquidez equivalente")
    if zone_label.lower() == "sin zona":
        risks.append("falta una zona oficial fiable")

    summary = _join_sentence(
        [
            f"{zone_label}: {_zone_phase_label(capture, confidence)}",
            f"Destaca por {', '.join(strengths[:3])}" if strengths else "",
            f"Ojo: {', '.join(risks[:2])}" if risks else "",
        ]
    )

    context_bits: list[str] = []
    if population > 0:
        context_bits.append(f"poblacion {population:,}".replace(",", "."))
    if events_per_10k > 0:
        context_bits.append(f"{events_per_10k:.1f} eventos por 10k hab")
    if vulnerability is not None:
        context_bits.append(f"IVT {float(vulnerability):.1f}")

    next_step = action
    if confidence < 40:
        next_step = "Validar primero direcciones y cobertura geo antes de tomar decisiones."
    elif prediction >= 65:
        next_step = f"{action}. Conviene entrar en la siguiente ventana de {int(row.get('predicted_action_window_days') or 14)} dias."
    elif prediction >= 50:
        next_step = f"{action}. Merece seguimiento activo durante las proximas 2 semanas."
    else:
        next_step = f"{action}. Mejor observar y esperar mas senal."

    brief = summary or f"{zone_label}: lectura disponible."
    return {
        "ai_summary": brief if brief.endswith(".") else brief + ".",
        "ai_brief": strengths[0].capitalize() + "." if strengths else f"{_zone_phase_label(capture, confidence).capitalize()}.",
        "ai_context_line": " | ".join(context_bits) if context_bits else "Sin contexto oficial suficiente.",
        "ai_next_step": next_step,
    }


def explain_opportunity_row(row: dict) -> dict:
    event_label = _event_label(row.get("event_type"))
    zone_label = str(row.get("zone_label") or row.get("asset_neighborhood") or "Sin zona")
    priority = str(row.get("priority_label") or "seguimiento")
    zone_signal = _safe_float(row.get("score_zone_signal"))
    micro_signal = _safe_float(row.get("score_microzone_signal"))
    geo_signal = _safe_float(row.get("score_geo_signal"))
    price_signal = _safe_float(row.get("score_price_signal"))
    prediction = _safe_float(row.get("predicted_opportunity_30d_score"))
    predicted_band = str(row.get("predicted_opportunity_30d_band") or "baja")
    window_days = int(row.get("predicted_action_window_days") or 30)

    drivers: list[str] = []
    if price_signal >= 8:
        drivers.append("la bajada de precio pesa de verdad")
    if zone_signal >= 8:
        drivers.append("la zona acompana")
    if micro_signal >= 5:
        drivers.append("la microzona suma")
    if geo_signal >= 10:
        drivers.append("la geo es precisa")
    elif geo_signal <= 3:
        drivers.append("la geo es todavia floja")
    if prediction >= 56:
        drivers.append("la ventana 30d es favorable")

    confidence_bits: list[str] = []
    if _safe_float(row.get("zone_confidence_score")) < 40:
        confidence_bits.append("la lectura de zona aun es fragil")
    if not row.get("has_geo_point"):
        confidence_bits.append("falta coordenada exacta")

    summary = _join_sentence(
        [
            f"{event_label.capitalize()} en {zone_label}",
            f"Prioridad {priority}",
            f"Sube porque {', '.join(drivers[:3])}" if drivers else "",
            f"Ojo: {', '.join(confidence_bits[:2])}" if confidence_bits else "",
        ]
    )

    next_step = (
        f"Revisar este caso en los proximos {window_days} dias."
        if predicted_band in {"alta", "media", "moderada"}
        else "Mantener en seguimiento hasta que aparezca mas senal."
    )
    if priority == "alta":
        next_step = f"Priorizar validacion y accion en los proximos {window_days} dias."

    score_story = _join_sentence(
        [
            f"Evento {_safe_float(row.get('score_event_base')):.1f} + recencia {_safe_float(row.get('score_recency')):.1f}",
            f"Zona {zone_signal:.1f} + microzona {micro_signal:.1f}",
            f"Geo {geo_signal:.1f} + prediccion {_safe_float(row.get('score_predictive_signal')):.1f}",
        ]
    )

    return {
        "ai_summary": summary + "." if summary and not summary.endswith(".") else summary or "Sin lectura suficiente.",
        "ai_brief": f"{event_label.capitalize()} con prioridad {priority}.",
        "ai_next_step": next_step,
        "ai_score_story": score_story + "." if score_story and not score_story.endswith(".") else score_story,
    }


def explain_casafari_case(row: dict) -> dict:
    match_status = str(row.get("match_status") or "pending")
    reason_taxonomy = str(row.get("reason_taxonomy") or "")
    address_precision = str(row.get("address_precision") or "unknown")
    match_band = str(row.get("match_confidence_band") or "low")
    phone_profile = str(row.get("phone_profile") or "unknown")
    human_reason = _match_reason_human(reason_taxonomy)

    if match_status == "resolved":
        state = "Caso bastante encajado"
    elif match_status == "ambiguous":
        state = "Caso dudoso"
    elif match_status == "pending":
        state = "Caso pendiente"
    else:
        state = "Caso sin resolver"

    clues: list[str] = [human_reason]
    if address_precision == "zone_like":
        clues.append("la direccion apunta mas a zona que a inmueble")
    elif address_precision == "unknown":
        clues.append("la direccion llega pobre")

    if phone_profile == "broker_like":
        clues.append("el telefono aparece en muchos anuncios")
    elif phone_profile == "owner_like":
        clues.append("el telefono parece bastante especifico")

    if row.get("price_confidence") == "high":
        clues.append("el precio es util para contrastar")
    elif row.get("price_confidence") == "low":
        clues.append("el precio ayuda poco")

    summary = _join_sentence(
        [
            state,
            f"Banda {match_band}",
            f"La lectura actual sugiere que {', '.join(clues[:3])}",
        ]
    )

    if match_status == "resolved":
        next_step = "Se puede aceptar el enlace, pero conviene revisar una muestra manual para seguir afinando el matching."
    elif match_status == "ambiguous":
        next_step = "Abrir el caso y elegir entre los candidatos antes de crear confianza falsa."
    elif reason_taxonomy in {"weak_identity", "zone_only_address", "repeated_phone_conflict"}:
        next_step = "No enlazar aun. Hace falta validar direccion, telefono o portal con mas contexto."
    elif reason_taxonomy == "not_in_csv_yet":
        next_step = "Esperar la siguiente importacion baseline o buscar si el activo aun no ha entrado."
    else:
        next_step = "Mantener el caso en revision hasta tener una senal mas clara."

    return {
        "ai_summary": summary + "." if summary and not summary.endswith(".") else summary or "Caso sin lectura suficiente.",
        "ai_brief": human_reason.capitalize() + ".",
        "ai_next_step": next_step,
    }
