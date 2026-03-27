from __future__ import annotations

import json
import os
from typing import Any
from urllib import error, request


def llm_is_enabled() -> bool:
    return bool(os.getenv("OPENAI_API_KEY")) and os.getenv("NEXUS_COPILOT_LLM_ENABLED", "0") == "1"


def _extract_output_text(response_payload: dict[str, Any]) -> str:
    output = response_payload.get("output") or []
    for item in output:
        if item.get("type") != "message":
            continue
        for content in item.get("content") or []:
            if content.get("type") == "output_text":
                return str(content.get("text") or "")
    return ""


def _call_openai_responses(query: str, payload: dict[str, Any], context: dict[str, Any] | None) -> dict[str, Any] | None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    model = os.getenv("NEXUS_COPILOT_LLM_MODEL", "gpt-5")
    prompt = {
        "query": query,
        "context": context or {},
        "payload": {
            "intent": payload.get("intent"),
            "title": payload.get("title"),
            "answer": payload.get("answer"),
            "next_step": payload.get("next_step"),
            "followups": payload.get("followups"),
            "understanding": payload.get("understanding"),
        },
    }

    request_payload = {
        "model": model,
        "instructions": (
            "Eres una capa de refinado para un copiloto inmobiliario. "
            "No inventes datos. Usa solo la estructura recibida. "
            "Devuelve JSON valido con las claves: title, answer, next_step, followups, understanding_text. "
            "Mantente breve, operativo y en espanol."
        ),
        "input": json.dumps(prompt, ensure_ascii=False),
        "text": {"format": {"type": "text"}},
        "tool_choice": "none",
    }

    http_request = request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(request_payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    try:
        with request.urlopen(http_request, timeout=20) as response:
            response_body = response.read().decode("utf-8")
    except (TimeoutError, error.URLError, error.HTTPError, ValueError):
        return None

    try:
        parsed = json.loads(response_body)
        text = _extract_output_text(parsed)
        if not text:
            return None
        return json.loads(text)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def maybe_enhance_copilot_payload(
    query: str,
    payload: dict[str, Any],
    *,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not llm_is_enabled():
        return payload

    enriched = _call_openai_responses(query, payload, context)
    if not enriched:
        return payload

    result = dict(payload)
    if enriched.get("title"):
        result["title"] = str(enriched["title"])
    if enriched.get("answer"):
        result["answer"] = str(enriched["answer"])
    if enriched.get("next_step"):
        result["next_step"] = str(enriched["next_step"])
    if isinstance(enriched.get("followups"), list):
        result["followups"] = [str(item) for item in enriched["followups"][:3]]

    understanding = dict(result.get("understanding") or {})
    if enriched.get("understanding_text"):
        understanding["understanding_text"] = str(enriched["understanding_text"])
    result["understanding"] = understanding
    result["llm_enhanced"] = True
    return result
