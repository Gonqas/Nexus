import json
import os

import core.services.copilot_llm_service as llm_service


def test_llm_service_is_disabled_without_env(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("NEXUS_COPILOT_LLM_ENABLED", raising=False)

    payload = {"title": "Base", "answer": "Base", "understanding": {"understanding_text": "base"}}
    result = llm_service.maybe_enhance_copilot_payload("hola", payload)

    assert result == payload


def test_llm_service_can_merge_enriched_payload(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("NEXUS_COPILOT_LLM_ENABLED", "1")

    monkeypatch.setattr(
        llm_service,
        "_call_openai_responses",
        lambda query, payload, context: {
            "title": "Titulo refinado",
            "answer": "Respuesta refinada",
            "next_step": "Siguiente paso refinado",
            "followups": ["uno", "dos", "tres", "cuatro"],
            "understanding_text": "intencion zone_capture | zona Prosperidad",
        },
    )

    payload = {
        "title": "Base",
        "answer": "Base",
        "next_step": "Base",
        "followups": [],
        "understanding": {"understanding_text": "base", "confidence": "high"},
    }
    result = llm_service.maybe_enhance_copilot_payload("hola", payload)

    assert result["title"] == "Titulo refinado"
    assert result["followups"] == ["uno", "dos", "tres"]
    assert result["understanding"]["understanding_text"] == "intencion zone_capture | zona Prosperidad"
    assert result["llm_enhanced"] is True


def test_llm_service_extracts_output_text() -> None:
    parsed = {
        "output": [
            {
                "type": "message",
                "content": [
                    {"type": "output_text", "text": json.dumps({"answer": "ok"})}
                ],
            }
        ]
    }
    assert llm_service._extract_output_text(parsed) == json.dumps({"answer": "ok"})
