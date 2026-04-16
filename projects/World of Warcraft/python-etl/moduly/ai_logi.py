from datetime import datetime, timezone
from typing import Any, Literal
from zoneinfo import ZoneInfo

from langchain_core.messages import AIMessage
from langchain_openai import ChatOpenAI

from moduly.ai_modele import (
    TEMPERATURE_TRANSLATOR,
    TEMPERATURE_EDITOR
)


def format_created_at(created_at: Any) -> str | None:
    if isinstance(created_at, (int, float)):
        return datetime.fromtimestamp(created_at, tz=timezone.utc).astimezone(ZoneInfo("Europe/Warsaw")).isoformat()
    return None

def calculate_usd_tokens():
    pass

def create_logs(
    raw_response: AIMessage,
    llm: ChatOpenAI,
    quest_id: int,
    input_chars: int,
    output_chars: int,
    etap: Literal["tlumacz", "redaktor"],
    duration_ms: int | None = None,
    parsing_error: str | None = None
) -> dict[str, Any]:
    
    response_metadata = raw_response.response_metadata or {}
    usage_metadata = raw_response.usage_metadata or {}
    output_token_details = usage_metadata.get("output_token_details", {}) or {}
    created_at = response_metadata.get("created_at")
    duration_s = round(duration_ms / 1000, 3) if duration_ms is not None else None

    return {
        "answer_id": raw_response.id or response_metadata.get("id"),
        "provider": response_metadata.get("model_provider"),
        "service_tier": response_metadata.get("service_tier"),
        "etap": etap,
        "status": "error" if parsing_error else response_metadata.get("status"),
        "duration_s": duration_s,
        "quest_id": quest_id,
        "created_at": format_created_at(created_at),
        "model": llm.model_name,
        "model_api": response_metadata.get("model"),
        "total_tokens": usage_metadata.get("total_tokens"),
        "input_tokens": usage_metadata.get("input_tokens"),
        "output_tokens": usage_metadata.get("output_tokens"),
        "cached_tokens": raw_response.usage_metadata.get("input_token_details", {}).get("cache_read"),
        "thinking_tokens": output_token_details.get("reasoning"),
        "input_chars_only_json": input_chars,
        "output_chars_only_json": output_chars,
        "reasoning_effort": getattr(llm, "reasoning_effort", None),
        "temperature": TEMPERATURE_TRANSLATOR if etap == "tlumacz" else TEMPERATURE_EDITOR,
        "parsing_error": parsing_error
    }
