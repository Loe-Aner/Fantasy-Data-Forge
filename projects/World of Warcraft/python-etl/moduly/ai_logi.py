from datetime import datetime, timezone
from typing import Any, Literal
from zoneinfo import ZoneInfo

from sqlalchemy import text

from langchain_core.messages import AIMessage
from langchain_openai import ChatOpenAI

from moduly.ai_modele import (
    TEMPERATURE_TRANSLATOR,
    TEMPERATURE_EDITOR
)

from moduly.ai_pricing import MODEL_PRICING


def format_created_at(created_at: Any) -> str | None:
    if isinstance(created_at, (int, float)):
        return datetime.fromtimestamp(created_at, tz=timezone.utc).astimezone(ZoneInfo("Europe/Warsaw")).isoformat()
    return None


def calculate_price_for_tokens(
    model_name: str,
    tokens: int | None,
    stage: Literal["input", "output"]
) -> str:
    if model_name not in MODEL_PRICING:
        raise KeyError(f"---Brak cennika dla modelu: {model_name}")

    pricing = MODEL_PRICING[model_name]
    currency = pricing["currency"]
    tokens = tokens or 0

    if stage == "input":
        price_per_1m = pricing["input_per_1m"]
    else:
        price_per_1m = pricing["output_per_1m"]

    value = tokens / 1_000_000 * price_per_1m
    return f"{value:.5f} {currency}"


def create_logs(
    raw_response: AIMessage,
    llm: ChatOpenAI,
    misja_id_moje_fk: int,
    input_chars: int,
    output_chars: int,
    stage: Literal["translator", "editor"],
    duration_ms: int | None = None,
    parsing_error: str | None = None
) -> dict[str, Any]:

    response_metadata = raw_response.response_metadata or {}
    usage_metadata = raw_response.usage_metadata or {}
    output_token_details = usage_metadata.get("output_token_details", {}) or {}
    input_token_details = usage_metadata.get("input_token_details", {}) or {}
    created_at = response_metadata.get("created_at")
    duration_s = round(duration_ms / 1000, 3) if duration_ms is not None else None

    model_name = llm.model_name
    input_cached_tokens = input_token_details.get("cache_read") or 0
    input_tokens = usage_metadata.get("input_tokens") or 0
    output_tokens = usage_metadata.get("output_tokens") or 0
    total_tokens = usage_metadata.get("total_tokens") or 0

    currency = MODEL_PRICING[model_name]["currency"]
    input_per_1m = MODEL_PRICING[model_name]["input_per_1m"]
    output_per_1m = MODEL_PRICING[model_name]["output_per_1m"]
    input_uncached_tokens = max(input_tokens - input_cached_tokens, 0)

    input_tokens_price_value = (
        (input_uncached_tokens / 1_000_000) * input_per_1m +
        (input_cached_tokens / 1_000_000) * input_per_1m * 0.1
    )
    output_tokens_price_value = (output_tokens / 1_000_000) * output_per_1m

    input_tokens_price = round(input_tokens_price_value, 8)
    output_tokens_price = round(output_tokens_price_value, 8)

    return {
        "ANSWER_ID": raw_response.id or response_metadata.get("id"),
        "PROVIDER": response_metadata.get("model_provider"),
        "SERVICE_TIER": response_metadata.get("service_tier"),
        "STAGE": stage,
        "STATUS": "error" if parsing_error else response_metadata.get("status"),
        "DURATION_S": duration_s,
        "MISJA_ID_MOJE_FK": misja_id_moje_fk,
        "CREATED_AT": format_created_at(created_at),
        "MODEL": model_name,
        "MODEL_API": response_metadata.get("model"),
        "TOTAL_TOKENS": total_tokens,
        "INPUT_TOKENS": input_tokens,
        "OUTPUT_TOKENS": output_tokens,
        "INPUT_TOKENS_PRICE": input_tokens_price,
        "OUTPUT_TOKENS_PRICE": output_tokens_price,
        "CURRENCY": currency,
        "CACHED_TOKENS": input_cached_tokens,
        "THINKING_TOKENS": output_token_details.get("reasoning"),
        "INPUT_CHARS_ONLY_JSON": input_chars,
        "OUTPUT_CHARS_ONLY_JSON": output_chars,
        "REASONING_EFFORT": getattr(llm, "reasoning_effort", None),
        "TEMPERATURE_FROM_LLM": getattr(llm, "temperature", None),
        "TEMPERATURE_FROM_CONST": TEMPERATURE_TRANSLATOR if stage == "translator" else TEMPERATURE_EDITOR,
        "PARSING_ERROR": parsing_error,
    }


def save_ai_logs_to_db(
    silnik,
    logs
):
    with silnik.begin() as conn:
        q_insert = text("""
            INSERT INTO dbo.AI_LOGI (
                ANSWER_ID, PROVIDER, SERVICE_TIER, STAGE,
                STATUS, DURATION_S, MISJA_ID_MOJE_FK, CREATED_AT,
                MODEL, MODEL_API, TOTAL_TOKENS, INPUT_TOKENS,
                OUTPUT_TOKENS, INPUT_TOKENS_PRICE, OUTPUT_TOKENS_PRICE, CURRENCY,
                CACHED_TOKENS, THINKING_TOKENS, INPUT_CHARS_ONLY_JSON, OUTPUT_CHARS_ONLY_JSON,
                REASONING_EFFORT, TEMPERATURE_FROM_LLM, TEMPERATURE_FROM_CONST, PARSING_ERROR
            )
            VALUES (
                :ANSWER_ID, :PROVIDER, :SERVICE_TIER, :STAGE,
                :STATUS, :DURATION_S, :MISJA_ID_MOJE_FK, :CREATED_AT,
                :MODEL, :MODEL_API, :TOTAL_TOKENS, :INPUT_TOKENS,
                :OUTPUT_TOKENS, :INPUT_TOKENS_PRICE, :OUTPUT_TOKENS_PRICE, :CURRENCY,
                :CACHED_TOKENS, :THINKING_TOKENS, :INPUT_CHARS_ONLY_JSON, :OUTPUT_CHARS_ONLY_JSON,
                :REASONING_EFFORT, :TEMPERATURE_FROM_LLM, :TEMPERATURE_FROM_CONST, :PARSING_ERROR
            )
        """)
        conn.execute(q_insert, logs)
