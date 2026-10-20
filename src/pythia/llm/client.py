"""LLM client: roles, structured output, agent loop, cost ledger and cap.

Without an LLM the system interprets nothing: any unavailability becomes
`LLMUnavailable` (try later) or `LLMHalted` (stop until someone intervenes), and the
jobs that depend on an LLM go back to the queue. There is no alternative path.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

import anthropic

from ..db import Database
from ..settings import Settings
from .. import vault


class LLMError(Exception):
    """LLM error that must not be retried automatically (e.g. invalid request)."""


class LLMUnavailable(Exception):
    """Provider unavailable/overloaded/offline. The job goes back to the queue."""


class LLMHalted(LLMUnavailable):
    """Halted until intervention: cap reached, no key, invalid key."""


class BudgetExceeded(LLMHalted):
    pass


@dataclass
class AgentTool:
    name: str
    description: str
    input_schema: dict
    run: Callable[[dict], Any]


@dataclass
class CallContext:
    source_id: str | None = None
    job_id: int | None = None


@dataclass
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read: int = 0
    cache_write: int = 0


def schema_object(properties: dict, required: list[str] | None = None) -> dict:
    """Closed JSON Schema object (additionalProperties: false, all required)."""
    return {
        "type": "object",
        "properties": properties,
        "required": required if required is not None else list(properties),
        "additionalProperties": False,
    }


class LLM:
    def __init__(self, db: Database, settings: Settings):
        self.db = db
        self.settings = settings
        self._client: anthropic.Anthropic | None = None
        self._client_key: str | None = None
        self.last_ok: float = 0.0  # instant of the last successful call

    # ------------------------------------------------------------ cost

    def month_spend(self) -> float:
        row = self.db.one(
            "SELECT coalesce(sum(cost_usd), 0) AS s FROM llm_calls WHERE ts >= date_trunc('month', now())"
        )
        return float(row["s"])

    def check_budget(self) -> None:
        cap = float(self.settings.get("monthly_cap_usd"))
        spent = self.month_spend()
        if spent >= cap:
            raise BudgetExceeded(f"monthly cap reached: US${spent:.2f} of US${cap:.2f}")

    def price(self, model: str, usage: Usage, batch: bool = False) -> float:
        prices = self.settings.get("prices").get(model)
        if not prices:
            # With no known price, assume the most expensive in the table so spend is not underestimated.
            prices = max(self.settings.get("prices").values(), key=lambda p: p["output"])
        cost = (
            usage.input_tokens * prices["input"]
            + usage.output_tokens * prices["output"]
            + usage.cache_read * prices["cache_read"]
            + usage.cache_write * prices["cache_write"]
        ) / 1_000_000
        return cost * BATCH_DISCOUNT if batch else cost

    def record(self, role: str, model: str, usage: Usage, ctx: CallContext, duration_ms: int,
               ok: bool, error: str | None = None, batch: bool = False) -> None:
        if ok:
            self.last_ok = time.monotonic()
        cost = self.price(model, usage, batch)
        self.db.execute(
            """INSERT INTO llm_calls (role, model, source_id, job_id, input_tokens, output_tokens,
                                      cache_read, cache_write, cost_usd, duration_ms, ok, error, batch)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (role, model, ctx.source_id, ctx.job_id, usage.input_tokens, usage.output_tokens,
             usage.cache_read, usage.cache_write, Decimal(str(round(cost, 6))), duration_ms, ok, error, batch),
        )

    # ------------------------------------------------------------ transport

    def client(self) -> anthropic.Anthropic:
        key = vault.get_secret(vault.ANTHROPIC_KEY)
        if not key:
            raise LLMHalted("Anthropic API key not configured")
        if self._client is None or key != self._client_key:
            base_url = self.settings.get("anthropic_base_url") or "https://api.anthropic.com"
            self._client = anthropic.Anthropic(api_key=key, base_url=base_url, max_retries=2, timeout=600)
            self._client_key = key
        return self._client

    def _params(self, role: str, system: list[dict], messages: list[dict], max_tokens: int,
                output_schema: dict | None, tools: list[dict] | None) -> dict:
        model = self.settings.model_for(role)
        params: dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": messages,
        }
        output_config: dict[str, Any] = {}
        if output_schema is not None:
            output_config["format"] = {"type": "json_schema", "schema": output_schema}
        if not model.startswith("claude-haiku"):
            output_config["effort"] = self.settings.effort_for(role)
        if output_config:
            params["output_config"] = output_config
        if tools:
            params["tools"] = tools
            params["tool_choice"] = {"type": "auto"}
        return params

    def _api(self, role: str, model: str, ctx: CallContext, fn: Callable[[], Any]) -> Any:
        """Calls the API mapping errors to halt semantics (no LLM, nothing is interpreted)."""
        started = time.monotonic()
        try:
            return fn()
        except anthropic.AuthenticationError as exc:
            self.record(role, model, Usage(), ctx, _ms(started), False, "auth")
            raise LLMHalted(f"API key rejected: {exc}") from exc
        except anthropic.PermissionDeniedError as exc:
            self.record(role, model, Usage(), ctx, _ms(started), False, "permission")
            raise LLMHalted(f"permission denied by the API: {exc}") from exc
        except (anthropic.APIConnectionError, anthropic.RateLimitError, anthropic.InternalServerError) as exc:
            self.record(role, model, Usage(), ctx, _ms(started), False, type(exc).__name__)
            raise LLMUnavailable(str(exc)) from exc
        except anthropic.APIStatusError as exc:
            self.record(role, model, Usage(), ctx, _ms(started), False, f"status {exc.status_code}")
            if exc.status_code in (408, 409, 429) or exc.status_code >= 500:
                raise LLMUnavailable(str(exc)) from exc
            raise LLMError(str(exc)) from exc

    def _send(self, role: str, params: dict, ctx: CallContext):
        self.check_budget()
        client = self.client()
        started = time.monotonic()

        def call():
            with client.messages.stream(**params) as stream:
                return stream.get_final_message()

        message = self._api(role, params["model"], ctx, call)
        self.record(role, params["model"], usage_of(message), ctx, _ms(started), True)
        check_stop(message)
        return message

    # ------------------------------------------------------------ Batch API (backfill, 50% discount)

    def submit_batch(self, requests: list[tuple[str, dict]], role_for_errors: str = "batch") -> str:
        """Sends already-assembled requests (custom_id, params) as a batch. Returns the batch id."""
        self.check_budget()
        client = self.client()
        body = [{"custom_id": cid, "params": params} for cid, params in requests]
        batch = self._api(role_for_errors, "batch", CallContext(), lambda: client.messages.batches.create(requests=body))
        return batch.id

    def batch_status(self, batch_id: str) -> str:
        return self.batch_info(batch_id)["status"]

    def batch_info(self, batch_id: str) -> dict:
        """Batch status and request counts (completed and processing), as the API reports them."""
        client = self.client()
        batch = self._api("batch", "batch", CallContext(), lambda: client.messages.batches.retrieve(batch_id))
        c = batch.request_counts
        return {"status": batch.processing_status, "ended_at": getattr(batch, "ended_at", None),
                "counts": {k: getattr(c, k, 0) for k in ("processing", "succeeded", "errored", "canceled", "expired")}}

    def batch_results(self, batch_id: str):
        """Iterates (custom_id, message | None, error | None). Whoever consumes records the cost (with discount)."""
        client = self.client()
        results = self._api("batch", "batch", CallContext(), lambda: list(client.messages.batches.results(batch_id)))
        for r in results:
            if r.result.type == "succeeded":
                yield r.custom_id, r.result.message, None
            else:
                err = getattr(getattr(r.result, "error", None), "type", None) or r.result.type
                yield r.custom_id, None, str(err)

    # ------------------------------------------------------------ public API

    def structured_params(self, role: str, system: str, user: str, schema: dict, *, max_tokens: int = 16000,
                          cached_context: str | None = None) -> dict:
        """Assembles the request for a structured call (the same serves a direct call and a batch)."""
        system_blocks = [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}]
        if cached_context:
            system_blocks.append({"type": "text", "text": cached_context, "cache_control": {"type": "ephemeral"}})
        return self._params(role, system_blocks, [{"role": "user", "content": user}], max_tokens, schema, None)

    def structured(self, role: str, system: str, user: str, schema: dict, *, max_tokens: int = 16000,
                   cached_context: str | None = None, ctx: CallContext | None = None) -> dict:
        """A call with output forced by a closed JSON Schema."""
        ctx = ctx or CallContext()
        params = self.structured_params(role, system, user, schema, max_tokens=max_tokens, cached_context=cached_context)
        message = self._send(role, params, ctx)
        return _parse_json(message.content)

    def agent(self, role: str, system: str, user: str, tools: list[AgentTool], final_schema: dict, *,
              max_steps: int = 12, max_tokens: int = 16000, ctx: CallContext | None = None,
              trace: list | None = None) -> dict:
        """Agent loop: the model uses tools freely and ends with JSON in the final schema.

        `trace` (optional) receives each tool call made by the model."""
        ctx = ctx or CallContext()
        by_name = {t.name: t for t in tools}
        tool_defs = [
            {"name": t.name, "description": t.description, "input_schema": t.input_schema, "strict": True}
            for t in tools
        ]
        tool_defs[-1]["cache_control"] = {"type": "ephemeral"}
        system_blocks = [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}]
        messages: list[dict] = [{"role": "user", "content": user}]
        for _ in range(max_steps):
            params = self._params(role, system_blocks, messages, max_tokens, final_schema, tool_defs)
            message = self._send(role, params, ctx)
            if message.stop_reason != "tool_use":
                return _parse_json(message.content)
            messages.append({"role": "assistant", "content": message.content})
            results = []
            for block in message.content:
                if block.type != "tool_use":
                    continue
                tool = by_name.get(block.name)
                if trace is not None:
                    trace.append({"tool": block.name, "input": block.input})
                try:
                    if tool is None:
                        raise ValueError(f"unknown tool: {block.name}")
                    out = tool.run(block.input)
                    results.append({"type": "tool_result", "tool_use_id": block.id,
                                    "content": _as_text(out)})
                except Exception as exc:  # a tool error goes back to the model to decide
                    if trace is not None:
                        trace[-1]["error"] = str(exc)[:300]
                    results.append({"type": "tool_result", "tool_use_id": block.id,
                                    "content": f"error: {exc}", "is_error": True})
            messages.append({"role": "user", "content": results})
        # Steps exhausted: ask for the conclusion without tools.
        messages.append({"role": "user", "content": "Step limit reached. Conclude now with the final JSON."})
        # The history has tool_use: the tools stay declared, but the model can no longer use them.
        params = self._params(role, system_blocks, messages, max_tokens, final_schema, tool_defs)
        params["tool_choice"] = {"type": "none"}
        message = self._send(role, params, ctx)
        return _parse_json(message.content)


BATCH_DISCOUNT = 0.5


def usage_of(message) -> Usage:
    u = message.usage
    return Usage(
        input_tokens=u.input_tokens or 0,
        output_tokens=u.output_tokens or 0,
        cache_read=getattr(u, "cache_read_input_tokens", 0) or 0,
        cache_write=getattr(u, "cache_creation_input_tokens", 0) or 0,
    )


def check_stop(message) -> None:
    if message.stop_reason == "refusal":
        raise LLMError("the model refused the request")
    if message.stop_reason == "max_tokens":
        raise LLMError("response truncated by max_tokens")


def parse_message(message) -> dict:
    """Validates how the response ended and extracts the JSON from the structured output."""
    check_stop(message)
    return _parse_json(message.content)


def _ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def _parse_json(content) -> dict:
    for block in content:
        if getattr(block, "type", None) == "text" and block.text.strip():
            try:
                return json.loads(block.text)
            except json.JSONDecodeError as exc:
                raise LLMError(f"response is not valid JSON: {exc}") from exc
    raise LLMError("response without a text block")
