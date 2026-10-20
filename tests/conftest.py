"""Fixtures: real Postgres (pythia_test database), fake LLM and embedder (tests only)."""

from __future__ import annotations

import hashlib
import math
import os
import re
from typing import Any, Callable

import psycopg
import pytest

TEST_URL = os.environ.get("PYTHIA_TEST_DATABASE_URL", "postgresql://pythia:pythia@127.0.0.1:54329/pythia_test")
os.environ["PYTHIA_DATABASE_URL"] = TEST_URL

from pythia.db import Database  # noqa: E402
from pythia.embed import DIM  # noqa: E402
from pythia.llm.client import LLM, CallContext, Usage  # noqa: E402
from pythia.services import build  # noqa: E402


class FakeEmbedder:
    """Bag-of-words with hashing: deterministic and good enough to test search."""

    def embed(self, texts: list[str]) -> list[list[float]]:
        out = []
        for t in texts:
            v = [0.0] * DIM
            for w in re.findall(r"\w+", t.lower()):
                h = int(hashlib.md5(w.encode()).hexdigest(), 16)
                v[h % DIM] += 1.0
            n = math.sqrt(sum(x * x for x in v)) or 1.0
            out.append([x / n for x in v])
        return out


class FakeLLM(LLM):
    """Answers per role with functions registered in the test; records cost like the real one."""

    def __init__(self, db, settings):
        super().__init__(db, settings)
        self.handlers: dict[str, Callable[..., dict]] = {}
        self.calls: list[tuple[str, str]] = []
        self.unavailable: Exception | None = None

    def _fake_call(self, role: str, ctx: CallContext | None) -> None:
        self.check_budget()
        if self.unavailable:
            raise self.unavailable
        self.record(role, self.settings.model_for(role), Usage(input_tokens=1000, output_tokens=200),
                    ctx or CallContext(), 1, True)

    def structured(self, role, system, user, schema, *, max_tokens=16000, cached_context=None, ctx=None) -> dict:
        self._fake_call(role, ctx)
        self.calls.append((role, user))
        if role not in self.handlers:
            raise AssertionError(f"no handler for role {role}")
        return self.handlers[role](system=system, user=user, schema=schema, cached_context=cached_context)

    def agent(self, role, system, user, tools, final_schema, *, max_steps=12, max_tokens=16000, ctx=None,
              trace=None) -> dict:
        self._fake_call(role, ctx)
        self.calls.append((role, user))
        from pythia.llm.client import AgentTool

        def traced(t):
            def run(inp):
                if trace is not None:
                    trace.append({"tool": t.name, "input": inp})
                return t.run(inp)
            return AgentTool(t.name, t.description, t.input_schema, run)

        return self.handlers[role](system=system, user=user, tools={t.name: traced(t) for t in tools})

    # Fake Batch API: the batch "finishes" right away; each request is answered by its role's handler.
    batches: dict = {}
    bad_results: dict = {}  # custom_id → invalid text to return

    def submit_batch(self, requests, role_for_errors="batch"):
        self.check_budget()
        if self.unavailable:
            raise self.unavailable
        import secrets
        bid = f"msgbatch_{secrets.token_hex(4)}"
        self.batches[bid] = list(requests)
        return bid

    def batch_status(self, batch_id):
        return "ended"

    def batch_info(self, batch_id):
        n = len(self.batches.get(batch_id, []))
        return {"status": "ended", "counts": {"processing": 0, "succeeded": n, "errored": 0, "canceled": 0, "expired": 0}}

    def batch_results(self, batch_id):
        import json
        from types import SimpleNamespace
        for custom_id, params in self.batches.pop(batch_id):
            row = self.db.one("SELECT role FROM batch_requests WHERE custom_id = %s", (custom_id,))
            role = row["role"]
            system = params["system"][0]["text"]
            cached = params["system"][1]["text"] if len(params["system"]) > 1 else None
            user = params["messages"][0]["content"]
            self.calls.append((f"batch:{role}", user))
            text = self.bad_results.pop(custom_id, None) or json.dumps(
                self.handlers[role](system=system, user=user, schema=None, cached_context=cached))
            yield custom_id, SimpleNamespace(
                content=[SimpleNamespace(type="text", text=text)], stop_reason="end_turn", model=params["model"],
                usage=SimpleNamespace(input_tokens=1000, output_tokens=200, cache_read_input_tokens=0,
                                      cache_creation_input_tokens=0)), None


@pytest.fixture()
def db():
    with psycopg.connect(TEST_URL, autocommit=True) as conn:
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")
    database = Database(TEST_URL, min_size=1, max_size=4)
    database.open()
    yield database
    database.close()


@pytest.fixture()
def svc(db, monkeypatch, tmp_path):
    from pythia import vault

    monkeypatch.setattr(vault, "get_secret", lambda name: "sk-test")
    services = build(db, embedder=FakeEmbedder())
    # Never read the owner's real chats from disk in tests.
    cc = services.settings.get("claude_code")
    cc.update(projects_root=str(tmp_path / "cc-projects"), app_sessions_root=str(tmp_path / "cc-app"))
    services.settings.set("claude_code", cc)
    services.llm = FakeLLM(db, services.settings)
    from pythia.daemon import bootstrap

    bootstrap(services)
    return services


def owner_id(svc) -> str:
    return svc.db.one("SELECT id FROM entities WHERE is_owner")["id"]
