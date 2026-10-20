import json
import re
from types import SimpleNamespace

import anyio
import pytest
from fastapi.testclient import TestClient

from pythia import consumer
from pythia import knowledge as K
from pythia.daemon import drain
from pythia.llm.client import LLM, BudgetExceeded, LLMHalted, LLMUnavailable
from pythia.pipeline import correct
from pythia.quality import correction_metrics, start_run
from pythia.web.app import create_app

from .conftest import owner_id
from .helpers import add_unit, claim, default_handlers, mention


def seed(svc):
    default_handlers(svc, [[
        claim("profile", mention("Alex"), "prefers_writing_style", "short sentences", "Alex prefers short sentences."),
        claim("state", mention("atlas", "system"), "owner", "Zephyr", "atlas belongs to Zephyr.",
              obj=mention("Zephyr", "team")),
    ]])
    add_unit(svc, ("user", "shorten this; atlas belongs to Zephyr"))
    drain(svc)
    svc.db.execute("UPDATE jobs SET run_after = now() WHERE status = 'queued'")
    drain(svc)


# ---------------------------------------------------------------- halt without an LLM


def test_unavailable_llm_requeues_without_spending_attempts_and_pull_keeps_working(svc):
    seed(svc)
    svc.llm.unavailable = LLMUnavailable("no network")
    uid = add_unit(svc, ("user", "something else"))
    drain(svc, lanes=("cognition",))
    job = svc.db.one("SELECT * FROM jobs WHERE payload->>'unit_id' = %s", (str(uid),))
    assert job["status"] == "queued" and job["attempts"] == 0 and "no network" in job["last_error"]
    assert svc.db.one("SELECT status FROM units WHERE id = %s", (uid,))["status"] == "pending"
    state = svc.state.get("cognition")
    assert state["paused_since"] and "no network" in state["reason"]
    # Pull serves what has already been concluded, with a notice.
    out = consumer.search(svc, "atlas Zephyr")
    assert "Cognition paused" in out and "atlas" in out
    # Push does not assemble a briefing without an LLM (no heuristic in its place).
    assert consumer.briefing(svc, event="UserPromptSubmit", prompt="atlas?", cwd="/repo", session_id="s") == ""
    # A correction is recorded anyway; the application stays in the queue.
    cid = correct.request_correction(svc, None, "atlas belongs to Order Mgmt", via="mcp")
    assert svc.db.one("SELECT status FROM corrections WHERE id = %s", (cid,))["status"] == "pending"
    # Network back: processes and clears the pause.
    svc.llm.unavailable = None
    svc.db.execute("UPDATE jobs SET run_after = now() WHERE status = 'queued'")
    svc.llm.handlers["correction"] = lambda **_: {"ops": [], "error_type": "other", "blamed_role": "none", "summary": "nothing"}
    drain(svc, lanes=("cognition",))
    assert svc.state.get("cognition") is None
    assert svc.db.one("SELECT status FROM units WHERE id = %s", (uid,))["status"] == "processed"


def test_budget_cap_halts_llm_calls(svc):
    default_handlers(svc, [])
    svc.settings.set("monthly_cap_usd", 0.001)
    svc.db.execute("INSERT INTO llm_calls (role, model, cost_usd, ok) VALUES ('triage', 'claude-haiku-4-5', 0.01, true)")
    with pytest.raises(BudgetExceeded):
        svc.llm.check_budget()
    uid = add_unit(svc, ("user", "something"))
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT status FROM units WHERE id = %s", (uid,))["status"] == "pending"
    assert "cap" in svc.state.get("cognition")["reason"]


def test_missing_key_halts(db):
    from pythia import vault
    from pythia.settings import Settings

    llm = LLM(db, Settings(db))
    orig = vault.get_secret
    vault.get_secret = lambda name: None
    try:
        with pytest.raises(LLMHalted):
            llm.client()
    finally:
        vault.get_secret = orig


# ---------------------------------------------------------------- corrections


def test_text_correction_replaces_claim_with_human_authority(svc):
    seed(svc)
    target = svc.db.one("SELECT id FROM claims WHERE predicate = 'owner'")["id"]

    def corrector(user, **_):
        assert target in user and "Order Mgmt" in user
        return {"ops": [{"op": "replace_claim", "claim_id": target, "mention_id": None, "entity_id": None,
                         "into_entity_id": None, "verdict_id": None, "new_entity": None, "name": None, "type": None,
                         "add_aliases": [], "kind": None, "predicate": None, "value": "Order Mgmt",
                         "statement": "atlas belongs to Order Mgmt."}],
                "error_type": "claim", "blamed_role": "extraction", "summary": "wrong owner"}

    svc.llm.handlers["correction"] = corrector
    cid = consumer.request_correction(svc, target, "atlas belongs to Order Mgmt, not to Zephyr")
    drain(svc, lanes=("cognition",))
    old = svc.db.one("SELECT status FROM claims WHERE id = %s", (target,))
    new = svc.db.one("SELECT * FROM claims WHERE origin = 'human'")
    assert old["status"] == "superseded"
    assert new["statement"] == "atlas belongs to Order Mgmt." and new["source_label"] == K.HUMAN
    v = svc.db.one("SELECT * FROM verdicts WHERE kind = 'human_correction'")
    assert v["decided_by"] == K.HUMAN
    m = correction_metrics(svc)
    assert m["total"] == 1 and m["by_role"][0]["k"] == "extraction"


def test_definition_is_processed_with_max_authority_and_no_triage(svc):
    default_handlers(svc, [[claim("glossary", mention("ZEPH", "concept"), "means", "Zephyr",
                                  "ZEPH is the acronym of the Zephyr team.")]])
    consumer.request_definition(svc, "ZEPH = Zephyr")
    drain(svc, lanes=("cognition",))
    c = svc.db.one("SELECT * FROM claims")
    assert c["origin"] == "human" and c["confidence"] == 1.0
    assert not any(r == "triage" for r, _ in svc.llm.calls)


def test_web_direct_actions(svc):
    seed(svc)
    client = TestClient(create_app(svc, with_mcp=False))
    c = svc.db.one("SELECT id, subject_mention_id FROM claims WHERE predicate = 'owner'")
    r = client.post("/api/retract", json={"claim_id": c["id"]})
    assert r.status_code == 200
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT status FROM claims WHERE id = %s", (c["id"],))["status"] == "retracted"
    r = client.post("/api/relink", json={"mention_id": c["subject_mention_id"], "new_name": "atlas-core",
                                          "new_type": "system"})
    assert r.status_code == 200
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT e.name FROM claims c JOIN entities e ON e.id = c.subject_entity_id WHERE c.id = %s",
                      (c["id"],))["name"] == "atlas-core"
    v = svc.db.one("SELECT id FROM verdicts WHERE kind = 'human_correction'")
    r = client.post(f"/action/revert/{v['id']}", follow_redirects=False)
    assert r.status_code == 303
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT status FROM claims WHERE id = %s", (c["id"],))["status"] == "current"
    assert correction_metrics(svc)["total"] == 3


# ---------------------------------------------------------------- web, push e MCP


def test_pages_render_and_graph_supports_time(svc):
    seed(svc)
    client = TestClient(create_app(svc, with_mcp=False))
    for path in ("/evolution", "/evolution?no_effect=1", "/evolution?verdicts=1", "/explore", "/sources", "/costs", "/quality", "/config"):
        r = client.get(path)
        assert r.status_code == 200, path
    g = client.get("/api/graph").json()
    assert g["center"] == owner_id(svc)
    atlas = svc.db.one("SELECT id FROM entities WHERE name = 'atlas'")["id"]
    g = client.get(f"/api/graph?center={atlas}").json()
    labels = {n["label"] for n in g["nodes"]}
    assert {"atlas", "Zephyr"} <= labels and g["edges"][0]["label"] == "owner"
    early = svc.db.one("SELECT min(ts) AS t FROM events")["t"].isoformat()
    g_early = client.get(f"/api/graph?center={atlas}&t={early}").json()
    assert len(g_early["edges"]) == 0
    detail = client.get(f"/api/node/{atlas}").json()
    assert detail["l0"] == "L0 of atlas" and detail["claims"] and detail["mentions"]
    assert client.get("/api/search?q=atlas").json()


def test_config_saves_and_stores_key_in_keychain(svc, monkeypatch):
    from pythia import vault
    stored = {}
    monkeypatch.setattr(vault, "set_secret", lambda k, v: stored.__setitem__(k, v))
    client = TestClient(create_app(svc, with_mcp=False))
    form = {f"model_{r}": "claude-sonnet-5-5" for r in svc.settings.get("models")}
    form.update({"cap": "12.5", "owner_name": "Alex", "aliases": "Alex, alexm", "policy": "p", "debounce": "2",
                 "projects_root": "~/.claude/projects", "push_enabled": "on", "push_max": "3000", "api_key": "sk-new",
                 "prices": json.dumps(svc.settings.get("prices"))})
    r = client.post("/action/config", data=form, follow_redirects=False)
    assert r.status_code == 303
    assert svc.settings.get("monthly_cap_usd") == 12.5
    assert svc.settings.model_for("triage") == "claude-sonnet-5-5"
    assert svc.settings.get("claude_code")["debounce_minutes"] == 2
    assert stored == {"anthropic_api_key": "sk-new"}


def test_hook_injects_briefing(svc):
    seed(svc)
    views = {r["title"]: r["id"] for r in svc.db.query("SELECT id, title FROM views")}

    def brief(user, cached_context, **_):
        assert "KNOWLEDGE MAP" in cached_context and "atlas" in cached_context
        assert "who takes care of atlas?" in user
        return {"relevant_ids": [views["atlas"], "vw_inexistente"], "verdict_ids": []}

    svc.llm.handlers["briefing"] = brief
    client = TestClient(create_app(svc, with_mcp=False))
    prompt = "who takes care of atlas? New suggestion: create an E2E test simulating a Kafka failure."
    r = client.post("/hook/claude-code", json={"hook_event_name": "UserPromptSubmit", "prompt": prompt,
                                               "cwd": "/repo/atlas", "session_id": "s1"})
    out = r.json()["hookSpecificOutput"]
    assert out["hookEventName"] == "UserPromptSubmit"
    ctx = out["additionalContext"]
    haven_l0 = svc.db.one("SELECT l0 FROM views WHERE id = %s", (views["atlas"],))["l0"]
    assert f"{views['atlas']} · atlas: {haven_l0}" in ctx and "About Alex: L0 of Alex" in ctx
    assert "vw_inexistente" not in ctx
    # only the map's L0; nothing from the request nor free LLM text gets into the context.
    assert "Kafka" not in ctx and "E2E" not in ctx
    push = svc.db.one("SELECT * FROM pushes ORDER BY id DESC LIMIT 1")
    assert push["selected_ids"] == [views["atlas"]]


def test_mcp_exposes_exactly_five_tools(svc):
    from pythia.mcp_server import build_mcp

    mcp = build_mcp(svc)
    tools = anyio.run(mcp.list_tools)
    assert sorted(t.name for t in tools) == ["correct", "define", "open", "search", "timeline"]


def test_golden_run(svc):
    seed(svc)
    from pythia.quality import add_question
    add_question(svc, "Who owns atlas?", "Zephyr")

    def answer(user, tools, **_):
        found = tools["search"].run({"query": "atlas owner"})
        return {"answer": "Zephyr" if "atlas" in found else "I don't know", "used_ids": []}

    svc.llm.handlers["golden"] = lambda **kw: answer(**kw) if "tools" in kw else {"passed": "Zephyr" in kw["user"].split("Answer given:")[1], "rationale": "ok"}
    rid = start_run(svc)
    drain(svc, lanes=("derivation",))
    run = svc.db.one("SELECT * FROM golden_runs WHERE id = %s", (rid,))
    assert run["status"] == "done" and run["passed"] == 1 and run["total"] == 1


# ---------------------------------------------------------------- LLM client


def test_llm_params_and_cost(db):
    from pythia.settings import Settings
    llm = LLM(db, Settings(db))
    p = llm._params("triage", [{"type": "text", "text": "s"}], [{"role": "user", "content": "u"}], 100, {"type": "object"}, None)
    assert p["model"] == "claude-haiku-4-5" and "effort" not in p["output_config"]
    assert p["output_config"]["format"]["type"] == "json_schema" and "tool_choice" not in p
    p = llm._params("adjudication", [], [], 100, {"type": "object"}, [{"name": "x"}])
    assert p["model"] == "claude-sonnet-5-5" and p["output_config"]["effort"] == "medium"
    assert p["tool_choice"] == {"type": "auto"}  # Sonnet 5.5 rejects forced tool use
    from pythia.llm.client import Usage
    cost = llm.price("claude-sonnet-5-5", Usage(input_tokens=1_000_000, output_tokens=100_000, cache_read=1_000_000))
    assert round(cost, 4) == round(2.0 + 1.0 + 0.2, 4)


def test_llm_agent_loop_runs_tools_and_returns_final_json(db, monkeypatch):
    from pythia import vault
    from pythia.llm.client import AgentTool, schema_object
    from pythia.settings import Settings

    monkeypatch.setattr(vault, "get_secret", lambda name: "sk")
    llm = LLM(db, Settings(db))
    replies = [
        SimpleNamespace(stop_reason="tool_use", content=[SimpleNamespace(type="tool_use", id="t1", name="double", input={"n": 21})],
                        usage=SimpleNamespace(input_tokens=10, output_tokens=5, cache_read_input_tokens=0, cache_creation_input_tokens=0)),
        SimpleNamespace(stop_reason="end_turn", content=[SimpleNamespace(type="text", text='{"r": 42}')],
                        usage=SimpleNamespace(input_tokens=10, output_tokens=5, cache_read_input_tokens=8, cache_creation_input_tokens=0)),
    ]
    sent = []

    class Stream:
        def __init__(self, params):
            sent.append(params)
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False
        def get_final_message(self):
            return replies.pop(0)

    fake = SimpleNamespace(messages=SimpleNamespace(stream=lambda **params: Stream(params)))
    monkeypatch.setattr(llm, "client", lambda: fake)
    tool = AgentTool("double", "doubles", schema_object({"n": {"type": "integer"}}), lambda inp: inp["n"] * 2)
    out = llm.agent("adjudication", "sys", "u", [tool], schema_object({"r": {"type": "integer"}}))
    assert out == {"r": 42}
    assert sent[1]["messages"][-1]["content"][0] == {"type": "tool_result", "tool_use_id": "t1", "content": "42"}
    assert sent[0]["tools"][0]["strict"] is True
    assert db.one("SELECT count(*) AS n, sum(cache_read) AS cr FROM llm_calls") == {"n": 2, "cr": 8}


def test_processing_now_shows_worker_stage_queue_and_batches(svc):
    """what each worker does, the stage, for how long, the queue per lane and the batches."""
    from datetime import datetime, timedelta, timezone
    from psycopg.types.json import Jsonb
    from pythia.web import processing
    svc.db.execute("INSERT INTO sources (id, kind, name, config) VALUES ('s1', 'claude_code', 'PROJ-661', '{}')")
    uid = svc.db.one("""INSERT INTO units (source_id, kind, title, occurred_at, meta) VALUES ('s1', 'session_delta', 'delta', now(), %s)
                        RETURNING id""", (Jsonb({"now": {"stage": "extraction",
                                                         "since": (datetime.now(timezone.utc) - timedelta(seconds=20)).isoformat()}}),))["id"]
    svc.db.execute("""INSERT INTO jobs (kind, lane, payload, status, worker, started_at) VALUES ('process_unit', 'cognition', %s,
                      'running', 'cognition-0', now() - interval '25 seconds')""", (Jsonb({"unit_id": uid}),))
    svc.db.execute("""INSERT INTO llm_calls (role, model, duration_ms, ok, cost_usd) VALUES ('extraction', 'm', 50000, true, 0)""")
    for _ in range(3):
        svc.db.execute("INSERT INTO jobs (kind, lane, payload) VALUES ('derive_entity', 'derivation', '{}')")
    svc.db.execute("""INSERT INTO jobs (kind, lane, status, started_at, finished_at) VALUES ('derive_entity', 'derivation',
                      'done', now() - interval '10 seconds', now())""")
    svc.db.execute("INSERT INTO llm_batches (id, status, request_count, submitted_at) VALUES ('b1', 'in_progress', 12, now() - interval '60 seconds')")
    svc.db.execute("""INSERT INTO llm_batches (id, status, request_count, submitted_at, ended_at) VALUES ('b0', 'ended', 5,
                      now() - interval '1 hour', now() - interval '1 hour' + interval '300 seconds')""")
    snap = processing.snapshot(svc)
    r = snap["running"][0]
    assert r["stage"] == "extraction" and "PROJ-661" in r["what"] and 18 <= r["elapsed"] <= 40 and 5 <= r["remaining"] <= 35
    lane = next(l for l in snap["lanes"] if l["lane"] == "derivation")
    assert lane["ready"] == 3 and abs(lane["eta"] - 15) < 2  # 3 × 10 s ÷ 2 workers
    assert snap["batches"][0]["requests"] == 12 and 200 <= snap["batches"][0]["remaining"] <= 245
    html = TestClient(create_app(svc, with_mcp=False)).get("/sources/now").text
    assert "extraction" in html and "empties in" in html and "12 stages" in html


# ---------------------------------------------------------------- Usage


def test_usage_rebuilds_exactly_what_was_delivered_from_versioned_refs(svc, tmp_path):
    from pythia import usage
    cc_cfg = svc.settings.get("claude_code")
    cc_cfg.update(projects_root=str(tmp_path / "p"), app_sessions_root=str(tmp_path / "a"))
    svc.settings.set("claude_code", cc_cfg)
    from pythia.mcp_server import build_mcp
    from pythia.pipeline import derive
    seed(svc)
    view = svc.db.one("SELECT * FROM views WHERE kind = 'entity' AND title = 'atlas'")
    svc.llm.handlers["briefing"] = lambda **_: {"relevant_ids": [view["id"]], "verdict_ids": []}
    client = TestClient(create_app(svc, with_mcp=False))
    client.post("/hook/claude-code", json={"hook_event_name": "UserPromptSubmit", "prompt": "atlas?",
                                           "cwd": "/repo/atlas", "session_id": "sess-1"})
    mcp = build_mcp(svc)
    anyio.run(lambda: mcp.call_tool("open", {"id": view["id"], "level": 1}))
    delivered_l1 = view["l1"]
    # The page changes after delivery: the screen shows the delivered version, not the current one.
    with svc.db.tx() as conn:
        derive._save_view(conn, svc, view_id=view["id"], kind="entity", entity_id=view["entity_id"], title="atlas",
                          l0="New L0", l1="New L1", cited=view["cited_claim_ids"], members=[], watermark="w2", vec=None)
    push = usage.push_by_chat(svc, {"sess-1": "PROJ-661"})
    assert push[0]["title"] == "PROJ-661"
    texts = [it["text"] for it in push[0]["pushes"][0]["entries"]]
    assert view["l0"] in texts and "New L0" not in texts
    pull = usage.pull_calls(svc)[0]
    assert pull["tool"] == "open" and pull["entries"][0]["text"] == delivered_l1
    # Erased at the source: the versions that cited the content go away, and the screen says it was erased.
    ev = svc.db.query("SELECT id FROM evidence")
    with svc.db.tx() as conn:
        K.tombstone_evidence(conn, "connector", [e["id"] for e in ev], "erased")
    pull = usage.pull_calls(svc)[0]
    assert pull["entries"][0].get("gone")
    page = client.get("/usage").text
    assert "session sess-1" in page and "open" in page and "erased at the source" in page


def test_push_does_not_repeat_what_this_conversation_already_received(svc):
    from pythia.pipeline import derive
    seed(svc)
    atlas = svc.db.one("SELECT * FROM views WHERE kind = 'entity' AND title = 'atlas'")
    svc.llm.handlers["briefing"] = lambda **_: {"relevant_ids": [atlas["id"]], "verdict_ids": []}
    client = TestClient(create_app(svc, with_mcp=False))

    def push(event="UserPromptSubmit", session="s1"):
        r = client.post("/hook/claude-code", json={"hook_event_name": event, "prompt": "atlas?", "session_id": session})
        return (r.json().get("hookSpecificOutput") or {}).get("additionalContext", "")

    first = push()
    assert "About Alex" in first and atlas["id"] in first
    assert push() == "", "nothing new: repeats neither the profile nor the page"
    assert svc.db.one("SELECT skipped FROM pushes ORDER BY id DESC LIMIT 1")["skipped"] == "already delivered in this conversation"
    assert "About Alex" in push(session="s2"), "another conversation receives everything"
    with svc.db.tx() as conn:  # the page changed: the new version is delivered again, the profile is not
        derive._save_view(conn, svc, view_id=atlas["id"], kind="entity", entity_id=atlas["entity_id"], title="atlas",
                          l0="New L0 of atlas", l1="", cited=atlas["cited_claim_ids"], members=[], watermark="w2", vec=None)
    again = push()
    assert "New L0 of atlas" in again and "About Alex" not in again
    assert "About Alex" in push(event="SessionStart"), "start (or compaction) delivers everything again"


def test_batch_eta_is_redone_when_the_average_is_already_past():
    from pythia.web.processing import batch_eta
    history = [120.0, 300.0, 780.0]
    assert batch_eta(60, 100, None, history)["remaining"] == 60  # the fastest seen has not expired yet
    late = batch_eta(400, 100, None, history)
    assert late["remaining"] == 380 and late["basis"] == "batch history"
    pace = batch_eta(400, 100, {"succeeded": 80, "errored": 0, "canceled": 0, "expired": 0}, history)
    assert pace["basis"] == "current pace" and pace["remaining"] == 100 and pace["done"] == 80
    beyond = batch_eta(900, 100, None, history)
    assert beyond["remaining"] is None and "24 h" in beyond["basis"]


def test_push_counts_the_whole_chat_and_only_resets_when_the_llm_loses_context(svc):
    """Resuming (a new session file of the same chat) resends nothing; the owner profile does not come back just
    because the page was regenerated; compacting (or /clear, or a new chat) delivers everything again."""
    from pathlib import Path
    from pythia.pipeline import derive
    seed(svc)
    cfg = svc.settings.get("claude_code")
    proj = Path(cfg["projects_root"]) / "-repo"
    proj.mkdir(parents=True)
    for stem in ("s1", "s2"):
        (proj / f"{stem}.jsonl").write_text('{"type": "user", "uuid": "u1"}\n')
    app = Path(cfg["app_sessions_root"]) / "a" / "o"
    app.mkdir(parents=True)
    (app / "local_X.json").write_text(json.dumps({"sessionId": "local_X", "cliSessionId": "s2",
                                                  "priorCliSessionIds": ["s1"], "title": "PROJ-661"}))
    atlas = svc.db.one("SELECT * FROM views WHERE kind = 'entity' AND title = 'atlas'")
    svc.llm.handlers["briefing"] = lambda **_: {"relevant_ids": [atlas["id"]], "verdict_ids": []}
    client = TestClient(create_app(svc, with_mcp=False))

    def push(session, event="UserPromptSubmit", source=None):
        r = client.post("/hook/claude-code", json={"hook_event_name": event, "prompt": "atlas?", "session_id": session,
                                                   **({"source": source} if source else {})})
        return (r.json().get("hookSpecificOutput") or {}).get("additionalContext", "")

    assert "About Alex" in push("s1")
    assert push("s2", "SessionStart", "resume") == "", "resume: the history stays with the LLM"
    assert push("s2") == ""
    own = svc.db.one("SELECT v.* FROM views v JOIN entities e ON e.id = v.entity_id WHERE e.is_owner AND v.kind = 'entity'")
    with svc.db.tx() as conn:  # the profile was regenerated: it is not resent mid-conversation
        derive._save_view(conn, svc, view_id=own["id"], kind="entity", entity_id=own["entity_id"], title=own["title"],
                          l0="rewritten profile", l1="", cited=own["cited_claim_ids"], members=[], watermark="w9", vec=None)
    assert push("s2") == ""
    after = push("s2", "SessionStart", "compact")
    assert "rewritten profile" in after and atlas["id"] in after, "compaction: the LLM lost the context"
