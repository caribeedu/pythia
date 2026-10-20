"""Edge cases of the core: deletion, dedup, resumability, configuration."""

import json
import re
from types import SimpleNamespace

from fastapi.testclient import TestClient

from pythia import events
from pythia import knowledge as K
from pythia.daemon import drain, execute
from pythia.jobs import enqueue
from pythia.llm.client import LLMUnavailable

from .helpers import add_unit, claim, default_handlers, mention
from .test_events import snapshot


def test_retry_with_queued_twin_does_not_crash_and_is_superseded(svc):
    with svc.db.tx() as conn:
        enqueue(conn, "derive_entity", "derivation", {"entity_id": "en_x"}, dedup_key="derive:en_x")
    job = svc.queue.claim("derivation", "t")
    with svc.db.tx() as conn:  # a twin enters the queue while the first one runs
        assert enqueue(conn, "derive_entity", "derivation", {"entity_id": "en_x"}, dedup_key="derive:en_x")
    assert svc.queue.claim("derivation", "t2") is None, "does not run two identical ones at the same time"
    svc.queue.retry_later(job["id"], 60, "LLM fora", count_attempt=False)
    rows = svc.db.query("SELECT status FROM jobs ORDER BY id")
    assert [r["status"] for r in rows] == ["done", "queued"]
    svc.queue.requeue_stuck()  # no error


def test_tombstone_redacts_derived_text_and_rebuild_stays_identical(svc):
    default_handlers(svc, [[claim("state", mention("atlas", "system", "secret quoted in the DM"), "wifi_password",
                                  "hunter2", "Atlas's wifi password is hunter2.")]])
    uid = add_unit(svc, ("user", "the wifi password is hunter2"), title="the wifi password is hunter2")
    drain(svc)
    svc.db.execute("UPDATE jobs SET run_after = now() WHERE status = 'queued'")
    drain(svc)
    ev = svc.db.one("SELECT id FROM evidence")["id"]
    assert svc.db.one("SELECT count(*) AS n FROM view_versions WHERE l1 LIKE '%%cl_%%'")["n"] >= 1
    with svc.db.tx() as conn:
        K.tombstone_evidence(conn, "connector", [ev], "erased at the source")
    dump = json.dumps([r["payload"] for r in svc.db.query("SELECT payload FROM events")], ensure_ascii=False)
    assert "hunter2" not in dump and "quoted secret" not in dump
    for table, col in (("claims", "statement"), ("claims", "object_value"), ("mentions", "context"),
                       ("units", "title"), ("views", "l1"), ("view_versions", "l1")):
        rows = svc.db.query(f"SELECT {col} AS v FROM {table}")
        assert not any(r["v"] and "hunter2" in str(r["v"]) for r in rows), (table, col)
    assert svc.db.one("SELECT triage FROM units WHERE id = %s", (uid,))["triage"] is None
    with svc.db.tx() as conn:
        before = snapshot(conn)
        events.rebuild(conn)
        assert snapshot(conn) == before


def test_merge_moves_mentionless_claims_and_unmerge_restores(svc):
    with svc.db.tx() as conn:
        a = K.create_entity(conn, "t", name="QP", type="team", origin="discovered")
        b = K.create_entity(conn, "t", name="Zephyr", type="team", origin="discovered")
        m = K.record_mention(conn, "t", surface="QP", context="c", type_hint="team", entity_id=a, confidence=0.9,
                             evidence_id=None, unit_id=None)
        c1 = K.assert_claim(conn, "t", kind="state", subject_mention_id=m, subject_entity_id=a, predicate="p",
                            statement="QP has on-call", origin="observed")
        c2 = K.assert_claim(conn, K.HUMAN, kind="state", subject_entity_id=a, predicate="q",
                            statement="QP belongs to Alex", origin="human")
        K.merge_entities(conn, K.HUMAN, a, b, "same team")
        subj = {r["id"]: r["subject_entity_id"] for r in conn.execute("SELECT id, subject_entity_id FROM claims").fetchall()}
        assert subj == {c1: b, c2: b}
        K.unmerge_entity(conn, K.HUMAN, a)
        subj = {r["id"]: r["subject_entity_id"] for r in conn.execute("SELECT id, subject_entity_id FROM claims").fetchall()}
        assert subj == {c1: a, c2: a}
        assert K.entity(conn, a)["status"] == "active"
        before = snapshot(conn)
        events.rebuild(conn)
        assert snapshot(conn) == before


def test_revert_does_not_clobber_a_later_change(svc):
    with svc.db.tx() as conn:
        e = K.create_entity(conn, "t", name="atlas", type="system", origin="discovered")
        c = K.assert_claim(conn, "t", kind="state", subject_entity_id=e, predicate="p", statement="x", origin="observed")
        v1 = K.apply_verdict(conn, "t", kind="real_conflict", decided_by="adjudication",
                             changes=[{"claim_id": c, "to": "disputed"}], summary="s", rationale="r")
        K.apply_verdict(conn, K.HUMAN, kind="human_correction", decided_by=K.HUMAN,
                        changes=[{"claim_id": c, "to": "retracted"}], summary="s", rationale="r")
        K.revert_verdict(conn, K.HUMAN, v1)
        assert K.claim_status(conn, c) == "retracted"


def test_inference_whose_support_is_retracted_becomes_a_tension(svc):
    with svc.db.tx() as conn:
        e = K.create_entity(conn, "t", name="atlas", type="system", origin="discovered")
        base = K.assert_claim(conn, "t", kind="decision", subject_entity_id=e, predicate="p", statement="decided X",
                              origin="observed")
        inf = K.assert_claim(conn, "synthesis", kind="decision", subject_entity_id=e, predicate="q",
                             statement="X was reverted in practice", origin="inferred", supporting_claim_ids=[base])
        K.apply_verdict(conn, K.HUMAN, kind="human_correction", decided_by=K.HUMAN,
                        changes=[{"claim_id": base, "to": "retracted"}], summary="s", rationale="r")
        t = conn.execute("SELECT * FROM tensions").fetchone()
        assert t and set(t["claim_ids"]) == {base, inf}
        assert conn.execute("SELECT 1 FROM jobs WHERE kind = 'adjudicate' AND status = 'queued'").fetchone()


def test_pause_is_not_cleared_by_a_job_that_did_not_call_the_llm(svc):
    svc.state.set("cognition", {"paused_since": "x", "reason": "no network"})
    with svc.db.tx() as conn:
        enqueue(conn, "derive_entity", "derivation", {"entity_id": "en_inexistente"})
    execute(svc, svc.queue.claim("derivation", "t"))
    assert svc.state.get("cognition") is not None


def test_changing_models_triggers_golden_run(svc):
    from pythia.quality import add_question
    from pythia.web.app import create_app

    add_question(svc, "Who owns atlas?", "Zephyr")
    client = TestClient(create_app(svc, with_mcp=False))
    form = {f"model_{r}": "claude-sonnet-5-5" for r in svc.settings.get("models")}
    form.update({"cap": "50", "owner_name": "Alex", "aliases": "Alex", "policy": "p", "debounce": "5",
                 "projects_root": "~/.claude/projects", "push_max": "6000", "prices": json.dumps(svc.settings.get("prices"))})
    client.post("/action/config", data=form, follow_redirects=False)
    run = svc.db.one("SELECT config FROM golden_runs")
    assert run and run["config"]["reason"] == "models changed"


def test_agent_step_limit_keeps_tools_declared_with_tool_choice_none(db, monkeypatch):
    from pythia import vault
    from pythia.llm.client import LLM, AgentTool, schema_object
    from pythia.settings import Settings

    monkeypatch.setattr(vault, "get_secret", lambda name: "sk")
    llm = LLM(db, Settings(db))
    usage = SimpleNamespace(input_tokens=1, output_tokens=1, cache_read_input_tokens=0, cache_creation_input_tokens=0)
    loop = SimpleNamespace(stop_reason="tool_use", usage=usage,
                           content=[SimpleNamespace(type="tool_use", id="t", name="x", input={"n": 1})])
    final = SimpleNamespace(stop_reason="end_turn", usage=usage, content=[SimpleNamespace(type="text", text='{"ok": true}')])
    sent = []

    class Stream:
        def __init__(self, params):
            sent.append(params)
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False
        def get_final_message(self):
            return final if len(sent) > 2 else loop

    monkeypatch.setattr(llm, "client", lambda: SimpleNamespace(messages=SimpleNamespace(stream=lambda **p: Stream(p))))
    tool = AgentTool("x", "x", schema_object({"n": {"type": "integer"}}), lambda inp: 1)
    assert llm.agent("adjudication", "s", "u", [tool], schema_object({"ok": {"type": "boolean"}}), max_steps=2) == {"ok": True}
    assert sent[-1]["tool_choice"] == {"type": "none"} and sent[-1]["tools"]


def test_narrative_pass_retires_unsupported_and_shows_cited_claims(svc):
    """A narrative whose content was all erased at the source goes away through the cascade (before, it stayed as a
    placeholder forever); the ones that remain reach the LLM with the claims they cite, so that
    update/merge preserves the citations instead of creating another narrative."""
    default_handlers(svc, [[
        claim("state", mention("atlas", "system", "repository"), "owner", "Zephyr",
              "The atlas system belongs to the Zephyr team."),
    ]])
    add_unit(svc, ("user", "Atlas belongs to Zephyr."), ("assistant", "Ok."))
    drain(svc, lanes=("cognition", "derivation"))
    alive = svc.db.one("SELECT id FROM claims WHERE status = 'current'")["id"]
    from pythia.pipeline import derive
    with svc.db.tx() as conn:
        zombie = derive._save_view(conn, svc, view_id=None, kind="narrative", entity_id=None, title="Zumbi",
                                   l0="(regenerating: part of the content was removed at the source)", l1="",
                                   cited=["cl_apagado"], members=[], watermark="x", vec=None)
        derive._save_view(conn, svc, view_id=None, kind="narrative", entity_id=None, title="atlas",
                          l0="Atlas belongs to Zephyr.", l1="", cited=[alive], members=[], watermark="y", vec=None)
    seen = []

    def synth(user, **_):
        seen.append(user)
        return {"narratives": [], "inferences": []}

    svc.llm.handlers["synthesis"] = synth
    out = derive.derive_narratives(svc, review=True)
    assert out["retired_unsupported"] == 1
    assert svc.db.one("SELECT status FROM views WHERE id = %s", (zombie,))["status"] == "retired"
    assert "Zumbi" not in seen[0]
    assert f"{alive} (" in seen[0].split("ENTITY PAGES")[0], "cited claim appears under the narrative"
