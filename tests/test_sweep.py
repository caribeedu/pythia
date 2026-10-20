"""Drift sweep and live reads."""

from __future__ import annotations

import base64



from pythia import knowledge as K
from pythia import quality
from pythia.daemon import drain, scan_sources
from pythia.pipeline import live, sweep

from .helpers import add_unit, claim, default_handlers, mention


def seed(svc):
    default_handlers(svc, [[claim("state", mention("atlas", "system", "repository"), "owner", "team X",
                                  "Atlas belongs to team X.")]])
    add_unit(svc, ("user", "atlas belongs to team X"))
    drain(svc, lanes=("cognition",))
    return svc.db.one("SELECT id FROM claims WHERE predicate = 'owner'")["id"]


class FakeGH:
    def get(self, path, params=None):
        assert path == "/repos/acme-corp/atlas/contents/.github/CODEOWNERS"
        return {"content": base64.b64encode(b"* @acme-corp/zephyr\n").decode()}


def test_pick_prioritises_old_docs_with_newer_activity_and_skips_recently_reviewed(svc):
    with svc.db.tx() as conn:
        a = K.create_entity(conn, "t", name="system A", type="system", origin="discovered")
        b = K.create_entity(conn, "t", name="system B", type="system", origin="discovered")
        for eid, src, days in ((a, "confluence:space:QP", 30), (a, "github:acme-corp/x", 1), (b, "slack:C1", 5)):
            conn.execute("""INSERT INTO claims (id, kind, subject_entity_id, predicate, object_value, statement, recorded_at,
                            origin, source_id, status, confidence, evidence_ids) VALUES
                            (%s, 'state', %s, 'p', 'v', 's', now() - %s * interval '1 day', 'observed', %s, 'current', 1, '{}')""",
                         (f"cl_{eid[-6:]}{days:06d}", eid, days, src))
    with svc.db.tx() as conn:
        assert sweep.pick(conn, 2)[0]["id"] == a, "old documentation with newer activity in another source comes first"
        conn.execute("INSERT INTO sweep_reviews (entity_id) VALUES (%s)", (a,))
        assert a not in [r["id"] for r in sweep.pick(conn, 5)], "reviewed today: leaves the rotation"


def test_sweep_accuses_with_live_proof_and_the_adjudicator_judges_within_the_budget(svc, monkeypatch):
    old = seed(svc)
    monkeypatch.setattr(live.LiveReader, "_gh", lambda self: FakeGH())
    monkeypatch.setattr(sweep, "pick", lambda conn, limit=1: [conn.execute(
        "SELECT id, name, type FROM entities WHERE name = 'atlas'").fetchone()])

    def detect(user, tools, **_):
        assert old in user and "atlas" in user
        read = tools["read_codeowners"].run({"repo": "acme-corp/atlas"})
        return {"summary": "owner changed", "suspicions": [{
            "claim_ids": [old, "cl_nonexistent"], "hypothesis": "atlas moved to Zephyr",
            "reason": "the current CODEOWNERS points to @acme-corp/zephyr", "evidence": [], "proofs": [read["read"]]}]}

    seen = []

    def judge(user, tools, **_):
        seen.append(user)
        return {"kind": "outdated_source", "changes": [{"claim_id": old, "to": "superseded"}], "summary": "owner changed",
                "rationale": "current CODEOWNERS", "cited_evidence": [], "confidence": 0.9}

    svc.llm.handlers["sweep"] = detect
    svc.llm.handlers["adjudication"] = judge
    out = sweep.run(svc)
    assert out["suspicions"] == 1
    t = svc.db.one("SELECT * FROM tensions WHERE opened_by = 'sweep'")
    assert t["claim_ids"] == [old], "nonexistent claim is left out"
    proof = svc.db.one("SELECT * FROM evidence WHERE id = %s", (t["dossier"]["evidence_ids"][0],))
    assert proof["source_id"] == "live:github" and "@acme-corp/zephyr" in proof["content"] and "Read live on" in proof["content"]
    assert svc.db.one("SELECT kind FROM units WHERE id = %s", (proof["unit_id"],))["kind"] == "live_read"
    assert svc.db.one("SELECT status FROM claims WHERE id = %s", (old,))["status"] == "current", "the sweep only accuses"
    drain(svc, lanes=("cognition",))
    assert "accusation" in seen[0] and "@acme-corp/zephyr" in seen[0] and "atlas moved to Zephyr" in seen[0]
    assert svc.db.one("SELECT status FROM claims WHERE id = %s", (old,))["status"] == "superseded"
    roles = {r["role"] for r in svc.db.query("SELECT role FROM llm_calls WHERE source_id = 'sweep'")}
    assert {"sweep", "adjudication", "triage"} <= roles, "judgment and processing of the proof count against the sweep"
    assert svc.db.one("SELECT count(*) AS n FROM llm_calls WHERE source_id LIKE 'live:%%'")["n"] == 0
    rv = svc.db.one("SELECT suspicions, summary, tools FROM sweep_reviews")
    assert rv["suspicions"] == 1 and rv["summary"] == "owner changed"
    assert rv["tools"] == [{"tool": "read_codeowners", "input": {"repo": "acme-corp/atlas"}}], "auditable, without the content read"
    assert "read_codeowners(acme-corp/atlas)" in quality.recent_sweeps(svc)[0]["investigated"]
    prec = {d["detector"]: d for d in quality.detector_precision(svc)}
    assert prec["sweep"]["confirmed"] == 1 and prec["sweep"]["precision"] == 1.0


def test_budget_and_idle_queue_gate_the_sweep(svc, monkeypatch):
    seed(svc)
    svc.settings.set("sweep", {"enabled": True, "daily_budget_usd": 0.001})
    svc.llm.handlers["sweep"] = lambda **_: {"summary": "nothing", "suspicions": []}
    assert sweep.run(svc)["suspicions"] == 0
    assert sweep.budget_left(svc) <= 0
    assert sweep.run(svc) == {"skipped": "today's budget spent (or sweep turned off)"}
    svc.settings.set("sweep", {"enabled": True, "daily_budget_usd": 5})
    svc.db.execute("INSERT INTO jobs (kind, lane, payload) VALUES ('process_unit', 'cognition', '{}')")
    scan_sources(svc)
    assert svc.db.one("SELECT count(*) AS n FROM jobs WHERE kind = 'drift_sweep'")["n"] == 0, "cognition queue busy"
    svc.db.execute("DELETE FROM jobs WHERE kind = 'process_unit'")
    scan_sources(svc)
    assert svc.db.one("SELECT count(*) AS n FROM jobs WHERE kind = 'drift_sweep'")["n"] == 1
    svc.settings.set("sweep", {"enabled": False, "daily_budget_usd": 5})
    assert sweep.budget_left(svc) == 0


def test_adjudicator_live_read_cited_as_proof_becomes_evidence(svc, monkeypatch):
    old = seed(svc)
    monkeypatch.setattr(live.LiveReader, "_gh", lambda self: FakeGH())
    with svc.db.tx() as conn:
        tid = K.open_tension(conn, "integration", [old], "diverging owner")

    def judge(user, tools, **_):
        r = tools["read_codeowners"].run({"repo": "acme-corp/atlas"})
        return {"kind": "outdated_source", "changes": [], "summary": "s", "rationale": "r",
                "cited_evidence": [r["read"], "read:99"], "confidence": 0.8}

    svc.llm.handlers["adjudication"] = judge
    drain(svc, lanes=("cognition",))
    v = svc.db.one("SELECT cited_evidence FROM verdicts WHERE tension_id = %s", (tid,))
    assert len(v["cited_evidence"]) == 1 and v["cited_evidence"][0].startswith("ev_"), "nonexistent read disappears"
    assert svc.db.one("SELECT source_id FROM evidence WHERE id = %s", (v["cited_evidence"][0],))["source_id"] == "live:github"


def test_live_proof_of_same_content_is_reused(svc, monkeypatch):
    monkeypatch.setattr(live.LiveReader, "_gh", lambda self: FakeGH())
    r1, r2 = live.LiveReader(svc), live.LiveReader(svc)
    a = r1.store_proofs([r1.read_codeowners({"repo": "acme-corp/atlas"})["read"]])
    b = r2.store_proofs([r2.read_codeowners({"repo": "acme-corp/atlas"})["read"]])
    assert list(a.values()) == list(b.values())



class DownGH:
    def get(self, path, params=None):
        from pythia.connectors.common import SourceUnavailable
        raise SourceUnavailable("GitHub unreachable: timeout")


def test_failed_live_read_makes_the_sweep_inconclusive_not_clean(svc, monkeypatch):
    """a read that did not happen does not become "nothing found"; the entity goes back into the rotation."""
    seed(svc)
    monkeypatch.setattr(live.LiveReader, "_gh", lambda self: DownGH())
    monkeypatch.setattr(sweep, "pick", lambda conn, limit=1: [conn.execute(
        "SELECT id, name, type FROM entities WHERE name = 'atlas'").fetchone()])

    def detect(tools, **_):
        tools["read_codeowners"].run({"repo": "acme-corp/atlas"})
        return {"summary": "nothing found", "suspicions": []}

    svc.llm.handlers["sweep"] = detect
    assert sweep.run(svc)["failed_reads"] == 3, "the three CODEOWNERS paths failed"
    rv = quality.recent_sweeps(svc)[0]
    assert rv["inconclusive"] and "✗" in rv["investigated"]
    assert svc.db.one("SELECT reviewed_at < now() - interval '22 hours' AS back FROM sweep_reviews")["back"]


def test_not_found_is_an_answer_not_a_failure(svc, monkeypatch):
    class NoFile:
        def get(self, path, params=None):
            from pythia.connectors.common import NotFound
            raise NotFound("not found or no access")
    monkeypatch.setattr(live.LiveReader, "_gh", lambda self: NoFile())
    r = live.LiveReader(svc)
    r.read_codeowners({"repo": "acme-corp/atlas"})
    assert r.failures == []


def test_detector_precision_shows_a_wide_interval_with_few_cases():
    low, high = quality.wilson(1, 1)
    assert low < 0.3 and high == 1.0
    low, high = quality.wilson(90, 100)
    assert 0.82 < low < 0.9 and 0.9 < high < 0.96
    assert quality.wilson(0, 0) == (None, None)
