"""Narratives: at most one pass per hour (A) and incremental input (B), without losing text or citations."""

from pythia import knowledge as K
from pythia.daemon import drain
from pythia.pipeline import derive

from .helpers import add_unit, claim, default_handlers, mention


def _setup(svc):
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "team X", "Atlas belongs to team X."),
                            claim("state", mention("nimbus", "system"), "owner", "team Y", "Nimbus belongs to team Y.")]])
    calls = []
    ids = {}

    def synth(user, **_):
        if user.startswith("Entity"):
            from .helpers import synth_entity
            return synth_entity(user)
        calls.append(user)
        if len(calls) == 1:  # the first pass creates the two narratives
            h, f = ids["atlas"], ids["nimbus"]
            return {"narratives": [
                {"op": "create", "id": None, "title": "History of atlas", "l0": "atlas", "l1": f"[{h['id']}]",
                 "member_entity_ids": [h["subject_entity_id"]], "cited_claim_ids": [h["id"]]},
                {"op": "create", "id": None, "title": "History of nimbus", "l0": "nimbus", "l1": f"[{f['id']}]",
                 "member_entity_ids": [f["subject_entity_id"]], "cited_claim_ids": [f["id"]]}],
                "inferences": []}
        return {"narratives": [], "inferences": []}

    svc.llm.handlers["synthesis"] = synth
    add_unit(svc, ("user", "atlas is X's, nimbus is Y's"))
    drain(svc, lanes=("cognition",))
    c = {r["statement"]: r for r in svc.db.query("SELECT id, statement, subject_entity_id FROM claims")}
    ids["atlas"], ids["nimbus"] = c["Atlas belongs to team X."], c["Nimbus belongs to team Y."]
    svc.db.execute("UPDATE jobs SET run_after = now() WHERE status = 'queued'")
    drain(svc)
    assert svc.db.one("SELECT count(*) AS n FROM views WHERE kind = 'narrative'")["n"] == 2
    return ids["atlas"], ids["nimbus"], calls


def test_narrative_pass_waits_an_hour_after_a_change(svc):
    _setup(svc)
    with svc.db.tx() as conn:
        K.invalidate(conn, entity_ids=["en_x"])
    j = svc.db.one("SELECT run_after - now() AS wait FROM jobs WHERE dedup_key = 'narratives' AND status = 'queued'")
    assert j["wait"].total_seconds() > 3500


def test_only_affected_narratives_go_in_full(svc):
    atlas, nimbus, calls = _setup(svc)
    with svc.db.tx() as conn:
        K.invalidate(conn, entity_ids=[atlas["subject_entity_id"]])
    derive.derive_narratives(svc)
    user = calls[-1]
    full, rest = user.split("UNAFFECTED NARRATIVES")
    assert "History of atlas" in full and atlas["id"] in full
    assert "History of nimbus" in rest and nimbus["id"] not in user, "the unaffected one goes without the claims"


def test_changing_a_summarized_narrative_is_deferred_until_it_is_seen_in_full(svc):
    atlas, nimbus, calls = _setup(svc)
    fer = svc.db.one("SELECT id, version, l1 FROM views WHERE title = 'History of nimbus'")
    with svc.db.tx() as conn:
        K.invalidate(conn, entity_ids=[atlas["subject_entity_id"]])
    before = svc.db.one("SELECT value FROM system_state WHERE key = 'narratives_pass'")["value"]

    def wants_nimbus(user, **_):
        calls.append(user)
        return {"narratives": [{"op": "update", "id": fer["id"], "title": "History of nimbus", "l0": "x",
                                "l1": "text without the citations", "member_entity_ids": [], "cited_claim_ids": []}],
                "inferences": []}

    svc.llm.handlers["synthesis"] = wants_nimbus
    out = derive.derive_narratives(svc)
    assert out["deferred"] == 1
    now = svc.db.one("SELECT version, l1, stale FROM views WHERE id = %s", (fer["id"],))
    assert now["version"] == fer["version"] and now["l1"] == fer["l1"] and now["stale"], "was not applied blindly"
    assert svc.db.one("SELECT value FROM system_state WHERE key = 'narratives_pass'")["value"] == before
    j = svc.db.one("SELECT run_after - now() AS wait FROM jobs WHERE dedup_key = 'narratives' AND status = 'queued'")
    assert j["wait"].total_seconds() <= 61, "the next pass runs soon"
    derive.derive_narratives(svc)
    assert nimbus["id"] in calls[-1].split("UNAFFECTED NARRATIVES")[0], "now it goes in full"


def test_passes_per_hour_is_configurable_on_the_web(svc):
    from fastapi.testclient import TestClient
    from pythia.web.app import create_app
    _setup(svc)
    client = TestClient(create_app(svc, with_mcp=False))
    assert 'name="nar_passes"' in client.get("/config").text
    with svc.db.tx() as conn:
        K.invalidate(conn, entity_ids=["en_x"])
    client.post("/action/config", data={"nar_passes": "4"}, follow_redirects=False)
    assert svc.settings.get("narratives")["passes_per_hour"] == 4
    j = svc.db.one("SELECT run_after - created_at AS wait FROM jobs WHERE dedup_key = 'narratives' AND status = 'queued'")
    assert abs(j["wait"].total_seconds() - 900) < 5, "the pass in the queue now respects the new interval"
    assert "Every 15 minutes" in client.get("/config").text
    client.post("/action/config", data={"nar_passes": "100"}, follow_redirects=False)
    assert svc.settings.get("narratives")["passes_per_hour"] == 12, "a value outside the options becomes the nearest one"
    with svc.db.tx() as conn:
        assert K.narrative_delay_s(conn) == 300
