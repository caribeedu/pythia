"""Evolution: what changed and why, built from the log, without an LLM."""

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from pythia import evolution
from pythia import knowledge as K
from pythia.daemon import drain
from pythia.web.app import create_app

from .helpers import SOURCE, add_unit, claim, default_handlers, integrator, mention


def _feed(svc, **kw):
    with svc.db.tx() as conn:
        return evolution.feed(conn, **kw)


def test_one_line_per_cause_with_what_it_touched(svc):
    """the unit becomes one line, with the claims it created; the events keep the cause."""
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "team X", "Atlas belongs to team X."),
                            claim("state", mention("nimbus", "system"), "owner", "team Y", "Nimbus belongs to team Y.")]])
    uid = add_unit(svc, ("user", "atlas is X's, nimbus is Y's"), title="conversation about owners")
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT count(*) AS n FROM events WHERE cause = %s", (f"unit:{uid}",))["n"] > 0
    lines = [ln for ln in _feed(svc)["lines"] if ln["type"] == "events" and ln["kind"] != "routine"]
    assert len(lines) == 1, [(ln["key"], ln["title"], ln["counts"]) for ln in lines]
    ln = lines[0]
    assert ln["kind"] == "chat" and ln["title"] == "conversation about owners" and "conversation about owners" not in ln["detail"]
    assert ("new", 2) in ln["counts"]
    assert {it["text"] for it in ln["items"] if it["t"] == "new"} == {"Atlas belongs to team X.", "Nimbus belongs to team Y."}


def test_substitution_shows_before_and_after_and_trajectory_uses_the_structured_link(svc):
    """the verdict shows the before and the after; the trajectory uses the "by" link, not the summary text."""
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "Order Mgmt", "Atlas belongs to Order Mgmt.")],
                           [claim("state", mention("atlas", "system"), "owner", "Zephyr", "Atlas belongs to Zephyr.")]])
    add_unit(svc, ("user", "atlas belongs to Order Mgmt"))
    drain(svc, lanes=("cognition",))
    svc.llm.handlers["integration"] = integrator(
        lambda ref, st, existing: {"decision": "supersedes", "target_claim_ids": existing[:1], "reason": "changed"})
    add_unit(svc, ("user", "atlas moved to Zephyr"))
    drain(svc, lanes=("cognition",))
    old = svc.db.one("SELECT id FROM claims WHERE statement LIKE '%%Order Mgmt%%'")["id"]
    new = svc.db.one("SELECT id, subject_entity_id FROM claims WHERE statement LIKE '%%Zephyr%%'")
    v = svc.db.one("SELECT summary, changes FROM verdicts")
    assert v["changes"][0]["by"] == new["id"]
    svc.db.execute("UPDATE verdicts SET summary = 'any text'")  # the trajectory does not read the summary
    item = next(it for ln in _feed(svc)["lines"] if ln["type"] == "events" for it in ln["items"] if it["t"] == "verdict")
    assert item["changes"][0]["before"] == "Atlas belongs to Order Mgmt." and item["changes"][0]["after"] == "Atlas belongs to Zephyr."
    with svc.db.tx() as conn:
        traj = evolution.trajectory(conn, new["subject_entity_id"])
    owner_traj = next(a for a in traj if a["predicate"] == "owner")
    assert [c["id"] for c in owner_traj["claims"]] == [old, new["id"]]
    assert any(h["by"] == new["id"] for h in owner_traj["claims"][0]["history"])


def test_units_without_effect_show_their_reason_only_when_asked(svc):
    """"hide no effect" is the default; unchecked, the unit shows up with the triage reason."""
    default_handlers(svc, [])
    svc.llm.handlers["triage"] = lambda **_: {"durable": False, "reason": "operational chatter", "kinds": []}
    add_unit(svc, ("user", "good morning"), title="small talk")
    drain(svc, lanes=("cognition",))
    assert not [ln for ln in _feed(svc)["lines"] if ln["type"] == "no_effect"]
    noop = [ln for ln in _feed(svc, show_noop=True)["lines"] if ln["type"] == "no_effect"]
    assert len(noop) == 1 and noop[0]["title"] == "small talk" and "operational chatter" in noop[0]["why"]


def test_backfill_of_a_source_is_one_line(svc):
    default_handlers(svc, [[claim("state", mention(s, "system"), "language", "Go", f"{s} is in Go.")]
                           for s in ("atlas", "nimbus", "zephyr")])
    for s in ("atlas", "nimbus", "zephyr"):
        add_unit(svc, ("user", f"{s} is in Go"))
    drain(svc, lanes=("cognition",))
    svc.db.execute("UPDATE sources SET backfill_until = now() + interval '1 hour', backfill_done_at = now() WHERE id = %s",
                   (SOURCE,))
    lines = [ln for ln in _feed(svc)["lines"] if ln["type"] == "events" and ln["kind"] != "routine"]
    assert len(lines) == 1 and lines[0]["kind"] == "backfill" and ("new", 3) in lines[0]["counts"]


def test_page_diff_and_updating_since(svc):
    """diff of the L1 against the previous version. the page marked for regeneration says since when."""
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "team X", "Atlas belongs to team X.")],
                           [claim("state", mention("atlas", "system"), "language", "Go", "Atlas is in Go.")]])
    add_unit(svc, ("user", "atlas belongs to X"))
    drain(svc)
    svc.db.execute("UPDATE jobs SET run_after = now() WHERE status = 'queued'")
    drain(svc)
    view = svc.db.one("SELECT id, version, stale_since FROM views WHERE kind = 'entity' AND title = 'atlas'")
    assert view["stale_since"] is None
    add_unit(svc, ("user", "atlas is in Go"))
    drain(svc, lanes=("cognition",))
    stale = svc.db.one("SELECT stale, stale_since FROM views WHERE id = %s", (view["id"],))
    assert stale["stale"] and stale["stale_since"] is not None
    with svc.db.tx() as conn:
        assert evolution.updating(conn)["n"] >= 1
    svc.db.execute("UPDATE jobs SET run_after = now() WHERE status = 'queued'")
    drain(svc)
    after = svc.db.one("SELECT version, stale, stale_since FROM views WHERE id = %s", (view["id"],))
    assert after["version"] == view["version"] + 1 and after["stale_since"] is None
    with svc.db.tx() as conn:
        d = evolution.page_diff(conn, view["id"], after["version"])
        first = evolution.page_diff(conn, view["id"], 1)
    assert any(k == "+" for k, _ in d["lines"]) and not d["first"]
    assert first["first"]


def test_web_tab_filters_and_revert_from_the_line(svc):
    """the Verdicts tab is gone; a verdict is an Evolution line, with Revert on the line."""
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "Order Mgmt", "Atlas belongs to Order Mgmt.")],
                           [claim("state", mention("atlas", "system"), "owner", "Zephyr", "Atlas belongs to Zephyr.")]])
    add_unit(svc, ("user", "atlas belongs to Order Mgmt"))
    drain(svc, lanes=("cognition",))
    svc.llm.handlers["integration"] = integrator(
        lambda ref, st, existing: {"decision": "supersedes", "target_claim_ids": existing[:1], "reason": "changed"})
    add_unit(svc, ("user", "atlas moved to Zephyr"))
    drain(svc, lanes=("cognition",))
    vid = svc.db.one("SELECT id FROM verdicts")["id"]
    ent = svc.db.one("SELECT subject_entity_id FROM claims LIMIT 1")["subject_entity_id"]
    client = TestClient(create_app(svc, with_mcp=False))
    r = client.get("/verdicts", follow_redirects=False)
    assert r.status_code in (302, 307) and r.headers["location"] == "/evolution?verdicts=1"
    page = client.get("/evolution?verdicts=1").text
    assert f"/action/revert/{vid}" in page and "Atlas belongs to Zephyr." in page
    assert "Trajectory of atlas" in client.get(f"/evolution?entity={ent}").text
    assert client.get(f"/evolution?source={SOURCE}&no_effect=1").status_code == 200
    r = client.post(f"/action/revert/{vid}", headers={"referer": "/evolution?verdicts=1"}, follow_redirects=False)
    assert r.headers["location"] == "/evolution?verdicts=1"
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT status FROM verdicts WHERE id = %s", (vid,))["status"] == "reverted"


def test_pagination_goes_back_in_time(svc):
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "team X", "Atlas belongs to team X.")]])
    add_unit(svc, ("user", "atlas belongs to X"))
    drain(svc, lanes=("cognition",))
    svc.db.execute("UPDATE events SET ts = ts - interval '10 days'")
    now = _feed(svc)
    assert not [ln for ln in now["lines"] if ln["type"] == "events"]
    older = _feed(svc, before=now["next_before"])
    assert [ln for ln in older["lines"] if ln["type"] == "events"]


def test_adjudicator_records_who_replaced_a_superseded_claim(svc):
    """the adjudicator reports `superseded_by`; it becomes the structured link (only if the claim exists)."""
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "Order Mgmt", "Atlas belongs to Order Mgmt."),
                            claim("state", mention("atlas", "system"), "owner", "Zephyr", "Atlas belongs to Zephyr.")]])
    add_unit(svc, ("user", "atlas used to belong to Order Mgmt and now belongs to Zephyr"))
    drain(svc, lanes=("cognition",))
    old = svc.db.one("SELECT id FROM claims WHERE statement LIKE '%%Order Mgmt%%'")["id"]
    new = svc.db.one("SELECT id FROM claims WHERE statement LIKE '%%Zephyr%%'")["id"]
    with svc.db.tx() as conn:
        K.open_tension(conn, "integration", [old, new], "did the owner change?")
    svc.llm.handlers["adjudication"] = lambda **_: {
        "kind": "temporal_supersession", "summary": "s", "rationale": "r", "cited_evidence": [], "confidence": 0.9,
        "changes": [{"claim_id": old, "to": "superseded", "superseded_by": new},
                    {"claim_id": new, "to": "current", "superseded_by": "cl_inexistente"}]}
    drain(svc, lanes=("cognition",))
    changes = {c["claim_id"]: c for c in svc.db.one("SELECT changes FROM verdicts WHERE decided_by = 'adjudication'")["changes"]}
    assert changes[old]["by"] == new and "by" not in changes[new]


def test_merges_show_entity_names_not_ids(svc):
    with svc.db.tx() as conn:
        a = K.create_entity(conn, "t", name="ZEPH", type="team", origin="discovered")
        b = K.create_entity(conn, "t", name="Zephyr", type="team", origin="anchored")
        K.merge_entities(conn, "reconciliation", a, b, "ZEPH is the nickname of Zephyr")
    item = next(it for ln in _feed(svc)["lines"] if ln["type"] == "events" for it in ln["items"] if it["t"] == "merge")
    assert item["text"] == "ZEPH (team) merged into Zephyr (team)" and "nickname" in item["why"]
    assert a not in item["text"] and b not in item["text"]


def test_ids_inside_llm_texts_render_as_readable_labels(svc):
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "team X", "Atlas belongs to team X.")]])
    add_unit(svc, ("user", "atlas belongs to X"))
    drain(svc, lanes=("cognition",))
    cid = svc.db.one("SELECT id FROM claims")["id"]
    svc.llm.handlers["adjudication"] = lambda **_: {"kind": "different_scope", "changes": [], "summary": f"{cid} still holds",
                                                   "rationale": "r", "cited_evidence": [], "confidence": 0.5}
    with svc.db.tx() as conn:
        K.open_tension(conn, "integration", [cid], f"{cid} looks odd")
    drain(svc, lanes=("cognition",))
    page = TestClient(create_app(svc, with_mcp=False)).get("/evolution").text
    assert f'title="{cid}">«Atlas belongs to team X.»</a> still holds' in page
    assert f">{cid}<" not in page
