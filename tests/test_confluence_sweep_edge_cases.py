"""Edge cases: Confluence, sweep and live reads."""

from __future__ import annotations

from datetime import timedelta

import httpx
import pytest

from pythia import knowledge as K
from pythia import quality
from pythia.connectors import confluence, github
from pythia.connectors.common import SourceUnavailable
from pythia.daemon import drain
from pythia.pipeline import live, sweep

from .helpers import claim, default_handlers, mention
from .test_confluence import NOW, FakeConfluence, add_source, body, iso, scan
from .test_sweep import FakeGH, seed


def evid(svc, like, status="active"):
    return svc.db.query("SELECT * FROM evidence WHERE content LIKE %s AND status = %s", (f"%{like}%", status))


def test_code_blocks_and_macro_parameters_are_content():
    st = ('<ac:structured-macro ac:name="code"><ac:plain-text-body><![CDATA[kubectl rollout restart deploy/qp]]>'
          '</ac:plain-text-body></ac:structured-macro><p>Status <ac:structured-macro ac:name="status">'
          '<ac:parameter ac:name="title">OBSOLETE</ac:parameter></ac:structured-macro></p>'
          '<ac:structured-macro ac:name="jira"><ac:parameter ac:name="key">PROJ-9</ac:parameter></ac:structured-macro>')
    text = confluence.sections_of(st)[0][1]
    assert "kubectl rollout restart deploy/qp" in text and "OBSOLETE" in text and "PROJ-9" in text


def test_blog_post_comments_use_the_blog_path_and_do_not_abort_the_scan(svc):
    fake = FakeConfluence()
    fake.blogs = [{"id": "B1", "title": "Announcement", "status": "current", "spaceId": "S1",
                   "version": {"number": 1, "createdAt": iso(NOW - timedelta(hours=10)), "authorId": "A-ALEX"},
                   "_links": {"webui": "/spaces/QP/blog/2026/10/01/B1"}}]
    fake.pages["B1"] = {**fake.blogs[0], "body": {"storage": {"value": "<p>news</p>"}}}
    fake.page("2", "Runbook", body(("Deploy", "via ArgoCD")))
    sid = add_source(svc)
    assert scan(svc, fake, sid)["read"] == 2
    assert evid(svc, "news") and evid(svc, "via ArgoCD")


def test_page_moving_between_sources_changes_owner_without_reading_or_deleting_again(svc):
    fake = FakeConfluence()
    fake.page("10", "Tree A", body((None, "a")))
    fake.page("20", "Tree B", body((None, "b")))
    fake.page("5", "Traveler", body((None, "traveling content")), parent="10")
    a, b = add_source(svc, root_id="10"), add_source(svc, root_id="20")
    scan(svc, fake, a)
    scan(svc, fake, b)
    units_before = svc.db.one("SELECT count(*) AS n FROM units")["n"]
    fake.pages["5"]["parentId"] = "20"
    for i in range(4):  # several alternating rounds: no read-and-delete cycle
        scan(svc, fake, a, now=NOW + timedelta(hours=i))
        scan(svc, fake, b, now=NOW + timedelta(hours=i))
    assert svc.db.one("SELECT source_id FROM streams WHERE id = 'confluence:page:5'")["source_id"] == b
    ev = evid(svc, "traveling")
    assert len(ev) == 1 and ev[0]["source_id"] == b
    assert svc.db.one("SELECT count(*) AS n FROM units")["n"] == units_before, "nothing was re-read by the LLM"


def test_page_restored_from_trash_is_read_again(svc):
    fake = FakeConfluence()
    fake.page("2", "Runbook", body(("Deploy", "via ArgoCD")))
    sid = add_source(svc)
    scan(svc, fake, sid)
    fake.gone.add("2")
    scan(svc, fake, sid)
    assert not evid(svc, "ArgoCD")
    fake.gone.discard("2")  # restored: the version does not change
    assert scan(svc, fake, sid)["read"] == 1
    assert evid(svc, "ArgoCD")


def test_unreadable_space_deletes_nothing(svc):
    fake = FakeConfluence()
    fake.page("2", "Runbook", body(("Deploy", "via ArgoCD")))
    sid = add_source(svc)
    scan(svc, fake, sid)
    fake.gone.add("2")
    fake.space_gone = True
    with pytest.raises(SourceUnavailable, match="nothing was deleted"):
        scan(svc, fake, sid)
    assert evid(svc, "ArgoCD")


def test_missing_user_names_or_renamed_people_are_not_edits(svc):
    fake = FakeConfluence()
    fake.page("2", "Runbook", body(("Owner", 'talk to <ri:user ri:account-id="A-ANA"/>')))
    sid = add_source(svc)
    scan(svc, fake, sid)
    assert evid(svc, "@Ana Lima")
    fake.edit("2", body(("Owner", 'talk to <ri:user ri:account-id="A-ANA"/>')))  # new version, same text
    orig = fake.handler
    fake.handler = lambda r: httpx.Response(429, headers={"retry-after": "999"}) if "users-bulk" in r.url.path else orig(r)
    scan(svc, fake, sid)
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE status = 'superseded'")["n"] == 0
    assert svc.db.one("SELECT count(*) AS n FROM tensions")["n"] == 0


def _claims_from_sections(svc):
    default_handlers(svc, [])
    c = claim("procedure", mention("zephyr", "system", "service"), "deploy", "ArgoCD", "The deploy is via ArgoCD.")
    svc.llm.handlers["extraction"] = lambda user, **_: {"claims": [c] if "ArgoCD" in user else []}


def test_claim_kept_in_one_edit_follows_the_next_edits(svc):
    _claims_from_sections(svc)
    fake = FakeConfluence()
    fake.page("2", "Runbook", body(("Deploy", "via ArgoCD")))
    sid = add_source(svc)
    scan(svc, fake, sid)
    drain(svc, lanes=("cognition",))
    cid = svc.db.one("SELECT id FROM claims WHERE predicate = 'deploy'")["id"]
    keep = {"kind": "different_scope", "changes": [], "summary": "still holds", "rationale": "r",
            "cited_evidence": [], "confidence": 0.8}
    svc.llm.handlers["adjudication"] = lambda **_: keep
    svc.llm.handlers["extraction"] = lambda user, **_: {"claims": []}
    fake.edit("2", body(("Deploy", "via ArgoCD, with approval")))
    scan(svc, fake, sid)
    drain(svc, lanes=("cognition",))
    fake.edit("2", body(("Deploy", "via GitHub Actions")))
    scan(svc, fake, sid)
    t = svc.db.query("SELECT claim_ids FROM tensions WHERE opened_by = 'connector' ORDER BY created_at")
    assert len(t) == 2 and t[1]["claim_ids"] == [cid], "v3 also carries the claim kept in v2"


def test_renamed_heading_with_same_text_is_not_a_change(svc):
    _claims_from_sections(svc)
    fake = FakeConfluence()
    fake.page("2", "Runbook", body(("Deploy", "via ArgoCD")))
    sid = add_source(svc)
    scan(svc, fake, sid)
    drain(svc, lanes=("cognition",))
    cid = svc.db.one("SELECT id FROM claims WHERE predicate = 'deploy'")["id"]
    units = svc.db.one("SELECT count(*) AS n FROM units")["n"]
    fake.edit("2", body(("How to deploy", "via ArgoCD")))
    scan(svc, fake, sid)
    assert svc.db.one("SELECT count(*) AS n FROM tensions")["n"] == 0
    assert svc.db.one("SELECT count(*) AS n FROM units")["n"] == units, "nothing re-extracted"
    new = svc.db.one("SELECT id FROM evidence WHERE status = 'active' AND content LIKE '%%How to deploy%%'")["id"]
    assert new in svc.db.one("SELECT evidence_ids FROM claims WHERE id = %s", (cid,))["evidence_ids"]


def test_evolution_closes_validity_at_the_edit_and_revert_reopens(svc):
    _claims_from_sections(svc)
    fake = FakeConfluence()
    fake.page("2", "Runbook", body(("Deploy", "via ArgoCD")))
    sid = add_source(svc)
    scan(svc, fake, sid)
    drain(svc, lanes=("cognition",))
    cid = svc.db.one("SELECT id FROM claims WHERE predicate = 'deploy'")["id"]
    svc.llm.handlers["adjudication"] = lambda **_: {"kind": "temporal_supersession", "changes": [{"claim_id": cid, "to": "superseded"}],
                                                   "summary": "evolved", "rationale": "r", "cited_evidence": [], "confidence": 0.9}
    svc.llm.handlers["extraction"] = lambda user, **_: {"claims": []}
    fake.edit("2", body(("Deploy", "via GitHub Actions")), minutes_ago=120)
    scan(svc, fake, sid)
    drain(svc, lanes=("cognition",))
    c = svc.db.one("SELECT status, valid_to FROM claims WHERE id = %s", (cid,))
    assert c["status"] == "superseded" and abs((c["valid_to"] - (NOW - timedelta(minutes=120))).total_seconds()) < 1
    vid = svc.db.one("SELECT id FROM verdicts")["id"]
    with svc.db.tx() as conn:
        K.revert_verdict(conn, "human:owner", vid)
    assert svc.db.one("SELECT valid_to FROM claims WHERE id = %s", (cid,))["valid_to"] is None


def test_excluded_author_pages_and_comments_leave_and_come_back(svc):
    fake = FakeConfluence()
    fake.page("2", "Bob's", body((None, "bob's text")))
    fake.pages["2"]["authorId"] = "A-BOB"
    fake.page("3", "Ana's", body((None, "ana's text")))
    fake.pages["3"]["authorId"] = "A-ANA"
    sid = add_source(svc)
    scan(svc, fake, sid)
    cfg = svc.settings.get("confluence")
    cfg["exclude_authors"] = ["Bob Reis"]
    svc.settings.set("confluence", cfg)
    src = svc.db.one("SELECT * FROM sources WHERE id = %s", (sid,))
    out = confluence.reapply_filters(svc.db, src, cfg)
    assert out["pages"] == 1 and not evid(svc, "bob's text") and evid(svc, "ana's text")
    assert scan(svc, fake, sid)["read"] == 0, "stays out"
    cfg["exclude_authors"] = []
    svc.settings.set("confluence", cfg)
    assert scan(svc, fake, sid)["read"] == 1 and evid(svc, "bob's text"), "comes back when leaving the list"


def test_reply_comes_with_the_comment_it_answers(svc):
    fake = FakeConfluence()
    fake.page("2", "Runbook", body(("Deploy", "via ArgoCD")))
    root = {"id": "C1", "version": {"authorId": "A-ANA", "createdAt": iso(NOW - timedelta(hours=3))},
            "body": {"storage": {"value": "<p>does this still hold?</p>"}}}
    fake.comments["2"] = {"footer": [root], "inline": []}
    sid = add_source(svc)
    scan(svc, fake, sid)
    orig = fake.handler

    def with_reply(r):
        if r.url.path.endswith("/footer-comments/C1/children"):
            return httpx.Response(200, json={"results": [{"id": "C2", "version": {"authorId": "A-ALEX", "createdAt": iso(NOW)},
                                                          "body": {"storage": {"value": "<p>no, it changed</p>"}}}], "_links": {}})
        return orig(r)

    fake.handler = with_reply
    scan(svc, fake, sid, now=NOW + timedelta(hours=7))
    unit = svc.db.query("SELECT * FROM units WHERE kind = 'confluence_comments' ORDER BY id")[-1]
    ev = svc.db.query("SELECT * FROM evidence WHERE unit_id = %s", (unit["id"],))
    with svc.db.tx() as conn:
        ctx = confluence.unit_context(conn, unit, ev)
    assert "no, it changed" in ev[0]["content"] and "does this still hold?" in ctx[0]["content"]


def test_live_read_understands_page_links_and_does_not_call_missing_deleted():
    fake = FakeConfluence()
    fake.page("77", "Runbook", body(("Deploy", "via ArgoCD")))
    c = fake.client()
    assert "via ArgoCD" in confluence.read_live(c, "https://acme.atlassian.net/wiki/pages/viewpage.action?pageId=77")[2]
    assert "via ArgoCD" in confluence.read_live(c, "https://acme.atlassian.net/wiki/spaces/QP/pages/77/Runbook")[2]
    assert "could not find" in confluence.read_live(c, "999")[2]
    assert "short /x/ links" in confluence.read_live(c, "https://acme.atlassian.net/wiki/x/AbC")[2]


def test_deleted_page_takes_its_live_proofs_and_derived_text_with_it(svc, monkeypatch):
    fake = FakeConfluence()
    fake.page("2", "Sensitive runbook", body(("Deploy", "deploy secret")))
    sid = add_source(svc)
    scan(svc, fake, sid)
    monkeypatch.setattr(confluence, "client_from", lambda cfg: fake.client())
    r = live.LiveReader(svc)
    proof = r.store_proofs([r.read_confluence({"page": "2"})["read"]])
    ev_id = list(proof.values())[0]
    page_ev = evid(svc, "deploy secret")[0]["id"]
    with svc.db.tx() as conn:
        K.open_tension(conn, "sweep", [], "«Sensitive runbook» changed", dossier={"hypothesis": "deploy secret",
                                                                                  "evidence_ids": [page_ev]})
    fake.gone.add("2")
    scan(svc, fake, sid)
    assert svc.db.one("SELECT status FROM evidence WHERE id = %s", (ev_id,))["status"] == "tombstoned"
    t = svc.db.one("SELECT reason, dossier FROM tensions")
    assert "secret" not in t["reason"] and t["dossier"] is None
    assert not svc.db.query("""SELECT 1 FROM events WHERE type = 'tension_opened' AND payload::text LIKE '%%secret%%'""")
    assert svc.db.one("SELECT title FROM streams WHERE id = 'confluence:page:2'")["title"] is None


def test_removing_a_repo_takes_its_live_proofs(svc, monkeypatch):
    from psycopg.types.json import Jsonb
    monkeypatch.setattr(live.LiveReader, "_gh", lambda self: FakeGH())
    svc.db.execute("INSERT INTO sources (id, kind, name, config) VALUES ('github:acme-corp/atlas', 'github', 'atlas', %s)",
                   (Jsonb({"repo": "acme-corp/atlas"}),))
    r = live.LiveReader(svc)
    ev_id = list(r.store_proofs([r.read_codeowners({"repo": "acme-corp/atlas"})["read"]]).values())[0]
    github.remove_source(svc.db, "github:acme-corp/atlas")
    assert svc.db.one("SELECT status FROM evidence WHERE id = %s", (ev_id,))["status"] == "tombstoned"


def test_tree_with_folders(svc):
    fake = FakeConfluence()
    fake.page("2", "Root", body((None, "root")))
    fake.home = "2"
    fake.folders["F1"] = {"id": "F1", "parentId": "2"}
    fake.page("6", "Inside the folder", body((None, "inside the folder")), parent="F1")
    sid = add_source(svc, root_id="2")
    assert scan(svc, fake, sid)["pages"] == 2 and evid(svc, "inside the folder")


def test_sweep_does_not_open_more_judgements_than_the_budget_allows(svc, monkeypatch):
    old = seed(svc)
    svc.settings.set("sweep", {"enabled": True, "daily_budget_usd": 0.06})
    monkeypatch.setattr(sweep, "judgement_cost", lambda svc: 0.05)
    monkeypatch.setattr(sweep, "pick", lambda conn, limit=1: [conn.execute(
        "SELECT id, name, type FROM entities WHERE name = 'atlas'").fetchone()])
    suspicion = {"claim_ids": [old], "hypothesis": "h", "reason": "m", "evidence": [], "proofs": []}
    svc.llm.handlers["sweep"] = lambda **_: {"summary": "r", "suspicions": [suspicion, dict(suspicion), dict(suspicion)]}
    out = sweep.run(svc)
    assert out["suspicions"] == 1, "one judgment fits in what is left of the day"
    assert "beyond the day's budget" in svc.db.one("SELECT summary FROM sweep_reviews")["summary"]


def test_precision_counts_tensions_once_and_ignores_reverted_or_noop_verdicts(svc):
    old = seed(svc)
    with svc.db.tx() as conn:
        t1 = K.open_tension(conn, "sweep", [old], "a")
        t2 = K.open_tension(conn, "sweep", [old], "b")
        K.apply_verdict(conn, "adjudication", kind="outdated_source", decided_by="adjudication",
                        changes=[{"claim_id": old, "to": "disputed"}], summary="s", rationale="r", tension_id=t1)
        v2 = K.apply_verdict(conn, "adjudication", kind="outdated_source", decided_by="adjudication",
                             changes=[{"claim_id": old, "to": "disputed"}], summary="s", rationale="r", tension_id=t2)
        K.revert_verdict(conn, "human:owner", v2)
    d = {r["detector"]: r for r in quality.detector_precision(svc)}["sweep"]
    assert d["total"] == 2 and d["judged"] == 2 and d["confirmed"] == 1 and d["reverted"] == 1


def test_table_cells_with_paragraphs_stay_on_one_row():
    st = "<table><tr><th><p>Field</p></th><th><p>Value</p></th></tr><tr><td><p>Service</p></td><td><p>zephyr</p></td></tr></table>"
    assert confluence.sections_of(st)[0][1] == "Field | Value\nService | zephyr"


def test_outline_shows_folders_as_nodes():
    fake = FakeConfluence()
    fake.page("1", "Home", body((None, "home")))
    fake.page("2", "LLD", body((None, "lld")), parent="F1")
    fake.folders["F1"] = {"id": "F1", "title": "Projects", "parentId": "1"}
    fake.home = "1"
    c = fake.client()
    out = confluence.page_outline(confluence.space_nodes(c, "S1", confluence.space_pages(c, "S1")))
    assert [(o["depth"], o["title"], o["folder"]) for o in out] == [(0, "Home", False), (1, "Projects", True), (2, "LLD", False)]
