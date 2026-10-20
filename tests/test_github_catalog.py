"""GitHub connector, incident.io catalog and identity reconciliation."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from psycopg.types.json import Jsonb

from pythia import knowledge as K
from pythia.connectors import github, incident_io
from pythia.connectors.common import SourceUnavailable
from pythia.daemon import drain, execute
from pythia.pipeline import reconcile
from pythia.pipeline.process import render_unit

from .helpers import add_unit, claim, default_handlers, integrator, mention

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def iso(dt):
    return dt.isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------- Fake GitHub


class FakeGitHub:
    def __init__(self):
        self.prs = {}
        self.reviews, self.rcomments, self.icomments, self.files = {}, {}, {}, {}
        self.calls = []
        self.rate_limited = False

    def add_pr(self, n, *, state="closed", merged=True, updated, author="alex-acme", body="Description"):
        self.prs[n] = {
            "id": 1000 + n, "number": n, "title": f"PR {n}", "state": state, "user": {"login": author, "type": "User"},
            "body": body, "created_at": iso(updated - timedelta(days=1)), "updated_at": iso(updated),
            "merged_at": iso(updated) if merged else None, "closed_at": iso(updated) if state == "closed" else None,
            "html_url": f"https://github.com/acme-corp/atlas/pull/{n}", "base": {"ref": "main"},
            "head": {"ref": f"feat-{n}", "sha": f"sha{n}"}, "additions": 10, "deletions": 2}
        self.reviews.setdefault(n, [])
        self.rcomments.setdefault(n, [])
        self.icomments.setdefault(n, [])
        self.files[n] = [{"filename": "storm/metrics.go", "additions": 10, "deletions": 2, "status": "modified",
                          "patch": "@@ -1 +1 @@\n-old\n+new"}]

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append(path)
        if self.rate_limited:
            return httpx.Response(403, headers={"x-ratelimit-remaining": "0"}, json={})
        if path == "/user":
            return httpx.Response(200, json={"login": "alex-acme"})
        if path == "/search/issues":
            q = request.url.params["q"]
            person = q.split("involves:")[1] if "involves:" in q else None
            items = [p for p in self.prs.values() if person is None or p["user"]["login"] == person]
            return httpx.Response(200, json={"total_count": len(items), "items": [{"number": p["number"]} for p in items]})
        if path == "/repos/acme-corp/atlas":
            return httpx.Response(200, json={"archived": False})
        if path == "/repos/acme-corp/atlas/pulls":
            items = sorted(self.prs.values(), key=lambda p: p["updated_at"], reverse=True)
            return httpx.Response(200, json=[{k: p[k] for k in ("number", "title", "state", "user", "merged_at",
                                                                "updated_at", "html_url")} for p in items])
        m = re.match(r"/repos/acme-corp/atlas/pulls/(\d+)(/(\w+))?$", path)
        if m:
            n, sub = int(m.group(1)), m.group(3)
            data = {None: self.prs.get(n), "reviews": self.reviews.get(n), "comments": self.rcomments.get(n),
                    "files": self.files.get(n), "commits": [{"commit": {"message": "feat: metrics\n\nbody"}}]}[sub]
            return httpx.Response(200, json=data)
        m = re.match(r"/repos/acme-corp/atlas/issues/(\d+)/comments$", path)
        if m:
            return httpx.Response(200, json=self.icomments.get(int(m.group(1)), []))
        return httpx.Response(404, json={})


def gh_source(svc):
    sid = github.source_id_for("acme-corp/atlas")
    svc.db.execute("INSERT INTO sources (id, kind, name, config) VALUES (%s, 'github', 'acme-corp/atlas', %s)",
                   (sid, Jsonb({"repo": "acme-corp/atlas"})))
    return svc.db.one("SELECT * FROM sources WHERE id = %s", (sid,))


def gh_connector(svc, fake):
    client = github.GitHubClient("t", transport=httpx.MockTransport(fake.handler))
    return github.GitHubConnector(svc.db, client, svc.settings.get("github"), "Alex")


def test_github_settles_merged_closed_and_stale_open_prs_only(svc):
    fake = FakeGitHub()
    fake.add_pr(1, updated=NOW - timedelta(days=2))                              # merged → settles
    fake.add_pr(2, state="open", merged=False, updated=NOW - timedelta(days=1))  # open and active → waits
    fake.add_pr(3, state="open", merged=False, updated=NOW - timedelta(days=9))  # open and idle → settles
    fake.add_pr(4, state="closed", merged=False, updated=NOW - timedelta(days=3))  # closed without merge → settles
    fake.add_pr(5, updated=NOW - timedelta(days=400))                            # outside the horizon
    fake.reviews[1] = [{"id": 11, "user": {"login": "jef"}, "state": "APPROVED", "body": "LGTM", "submitted_at": iso(NOW)}]
    fake.rcomments[1] = [{"id": 21, "user": {"login": "jef", "type": "User"}, "path": "a.go", "line": 3,
                          "body": "use a counter per status", "created_at": iso(NOW)}]
    src = gh_source(svc)
    stats = gh_connector(svc, fake).scan(src, now=NOW)
    assert stats["listed"] == 4  # stopped at the PR outside the horizon
    units = svc.db.query("SELECT title, meta FROM units ORDER BY id")
    assert sorted(u["meta"]["number"] for u in units) == [1, 3, 4]
    ev = svc.db.query("SELECT external_id, role, content FROM evidence WHERE stream_id = 'github:acme-corp/atlas#1' ORDER BY seq")
    by = {e["external_id"]: e for e in ev}
    assert set(by) == {"pr:1001", "review:11", "rcomment:21", "file:1001:storm/metrics.go"}
    assert "alex-acme (Alex, the owner)" in by["pr:1001"]["content"]
    assert "a.go:3" in by["rcomment:21"]["content"]
    assert "+new" in by["file:1001:storm/metrics.go"]["content"]
    assert not any("feat: metrics" in e["content"] for e in ev), "commit messages do not come in"
    # A second scan without changes creates nothing.
    assert gh_connector(svc, fake).scan(src, now=NOW)["units"] == 0
    # PR 2 stays idle: settles without new activity when it passes the limit.
    assert gh_connector(svc, fake).scan(src, now=NOW + timedelta(days=8))["units"] == 1


def test_github_delta_edit_and_delete(svc):
    fake = FakeGitHub()
    fake.add_pr(1, updated=NOW - timedelta(days=2), body="description v1")
    fake.icomments[1] = [{"id": 31, "user": {"login": "ana", "type": "User"}, "body": "this will break X", "created_at": iso(NOW)},
                         {"id": 32, "user": {"login": "bot", "type": "Bot"}, "body": "CI green", "created_at": iso(NOW)}]
    src = gh_source(svc)
    gh_connector(svc, fake).scan(src, now=NOW)
    # New activity after the merge: description edited, a comment deleted, a new review.
    fake.prs[1]["body"] = "description v2"
    fake.prs[1]["updated_at"] = iso(NOW + timedelta(hours=1))
    fake.icomments[1] = [fake.icomments[1][1]]
    fake.reviews[1] = [{"id": 12, "user": {"login": "jef"}, "state": "COMMENTED", "body": "post-merge: ok", "submitted_at": iso(NOW)}]
    gh_connector(svc, fake).scan(src, now=NOW + timedelta(hours=2))
    units = svc.db.query("SELECT id, meta FROM units ORDER BY id")
    assert len(units) == 2 and units[1]["meta"]["delta"] is True
    delta = svc.db.query("SELECT external_id, content FROM evidence WHERE unit_id = %s ORDER BY seq", (units[1]["id"],))
    assert sorted(d["external_id"] for d in delta) == ["pr:1001", "review:12"]
    assert any("description v2" in d["content"] for d in delta)
    gone = svc.db.query("SELECT content, status FROM evidence WHERE status = 'tombstoned'")
    assert len(gone) == 2 and all(g["content"] is None for g in gone)  # description v1 and the deleted comment


def test_github_rate_limit_retries_later_without_spending_attempt(svc):
    fake = FakeGitHub()
    fake.rate_limited = True
    src = gh_source(svc)
    with pytest.raises(SourceUnavailable):
        gh_connector(svc, fake).scan(src, now=NOW)
    from pythia import daemon
    orig = daemon.github.resolve_token
    daemon.github.resolve_token = lambda cfg: (_ for _ in ()).throw(SourceUnavailable("GitHub token not configured"))
    try:
        svc.queue.enqueue("scan_github", "io", {"source_id": src["id"]})
        execute(svc, svc.queue.claim("io", "t"))
    finally:
        daemon.github.resolve_token = orig
    job = svc.db.one("SELECT * FROM jobs WHERE kind = 'scan_github'")
    assert job["status"] == "queued" and job["attempts"] == 0
    assert svc.state.get(f"source_error:{src['id']}")["error"] == "GitHub token not configured"


def test_pr_unit_rendering_and_extraction_flow(svc):
    fake = FakeGitHub()
    fake.add_pr(7, updated=NOW - timedelta(days=1), body="Adds counter acme.order_updated.publish.attempts")
    src = gh_source(svc)
    gh_connector(svc, fake).scan(src, now=NOW)
    unit = svc.db.one("SELECT * FROM units")
    ev = svc.db.query("SELECT * FROM evidence WHERE unit_id = %s ORDER BY seq", (unit["id"],))
    text, labels = render_unit(unit, ev, "Alex")
    assert "Pull request acme-corp/atlas#7 (merged on" in text and "PR diff" in text
    default_handlers(svc, [[claim("episode", mention("acme-corp/atlas#7", "project"), "delivered", "counter",
                                  "PR atlas#7 added the publication counter.")]])
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT source_label FROM claims")["source_label"] == "github"


# ---------------------------------------------------------------- Fake incident.io


def catalog_api(entries_by_type):
    types = [
        {"id": "T_USER", "type_name": "User", "name": "User", "schema": {"attributes": [
            {"id": "a_email", "name": "Email", "type": "String"},
            {"id": "a_github", "name": "GitHub User", "type": "GithubUser"},
            {"id": "a_title", "name": "Job title", "type": "String"}]}},
        {"id": "T_TEAM", "type_name": 'Custom["Team"]', "name": "Teams", "schema": {"attributes": [
            {"id": "a_mgr", "name": "Manager", "type": "User"},
            {"id": "a_members", "name": "Members", "type": "User", "array": True},
            {"id": "a_slack", "name": "Slack Channel", "type": "SlackChannel"},
            {"id": "a_scim", "name": "SCIM Group", "type": "SCIMGroup"}]}},
        {"id": "T_SCIM", "type_name": "SCIMGroup", "name": "SCIM Group", "schema": {"attributes": [
            {"id": "a_gmembers", "name": "Members", "type": "User", "array": True}]}},
        {"id": "T_GHTEAM", "type_name": "GithubTeam", "name": "Github Team", "schema": {"attributes": []}},
    ]

    def handler(request: httpx.Request):
        if request.url.path == "/v3/catalog_types":
            return httpx.Response(200, json={"catalog_types": types})
        if request.url.path == "/v3/catalog_entries":
            tid = request.url.params["catalog_type_id"]
            return httpx.Response(200, json={"catalog_entries": entries_by_type.get(tid, []), "pagination_meta": {}})
        return httpx.Response(404)
    return handler


def user(uid, name, login, title):
    return {"id": uid, "name": name, "aliases": [f"{login}@x"], "attribute_values": {
        "a_email": {"value": {"literal": f"{login}@acme.com"}}, "a_github": {"value": {"label": login, "literal": "GH1"}},
        "a_title": {"value": {"literal": title}}}, "updated_at": iso(NOW)}


def team(manager_id, members):
    return {"id": "TEAM1", "name": "Zephyr", "aliases": ["ZEPH", "qp"], "attribute_values": {
        "a_mgr": {"value": {"literal": manager_id, "label": "?"}},
        "a_members": {"array_value": [{"literal": m, "label": "?"} for m in members]},
        "a_slack": {"value": {"literal": "C1", "label": "#zephyr-alerts"}}}, "updated_at": iso(NOW)}


def run_catalog(svc, entries):
    cfg = svc.settings.get("incident_io")
    client = incident_io.IncidentIOClient("k", transport=httpx.MockTransport(catalog_api(entries)))
    return incident_io.CatalogConnector(svc.db, client, cfg).sync()


def test_catalog_seeds_anchored_entities_with_identities_and_relation_claims(svc):
    default_handlers(svc, [])
    entries = {"T_USER": [user("U1", "Alex Moreira", "alex-acme", "Software Engineer"),
                          user("U2", "Ana Lima", "ana", "Engineering Manager")],
               "T_TEAM": [team("U2", ["U1", "U2"])]}
    stats = run_catalog(svc, entries)
    assert stats["entities_created"] == 3 and stats["types"] == 2
    owner_entity = svc.db.one("SELECT * FROM entities WHERE external_ids @> '{\"github\": \"alex-acme\"}'")
    assert owner_entity["origin"] == "anchored" and owner_entity["type"] == "person" and owner_entity["external_ids"]["email"] == "alex-acme@acme.com"
    qp = svc.db.one("SELECT * FROM entities WHERE name = 'Zephyr'")
    assert qp["type"] == "team" and "ZEPH" in qp["aliases"]
    drain(svc, lanes=("cognition",))
    claims = svc.db.query("SELECT predicate, object_value, object_entity_id, source_label, origin FROM claims ORDER BY predicate")
    preds = {(c["predicate"], c["object_entity_id"] is not None) for c in claims}
    assert ("manager", True) in preds and ("members", True) in preds and ("job_title", False) in preds
    assert not any(c["predicate"] in ("slack_channel", "email", "github_user") for c in claims)
    assert all(c["source_label"] == "incident_io_catalog" for c in claims)
    assert any(r == "integration" for r, _ in svc.llm.calls), "structured claims go through integration (LLM)"
    # No changes: nothing new.
    jobs_before = svc.db.one("SELECT count(*) AS n FROM jobs WHERE kind = 'integrate_structured'")["n"]
    assert run_catalog(svc, entries)["claims"] == 0
    assert svc.db.one("SELECT count(*) AS n FROM jobs WHERE kind = 'integrate_structured'")["n"] == jobs_before


def test_catalog_change_retracts_old_version_and_removed_entry_is_tombstoned(svc):
    default_handlers(svc, [])
    entries = {"T_USER": [user("U1", "Alex Moreira", "alex-acme", "SWE"), user("U2", "Ana Lima", "ana", "EM"),
                          user("U3", "Bia", "bia", "EM")],
               "T_TEAM": [team("U2", ["U1"])]}
    run_catalog(svc, entries)
    drain(svc, lanes=("cognition",))
    old_mgr = svc.db.one("SELECT id FROM claims WHERE predicate = 'manager'")["id"]
    entries["T_TEAM"] = [team("U3", ["U1"])]   # manager changed
    entries["T_USER"] = entries["T_USER"][:2] + []  # Bia stays; nobody leaves yet
    entries["T_USER"].append(user("U3", "Bia", "bia", "EM"))
    run_catalog(svc, entries)
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT status FROM claims WHERE id = %s", (old_mgr,))["status"] == "retracted"
    new = svc.db.one("SELECT c.status, e.name FROM claims c JOIN entities e ON e.id = c.object_entity_id WHERE c.predicate = 'manager' AND c.status = 'current'")
    assert new["name"] == "Bia"
    # Ana leaves the catalog: her snapshot becomes a tombstone and the role claim is retracted.
    entries["T_USER"] = [e for e in entries["T_USER"] if e["id"] != "U2"]
    stats = run_catalog(svc, entries)
    assert stats["removed"] == 1
    ana = svc.db.one("SELECT id FROM entities WHERE name = 'Ana Lima'")["id"]
    assert svc.db.one("SELECT status FROM claims WHERE subject_entity_id = %s AND predicate = 'job_title'", (ana,))["status"] == "retracted"


def test_catalog_without_key_is_source_unavailable(svc, monkeypatch):
    from pythia import vault
    monkeypatch.setattr(vault, "get_secret", lambda name: None)
    with pytest.raises(SourceUnavailable):
        incident_io.client_from(svc.settings.get("incident_io"))


# ---------------------------------------------------------------- reconciliation and candidates


def test_reconcile_merges_discovered_into_anchored_and_caches_decision(svc):
    default_handlers(svc, [[claim("profile", mention("Joe Park"), "suggests", "volume protection",
                                  "Joe Park suggested volume protection in the alerts.")]])
    add_unit(svc, ("user", "Joe Park suggested volume protection"))
    drain(svc, lanes=("cognition",))
    discovered = svc.db.one("SELECT id FROM entities WHERE name = 'Joe Park'")["id"]
    run_catalog(svc, {"T_USER": [user("U9", "Joe Park", "jefcook", "Staff Engineer")]})
    anchored = svc.db.one("SELECT id FROM entities WHERE origin = 'anchored' AND name = 'Joe Park'")["id"]
    seen_prompts = []

    def decide(user, **_):
        seen_prompts.append(user)
        ref = re.search(r"^(D\d+): Joe Park", user, re.M).group(1)
        return {"decisions": [{"ref": ref, "decision": "same", "entity_id": anchored, "confidence": 0.9,
                               "reason": "same name, same engineering context"}]}

    svc.llm.handlers["reconciliation"] = decide
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT status, merged_into FROM entities WHERE id = %s", (discovered,)) == {
        "status": "merged", "merged_into": anchored}
    c = svc.db.one("SELECT subject_entity_id FROM claims WHERE predicate = 'suggests'")
    assert c["subject_entity_id"] == anchored
    assert "github=jefcook" in seen_prompts[0]
    # Same candidates: does not ask again.
    svc.queue.enqueue("reconcile_entities", "cognition", {})
    n = len(seen_prompts)
    drain(svc, lanes=("cognition",))
    assert len(seen_prompts) == n


def test_reconcile_owner_absorbs_anchored_and_invalid_choice_is_skipped(svc):
    """The owner reconciles himself (the anchored one merges into him and he keeps the identifiers); a decision with
    a candidate from another block only skips that item, without bringing down the batch."""
    run_catalog(svc, {"T_USER": [user("U1", "Alex Moreira", "alexm", "SWE"), user("U2", "Ana Lima", "analima", "SWE")]})
    with svc.db.tx() as conn:
        K.create_entity(conn, "t", name="Ana Lima", type="person", origin="discovered")
    owner = svc.db.one("SELECT id FROM entities WHERE is_owner")["id"]
    anchored = {r["name"]: r["id"] for r in svc.db.query("SELECT id, name FROM entities WHERE origin = 'anchored' AND NOT is_owner")}

    def decide(user, **_):
        out = []
        for ref, name in re.findall(r"^(D\d+): (\S+)", user, re.M):
            if name == "Alex":
                out.append({"ref": ref, "decision": "same", "entity_id": anchored["Alex Moreira"], "confidence": 0.95, "reason": "is the owner"})
            else:  # picks the candidate from another block
                out.append({"ref": ref, "decision": "same", "entity_id": anchored["Alex Moreira"], "confidence": 0.5, "reason": "?"})
        return {"decisions": out}

    svc.llm.handlers["reconciliation"] = decide
    out = reconcile.reconcile(svc)
    assert out["merged"] == 1 and out["invalid"] >= 1
    own = svc.db.one("SELECT status, is_owner, external_ids FROM entities WHERE id = %s", (owner,))
    assert own["status"] == "active" and own["is_owner"] and own["external_ids"]["github"] == "alexm"
    assert svc.db.one("SELECT merged_into FROM entities WHERE id = %s", (anchored["Alex Moreira"],))["merged_into"] == owner
    assert svc.db.one("SELECT status FROM entities WHERE name = 'Ana Lima' AND origin = 'discovered'")["status"] == "active"
    # A new catalog sync finds the owner through the merged entry and does not rename him.
    run_catalog(svc, {"T_USER": [user("U1", "Alex Moreira", "alexm", "SWE"), user("U2", "Ana Lima", "analima", "SWE")]})
    assert svc.db.one("SELECT name FROM entities WHERE id = %s", (owner,))["name"] == "Alex"
    assert svc.db.one("SELECT count(*) AS n FROM entities WHERE name = 'Alex Moreira' AND status = 'active'")["n"] == 0


def test_unmerge_returns_aliases_and_external_ids(svc):
    with svc.db.tx() as conn:
        repo = K.create_entity(conn, "t", name="acme-corp/zephyr", type="repository", origin="anchored",
                               aliases=["zephyr"], external_ids={"incident_io": "R1"})
        pr = K.create_entity(conn, "t", name="acme-corp/zephyr#154", type="document", origin="discovered",
                             aliases=["#154"], external_ids={"github_pr": "154"})
        K.merge_entities(conn, "t", pr, repo, "teste")
    r = svc.db.one("SELECT aliases, external_ids FROM entities WHERE id = %s", (repo,))
    assert "acme-corp/zephyr#154" in r["aliases"] and r["external_ids"]["github_pr"] == "154"
    with svc.db.tx() as conn:
        K.unmerge_entity(conn, "t", pr)
    r = svc.db.one("SELECT aliases, external_ids FROM entities WHERE id = %s", (repo,))
    assert r["aliases"] == ["zephyr"] and r["external_ids"] == {"incident_io": "R1"}


def test_mention_candidates_match_external_ids(svc):
    run_catalog(svc, {"T_USER": [user("U1", "Alex Moreira", "alex-acme", "SWE")]})
    with svc.db.tx() as conn:
        cands = svc.retriever.entity_candidates(conn, "alex-acme", "PR author")
    assert any(c["name"] == "Alex Moreira" for c in cands)


def set_gh(svc, **kw):
    cfg = svc.settings.get("github")
    cfg.update(kw)
    svc.settings.set("github", cfg)


def test_bots_and_bot_threads_are_ignored_and_excluded_humans_too(svc):
    fake = FakeGitHub()
    fake.add_pr(1, updated=NOW - timedelta(days=2))
    bot = {"login": "cursor[bot]", "type": "Bot"}
    fake.icomments[1] = [{"id": 41, "user": {"login": "github-actions[bot]", "type": "Bot"}, "body": "x" * 10000, "created_at": iso(NOW)},
                         {"id": 42, "user": {"login": "noisy", "type": "User"}, "body": "noise", "created_at": iso(NOW)},
                         {"id": 43, "user": {"login": "friendbot", "type": "User"}, "body": "let's go with Kafka", "created_at": iso(NOW)}]
    fake.rcomments[1] = [
        {"id": 51, "user": bot, "path": "a.go", "line": 1, "body": "bug here", "diff_hunk": "@@ -1 +1 @@", "created_at": iso(NOW)},
        {"id": 52, "in_reply_to_id": 51, "user": {"login": "ana", "type": "User"}, "path": "a.go", "line": 1,
         "body": "good catch, fixed", "created_at": iso(NOW)},
        {"id": 53, "user": {"login": "ana", "type": "User"}, "path": "a.go", "line": 9, "body": "rename this",
         "diff_hunk": "@@ -9 +9 @@\n-old\n+new", "created_at": iso(NOW)}]
    set_gh(svc, exclude_authors=["noisy"])
    gh_connector(svc, fake).scan(gh_source(svc), now=NOW)
    ev = {e["external_id"]: e["content"] for e in svc.db.query("SELECT external_id, content FROM evidence")}
    assert "icomment:41" not in ev and "icomment:42" not in ev
    assert "icomment:43" in ev, "friendbot is human: the criterion is GitHub's flag, not the name"
    assert "rcomment:51" not in ev and "rcomment:52" not in ev, "a thread that starts with a bot leaves whole"
    assert "rcomment:53" in ev and "+new" in ev["rcomment:53"], "an inline comment carries the diff_hunk"


def test_bot_exception_list_reads_that_bot(svc):
    fake = FakeGitHub()
    fake.add_pr(1, updated=NOW - timedelta(days=2))
    fake.icomments[1] = [{"id": 41, "user": {"login": "cursor[bot]", "type": "Bot"}, "body": "x" * 10000, "created_at": iso(NOW)}]
    set_gh(svc, read_bots=["cursor[bot]"])
    gh_connector(svc, fake).scan(gh_source(svc), now=NOW)
    ev = {e["external_id"]: e["content"] for e in svc.db.query("SELECT external_id, content FROM evidence")}
    assert "x" * 10000 in ev["icomment:41"], "content is never cut"


def test_full_file_diffs_excluded_paths_and_missing_patch(svc):
    fake = FakeGitHub()
    fake.add_pr(1, updated=NOW - timedelta(days=2))
    big = "+linha\n" * 5000
    fake.files[1] = [{"filename": "a.go", "additions": 5000, "deletions": 0, "status": "modified", "patch": big},
                     {"filename": "poetry.lock", "additions": 10, "deletions": 1, "status": "modified", "patch": "+x"},
                     {"filename": "img.png", "additions": 0, "deletions": 0, "status": "added"}]
    set_gh(svc, exclude_paths=["*.lock"])
    gh_connector(svc, fake).scan(gh_source(svc), now=NOW)
    ev = {e["external_id"]: e["content"] for e in svc.db.query("SELECT external_id, content FROM evidence")}
    assert big in ev["file:1001:a.go"], "whole diff, no cut"
    assert "file:1001:poetry.lock" not in ev
    assert "did not provide this file's patch" in ev["file:1001:img.png"]


def test_repository_modes_filter_prs_by_participants(svc):
    from pythia.connectors.github import passes_mode
    src = {"config": {"repo": "x/y"}}
    assert passes_mode(src, ["alex-acme", "ana"], "alex-acme", set())
    assert not passes_mode(src, ["ana"], "alex-acme", set())
    assert passes_mode(src, ["ana"], "alex-acme", {"ana"})
    assert passes_mode({"config": {"mode": "all"}}, ["ze"], "alex-acme", set())
    assert not passes_mode({"config": {"mode": "owner"}}, ["ana"], "alex-acme", {"ana"})
    fake = FakeGitHub()
    fake.add_pr(1, updated=NOW - timedelta(days=2), author="ze")
    fake.add_pr(2, updated=NOW - timedelta(days=2), author="ana")
    source = gh_source(svc)
    connector = gh_connector(svc, fake)
    connector.team = [{"name": "Ana", "github": "ana"}]
    connector.scan(source, now=NOW)
    numbers = sorted(u["meta"]["number"] for u in svc.db.query("SELECT meta FROM units"))
    assert numbers == [2]
    assert "/repos/acme-corp/atlas/pulls/1/files" not in fake.calls, "a PR outside the mode does not even have its files fetched"
    st = svc.db.one("SELECT meta FROM streams WHERE id = 'github:acme-corp/atlas#1'")["meta"]
    assert st["filtered"] is True and st["participants"] == ["ze"]


def test_per_repo_horizon(svc):
    fake = FakeGitHub()
    fake.add_pr(1, updated=NOW - timedelta(days=40))
    src = gh_source(svc)
    svc.db.execute("UPDATE sources SET config = config || '{\"horizon_days\": 30}' WHERE id = %s", (src["id"],))
    stats = gh_connector(svc, fake).scan(svc.db.one("SELECT * FROM sources WHERE id = %s", (src["id"],)), now=NOW)
    assert stats["listed"] == 0


def test_large_pr_is_split_into_units_without_dropping_anything():
    from pythia.connectors.common import Item
    from pythia.connectors.github import split_items
    head = Item("pr:1", "a", "author", NOW, "description", {"kind": "description"})
    files = [Item(f"file:1:{i}", "a", "artifact", NOW, "x" * 200_000, {"kind": "file"}) for i in range(3)]
    parts = split_items([head, *files])
    assert len(parts) == 3 and parts[0][0] is head
    assert sum(len(p) for p in parts) == 4


def test_github_requires_token_from_keychain(svc, monkeypatch):
    from pythia import vault
    monkeypatch.setattr(vault, "get_secret", lambda name: None)
    with pytest.raises(SourceUnavailable):
        github.resolve_token(svc.settings.get("github"))


def ingest_two_authors(svc, fake):
    fake.add_pr(1, updated=NOW - timedelta(days=2))
    fake.icomments[1] = [{"id": 61, "user": {"login": "ana", "type": "User"}, "body": "atlas belongs to Zephyr", "created_at": iso(NOW)},
                         {"id": 62, "user": {"login": "ze", "type": "User"}, "body": "ok", "created_at": iso(NOW)}]
    src = gh_source(svc)
    gh_connector(svc, fake).scan(src, now=NOW)
    return src


def test_reapply_filters_retracts_excluded_author_and_brings_back_when_removed(svc):
    from pythia.connectors.github import reapply_filters
    fake = FakeGitHub()
    src = ingest_two_authors(svc, fake)
    ana_ev = svc.db.one("SELECT id FROM evidence WHERE external_id = 'icomment:61'")["id"]
    with svc.db.tx() as conn:
        from pythia import knowledge as K
        e = K.create_entity(conn, "t", name="atlas", type="system", origin="discovered")
        c = K.assert_claim(conn, "t", kind="state", subject_entity_id=e, predicate="owner", statement="atlas belongs to Zephyr",
                           origin="observed", evidence_ids=[ana_ev], source_id=src["id"])
    set_gh(svc, exclude_authors=["ana"])
    stats = reapply_filters(svc.db, src, svc.settings.get("github"), "alex-acme", set())
    assert stats["retracted"] == 1
    assert svc.db.one("SELECT status FROM claims WHERE id = %s", (c,))["status"] == "retracted"
    assert svc.db.one("SELECT status, external_id FROM evidence WHERE id = %s", (ana_ev,)) == {"status": "tombstoned", "external_id": None}
    # Taken off the list: the next read brings it back as new evidence.
    set_gh(svc, exclude_authors=[])
    reapply_filters(svc.db, src, svc.settings.get("github"), "alex-acme", set())
    gh_connector(svc, fake).scan(src, now=NOW)
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE external_id = 'icomment:61' AND status = 'active'")["n"] == 1


def test_mode_change_retracts_prs_outside_the_filter(svc):
    from pythia.connectors.github import reapply_filters
    fake = FakeGitHub()
    fake.add_pr(1, updated=NOW - timedelta(days=2), author="ana")
    src = gh_source(svc)
    svc.db.execute("UPDATE sources SET config = config || '{\"mode\": \"all\"}' WHERE id = %s", (src["id"],))
    src = svc.db.one("SELECT * FROM sources WHERE id = %s", (src["id"],))
    gh_connector(svc, fake).scan(src, now=NOW)
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE status = 'active'")["n"] > 0
    svc.db.execute("UPDATE sources SET config = config || '{\"mode\": \"owner\"}' WHERE id = %s", (src["id"],))
    src = svc.db.one("SELECT * FROM sources WHERE id = %s", (src["id"],))
    reapply_filters(svc.db, src, svc.settings.get("github"), "alex-acme", set())
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE status = 'active'")["n"] == 0


def test_remove_source_retracts_everything_and_allows_reading_again(svc):
    from pythia.connectors.github import remove_source
    fake = FakeGitHub()
    src = ingest_two_authors(svc, fake)
    remove_source(svc.db, src["id"])
    assert svc.db.one("SELECT count(*) AS n FROM sources WHERE id = %s", (src["id"],))["n"] == 0
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE status = 'active'")["n"] == 0
    src = gh_source(svc)
    gh_connector(svc, fake).scan(src, now=NOW)
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE status = 'active'")["n"] > 0
    assert svc.db.one("SELECT count(*) AS n FROM units")["n"] == 1


def test_estimate_counts_prs_in_mode(svc):
    from pythia.connectors.github import estimate_backfill
    import pythia.connectors.github as ghmod
    fake = FakeGitHub()
    fake.add_pr(1, updated=NOW - timedelta(days=2), author="alex-acme")
    fake.add_pr(2, updated=NOW - timedelta(days=2), author="ana")
    fake.add_pr(3, updated=NOW - timedelta(days=2), author="ze")
    client = github.GitHubClient("t", transport=httpx.MockTransport(fake.handler))
    orig = ghmod.time if hasattr(ghmod, "time") else None
    import time as real_time
    real_sleep = real_time.sleep
    real_time.sleep = lambda s: None
    try:
        est = estimate_backfill(client, {"config": {"repo": "acme-corp/atlas", "mode": "team"}}, svc.settings.get("github"),
                                "alex-acme", {"ana"}, 0.1)
        # After the backfill: only what remains to be read enters the account of what lies ahead.
        after = estimate_backfill(client, {"config": {"repo": "acme-corp/atlas", "mode": "all"}}, svc.settings.get("github"),
                                  "alex-acme", {"ana"}, 0.1, already_read={1, 2})
    finally:
        real_time.sleep = real_sleep
    assert est["prs"] == 2 and est["usd"] == 0.2 and est["new"] == 2
    assert after["prs"] == 3 and after["new"] == 1 and after["usd_new"] == 0.1


def test_web_add_repo_is_paused_with_estimate_and_enable_starts_backfill(svc):
    from fastapi.testclient import TestClient
    from pythia.web.app import create_app
    client = TestClient(create_app(svc, with_mcp=False))
    client.post("/action/github/add", data={"repo": "acme-corp/atlas"}, follow_redirects=False)
    src = svc.db.one("SELECT * FROM sources WHERE id = 'github:acme-corp/atlas'")
    assert src["enabled"] is False and src["backfill_until"] is None and src["config"]["mode"] == "team"
    assert svc.db.one("SELECT count(*) AS n FROM jobs WHERE kind = 'estimate_backfill'")["n"] == 1
    client.post("/action/source/toggle", data={"source_id": src["id"]}, follow_redirects=False)
    src = svc.db.one("SELECT * FROM sources WHERE id = 'github:acme-corp/atlas'")
    assert src["enabled"] is True and src["backfill_until"] is not None and src["backfill_done_at"] is None
    client.post("/action/github/config", data={"source_id": src["id"], "mode": "all", "horizon_days": "30"}, follow_redirects=False)
    cfg = svc.db.one("SELECT config FROM sources WHERE id = 'github:acme-corp/atlas'")["config"]
    assert cfg["mode"] == "all" and cfg["horizon_days"] == 30
    assert svc.db.one("SELECT count(*) AS n FROM jobs WHERE kind = 'reapply_filters'")["n"] == 1
    assert client.get("/sources").status_code == 200
    client.post("/action/source/remove", data={"source_id": src["id"]}, follow_redirects=False)
    assert svc.db.one("SELECT count(*) AS n FROM jobs WHERE kind = 'remove_source'")["n"] == 1


def test_web_team_save_and_import_from_catalog(svc):
    from fastapi.testclient import TestClient
    from pythia.web.app import create_app
    default_handlers(svc, [])
    run_catalog(svc, {"T_USER": [user("U1", "Alex Moreira", "alex-acme", "SWE"), user("U2", "Ana Lima", "ana", "EM")],
                      "T_TEAM": [team("U2", ["U1", "U2"])]})
    drain(svc, lanes=("cognition",))
    client = TestClient(create_app(svc, with_mcp=False))
    assert "Import members from the catalog" in client.get("/config").text
    client.post("/action/team", data={"rows": "0", "name_0": "Partner", "github_0": "partner"}, follow_redirects=False)
    assert svc.settings.get("team")["members"][0]["github"] == "partner"
    qp = svc.db.one("SELECT id FROM entities WHERE name = 'Zephyr'")["id"]
    client.post("/action/team/import", data={"team_entity_id": qp}, follow_redirects=False)
    logins = sorted(m["github"] for m in svc.settings.get("team")["members"])
    assert logins == ["alex-acme", "ana", "partner"]


def test_classic_token_requires_repo_scope_and_fine_grained_skips_check(svc):
    from pythia.connectors.github import verify_token

    def handler(scopes):
        return lambda request: httpx.Response(200, json={"login": "alex-acme"}, headers={"x-oauth-scopes": scopes})

    ok = github.GitHubClient("t", transport=httpx.MockTransport(handler("repo, read:org")))
    verify_token(ok, {"token_type": "classic"})
    weak = github.GitHubClient("t", transport=httpx.MockTransport(handler("read:user")))
    with pytest.raises(SourceUnavailable):
        verify_token(weak, {"token_type": "classic"})
    verify_token(weak, {"token_type": "fine_grained"})


def test_config_saves_token_type_and_shows_both_instructions(svc):
    from fastapi.testclient import TestClient
    from pythia.web.app import create_app
    client = TestClient(create_app(svc, with_mcp=False))
    html = client.get("/config").text
    assert 'data-type="classic"' in html and 'data-type="fine_grained"' in html and "Tokens (classic)" in html
    client.post("/action/config", data={"gh_token_type": "fine_grained", "cap": "50", "owner_name": "Alex"}, follow_redirects=False)
    assert svc.settings.get("github")["token_type"] == "fine_grained"


def test_config_lists_every_catalog_type_and_new_defaults_survive_saved_settings(svc):
    saved = svc.settings.get("incident_io")
    saved["types"] = {"User": "person"}  # settings saved before SCIMGroup existed in the default
    svc.settings.set("incident_io", saved)
    assert "SCIMGroup" in svc.settings.get("incident_io")["types"]
    with svc.db.tx() as conn:
        incident_io.store_available_types(conn, [{"type_name": "User", "name": "User"},
                                                 {"type_name": 'Custom["New"]', "name": "New"}])
        options = incident_io.type_options(conn, svc.settings.get("incident_io")["types"])
    assert options['Custom["New"]'] == "" and options["User"] == "person"


def test_catalog_excluded_attribute_leaves_and_unchanged_claims_carry_over(svc):
    """An ignored attribute disappears from the evidence and its claim is retracted; what stays the same in the
    changed entry only starts citing the new evidence (does not go back to the LLM)."""
    default_handlers(svc, [])
    entries = {"T_USER": [user("U1", "Alex Moreira", "alex-acme", "SWE"), user("U2", "Ana Lima", "ana", "EM")],
               "T_TEAM": [team("U2", ["U1"])]}
    run_catalog(svc, entries)
    drain(svc, lanes=("cognition",))
    title = svc.db.one("SELECT id FROM claims WHERE predicate = 'job_title' AND object_value = 'SWE'")["id"]
    members = svc.db.one("SELECT id FROM claims WHERE predicate = 'members'")["id"]
    cfg = svc.settings.get("incident_io")
    cfg["exclude_attributes"] = cfg["exclude_attributes"] + ["User: Job title"]
    svc.settings.set("incident_io", cfg)
    stats = run_catalog(svc, entries)
    assert stats["claims"] == 0, "nothing new to integrate"
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT status FROM claims WHERE id = %s", (title,))["status"] == "retracted"
    m = svc.db.one("SELECT status, evidence_ids FROM claims WHERE id = %s", (members,))
    assert m["status"] == "current"
    live = svc.db.one("SELECT id, content FROM evidence WHERE status = 'active' AND id = ANY(%s)", (m["evidence_ids"],))
    assert live is None or "Job title" not in live["content"]
    assert not any("Job title" in r["content"] for r in svc.db.query(
        "SELECT content FROM evidence WHERE source_id = 'incident_io:catalog' AND status = 'active'"))


def test_catalog_changed_entry_only_integrates_what_changed(svc):
    default_handlers(svc, [])
    entries = {"T_USER": [user("U1", "Alex Moreira", "alex-acme", "SWE"), user("U2", "Ana Lima", "ana", "EM"),
                          user("U3", "Bia", "bia", "EM")],
               "T_TEAM": [team("U2", ["U1"])]}
    run_catalog(svc, entries)
    drain(svc, lanes=("cognition",))
    members = svc.db.one("SELECT id FROM claims WHERE predicate = 'members'")["id"]
    entries["T_TEAM"] = [team("U3", ["U1"])]  # only the manager changed
    stats = run_catalog(svc, entries)
    assert stats["claims"] == 1
    drain(svc, lanes=("cognition",))
    m = svc.db.one("SELECT status, evidence_ids FROM claims WHERE id = %s", (members,))
    assert m["status"] == "current"
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE id = ANY(%s) AND status = 'active'", (m["evidence_ids"],))["n"] == 1


def test_team_list_github_login_reaches_the_anchored_person_and_feeds_reconciliation(svc):
    run_catalog(svc, {"T_USER": [{"id": "U7", "name": "Joe Park", "aliases": ["joe.park@acme.com"], "attribute_values": {
        "a_email": {"value": {"literal": "joe.park@acme.com"}}}, "updated_at": iso(NOW)}]})
    with svc.db.tx() as conn:
        discovered = K.create_entity(conn, "t", name="joepark", type="person", origin="discovered")
    svc.settings.set("team", {"members": [{"name": "Joe Park", "email": "joe.park@acme.com", "github": "joepark"}]})
    assert reconcile.apply_team_identities(svc)["updated"] == 1
    anchored = svc.db.one("SELECT * FROM entities WHERE name = 'Joe Park' AND origin = 'anchored'")
    assert anchored["external_ids"]["github"] == "joepark" and "joepark" in anchored["aliases"]
    seen = []

    def decide(user, **_):
        seen.append(user)
        ref = re.search(r"^(D\d+): joepark", user, re.M).group(1)
        return {"decisions": [{"ref": ref, "discovered_is": "a person (GitHub login)", "decision": "same",
                               "entity_id": anchored["id"], "confidence": 0.95, "reason": "login declared in the team list"}]}

    svc.llm.handlers["reconciliation"] = decide
    reconcile.reconcile(svc)
    assert "github=joepark" in seen[0]
    assert svc.db.one("SELECT merged_into FROM entities WHERE id = %s", (discovered,))["merged_into"] == anchored["id"]
    assert reconcile.apply_team_identities(svc)["updated"] == 0, "idempotent"


def test_scim_group_pointed_by_a_team_is_folded_into_it(svc):
    cfg = svc.settings.get("incident_io")
    cfg["types"]["SCIMGroup"] = "team"
    svc.settings.set("incident_io", cfg)
    t = team("U2", ["U1"])
    # As in the real catalog: the Team points to the group's external_id, not to the entry ID.
    t["attribute_values"]["a_scim"] = {"value": {"literal": "EXT-G1", "label": "EngZephyr"}}
    group = {"id": "G1", "external_id": "EXT-G1", "name": "EngZephyr", "aliases": [], "updated_at": iso(NOW),
             "attribute_values": {"a_gmembers": {"array_value": [{"literal": "U1"}, {"literal": "U2"}]}}}
    lonely = {"id": "G2", "name": "Bizapps", "aliases": [], "updated_at": iso(NOW),
              "attribute_values": {"a_gmembers": {"array_value": [{"literal": "U1"}]}}}
    entries = {"T_USER": [user("U1", "Alex Moreira", "alex-acme", "SWE"), user("U2", "Ana Lima", "ana", "EM")],
               "T_TEAM": [t], "T_SCIM": [group, lonely]}
    default_handlers(svc, [])
    run_catalog(svc, entries)
    drain(svc, lanes=("cognition",))
    qp = svc.db.one("SELECT * FROM entities WHERE name = 'Zephyr' AND status = 'active'")
    assert svc.db.one("SELECT status, merged_into FROM entities WHERE name = 'EngZephyr'") == {
        "status": "merged", "merged_into": qp["id"]}
    assert svc.db.one("SELECT status FROM entities WHERE name = 'Bizapps'")["status"] == "active", "a group without a Team stays"
    members = svc.db.query("""SELECT o.name FROM claims c JOIN entities o ON o.id = c.object_entity_id
                              WHERE c.subject_entity_id = %s AND c.predicate = 'members' AND c.status = 'current'""", (qp["id"],))
    assert {"Ana Lima"} <= {m["name"] for m in members}
    assert not svc.db.one("SELECT count(*) AS n FROM claims WHERE predicate = 'scim_group'")["n"], "no relation with itself"
    # Next sync: the group keeps pointing to the Team, without recreating anything.
    run_catalog(svc, entries)
    assert svc.db.one("SELECT count(*) AS n FROM entities WHERE name = 'EngZephyr' AND status = 'active'")["n"] == 0
