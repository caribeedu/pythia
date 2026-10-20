"""Confluence against a fake v2 API."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pytest
from psycopg.types.json import Jsonb

from pythia import knowledge as K
from pythia.connectors import confluence
from pythia.connectors.common import SourceUnavailable
from pythia.daemon import drain

from .helpers import claim, default_handlers, mention

NOW = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)


def iso(dt):
    return dt.isoformat().replace("+00:00", "Z")


def body(*sections):
    return "".join(f"<h2>{h}</h2><p>{t}</p>" if h else f"<p>{t}</p>" for h, t in sections)


class FakeConfluence:
    def __init__(self):
        self.spaces = [{"id": "S1", "key": "QP", "name": "Zephyr", "type": "global", "status": "current"}]
        self.pages = {}       # id → dict
        self.comments = {}    # page id → {"footer": [...], "inline": [...]}
        self.blogs = []
        self.gone = set()     # deleted ids (404)
        self.status = {}      # id → status when fetched (archived/trashed)
        self.calls = []
        self.folders = {}     # folder id → {"id", "parentId"}
        self.home = "HOME"
        self.space_gone = False

    def page(self, pid, title, storage, parent=None, version=1, minutes_ago=600):
        self.pages[pid] = {"id": pid, "title": title, "parentId": parent, "status": "current", "spaceId": "S1",
                           "version": {"number": version, "createdAt": iso(NOW - timedelta(minutes=minutes_ago)),
                                       "authorId": "A-ALEX"},
                           "body": {"storage": {"value": storage}}, "_links": {"webui": f"/spaces/QP/pages/{pid}"}}

    def edit(self, pid, storage, minutes_ago=120):
        p = self.pages[pid]
        p["version"] = {**p["version"], "number": p["version"]["number"] + 1, "createdAt": iso(NOW - timedelta(minutes=minutes_ago))}
        p["body"] = {"storage": {"value": storage}}

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/ex/confluence/CLOUD/wiki")
        q = dict(request.url.params)
        self.calls.append(path)
        if path.endswith("/_edge/tenant_info"):
            return httpx.Response(200, json={"cloudId": "CLOUD"})
        if path == "/api/v2/spaces":
            return httpx.Response(200, json={"results": self.spaces, "_links": {}})
        if path == "/api/v2/spaces/S1":
            return httpx.Response(404 if self.space_gone else 200, json={**self.spaces[0], "homepageId": self.home})
        if path.endswith("/descendants"):
            root = path.split("/")[4]
            kids = [{"id": f["id"], "type": "folder", "title": f.get("title", "pasta"), "parentId": f["parentId"], "depth": 1}
                    for f in self.folders.values()]
            kids += [{"id": p["id"], "type": "page", "title": p["title"], "parentId": p["parentId"], "depth": 1}
                     for p in self.pages.values() if p["id"] != root and p["id"] not in self.gone]
            return httpx.Response(200, json={"results": kids if root == self.home else [], "_links": {}})
        if path.startswith("/api/v2/folders/"):
            fid = path.rsplit("/", 1)[1]
            return httpx.Response(200, json=self.folders[fid]) if fid in self.folders else httpx.Response(404, json={})
        if path == "/api/v2/spaces/S1/pages":
            rows = [{k: v for k, v in p.items() if k != "body" or q.get("body-format")} for p in self.pages.values()
                    if p["id"] not in self.gone and self.status.get(p["id"], "current") == "current"]
            return httpx.Response(200, json={"results": rows, "_links": {}})
        if path == "/api/v2/spaces/S1/blogposts":
            return httpx.Response(200, json={"results": self.blogs, "_links": {}})
        if path.startswith("/api/v2/blogposts/") and path.count("/") == 4:
            bid = path.rsplit("/", 1)[1]
            blog = next((b for b in self.blogs if b["id"] == bid), None)
            if not blog or bid in self.gone:
                return httpx.Response(404, json={})
            return httpx.Response(200, json={**blog, **({"body": self.pages[bid]["body"]} if bid in self.pages else {})})
        if path.startswith("/api/v2/pages/") and path.count("/") == 4:
            pid = path.rsplit("/", 1)[1]
            if pid in self.gone or pid not in self.pages:
                return httpx.Response(404, json={})
            return httpx.Response(200, json={**self.pages[pid], "status": self.status.get(pid, "current")})
        if path.endswith("-comments") and ("/pages/" in path or "/blogposts/" in path):
            _, _, _, base, pid, kind = path.split("/")
            if (base == "blogposts") != any(b["id"] == pid for b in self.blogs):
                return httpx.Response(404, json={})
            return httpx.Response(200, json={"results": self.comments.get(pid, {}).get(kind.split("-")[0], []), "_links": {}})
        if path.endswith("/children"):
            return httpx.Response(200, json={"results": [], "_links": {}})
        if path == "/api/v2/users-bulk":
            names = {"A-ALEX": "Alex Moreira", "A-ANA": "Ana Lima", "A-BOB": "Bob Reis"}
            import json
            ids = json.loads(request.content)["accountIds"]
            return httpx.Response(200, json={"results": [{"accountId": a, "publicName": names.get(a, a)} for a in ids]})
        return httpx.Response(404, json={})

    def client(self):
        return confluence.ConfluenceClient("acme.atlassian.net", "alex@x", "tok", "scoped",
                                           transport=httpx.MockTransport(self.handler))


def cf_cfg(svc, **kw):
    cfg = svc.settings.get("confluence")
    cfg.update(site="acme.atlassian.net", email="alex@x", **kw)
    svc.settings.set("confluence", cfg)
    return cfg


def add_source(svc, root_id=None):
    sid = confluence.source_id_for("QP", root_id)
    svc.db.execute("""INSERT INTO sources (id, kind, name, config, enabled) VALUES (%s, 'confluence', 'QP', %s, true)
                      ON CONFLICT (id) DO NOTHING""",
                   (sid, Jsonb({"space_key": "QP", "space_id": "S1", "root_id": root_id})))
    return sid


def scan(svc, fake, sid, now=NOW):
    src = svc.db.one("SELECT * FROM sources WHERE id = %s", (sid,))
    return confluence.ConfluenceConnector(svc.db, fake.client(), cf_cfg(svc), "Alex", ["Alex Moreira"]).scan(src, now=now)


def test_storage_is_split_by_headings_with_tables_and_mentions():
    storage = ('<p>intro</p><h1>Runbook</h1><p>step <ac:link><ri:user ri:account-id="A1"/></ac:link></p>'
               '<h2>Alerts</h2><table><tr><th>alert</th><th>owner</th></tr><tr><td>5xx</td><td>Zephyr</td></tr></table>'
               '<ac:structured-macro ac:name="info"><ac:parameter ac:name="title">x</ac:parameter>'
               '<ac:rich-text-body><p>careful</p></ac:rich-text-body></ac:structured-macro>'
               '<h1>Other</h1><p>end &amp; so on</p>')
    secs = confluence.sections_of(storage, {"A1": "Ana Lima"})
    assert [p for p, _ in secs] == ["(start)", "Runbook", "Runbook > Alerts", "Other"]
    assert "@Ana Lima" in secs[1][1]
    assert "5xx | Zephyr" in secs[2][1] and "careful" in secs[2][1] and "[info: title=x]" in secs[2][1], \
        "macro parameter is content (status, panel title, Jira key): nothing is lost"
    assert secs[3][1] == "end & so on"


def test_tree_scope_reads_root_and_descendants_once_per_section(svc):
    fake = FakeConfluence()
    fake.page("1", "Home", body((None, "home")))
    fake.page("2", "Runbook", body(("Deploy", "via ArgoCD"), ("Alerts", "5xx goes to Zephyr")), parent="1")
    fake.page("3", "Runbook sub", body(("Step", "x")), parent="2")
    fake.page("4", "Other tree", body(("Y", "y")), parent="1")
    sid = add_source(svc, root_id="2")
    stats = scan(svc, fake, sid)
    assert stats["pages"] == 2 and stats["read"] == 2
    ev = svc.db.query("SELECT external_id, content, role FROM evidence WHERE source_id = %s ORDER BY seq", (sid,))
    assert len(ev) == 3 and all(e["role"] == "page" for e in ev)
    assert "Page «Runbook» · section: Alerts" in ev[1]["content"]
    units = svc.db.query("SELECT kind, meta FROM units WHERE source_id = %s", (sid,))
    assert {u["kind"] for u in units} == {"confluence_page"} and not any(u["meta"]["delta"] for u in units)
    assert scan(svc, fake, sid)["read"] == 0, "an already read version is not re-read"


def test_recent_edit_waits_to_settle(svc):
    fake = FakeConfluence()
    fake.page("2", "Runbook", body(("Deploy", "via ArgoCD")), minutes_ago=5)
    sid = add_source(svc)
    assert scan(svc, fake, sid)["read"] == 0
    assert scan(svc, fake, sid, now=NOW + timedelta(minutes=40))["read"] == 1


def test_edit_keeps_old_version_as_history_and_sends_its_claims_to_the_adjudicator(svc):
    """an edit is not a retraction. Only the changed section is re-extracted; the previous version stays, and its
    claims go to the adjudicator in a tension with a dossier (old + new version)."""
    default_handlers(svc, [])
    deploy = claim("procedure", mention("zephyr", "system", "runbook service"), "deploy", "ArgoCD",
                   "The zephyr deploy is done via ArgoCD.")
    alert = claim("state", mention("5xx alert", "concept", "runbook alert"), "owner", "Zephyr",
                  "The 5xx alert belongs to Zephyr.", refs=("E2",))
    svc.llm.handlers["extraction"] = lambda user, **_: {"claims": ([deploy] if "ArgoCD" in user else [])
                                                                + ([alert] if "5xx" in user else [])}
    fake = FakeConfluence()
    fake.page("2", "Runbook", body(("Deploy", "the deploy is via ArgoCD"), ("Alerts", "5xx belongs to Zephyr")))
    sid = add_source(svc)
    scan(svc, fake, sid)
    drain(svc, lanes=("cognition",))
    old_deploy = svc.db.one("SELECT id FROM evidence WHERE content LIKE '%%via ArgoCD%%'")["id"]
    deploy_claim = svc.db.one("SELECT id FROM claims WHERE predicate = 'deploy'")["id"]
    fake.edit("2", body(("Deploy", "the deploy is now via GitHub Actions"), ("Alerts", "5xx belongs to Zephyr")))
    seen = []
    svc.llm.handlers["adjudication"] = lambda user, tools, **_: (seen.append(user), {
        "kind": "temporal_supersession", "changes": [{"claim_id": deploy_claim, "to": "superseded"}],
        "summary": "the deploy evolved", "rationale": "new version", "cited_evidence": [], "confidence": 0.9})[1]
    svc.llm.handlers["extraction"] = lambda user, **_: {"claims": []}
    scan(svc, fake, sid)
    old = svc.db.one("SELECT status, content, external_id, meta FROM evidence WHERE id = %s", (old_deploy,))
    assert old["status"] == "superseded" and "ArgoCD" in old["content"] and old["external_id"] is None
    assert old["meta"]["superseded_by_version"] == 2
    delta = svc.db.query("SELECT * FROM units WHERE source_id = %s ORDER BY id", (sid,))[-1]
    ev = svc.db.query("SELECT content FROM evidence WHERE unit_id = %s", (delta["id"],))
    assert len(ev) == 1 and "GitHub Actions" in ev[0]["content"] and delta["meta"]["delta"], "only the changed section"
    t = svc.db.one("SELECT * FROM tensions WHERE opened_by = 'connector'")
    assert t["claim_ids"] == [deploy_claim] and t["dossier"]["detector"] == "confluence_edit"
    assert old_deploy in t["dossier"]["evidence_ids"] and len(t["dossier"]["evidence_ids"]) == 2
    assert svc.db.one("SELECT status FROM claims WHERE id = %s", (deploy_claim,))["status"] == "current", \
        "nothing is retracted before the judgment"
    drain(svc, lanes=("cognition",))
    assert "previous version, superseded in version 2" in seen[0] and "GitHub Actions" in seen[0]
    assert "accusation" in seen[0]
    assert svc.db.one("SELECT status FROM claims WHERE id = %s", (deploy_claim,))["status"] == "superseded"
    assert svc.db.one("SELECT status FROM claims WHERE predicate = 'owner'")["status"] == "current", "intact section"


def test_deleted_page_erases_everything_including_history_and_archived_only_marks(svc):
    fake = FakeConfluence()
    fake.page("2", "Runbook", body(("Deploy", "via ArgoCD")))
    fake.page("3", "Old", body(("X", "something")))
    sid = add_source(svc)
    scan(svc, fake, sid)
    fake.edit("2", body(("Deploy", "via GitHub Actions")))
    scan(svc, fake, sid)
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE status = 'superseded'")["n"] == 1
    fake.gone.add("2")
    fake.status["3"] = "archived"
    stats = scan(svc, fake, sid)
    assert stats["removed"] == 1 and stats["archived"] == 1
    left = svc.db.query("SELECT status, content FROM evidence WHERE content LIKE '%%ArgoCD%%' OR content LIKE '%%Actions%%'")
    assert left == [], "the version history goes away too"
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE content LIKE '%%something%%' AND status = 'active'")["n"] == 1


def test_page_moved_out_of_the_tree_leaves(svc):
    fake = FakeConfluence()
    fake.page("2", "Runbook", body((None, "root")))
    fake.page("5", "Child", body((None, "child content")), parent="2")
    sid = add_source(svc, root_id="2")
    scan(svc, fake, sid)
    fake.pages["5"]["parentId"] = "9"  # moved out of the tree
    assert scan(svc, fake, sid)["removed"] == 0, "first it stays loose: another enabled source may take over"
    assert scan(svc, fake, sid, now=NOW + timedelta(hours=2))["removed"] == 1
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE content LIKE '%%child%%' AND status = 'active'")["n"] == 0


def test_comments_inline_carry_the_anchor_and_edits_retract(svc):
    fake = FakeConfluence()
    fake.page("2", "Runbook", body(("Deploy", "via ArgoCD")))
    fake.comments["2"] = {
        "footer": [{"id": "C1", "version": {"authorId": "A-ANA", "createdAt": iso(NOW)},
                    "body": {"storage": {"value": "<p>needs an approval gate</p>"}}}],
        "inline": [{"id": "C2", "version": {"authorId": "A-ALEX", "createdAt": iso(NOW)},
                    "properties": {"inlineOriginalSelection": "via ArgoCD"}, "resolutionStatus": "open",
                    "body": {"storage": {"value": "<p>not anymore</p>"}}}]}
    sid = add_source(svc)
    scan(svc, fake, sid)
    cs = {e["external_id"]: e["content"] for e in svc.db.query(
        "SELECT external_id, content FROM evidence WHERE role = 'comment' AND status = 'active'")}
    assert "Ana Lima" in cs["comment:C1"] and "approval gate" in cs["comment:C1"]
    assert "Commented excerpt: «via ArgoCD»" in cs["comment:C2"] and "(Alex, the owner)" in cs["comment:C2"]
    fake.comments["2"]["footer"][0]["body"]["storage"]["value"] = "<p>needs an approval gate in prod</p>"
    scan(svc, fake, sid, now=NOW + timedelta(hours=7))  # comment revision due
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE role = 'comment' AND status = 'tombstoned'")["n"] == 1
    assert "in prod" in svc.db.one("""SELECT content FROM evidence WHERE external_id = 'comment:C1'
                                       AND status = 'active'""")["content"]


def test_whole_space_and_tree_overlap_reads_each_page_once(svc):
    fake = FakeConfluence()
    fake.page("2", "Runbook", body(("Deploy", "via ArgoCD")))
    space = add_source(svc)
    tree = add_source(svc, root_id="2")
    scan(svc, fake, space)
    scan(svc, fake, tree)
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE role = 'page'")["n"] == 1


def test_estimate_counts_text_and_what_is_left(svc):
    fake = FakeConfluence()
    fake.page("2", "Runbook", body(("Deploy", "x" * 100)))
    fake.page("3", "Other", body(("Y", "y" * 50)))
    sid = add_source(svc)
    src = svc.db.one("SELECT * FROM sources WHERE id = %s", (sid,))
    est = confluence.estimate_backfill(fake.client(), src, 0.001, {"2"})
    assert est["pages"] == 2 and est["chars"] == 150 and est["new"] == 1 and est["usd_new"] == 0.05
    assert not svc.llm.calls


def test_scoped_token_goes_through_the_gateway_with_discovered_cloud_id():
    fake = FakeConfluence()
    c = fake.client()
    assert str(c.http.base_url) == "https://api.atlassian.com/ex/confluence/CLOUD/wiki/"
    confluence.verify(c)
    classic = confluence.ConfluenceClient("acme.atlassian.net", "e", "t", "classic", transport=httpx.MockTransport(fake.handler))
    assert str(classic.http.base_url) == "https://acme.atlassian.net/wiki/"


def test_refused_token_is_source_unavailable():
    c = confluence.ConfluenceClient("x.atlassian.net", "e", "t", "classic",
                                    transport=httpx.MockTransport(lambda r: httpx.Response(401, json={})))
    with pytest.raises(SourceUnavailable, match="rejected"):
        confluence.verify(c)


def test_web_picks_space_and_tree_and_saves_settings(svc, monkeypatch):
    from fastapi.testclient import TestClient
    from pythia import vault
    from pythia.web.app import create_app
    stored = {}
    monkeypatch.setattr(vault, "get_secret", lambda name: stored.get(name))
    monkeypatch.setattr(vault, "set_secret", lambda name, value: stored.__setitem__(name, value))
    client = TestClient(create_app(svc, with_mcp=False))
    r = client.post("/action/config", data={"cf_site": "https://acme.atlassian.net/", "cf_email": "alex@x",
                                           "cf_token_type": "scoped", "cf_token": "tok", "sw_budget": "0.75",
                                           "sw_enabled": "on"}, follow_redirects=False)
    assert r.status_code == 303 and stored["confluence_api_token"] == "tok"
    assert svc.settings.get("confluence")["site"] == "acme.atlassian.net"
    assert svc.settings.get("sweep") == {"enabled": True, "daily_budget_usd": 0.75}
    assert svc.db.one("SELECT count(*) AS n FROM jobs WHERE kind = 'list_confluence_spaces'")["n"] == 1
    svc.state.set("confluence_spaces", {"items": [{"id": "S1", "key": "QP", "name": "Zephyr", "type": "global"}]})
    svc.state.set("confluence_outline:QP", {"items": [{"id": "2", "title": "Runbook", "depth": 0},
                                                       {"id": "3", "title": "Deploy", "depth": 1}]})
    page = client.get("/sources?cf_space=QP").text
    assert "Add the whole space" in page and "Add tree" in page and "Runbook" in page
    client.post("/action/confluence/add", data={"space_key": "QP", "root_id": "2", "root_title": "Runbook"})
    src = svc.db.one("SELECT * FROM sources WHERE kind = 'confluence'")
    assert src["id"] == "confluence:tree:2" and not src["enabled"] and src["name"] == "Zephyr › Runbook (tree)"
    assert "on the allowlist" in client.get("/sources?cf_space=QP").text
