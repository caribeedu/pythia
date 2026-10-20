"""Web interface: Evolution, Explore, Sources, Costs, Quality, Usage, Settings + hook and MCP."""

from __future__ import annotations

import contextlib
from html import escape as html_escape
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .. import consumer, quality, vault
from .. import knowledge as K
from .. import reexplain
from ..connectors import claude_code, confluence, github, incident_io, slack
from ..pipeline import correct
from ..services import Services
from ..settings import DEFAULTS, ROLES
from . import graph

HERE = Path(__file__).parent
EFFORTS = ["low", "medium", "high", "xhigh", "max"]
ASSET_VERSION = str(int(max(p.stat().st_mtime for p in (HERE / "static").iterdir())))


def _parse_t(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.strip().replace(" ", "+"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _csv(form, field: str, current: list | None) -> list[str]:
    if field not in form:
        return list(current or [])
    return [a.strip() for a in (form.get(field) or "").split(",") if a.strip()]


def create_app(svc: Services, with_mcp: bool = True) -> FastAPI:
    mcp_app = None
    mcp = None
    if with_mcp:
        from ..mcp_server import build_mcp

        mcp = build_mcp(svc)
        mcp_app = mcp.streamable_http_app(streamable_http_path="/", stateless_http=True, json_response=True)

    @contextlib.asynccontextmanager
    async def lifespan(app):
        if mcp is not None:
            async with mcp.session_manager.run():
                yield
        else:
            yield

    app = FastAPI(title="Pythia", lifespan=lifespan)
    templates = Jinja2Templates(directory=str(HERE / "templates"))
    templates.env.filters["dt"] = lambda d: (datetime.fromisoformat(d) if isinstance(d, str) else d).strftime(
        "%Y-%m-%d %H:%M") if d else "—"
    templates.env.filters["money"] = lambda v: f"US${float(v or 0):.4f}" if float(v or 0) < 1 else f"US${float(v):.2f}"
    def friendly(text, names: dict | None = None):
        """Replaces IDs in the middle of a text with a readable label, with a link (Evolution)."""
        from markupsafe import Markup
        from ..evolution import ID
        if text is None:
            return ""
        names = names or {}

        def link(m):
            i = m.group(0)
            label = names.get(i)
            if not label:
                return f'<span class="mono">{i}</span>'
            return f'<a href="/explore?id={i}" title="{i}">{html_escape(label)}</a>'
        return Markup(ID.sub(link, str(html_escape(str(text)))))

    templates.env.filters["friendly"] = friendly
    app.mount("/static", StaticFiles(directory=str(HERE / "static")), name="static")
    if mcp_app is not None:
        app.mount("/mcp", mcp_app)

    def page(request: Request, name: str, **ctx) -> HTMLResponse:
        ctx["cognition"] = svc.state.get("cognition")
        ctx["spend"] = svc.llm.month_spend()
        ctx["cap"] = float(svc.settings.get("monthly_cap_usd"))
        ctx["has_key"] = bool(vault.get_secret(vault.ANTHROPIC_KEY))
        ctx["active"] = name
        ctx["version"] = ASSET_VERSION
        resp = templates.TemplateResponse(request, f"{name}.html", ctx)
        # Live state (queue, estimates): the browser must not show a cached version of the page.
        resp.headers["Cache-Control"] = "no-store"
        return resp

    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse("/evolution")

    # ------------------------------------------------------------ Evolution

    @app.get("/evolution", response_class=HTMLResponse)
    def evolution_page(request: Request, before: str | None = None, source: str | None = None, entity: str | None = None,
                       no_effect: int = 0, verdicts: int = 0):
        from .. import evolution
        before_dt = datetime.fromisoformat(before) if before else None
        with svc.db.tx() as conn:
            data = evolution.feed(conn, before=before_dt, show_noop=bool(no_effect), source_id=source or None,
                                  entity_id=entity or None, only_verdicts=bool(verdicts))
            sources = conn.execute("SELECT id, name, kind FROM sources WHERE kind <> 'live' ORDER BY kind, name").fetchall()
            ent = conn.execute("SELECT id, name, type FROM entities WHERE id = %s", (entity,)).fetchone() if entity else None
            trajectory = evolution.trajectory(conn, entity) if entity else []
            upd = evolution.updating(conn)
            open_tensions = conn.execute("SELECT count(*) AS n FROM tensions WHERE status = 'open'").fetchone()["n"]
        args = {"source": source or "", "entity": entity or "", "no_effect": no_effect, "verdicts": verdicts}
        return page(request, "evolution", data=data, sources=sources, ent=ent, trajectory=trajectory, updating=upd,
                    open_tensions=open_tensions, args=args)

    @app.get("/evolution/diff/{view_id}/{version}", response_class=HTMLResponse)
    def evolution_diff(view_id: str, version: int):
        from .. import evolution
        with svc.db.tx() as conn:
            d = evolution.page_diff(conn, view_id, version)
        cls = {"+": "add", "-": "del", "@": "gap", "=": "same", " ": "same"}
        body = "".join(f'<div class="{cls.get(k, "same")}">{"+" if k == "+" else "−" if k == "-" else " "} {html_escape(t)}</div>'
                       for k, t in d["lines"])
        head = '<div class="muted">First version.</div>' if d["first"] else ""
        return HTMLResponse(head + f'<div class="diff">{body or "<span class=muted>no text change</span>"}</div>')

    @app.get("/verdicts", include_in_schema=False)
    def verdicts_redirect():
        # the tab is gone; verdicts are Evolution lines.
        return RedirectResponse("/evolution?verdicts=1")

    # ------------------------------------------------------------ Explore

    @app.get("/explore", response_class=HTMLResponse)
    def explore(request: Request, id: str | None = None):
        bounds = svc.db.one("SELECT min(ts) AS a, max(ts) AS b FROM events")
        return page(request, "explore", center=id or "", first=bounds["a"], last=bounds["b"])

    @app.get("/api/graph")
    def api_graph(center: str | None = None, depth: int = 1, t: str | None = None):
        with svc.db.tx() as conn:
            return JSONResponse(jsonable_encoder(graph.ego_graph(conn, center, depth, _parse_t(t))))

    @app.get("/api/node/{node_id}")
    def api_node(node_id: str, t: str | None = None):
        with svc.db.tx() as conn:
            return JSONResponse(jsonable_encoder(graph.node_detail(conn, node_id, _parse_t(t))))

    @app.get("/api/search")
    def api_search(q: str):
        return JSONResponse(jsonable_encoder(svc.retriever.search(q, limit=15)))

    @app.get("/api/open/{item_id}")
    def api_open(item_id: str, level: int = 3):
        return JSONResponse({"text": consumer.open_item(svc, item_id, level)})

    @app.get("/api/entities")
    def api_entities(q: str = ""):
        rows = svc.db.query(
            """SELECT id, name, type FROM entities WHERE status = 'active' AND (%s = '' OR lower(name) LIKE '%%' || lower(%s) || '%%')
               ORDER BY name LIMIT 30""", (q, q))
        return JSONResponse(rows)

    # ------------------------------------------------------------ human actions

    @app.post("/api/correct")
    async def api_correct(request: Request):
        body = await request.json()
        cid = correct.request_correction(svc, body.get("target_id") or None, body["text"], via="web")
        return {"correction_id": cid}

    @app.post("/api/define")
    async def api_define(request: Request):
        body = await request.json()
        return {"unit_id": correct.request_definition(svc, body["text"], via="web")}

    @app.post("/api/relink")
    async def api_relink(request: Request):
        b = await request.json()
        op = {"op": "relink_mention", "mention_id": b["mention_id"], "entity_id": b.get("entity_id") or None,
              "new_entity": {"name": b["new_name"], "type": b.get("new_type") or "other"} if b.get("new_name") else None}
        cid = correct.record_direct(svc, b["mention_id"], f"relink mention {b['mention_id']}", [op], "entity", "resolution")
        return {"correction_id": cid}

    @app.post("/api/retract")
    async def api_retract(request: Request):
        b = await request.json()
        cid = correct.record_direct(svc, b["claim_id"], f"retract {b['claim_id']}",
                                    [{"op": "retract_claim", "claim_id": b["claim_id"]}], "claim", "extraction")
        return {"correction_id": cid}

    @app.post("/api/merge")
    async def api_merge(request: Request):
        b = await request.json()
        cid = correct.record_direct(svc, b["from"], f"merge {b['from']} into {b['into']}",
                                    [{"op": "merge_entities", "entity_id": b["from"], "into_entity_id": b["into"]}],
                                    "entity", "resolution")
        return {"correction_id": cid}

    @app.post("/action/revert/{verdict_id}")
    def action_revert(request: Request, verdict_id: str):
        v = svc.db.one("SELECT decided_by FROM verdicts WHERE id = %s", (verdict_id,))
        correct.record_direct(svc, verdict_id, f"revert {verdict_id}",
                              [{"op": "revert_verdict", "verdict_id": verdict_id}], "verdict",
                              v["decided_by"] if v and v["decided_by"] in ROLES else "adjudication")
        return RedirectResponse(request.headers.get("referer") or "/evolution", status_code=303)

    @app.post("/action/override/{verdict_id}")
    def action_override(verdict_id: str, text: str = Form(...)):
        correct.request_correction(svc, verdict_id, text, via="web")
        return RedirectResponse("/evolution", status_code=303)

    # ------------------------------------------------------------ Sources

    def _spent(source_id: str) -> float:
        return float(svc.db.one("SELECT coalesce(sum(cost_usd), 0) AS c FROM llm_calls WHERE source_id = %s",
                                (source_id,))["c"])

    @app.get("/sources", response_class=HTMLResponse)
    def sources_page(request: Request, cf_space: str = ""):
        cfg = svc.settings.get("claude_code")
        root = claude_code.projects_root(cfg)
        # Allowlist per chat: a new chat shows up turned off; each one is turned on by hand.
        added = {r["id"]: r for r in svc.db.query("SELECT * FROM sources WHERE kind = 'claude_code'")}
        projects = []
        for c in claude_code.discover_chats(root, claude_code.app_sessions_root(cfg)):
            sid = claude_code.chat_source_id(c["id"])
            src = added.pop(sid, None)
            c.update(source_id=sid, added=bool(src), enabled=bool(src and src["enabled"]),
                     project=(c.get("cwd") or "").replace(str(Path.home()), "~"))
            projects.append(c)
        for sid, src in added.items():  # enabled before, but the chat vanished from disk: still shown
            projects.append({"id": src["config"].get("chat_id"), "title": src["name"], "files": src["config"].get("files") or [],
                             "project": "", "origin": "removed from disk", "archived": False, "last_activity": None,
                             "source_id": sid, "added": True, "enabled": src["enabled"]})
        stats = {r["source_id"]: r for r in svc.db.query(
            """SELECT s.id AS source_id,
                      (SELECT count(*) FROM streams st WHERE st.source_id = s.id AND st.status = 'active') AS streams,
                      (SELECT max(last_settled_at) FROM streams st WHERE st.source_id = s.id) AS last_settled,
                      (SELECT count(*) FROM units u WHERE u.source_id = s.id AND u.status = 'pending') AS pending,
                      (SELECT count(*) FROM units u WHERE u.source_id = s.id AND u.status = 'processed') AS processed,
                      (SELECT count(*) FROM units u WHERE u.source_id = s.id AND u.status = 'triaged_out') AS triaged_out,
                      (SELECT count(*) FROM units u WHERE u.source_id = s.id AND u.status = 'failed') AS failed,
                      s.backfill_until IS NOT NULL AND s.backfill_done_at IS NULL AS in_backfill,
                      (SELECT count(*) FROM batch_requests b WHERE b.source_id = s.id AND b.status IN ('pending', 'submitted')) AS in_batch,
                      (SELECT count(*) FROM deferred_derivations d WHERE d.source_id = s.id) AS deferred
               FROM sources s""")}
        jobs = svc.queue.counts()
        repos = svc.db.query("SELECT * FROM sources WHERE kind = 'github' ORDER BY id")
        for r in repos:
            r["error"] = svc.state.get(f"source_error:{r['id']}")
            r["estimate"] = svc.state.get(f"estimate:{r['id']}")
            if r["estimate"] is None:  # repositories without an estimate get one
                svc.queue.enqueue("estimate_backfill", "io", {"source_id": r["id"]}, priority=8,
                                  dedup_key=f"estimate:{r['id']}", max_attempts=3)
            r["mode"] = r["config"].get("mode") or github.DEFAULT_MODE
            r["horizon"] = r["config"].get("horizon_days") or svc.settings.get("github")["horizon_days"]
            r["prs"] = svc.db.one("SELECT count(*) AS n FROM streams WHERE source_id = %s", (r["id"],))["n"]
            r["spent"] = _spent(r["id"])
        channels = svc.db.query("SELECT * FROM sources WHERE kind = 'slack' ORDER BY name")
        for r in channels:
            r["error"] = svc.state.get(f"source_error:{r['id']}")
            r["estimate"] = svc.state.get(f"estimate:{r['id']}")
            if r["estimate"] is None and vault.get_secret(vault.SLACK_TOKEN):
                svc.queue.enqueue("estimate_backfill", "io", {"source_id": r["id"]}, priority=8,
                                  dedup_key=f"estimate:{r['id']}", max_attempts=3)
            r["horizon"] = r["config"].get("horizon_days") or svc.settings.get("slack")["horizon_days"]
            r["conversations"] = svc.db.one("SELECT count(*) AS n FROM streams WHERE source_id = %s", (r["id"],))["n"]
            r["spent"] = _spent(r["id"])
        added = {r["config"]["channel"] for r in channels}
        slack_list = svc.state.get("slack_conversations") or {}
        slack_state = {"has_token": bool(vault.get_secret(vault.SLACK_TOKEN)),
                       "error": svc.state.get("source_error:slack"), "at": slack_list.get("at"),
                       "available": [c for c in slack_list.get("items") or [] if c["id"] not in added]}
        catalog = svc.db.one("SELECT * FROM sources WHERE id = %s", (incident_io.SOURCE_ID,))
        catalog_state = {"last": svc.state.get("catalog_last_sync"),
                         "error": svc.state.get(f"source_error:{incident_io.SOURCE_ID}"),
                         "has_key": bool(vault.get_secret(vault.INCIDENT_IO_KEY)),
                         "entities": svc.db.one("SELECT count(*) AS n FROM entities WHERE origin = 'anchored' AND external_ids ? 'incident_io'")["n"],
                         "types": svc.settings.get("incident_io")["types"]}
        failed = svc.db.query("SELECT id, kind, last_error, finished_at FROM jobs WHERE status = 'failed' ORDER BY id DESC LIMIT 10")
        waiting = svc.db.query("SELECT kind, count(*) AS n, max(last_error) AS err FROM jobs WHERE status = 'queued' AND last_error IS NOT NULL GROUP BY kind")
        from . import processing
        cf_sources = svc.db.query("SELECT * FROM sources WHERE kind = 'confluence' ORDER BY name")
        for r in cf_sources:
            r["error"] = svc.state.get(f"source_error:{r['id']}")
            r["estimate"] = svc.state.get(f"estimate:{r['id']}")
            if r["estimate"] is None and vault.get_secret(vault.CONFLUENCE_TOKEN):
                svc.queue.enqueue("estimate_backfill", "io", {"source_id": r["id"]}, priority=8,
                                  dedup_key=f"estimate:{r['id']}", max_attempts=3)
            r["spent"] = _spent(r["id"])
            r["pages"] = svc.db.one("""SELECT count(*) AS n FROM streams WHERE source_id = %s
                                         AND meta->>'confluence' = 'page' AND status = 'active'""", (r["id"],))["n"]
        spaces = (svc.state.get("confluence_spaces") or {})
        outline = svc.state.get(f"confluence_outline:{cf_space}") if cf_space else None
        if cf_space and outline is None and vault.get_secret(vault.CONFLUENCE_TOKEN):
            svc.queue.enqueue("list_confluence_pages", "io", {"source_id": "confluence", "space_key": cf_space},
                              priority=8, dedup_key=f"confluence_pages:{cf_space}", max_attempts=3)
        added_roots = {(r["config"].get("space_key"), r["config"].get("root_id")) for r in cf_sources}
        cf_state = {"has_token": bool(vault.get_secret(vault.CONFLUENCE_TOKEN)),
                    "error": svc.state.get("source_error:confluence"), "spaces": spaces.get("items") or [],
                    "at": spaces.get("at"), "space": cf_space, "outline": outline, "added": added_roots}
        return page(request, "sources", now_state=processing.snapshot(svc), human=processing.human, now=datetime.now(timezone.utc), projects=projects, stats=stats, jobs=jobs, root=str(root), failed=failed,
                    waiting=waiting, repos=repos, catalog=catalog, catalog_state=catalog_state, modes=github.MODES,
                    channels=channels, slack_state=slack_state, cf_sources=cf_sources, cf_state=cf_state)

    @app.post("/action/confluence/refresh")
    def action_confluence_refresh():
        svc.queue.enqueue("list_confluence_spaces", "io", {"source_id": "confluence"}, priority=8,
                          dedup_key="confluence_spaces", max_attempts=3)
        return RedirectResponse("/sources#confluence", status_code=303)

    @app.post("/action/confluence/pages")
    def action_confluence_pages(space_key: str = Form(...)):
        """Loads (or reloads) the space's pages, as a tree, to choose roots."""
        svc.queue.enqueue("list_confluence_pages", "io", {"source_id": "confluence", "space_key": space_key}, priority=8,
                          dedup_key=f"confluence_pages:{space_key}", max_attempts=3)
        return RedirectResponse(f"/sources?cf_space={space_key}#confluence", status_code=303)

    @app.post("/action/confluence/add")
    def action_confluence_add(space_key: str = Form(...), root_id: str = Form(""), root_title: str = Form("")):
        """Whole space or page tree. Comes in paused, with the estimate."""
        from psycopg.types.json import Jsonb
        space = next((x for x in (svc.state.get("confluence_spaces") or {}).get("items") or [] if x["key"] == space_key), None)
        if space:
            sid = confluence.source_id_for(space_key, root_id or None)
            name = f"{space['name']} › {root_title} (tree)" if root_id else f"{space['name']} (whole space)"
            svc.db.execute(
                """INSERT INTO sources (id, kind, name, config, enabled) VALUES (%s, 'confluence', %s, %s, false)
                   ON CONFLICT (id) DO NOTHING""",
                (sid, name, Jsonb({"space_key": space_key, "space_id": space["id"], "root_id": root_id or None,
                                   "root_title": root_title or None})))
            svc.queue.enqueue("estimate_backfill", "io", {"source_id": sid}, priority=8,
                              dedup_key=f"estimate:{sid}", max_attempts=3)
        return RedirectResponse(f"/sources?cf_space={space_key}#confluence", status_code=303)

    @app.post("/action/slack/refresh")
    def action_slack_refresh():
        svc.queue.enqueue("list_slack_conversations", "io", {"source_id": "slack"}, priority=8, dedup_key="slack_conversations",
                          max_attempts=3)
        return RedirectResponse("/sources#slack", status_code=303)

    @app.post("/action/slack/add")
    def action_slack_add(channel: str = Form(...)):
        """Comes in paused, like the repositories: the backfill estimate first; you turn it on when you want."""
        from psycopg.types.json import Jsonb
        conv = next((c for c in (svc.state.get("slack_conversations") or {}).get("items") or [] if c["id"] == channel), None)
        if conv:
            sid = slack.source_id_for(conv["id"])
            svc.db.execute(
                """INSERT INTO sources (id, kind, name, config, enabled) VALUES (%s, 'slack', %s, %s, false)
                   ON CONFLICT (id) DO NOTHING""",
                (sid, conv["name"], Jsonb({"channel": conv["id"], "kind": conv["kind"]})))
            svc.queue.enqueue("estimate_backfill", "io", {"source_id": sid}, priority=8,
                              dedup_key=f"estimate:{sid}", max_attempts=3)
        return RedirectResponse("/sources#slack", status_code=303)

    @app.post("/action/slack/config")
    def action_slack_config(source_id: str = Form(...), horizon_days: int = Form(...)):
        from psycopg.types.json import Jsonb
        src = svc.db.one("SELECT config FROM sources WHERE id = %s", (source_id,))
        before = (src["config"].get("horizon_days") if src else None) or svc.settings.get("slack")["horizon_days"]
        svc.db.execute("UPDATE sources SET config = config || %s WHERE id = %s",
                       (Jsonb({"horizon_days": max(1, horizon_days)}), source_id))
        if src and horizon_days > before and (src["config"].get("state") or {}).get("initialized"):
            # Larger horizon: re-reads from the new start (what already exists is deduplicated, at no LLM cost).
            svc.db.execute("""UPDATE sources SET config = jsonb_set(config, '{state,full_rescan}', 'true'::jsonb, true)
                              WHERE id = %s""", (source_id,))
            svc.state.clear(f"last_scan:{source_id}")
        svc.state.clear(f"estimate:{source_id}")
        svc.queue.enqueue("estimate_backfill", "io", {"source_id": source_id}, priority=8,
                          dedup_key=f"estimate:{source_id}", max_attempts=3)
        return RedirectResponse("/sources#slack", status_code=303)

    @app.get("/sources/now", response_class=HTMLResponse)
    def sources_now(request: Request):
        from . import processing
        return templates.TemplateResponse(request, "_now.html", {"now_state": processing.snapshot(svc), "human": processing.human,
                                                                   "now": datetime.now(timezone.utc)})

    @app.post("/action/chat/enable")
    def action_chat_enable(chat_id: str = Form(...)):
        """Turns a chat on. First time: the whole chat comes in as backfill (batch and deferred synthesis)."""
        from psycopg.types.json import Jsonb
        cfg = svc.settings.get("claude_code")
        chat = next((c for c in claude_code.discover_chats(claude_code.projects_root(cfg), claude_code.app_sessions_root(cfg))
                     if c["id"] == chat_id), None)
        if chat:
            sid = claude_code.chat_source_id(chat_id)
            svc.db.execute(
                """INSERT INTO sources (id, kind, name, config, enabled, backfill_until) VALUES (%s, 'claude_code', %s, %s, true, now())
                   ON CONFLICT (id) DO UPDATE SET enabled = true""",
                (sid, chat["title"], Jsonb({"chat_id": chat_id, "files": chat["files"]})))
            svc.queue.enqueue("scan_sources", "io", {}, priority=5, dedup_key="scan_sources", max_attempts=1)
        return RedirectResponse("/sources#sessions", status_code=303)

    @app.post("/action/github/add")
    def action_github_add(repo: str = Form(...)):
        from psycopg.types.json import Jsonb
        repo = repo.strip().removeprefix("https://github.com/").strip("/")
        if repo.count("/") == 1:
            # Comes in paused: the backfill cost estimate first; you turn it on when you want.
            sid = github.source_id_for(repo)
            svc.db.execute(
                """INSERT INTO sources (id, kind, name, config, enabled) VALUES (%s, 'github', %s, %s, false)
                   ON CONFLICT (id) DO NOTHING""",
                (sid, repo, Jsonb({"repo": repo, "mode": github.DEFAULT_MODE})))
            svc.queue.enqueue("estimate_backfill", "io", {"source_id": sid}, priority=8,
                              dedup_key=f"estimate:{sid}", max_attempts=3)
        return RedirectResponse("/sources", status_code=303)

    @app.post("/action/source/toggle")
    def action_source_toggle(source_id: str = Form(...)):
        """Turn on / Pause. Pausing keeps what was read. The first time it is turned on, the backfill starts."""
        svc.db.execute("""UPDATE sources SET enabled = NOT enabled,
                                 backfill_until = CASE WHEN NOT enabled THEN coalesce(backfill_until, now())
                                                       ELSE backfill_until END
                          WHERE id = %s""", (source_id,))
        svc.state.clear(f"last_scan:{source_id}")
        return RedirectResponse("/sources", status_code=303)

    @app.post("/action/source/remove")
    def action_source_remove(source_id: str = Form(...)):
        """Remove: stops reading and retracts everything that came only from this source."""
        svc.db.execute("UPDATE sources SET enabled = false WHERE id = %s", (source_id,))
        svc.queue.enqueue("remove_source", "cognition", {"source_id": source_id}, priority=10,
                          dedup_key=f"remove:{source_id}")
        return RedirectResponse("/sources", status_code=303)

    @app.post("/action/github/config")
    def action_github_config(source_id: str = Form(...), mode: str = Form(...), horizon_days: int = Form(...)):
        from psycopg.types.json import Jsonb
        if mode not in github.MODES:
            return RedirectResponse("/sources", status_code=303)
        svc.db.execute("UPDATE sources SET config = config || %s WHERE id = %s",
                       (Jsonb({"mode": mode, "horizon_days": max(1, horizon_days)}), source_id))
        svc.queue.enqueue("reapply_filters", "io", {"source_id": source_id}, priority=8,
                          dedup_key=f"reapply:{source_id}", max_attempts=3)
        svc.queue.enqueue("estimate_backfill", "io", {"source_id": source_id}, priority=8,
                          dedup_key=f"estimate:{source_id}", max_attempts=3)
        return RedirectResponse("/sources", status_code=303)

    def _reapply_all_github():
        for r in svc.db.query("SELECT id FROM sources WHERE kind = 'github'"):
            svc.queue.enqueue("reapply_filters", "io", {"source_id": r["id"]}, priority=8,
                              dedup_key=f"reapply:{r['id']}", max_attempts=3)

    @app.post("/action/team")
    async def action_team(request: Request):
        form = await request.form()
        members = []
        for i in range(int(form.get("rows") or 0) + 1):
            m = {k: (form.get(f"{k}_{i}") or "").strip() for k in ("name", "email", "slack", "github", "jira", "zoom")}
            if any(m.values()) and form.get(f"remove_{i}") != "on":
                members.append(m)
        svc.settings.set("team", {"members": members})
        _reapply_all_github()
        svc.queue.enqueue("apply_team_identities", "cognition", {}, priority=40, dedup_key="team_identities")
        return RedirectResponse("/config?saved=1#team", status_code=303)

    @app.post("/action/team/import")
    def action_team_import(team_entity_id: str = Form(...)):
        """Copies a catalog team into the list. Never syncs by itself afterwards."""
        rows = svc.db.query(
            """SELECT DISTINCT e.name, e.external_ids FROM claims c JOIN entities e ON e.id = c.object_entity_id
               WHERE c.subject_entity_id = %s AND c.predicate = 'members' AND c.status IN ('current', 'disputed')
                 AND e.status = 'active'""", (team_entity_id,))
        members = svc.settings.get("team")["members"]
        known = {(m.get("email") or m.get("github") or m.get("name")).lower() for m in members}
        for r in rows:
            x = r["external_ids"]
            m = {"name": r["name"], "email": x.get("email", ""), "slack": x.get("slack", ""),
                 "github": x.get("github", ""), "jira": x.get("jira", ""), "zoom": x.get("zoom", "")}
            if (m["email"] or m["github"] or m["name"]).lower() not in known:
                members.append(m)
        svc.settings.set("team", {"members": members})
        _reapply_all_github()
        svc.queue.enqueue("apply_team_identities", "cognition", {}, priority=40, dedup_key="team_identities")
        return RedirectResponse("/config?saved=1#team", status_code=303)

    @app.post("/action/catalog")
    def action_catalog(enable: int = Form(...)):
        svc.db.execute(
            """INSERT INTO sources (id, kind, name, config, enabled, backfill_until) VALUES (%s, 'incident_io', 'incident.io catalog', '{}', %s, now())
               ON CONFLICT (id) DO UPDATE SET enabled = EXCLUDED.enabled""", (incident_io.SOURCE_ID, bool(enable)))
        svc.state.clear(f"last_scan:{incident_io.SOURCE_ID}")
        return RedirectResponse("/sources", status_code=303)

    @app.post("/action/scan")
    def action_scan():
        svc.db.execute("DELETE FROM system_state WHERE key LIKE 'last_scan:%%'")
        svc.queue.enqueue("scan_sources", "io", {}, priority=5, dedup_key="scan_sources", max_attempts=1)
        return RedirectResponse("/sources", status_code=303)

    @app.post("/action/resume")
    def action_resume():
        """Retries now the jobs waiting for the LLM (after setting a key or raising the cap)."""
        svc.db.execute("UPDATE jobs SET run_after = now() WHERE status = 'queued' AND run_after > now()")
        return RedirectResponse("/sources", status_code=303)

    # ------------------------------------------------------------ Usage

    @app.get("/usage", response_class=HTMLResponse)
    def usage_page(request: Request):
        from .. import usage
        cfg = svc.settings.get("claude_code")
        titles = {stem: c["title"] for c in claude_code.discover_chats(claude_code.projects_root(cfg),
                                                                         claude_code.app_sessions_root(cfg))
                  for stem in c["files"]}
        return page(request, "usage", pulls=usage.pull_calls(svc), chats=usage.push_by_chat(svc, titles),
                    owner=svc.owner_name)

    # ------------------------------------------------------------ Costs

    @app.get("/costs", response_class=HTMLResponse)
    def costs_page(request: Request):
        month = "ts >= date_trunc('month', now())"
        by_role = svc.db.query(f"SELECT role AS k, sum(cost_usd) AS c, count(*) AS n, sum(input_tokens) AS i, sum(output_tokens) AS o, sum(cache_read) AS cr FROM llm_calls WHERE {month} GROUP BY 1 ORDER BY 2 DESC")
        by_model = svc.db.query(f"SELECT model AS k, sum(cost_usd) AS c, count(*) AS n FROM llm_calls WHERE {month} GROUP BY 1 ORDER BY 2 DESC")
        by_source = svc.db.query(f"SELECT coalesce(source_id, '-') AS k, sum(cost_usd) AS c, count(*) AS n FROM llm_calls WHERE {month} GROUP BY 1 ORDER BY 2 DESC")
        by_day = svc.db.query(f"SELECT date_trunc('day', ts)::date AS d, sum(cost_usd) AS c, count(*) AS n FROM llm_calls WHERE {month} GROUP BY 1 ORDER BY 1")
        errors = svc.db.query(f"SELECT error, count(*) AS n FROM llm_calls WHERE {month} AND NOT ok GROUP BY 1 ORDER BY 2 DESC")
        by_mode = svc.db.query(f"SELECT CASE WHEN batch THEN 'batch (backfill, 50%)' ELSE 'immediate' END AS k, sum(cost_usd) AS c, count(*) AS n FROM llm_calls WHERE {month} GROUP BY 1 ORDER BY 2 DESC")
        return page(request, "costs", by_role=by_role, by_model=by_model, by_source=by_source, by_day=by_day, errors=errors,
                    by_mode=by_mode)

    # ------------------------------------------------------------ Quality

    @app.get("/quality", response_class=HTMLResponse)
    def quality_page(request: Request):
        metrics = quality.correction_metrics(svc)
        corrections = svc.db.query("SELECT * FROM corrections ORDER BY created_at DESC LIMIT 50")
        questions = svc.db.query("SELECT * FROM golden_questions WHERE active ORDER BY created_at DESC")
        runs = svc.db.query("SELECT * FROM golden_runs ORDER BY started_at DESC LIMIT 10")
        results = svc.db.query(
            """SELECT r.*, q.question, q.expected, q.kind, q.sealed FROM golden_results r JOIN golden_questions q ON q.id = r.question_id
               WHERE r.run_id = %s""", (runs[0]["id"],)) if runs else []
        from .. import value
        with svc.db.tx() as conn:
            val = value.report(conn)
        try:
            usage = value.usage_cached(svc)
        except OSError:
            usage = None  # transcripts inaccessible: the section warns about it
        return page(request, "quality", m=metrics, corrections=corrections, questions=questions, runs=runs,
                    results=results, detectors=quality.detector_precision(svc), sweeps=quality.recent_sweeps(svc),
                    val=val, usage=usage, battery=quality.battery_report(svc),
                    repeats=quality.REPEATS, rx=reexplain.summary(svc))

    @app.post("/action/measure-reexplain")
    def action_measure_reexplain(days: int = Form(30)):
        svc.queue.enqueue("reexplain_scan", "derivation", {"since_days": days}, priority=45,
                          dedup_key="reexplain_scan", max_attempts=3)
        return RedirectResponse("/quality", status_code=303)

    @app.get("/value/label", response_class=HTMLResponse)
    def value_label(request: Request):
        return page(request, "label", items=reexplain.sample_for_labeling(svc), agr=reexplain.agreement(svc))

    @app.post("/action/label/{message_id}")
    def action_label(message_id: str, repeats: str = Form(...)):
        svc.db.execute("UPDATE reexplain_messages SET owner_label = %s, labeled_at = now() WHERE message_id = %s",
                       (repeats == "yes", message_id))
        return RedirectResponse("/value/label", status_code=303)

    @app.post("/action/question")
    def action_question(question: str = Form(...), expected: str = Form(...), correction: str = Form("")):
        quality.add_question(svc, question, expected, correction or None)
        return RedirectResponse("/quality", status_code=303)

    @app.post("/action/question/{qid}/toggle")
    def action_question_toggle(qid: str):
        svc.db.execute("UPDATE golden_questions SET active = NOT active WHERE id = %s", (qid,))
        return RedirectResponse("/quality", status_code=303)

    @app.post("/action/run-battery")
    def action_run_battery():
        quality.start_battery(svc)
        return RedirectResponse("/quality", status_code=303)

    @app.post("/action/run-golden")
    def action_run_golden():
        quality.start_run(svc)
        return RedirectResponse("/quality", status_code=303)

    # ------------------------------------------------------------ Settings

    def _narratives_today() -> dict:
        return svc.db.one("""SELECT count(*) AS n, coalesce(sum(l.cost_usd), 0) AS usd FROM llm_calls l
                             JOIN jobs j ON j.id = l.job_id
                             WHERE j.kind = 'derive_narratives' AND l.ts >= date_trunc('day', now())""")

    def _sweep_spent() -> float:
        from ..pipeline import sweep
        return sweep.spent_today(svc)

    @app.get("/config", response_class=HTMLResponse)
    def config(request: Request, saved: int = 0):
        s = svc.settings
        port = s.get("web")["port"]
        hook_cmd = (f"curl -s --max-time 10 -X POST -H 'content-type: application/json' --data-binary @- "
                    f"http://127.0.0.1:{port}/hook/claude-code || true")
        hooks = {"hooks": {ev: [{"hooks": [{"type": "command", "command": hook_cmd, "timeout": 15}]}]
                           for ev in ("SessionStart", "UserPromptSubmit")}}
        return page(request, "config", roles=ROLES, models=s.get("models"), effort=s.get("effort"),
                    efforts=EFFORTS, prices=json.dumps(s.get("prices"), indent=2), cap=s.get("monthly_cap_usd"),
                    owner=s.get("owner"), policy=s.get("authority_policy"), cc=s.get("claude_code"),
                    push=s.get("push"), port=port, hooks=json.dumps(hooks, indent=2), saved=saved,
                    base_url=s.get("anthropic_base_url") or "https://api.anthropic.com",
                    gh=s.get("github"), inc=_inc_with_options(), team=s.get("team")["members"],
                    catalog_teams=svc.db.query(
                        """SELECT id, name FROM entities WHERE status = 'active' AND type = 'team'
                           AND external_ids ? 'incident_io' ORDER BY name"""),
                    has_gh_token=bool(vault.get_secret(vault.GITHUB_TOKEN)),
                    has_inc_key=bool(vault.get_secret(vault.INCIDENT_IO_KEY)),
                    has_slack_token=bool(vault.get_secret(vault.SLACK_TOKEN)), sl=s.get("slack"),
                    has_cf_token=bool(vault.get_secret(vault.CONFLUENCE_TOKEN)), cf=s.get("confluence"),
                    cf_scopes=confluence.SCOPES, sw=s.get("sweep"), sw_spent=_sweep_spent(),
                    nar=s.get("narratives"), nar_today=_narratives_today(), nar_options=K.PASSES_OPTIONS,
                    model_options=sorted(s.get("prices").keys()))

    def _inc_with_options() -> dict:
        inc = svc.settings.get("incident_io")
        with svc.db.tx() as conn:
            inc["types"] = incident_io.type_options(conn, inc["types"])
        return inc

    @app.post("/action/config")
    async def action_config(request: Request):
        form = await request.form()
        s = svc.settings
        before = (s.get("models"), s.get("effort"))
        s.set("models", {r: form.get(f"model_{r}") or ROLES[r]["model"] for r in ROLES})
        s.set("effort", {r: form.get(f"effort_{r}") or "medium" for r in ROLES})
        s.set("monthly_cap_usd", float(form.get("cap") or DEFAULTS["monthly_cap_usd"]))
        aliases = [a.strip() for a in (form.get("aliases") or "").split(",") if a.strip()]
        s.set("owner", {"name": (form.get("owner_name") or "Alex").strip(), "aliases": aliases})
        s.set("authority_policy", form.get("policy") or DEFAULTS["authority_policy"])
        s.set("claude_code", {"projects_root": form.get("projects_root") or DEFAULTS["claude_code"]["projects_root"],
                              "debounce_minutes": float(form.get("debounce") or 5),
                              "include_sidechains": form.get("sidechains") == "on"})
        push_before = s.get("push")
        s.set("push", {"enabled": form.get("push_enabled") == "on", "max_chars": int(form.get("push_max") or 6000),
                       "rules_enabled": form.get("rules_enabled") == "on" if "push_max" in form else push_before.get("rules_enabled", True),
                       "rules": (form.get("rules") or "").strip() or push_before.get("rules")})
        try:
            s.set("prices", json.loads(form.get("prices") or "{}") or DEFAULTS["prices"])
        except json.JSONDecodeError:
            pass
        if form.get("base_url"):
            s.set("anthropic_base_url", form.get("base_url").strip())
        gh = s.get("github")
        before_gh = dict(gh)
        gh.pop("credential", None)
        gh.pop("max_bot_comment_chars", None)
        if form.get("gh_token_type") in ("classic", "fine_grained"):
            gh["token_type"] = form.get("gh_token_type")
        gh.update({"owner_login": (form.get("gh_login") or "").strip(),
                   "poll_minutes": float(form.get("gh_poll") or gh["poll_minutes"]),
                   "horizon_days": int(form.get("gh_horizon") or gh["horizon_days"]),
                   "stale_days": int(form.get("gh_stale") or gh["stale_days"]),
                   "exclude_authors": _csv(form, "gh_exclude", gh.get("exclude_authors")),
                   "exclude_paths": _csv(form, "gh_exclude_paths", gh.get("exclude_paths")),
                   "read_bots": _csv(form, "gh_read_bots", gh.get("read_bots"))})
        filters_before = (before_gh.get("exclude_authors"), before_gh.get("exclude_paths"), before_gh.get("read_bots"))
        if (gh["exclude_authors"], gh["exclude_paths"], gh["read_bots"]) != filters_before:
            _reapply_all_github()
        s.set("github", gh)
        inc = s.get("incident_io")
        if "inc_type_0" in form:
            options = _inc_with_options()["types"]
            inc["types"] = {t: (form.get(f"inc_type_{i}") or "") for i, t in enumerate(options)}
        inc["poll_minutes"] = float(form.get("inc_poll") or inc["poll_minutes"])
        if "inc_exclude" in form:
            # A comma separates items, but types like Custom["Team"] have no comma; "Type: Attribute" stays whole.
            excl = [x.strip() for x in (form.get("inc_exclude") or "").split(",") if x.strip()]
            if excl != inc.get("exclude_attributes"):
                inc["exclude_attributes"] = excl
                from ..daemon import resync_catalog
                resync_catalog(svc)
        s.set("incident_io", inc)
        sl = s.get("slack")
        sl_filters = (sl.get("exclude_authors"), sl.get("read_bots"), sl.get("ignore_subtypes"))
        sl.update({"poll_minutes": float(form.get("sl_poll") or sl["poll_minutes"]),
                   "settle_minutes": float(form.get("sl_settle") or sl["settle_minutes"]),
                   "horizon_days": int(form.get("sl_horizon") or sl["horizon_days"]),
                   "revision_days": int(form.get("sl_revision") or sl["revision_days"]),
                   "window_max_hours": float(form.get("sl_window_max") or sl["window_max_hours"]),
                   "thread_watch_days": int(form.get("sl_watch") or sl["thread_watch_days"]),
                   "exclude_authors": _csv(form, "sl_exclude", sl.get("exclude_authors")),
                   "read_bots": _csv(form, "sl_read_bots", sl.get("read_bots")),
                   "ignore_subtypes": _csv(form, "sl_subtypes", sl.get("ignore_subtypes"))})
        s.set("slack", sl)
        if (sl["exclude_authors"], sl["read_bots"], sl["ignore_subtypes"]) != sl_filters:
            for r in svc.db.query("SELECT id FROM sources WHERE kind = 'slack'"):
                svc.queue.enqueue("reapply_filters", "io", {"source_id": r["id"]}, priority=8,
                                  dedup_key=f"reapply:{r['id']}", max_attempts=3)
        cf = s.get("confluence")
        cf_before = (cf.get("site"), cf.get("email"), cf.get("token_type"))
        cf.update({"site": (form.get("cf_site") or cf.get("site") or "").strip().removeprefix("https://").strip("/"),
                   "email": (form.get("cf_email") or cf.get("email") or "").strip(),
                   "token_type": form.get("cf_token_type") if form.get("cf_token_type") in ("scoped", "classic") else cf["token_type"],
                   "poll_minutes": float(form.get("cf_poll") or cf["poll_minutes"]),
                   "settle_minutes": float(form.get("cf_settle") or cf["settle_minutes"]),
                   "comment_revision_hours": float(form.get("cf_comments") or cf["comment_revision_hours"]),
                   "exclude_authors": _csv(form, "cf_exclude", cf.get("exclude_authors"))})
        if (cf.get("site"), cf.get("email"), cf.get("token_type")) != cf_before:
            cf["cloud_id"] = ""  # site or type changed: discover again
        cf_excl_changed = cf["exclude_authors"] != s.get("confluence").get("exclude_authors")
        s.set("confluence", cf)
        if cf_excl_changed:
            for r in svc.db.query("SELECT id FROM sources WHERE kind = 'confluence'"):
                svc.queue.enqueue("reapply_filters", "io", {"source_id": r["id"]}, priority=8,
                                  dedup_key=f"reapply:{r['id']}", max_attempts=3)
        if "nar_passes" in form:
            # Only the selector's options; any value becomes the nearest option.
            wanted = float(form.get("nar_passes") or 1)
            passes = min((v for v, _ in K.PASSES_OPTIONS), key=lambda v: abs(v - wanted))
            s.set("narratives", {"passes_per_hour": passes})
            with svc.db.tx() as conn:
                # The pass already in the queue now respects the new interval.
                conn.execute("""UPDATE jobs SET run_after = greatest(now(), created_at + %s * interval '1 second')
                                WHERE dedup_key = 'narratives' AND status = 'queued'""", (K.narrative_delay_s(conn),))
        if "sw_budget" in form:
            s.set("sweep", {"enabled": form.get("sw_enabled") == "on",
                            "daily_budget_usd": max(0.0, float(form.get("sw_budget") or 0))})
        for field, name in (("api_key", vault.ANTHROPIC_KEY), ("gh_token", vault.GITHUB_TOKEN),
                            ("inc_key", vault.INCIDENT_IO_KEY), ("slack_token", vault.SLACK_TOKEN),
                            ("cf_token", vault.CONFLUENCE_TOKEN)):
            value = (form.get(field) or "").strip()
            if value:
                vault.set_secret(name, value)
        if (form.get("cf_token") or "").strip() or ((cf.get("site"), cf.get("email")) != cf_before[:2] and cf.get("site")):
            svc.queue.enqueue("list_confluence_spaces", "io", {"source_id": "confluence"}, priority=8,
                              dedup_key="confluence_spaces", max_attempts=3)
        if (form.get("slack_token") or "").strip():  # new token: fetch the conversation list for the allowlist
            svc.queue.enqueue("list_slack_conversations", "io", {"source_id": "slack"}, priority=8, dedup_key="slack_conversations",
                              max_attempts=3)
        # A new key, higher cap or another model can unblock cognition: resume right away what waits for the LLM.
        svc.db.execute("UPDATE jobs SET run_after = now() WHERE status = 'queued' AND run_after > now() AND last_error IS NOT NULL")
        from ..daemon import bootstrap
        bootstrap(svc, requeue=False)  # workers are running: never requeue from here
        if (s.get("models"), s.get("effort")) != before:
            quality.run_if_questions(svc, "models changed")
        return RedirectResponse("/config?saved=1", status_code=303)

    # ------------------------------------------------------------ push hook

    @app.post("/hook/claude-code")
    async def hook(request: Request):
        try:
            body: dict[str, Any] = await request.json()
        except Exception:
            return JSONResponse({})
        event = body.get("hook_event_name") or "UserPromptSubmit"
        import anyio
        text = await anyio.to_thread.run_sync(lambda: consumer.briefing(
            svc, event=event, prompt=body.get("prompt"), cwd=body.get("cwd"), session_id=body.get("session_id"),
            source=body.get("source")))
        if not text:
            return JSONResponse({})
        return JSONResponse({"hookSpecificOutput": {"hookEventName": event, "additionalContext": text}})

    @app.get("/api/state")
    def api_state():
        return {"cognition": svc.state.get("cognition"), "jobs": svc.queue.counts(), "spend": svc.llm.month_spend()}

    return app
