"""Daemon: bootstrap, per-lane workers and periodic scheduler."""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from psycopg.types.json import Jsonb

from . import events
from . import knowledge as K
from .connectors import confluence, github, incident_io, slack
from .connectors import claude_code as cc
from .connectors.claude_code import KIND as CLAUDE_CODE, ClaudeCodeConnector
from .connectors.common import SourceUnavailable
from .jobs import enqueue
from .llm.client import LLMError, LLMHalted, LLMUnavailable
from .pipeline import adjudicate, batch, correct, derive, process, reconcile, sweep
from .quality import run_golden
from .services import Services

log = logging.getLogger("pythia")

LLM_KINDS = {"process_unit", "adjudicate", "drift_sweep", "derive_entity", "derive_narratives", "integrate_inferences",
             "integrate_structured", "reconcile_entities", "apply_correction", "golden_run", "poll_batches",
             "apply_batch_synthesis"}


def bootstrap(svc: Services, requeue: bool = True) -> None:
    """Ensures the owner entity (seeded from the settings) and the human source.

    `requeue` only at daemon startup (never with workers running)."""
    owner_cfg = svc.settings.get("owner")
    token = events.CAUSE.set("startup:config")
    try:
        _bootstrap_owner(svc, owner_cfg)
    finally:
        events.CAUSE.reset(token)
    if requeue:
        svc.queue.requeue_stuck()


def _bootstrap_owner(svc: Services, owner_cfg: dict) -> None:
    with svc.db.tx() as conn:
        correct.ensure_human_source(conn, owner_cfg["name"])
        own = K.owner(conn)
        if own is None:
            K.create_entity(conn, "config", name=owner_cfg["name"], type="person", origin="anchored",
                            aliases=owner_cfg.get("aliases", []), description="Owner of Pythia.", is_owner=True)
        else:
            wanted = sorted(set(owner_cfg.get("aliases", [])) - {own["name"]})
            # Aliases that came from other sources (catalog, Slack) stay; only writes if any from the settings is missing.
            if own["name"] != owner_cfg["name"] or not set(wanted) <= set(own["aliases"]):
                K.update_entity(conn, "config", own["id"], name=owner_cfg["name"],
                                aliases=sorted(set(own["aliases"]) | set(wanted)))


# ---------------------------------------------------------------- job execution


def run_job(svc: Services, job: dict) -> dict | None:
    kind, p, jid = job["kind"], job["payload"], job["id"]
    if kind == "scan_sources":
        return scan_sources(svc)
    if kind == "process_unit":
        return process.process_unit(svc, p["unit_id"], jid)
    if kind == "adjudicate":
        return adjudicate.adjudicate(svc, p["tension_id"], jid)
    if kind == "derive_entity":
        return derive.derive_entity(svc, p["entity_id"], jid)
    if kind == "derive_narratives":
        return derive.derive_narratives(svc, jid, review=bool(p.get("review")))
    if kind == "integrate_inferences":
        return derive.integrate_inferences(svc, p["candidates"], jid)
    if kind == "integrate_structured":
        return derive.integrate_structured(svc, p["candidates"], jid)
    if kind == "reconcile_entities":
        return reconcile.reconcile(svc, jid)
    if kind == "apply_team_identities":
        return reconcile.apply_team_identities(svc)
    if kind == "reapply_filters" and p["source_id"].startswith(confluence.KIND + ":"):
        source = svc.db.one("SELECT * FROM sources WHERE id = %s", (p["source_id"],))
        if not source:
            return {"skipped": True}
        with source_lock(svc, p["source_id"]):
            return confluence.reapply_filters(svc.db, source, svc.settings.get("confluence"))
    if kind == "reapply_filters":
        if p["source_id"].startswith(slack.KIND + ":"):
            return reapply_slack_filters(svc, p["source_id"])
        return reapply_github_filters(svc, p["source_id"])
    if kind == "remove_source":
        with source_lock(svc, p["source_id"]):
            return github.remove_source(svc.db, p["source_id"])
    if kind == "scan_confluence":
        return scan_confluence(svc, p["source_id"])
    if kind == "list_confluence_spaces":
        return list_confluence_spaces(svc)
    if kind == "list_confluence_pages":
        return list_confluence_pages(svc, p["space_key"])
    if kind == "drift_sweep":
        return sweep.run(svc, jid)
    if kind == "estimate_backfill" and p["source_id"].startswith(confluence.KIND + ":"):
        return estimate_confluence_backfill(svc, p["source_id"])
    if kind == "estimate_backfill":
        if p["source_id"].startswith(slack.KIND + ":"):
            return estimate_slack_backfill(svc, p["source_id"])
        return estimate_github_backfill(svc, p["source_id"])
    if kind == "scan_slack":
        return scan_slack(svc, p["source_id"])
    if kind == "list_slack_conversations":
        return list_slack_conversations(svc)
    if kind == "submit_batches":
        return batch.submit_pending(svc)
    if kind == "poll_batches":
        return batch.poll(svc)
    if kind == "check_backfills":
        return {"finished": batch.check_backfills(svc)}
    if kind == "backfill_synthesis":
        return batch.backfill_synthesis(svc, p["source_id"])
    if kind == "apply_batch_synthesis":
        return batch.apply_batch_synthesis(svc, p["custom_id"])
    if kind == "scan_github":
        return scan_github(svc, p["source_id"])
    if kind == "sync_catalog":
        return sync_catalog(svc)
    if kind == "apply_correction":
        return correct.apply_correction(svc, p["correction_id"], jid)
    if kind == "apply_ops":
        return correct.apply_direct(svc, p["correction_id"])
    if kind == "tombstone_stream":
        return tombstone_stream(svc, p["stream_id"], p.get("reset", False))
    if kind == "derive_sweep":
        return {"enqueued": derive.sweep(svc)}
    if kind == "reexplain_scan":
        from . import reexplain
        return reexplain.run(svc, int(p.get("since_days") or 30), jid)
    if kind == "golden_run":
        return run_golden(svc, p["run_id"], jid)
    raise ValueError(f"unknown job: {kind}")


def cause_of(job: dict) -> str:
    """The cause of the changes the job makes (Evolution). Plumbing: comes from the job's kind and payload."""
    kind, p = job["kind"], job["payload"] or {}
    if kind == "process_unit":
        return f"unit:{p['unit_id']}"
    if kind == "adjudicate":
        return f"tension:{p['tension_id']}"
    if kind in ("apply_correction", "apply_ops"):
        return f"correction:{p['correction_id']}"
    if kind == "drift_sweep":
        return f"sweep:{job['id']}"
    if kind in ("backfill_synthesis", "apply_batch_synthesis"):
        return f"synthesis:{p.get('source_id') or 'batch'}"
    if p.get("source_id"):
        return f"source:{p['source_id']}"
    if p.get("stream_id"):
        return f"stream:{p['stream_id']}"
    if p.get("entity_id"):
        return f"{kind}:{p['entity_id']}"
    return f"{kind}:{job['id']}"


def execute(svc: Services, job: dict) -> None:
    """Runs a job with halt semantics: without an LLM, it goes back to the queue without spending an attempt."""
    llm_ok_before = svc.llm.last_ok
    token = events.CAUSE.set(cause_of(job))
    try:
        try:
            run_job(svc, job)
        except batch.WaitingForBatch:
            pass  # the stage is in a batch; the result requeues the unit
        svc.queue.done(job["id"])
        # Only a successful LLM call proves cognition is back.
        if svc.llm.last_ok != llm_ok_before and svc.state.get("cognition"):
            svc.state.clear("cognition")
    except SourceUnavailable as exc:
        # Source down/without credentials: not a program or LLM error; try later.
        src = job["payload"].get("source_id") or job["kind"]
        svc.state.set(f"source_error:{src}", {"error": str(exc)[:300],
                                               "at": datetime.now(timezone.utc).isoformat(timespec="seconds")})
        svc.queue.retry_later(job["id"], 300, str(exc), count_attempt=False)
    except LLMHalted as exc:
        _pause(svc, str(exc))
        svc.queue.retry_later(job["id"], 300, str(exc), count_attempt=False)
    except LLMUnavailable as exc:
        _pause(svc, str(exc))
        svc.queue.retry_later(job["id"], 60, str(exc), count_attempt=False)
    except LLMError as exc:
        log.warning("job %s (%s) LLM error: %s", job["id"], job["kind"], exc)
        _final_failure(svc, job, svc.queue.fail(job, f"LLM: {exc}"))
    except Exception as exc:  # program error: records it and lets it retry up to the limit
        log.exception("job %s (%s) failed", job["id"], job["kind"])
        _final_failure(svc, job, svc.queue.fail(job, f"{type(exc).__name__}: {exc}"))
    finally:
        events.CAUSE.reset(token)


def _final_failure(svc: Services, job: dict, final: bool) -> None:
    if final and job["kind"] == "process_unit":
        svc.db.execute("UPDATE units SET status = 'failed' WHERE id = %s AND status = 'pending'",
                       (job["payload"]["unit_id"],))


def _pause(svc: Services, reason: str) -> None:
    current = svc.state.get("cognition") or {}
    svc.state.set("cognition", {
        "paused_since": current.get("paused_since") or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "reason": reason[:300],
    })


def drain(svc: Services, lanes=("io", "cognition", "derivation"), max_jobs: int = 1000) -> int:
    """Runs ready jobs until empty (used in tests and validation)."""
    ran = 0
    while ran < max_jobs:
        progressed = False
        for lane in lanes:
            job = svc.queue.claim(lane, "drain")
            if job:
                execute(svc, job)
                ran += 1
                progressed = True
        if not progressed:
            break
    return ran


# ---------------------------------------------------------------- sources


def _due(svc: Services, key: str, minutes: float) -> bool:
    last = svc.state.get(f"last_scan:{key}")
    return not last or time.time() - float(last["t"]) >= minutes * 60


def _mark(svc: Services, key: str) -> None:
    svc.state.set(f"last_scan:{key}", {"t": time.time()})


def scan_github(svc: Services, source_id: str) -> dict:
    cfg = svc.settings.get("github")
    source = svc.db.one("SELECT * FROM sources WHERE id = %s AND enabled", (source_id,))
    if not source:
        return {"skipped": "source turned off"}
    client = github.GitHubClient(github.resolve_token(cfg))
    github.verify_token(client, cfg)
    stats = github.GitHubConnector(svc.db, client, cfg, svc.owner_name,
                                   svc.settings.get("team")["members"]).scan(source)
    svc.state.clear(f"source_error:{source_id}")
    batch.mark_scanned(svc, source_id)
    return stats


def _gh(svc: Services):
    cfg = svc.settings.get("github")
    client = github.GitHubClient(github.resolve_token(cfg))
    github.verify_token(client, cfg)
    team = {m["github"].lower() for m in svc.settings.get("team")["members"] if m.get("github")}
    return cfg, client, github.owner_login(cfg, client), team


def reapply_github_filters(svc: Services, source_id: str) -> dict:
    source = svc.db.one("SELECT * FROM sources WHERE id = %s", (source_id,))
    if not source:
        return {"skipped": True}
    cfg, _client, me, team = _gh(svc)
    stats = github.reapply_filters(svc.db, source, cfg, me, team)
    svc.state.clear(f"last_scan:{source_id}")
    return stats


def estimate_github_backfill(svc: Services, source_id: str) -> dict:
    source = svc.db.one("SELECT * FROM sources WHERE id = %s", (source_id,))
    if not source:
        return {"skipped": True}
    cfg, client, me, team = _gh(svc)
    row = svc.db.one(
        """SELECT coalesce(sum(c.cost_usd), 0) AS usd,
                  (SELECT count(DISTINCT stream_id) FROM units WHERE source_id LIKE 'github:%%' AND status <> 'pending') AS prs
           FROM llm_calls c WHERE c.source_id LIKE 'github:%%'""")
    avg = float(row["usd"]) / row["prs"] if row["prs"] else 0.10
    read = {r["n"] for r in svc.db.query(
        """SELECT (meta->>'number')::int AS n FROM streams WHERE source_id = %s AND meta ? 'settled_updated_at'""",
        (source_id,))}
    est = github.estimate_backfill(client, source, cfg, me, team, round(avg, 4), already_read=read)
    svc.state.set(f"estimate:{source_id}", est)
    return est


from contextlib import contextmanager


@contextmanager
def source_lock(svc: Services, source_id: str):
    """One job per source at a time (read, reapply filters, remove): without it, a read that
    started before a filter changed re-reads what was just retracted, or stores evidence after
    Remove."""
    with svc.db.pool.connection() as conn:
        conn.execute("SELECT pg_advisory_lock(hashtext(%s))", (source_id,))
        try:
            yield
        finally:
            conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (source_id,))


def _slack(svc: Services):
    client = slack.SlackClient(slack.resolve_token())
    me = slack.verify_token(client)
    return client, me, slack.users_cached(svc.db, client)


def scan_slack(svc: Services, source_id: str) -> dict:
    client, me, users = _slack(svc)
    with source_lock(svc, source_id):
        # Read with the lock held: the source and the owner's lists as they are now.
        source = svc.db.one("SELECT * FROM sources WHERE id = %s AND enabled", (source_id,))
        if not source:
            return {"skipped": "source turned off or removed"}
        missing = slack.missing_scopes(client, source["config"].get("kind") or "channel")
        if missing:
            raise SourceUnavailable(f"Slack token without the scopes: {', '.join(sorted(missing))}")
        stats = slack.SlackConnector(svc.db, client, svc.settings.get("slack"), svc.owner_name, users, me).scan(source)
    svc.state.clear(f"source_error:{source_id}")
    batch.mark_scanned(svc, source_id)
    return stats


def list_slack_conversations(svc: Services) -> dict:
    client, me, users = _slack(svc)
    convs = slack.list_conversations(client, users, me.get("user_id"))
    svc.state.set("slack_conversations", {"items": convs, "team": me.get("team"),
                                          "at": datetime.now(timezone.utc).isoformat(timespec="seconds")})
    svc.state.clear("source_error:slack")
    return {"conversations": len(convs)}


def reapply_slack_filters(svc: Services, source_id: str) -> dict:
    source = svc.db.one("SELECT * FROM sources WHERE id = %s", (source_id,))
    if not source:
        return {"skipped": True}
    _client, _me, users = _slack(svc)
    with source_lock(svc, source_id):
        source = svc.db.one("SELECT * FROM sources WHERE id = %s", (source_id,))
        if not source:
            return {"skipped": True}
        stats = slack.reapply_filters(svc.db, source, svc.settings.get("slack"), users)
    svc.state.clear(f"last_scan:{source_id}")
    return stats


def usd_per_char(svc: Services) -> float:
    """Measured average cost per character read. Uses Slack itself once there is a measurement; before that,
    the conversations of the other sources (without GitHub diffs, which lower the per-character average).
    Counts the text that went through the LLM, including what was later erased."""
    for where in ("source_id LIKE 'slack:%%'", "source_id NOT LIKE 'slack:%%'"):
        row = svc.db.one(
            f"""SELECT (SELECT coalesce(sum(cost_usd), 0) FROM llm_calls WHERE {where}
                        AND source_id IN (SELECT DISTINCT source_id FROM units WHERE status <> 'pending')) AS usd,
                       (SELECT coalesce(sum(coalesce(length(content), 0)), 0) FROM evidence
                        WHERE unit_id IS NOT NULL AND {where} AND role <> 'artifact') AS chars""")
        if row["chars"] and row["usd"]:
            return float(row["usd"]) / row["chars"]
    return 0.00002


def estimate_slack_backfill(svc: Services, source_id: str) -> dict:
    source = svc.db.one("SELECT * FROM sources WHERE id = %s", (source_id,))
    if not source:
        return {"skipped": True}
    client, _me, _users = _slack(svc)
    known = {r["external_id"] for r in svc.db.query(
        "SELECT external_id FROM evidence WHERE source_id = %s AND external_id IS NOT NULL", (source_id,))}
    est = slack.estimate_backfill(client, source, svc.settings.get("slack"), usd_per_char(svc), known=known)
    svc.state.set(f"estimate:{source_id}", est)
    return est


def _confluence(svc: Services):
    cfg = svc.settings.get("confluence")
    client = confluence.client_from(cfg)
    if not cfg.get("cloud_id") and cfg.get("token_type", "scoped") == "scoped":
        # Stores the cloudId discovered from the site (avoids the lookup on every read).
        cfg["cloud_id"] = client.http.base_url.path.split("/")[3]
        svc.settings.set("confluence", cfg)
    return cfg, client


def scan_confluence(svc: Services, source_id: str) -> dict:
    cfg, client = _confluence(svc)
    with source_lock(svc, source_id):
        source = svc.db.one("SELECT * FROM sources WHERE id = %s AND enabled", (source_id,))
        if not source:
            return {"skipped": "source turned off or removed"}
        with svc.db.tx() as conn:
            in_backfill = batch.source_in_backfill(conn, source_id)
        token = K.DEFER.set(source_id) if in_backfill else None
        try:
            owner = svc.settings.get("owner")
            stats = confluence.ConfluenceConnector(svc.db, client, cfg, svc.owner_name,
                                                   owner.get("aliases")).scan(source)
        finally:
            if token is not None:
                K.DEFER.reset(token)
    svc.state.clear(f"source_error:{source_id}")
    batch.mark_scanned(svc, source_id)
    return stats


def list_confluence_spaces(svc: Services) -> dict:
    _cfg, client = _confluence(svc)
    spaces = confluence.list_spaces(client)
    svc.state.set("confluence_spaces", {"items": spaces, "at": datetime.now(timezone.utc).isoformat(timespec="seconds")})
    svc.state.clear("source_error:confluence")
    return {"spaces": len(spaces)}


def list_confluence_pages(svc: Services, space_key: str) -> dict:
    _cfg, client = _confluence(svc)
    spaces = {s["key"]: s for s in (svc.state.get("confluence_spaces") or {}).get("items") or []}
    space = spaces.get(space_key)
    if not space:
        return {"skipped": "unknown space"}
    pages = confluence.space_pages(client, space["id"])
    outline = confluence.page_outline(confluence.space_nodes(client, space["id"], pages))
    svc.state.set(f"confluence_outline:{space_key}", {"items": outline,
                                                      "at": datetime.now(timezone.utc).isoformat(timespec="seconds")})
    return {"pages": len(outline)}


def estimate_confluence_backfill(svc: Services, source_id: str) -> dict:
    source = svc.db.one("SELECT * FROM sources WHERE id = %s", (source_id,))
    if not source:
        return {"skipped": True}
    _cfg, client = _confluence(svc)
    read = {r["page_id"] for r in svc.db.query(
        """SELECT meta->>'page_id' AS page_id FROM streams WHERE source_id LIKE 'confluence:%%'
           AND meta->>'confluence' = 'page' AND meta ? 'version_seen'""")}
    est = confluence.estimate_backfill(client, source, usd_per_char(svc), read)
    svc.state.set(f"estimate:{source_id}", est)
    return est


def resync_catalog(svc: Services) -> None:
    """Full re-read of the catalog (e.g. the ignored-attributes list changed). Counts as backfill:
    page regeneration is left for a single batch pass at the end."""
    svc.db.execute("UPDATE sources SET backfill_until = now(), backfill_done_at = NULL WHERE id = %s",
                   (incident_io.SOURCE_ID,))
    svc.state.clear(f"last_scan:{incident_io.SOURCE_ID}")


def sync_catalog(svc: Services) -> dict:
    cfg = svc.settings.get("incident_io")
    with svc.db.tx() as conn:
        in_backfill = batch.source_in_backfill(conn, incident_io.SOURCE_ID)
    token = K.DEFER.set(incident_io.SOURCE_ID) if in_backfill else None
    try:
        stats = incident_io.CatalogConnector(svc.db, incident_io.client_from(cfg), cfg).sync()
    finally:
        if token is not None:
            K.DEFER.reset(token)
    svc.state.clear(f"source_error:{incident_io.SOURCE_ID}")
    batch.mark_scanned(svc, incident_io.SOURCE_ID)
    svc.state.set("catalog_last_sync", {"at": datetime.now(timezone.utc).isoformat(timespec="seconds"), **stats})
    # New people in the catalog may be those on the team list: the declared GitHub login goes to them.
    svc.queue.enqueue("apply_team_identities", "cognition", {}, priority=40, dedup_key="team_identities")
    return stats


def _sync_profile_source(svc: Services, cfg: dict) -> None:
    """"Owner profile" source: all chats that are not fully enabled, except the excluded ones. On creation, what
    already exists comes in as backfill (Batch API and deferred synthesis); what arrives later is processed right away."""
    chats = cc.discover_chats(cc.projects_root(cfg), cc.app_sessions_root(cfg))
    enabled = {r["chat_id"] for r in svc.db.query(
        """SELECT config->>'chat_id' AS chat_id FROM sources
           WHERE kind = %s AND id <> %s AND config ? 'chat_id'""", (CLAUDE_CODE, cc.PROFILE_SOURCE))}
    files = cc.profile_files(chats, enabled, cfg.get("profile_excluded_chats") or [])
    with svc.db.tx() as conn:
        row = conn.execute("SELECT config FROM sources WHERE id = %s", (cc.PROFILE_SOURCE,)).fetchone()
        if row is None:
            conn.execute("""INSERT INTO sources (id, kind, name, config, enabled, backfill_until)
                            VALUES (%s, %s, 'Chats: owner profile (all chats, only his messages)', %s, true, now())""",
                         (cc.PROFILE_SOURCE, CLAUDE_CODE, Jsonb({"profile_only": True, "files": files})))
        elif sorted(row["config"].get("files") or []) != files:
            conn.execute("UPDATE sources SET config = config || %s WHERE id = %s",
                         (Jsonb({"profile_only": True, "files": files}), cc.PROFILE_SOURCE))


def scan_sources(svc: Services) -> dict:
    # Sources with their own interval: only enqueue when due (the work runs in separate jobs).
    gh_cfg = svc.settings.get("github")
    for src in svc.db.query("SELECT id FROM sources WHERE enabled AND kind = 'github'"):
        if _due(svc, src["id"], float(gh_cfg["poll_minutes"])):
            svc.queue.enqueue("scan_github", "io", {"source_id": src["id"]}, priority=8,
                              dedup_key=f"scan:{src['id']}", max_attempts=3)
            _mark(svc, src["id"])
    sl_cfg = svc.settings.get("slack")
    for src in svc.db.query("SELECT id FROM sources WHERE enabled AND kind = 'slack'"):
        if _due(svc, src["id"], float(sl_cfg["poll_minutes"])):
            svc.queue.enqueue("scan_slack", "io", {"source_id": src["id"]}, priority=8,
                              dedup_key=f"scan:{src['id']}", max_attempts=3)
            _mark(svc, src["id"])
    cf_cfg = svc.settings.get("confluence")
    for src in svc.db.query("SELECT id FROM sources WHERE enabled AND kind = 'confluence'"):
        if _due(svc, src["id"], float(cf_cfg["poll_minutes"])):
            svc.queue.enqueue("scan_confluence", "io", {"source_id": src["id"]}, priority=8,
                              dedup_key=f"scan:{src['id']}", max_attempts=3)
            _mark(svc, src["id"])
    # Drift sweep: only with the cognition queue empty and budget left for the day.
    if sweep.budget_left(svc) > 0 and sweep.cognition_idle(svc):
        svc.queue.enqueue("drift_sweep", "cognition", {}, priority=5000, dedup_key="drift_sweep", max_attempts=1)
    inc = svc.db.one("SELECT enabled FROM sources WHERE id = %s", (incident_io.SOURCE_ID,))
    if inc and inc["enabled"] and _due(svc, incident_io.SOURCE_ID, float(svc.settings.get("incident_io")["poll_minutes"])):
        svc.queue.enqueue("sync_catalog", "io", {"source_id": incident_io.SOURCE_ID}, priority=6,
                          dedup_key="sync_catalog", max_attempts=3)
        _mark(svc, incident_io.SOURCE_ID)
    cfg = svc.settings.get("claude_code")
    connector = ClaudeCodeConnector(svc.db, svc.owner_name)
    totals = {"streams": 0, "units": 0, "deleted": 0}
    if cfg.get("profile_all_chats"):
        _sync_profile_source(svc, cfg)
    sources = svc.db.query("SELECT * FROM sources WHERE enabled AND kind = %s", (CLAUDE_CODE,))
    paths = cc.session_paths(cc.projects_root(cfg)) if sources else {}
    chats = {c["id"]: c for c in cc.discover_chats(cc.projects_root(cfg), cc.app_sessions_root(cfg))} if sources else {}
    for source in sources:
        chat = chats.get(source["config"].get("chat_id"))
        if chat:  # the chat may have gained files (resume) or another title since it was enabled
            files = sorted(set(source["config"].get("files") or []) | set(chat["files"]))
            if files != source["config"].get("files") or chat["title"] != source["name"]:
                svc.db.execute("UPDATE sources SET name = %s, config = config || %s WHERE id = %s",
                               (chat["title"], Jsonb({"files": files}), source["id"]))
                source["config"]["files"] = files
        stats = connector.scan(source, debounce_s=float(cfg["debounce_minutes"]) * 60,
                               include_sidechains=bool(cfg.get("include_sidechains")), paths=paths)
        batch.mark_scanned(svc, source["id"])
        for k in totals:
            totals[k] += stats[k]
    return totals


def _ids_in_other_streams(conn, stream_id: str) -> dict[str, str]:
    """external_id → another active stream of the same source that still contains the message."""
    from .connectors.claude_code import read_delta
    # In any chat: a chat resumed in the terminal may repeat messages from another.
    others = conn.execute(
        """SELECT o.id, o.locator FROM streams o
           WHERE o.source_id LIKE 'claude_code:%%' AND o.id <> %s AND o.status = 'active'""", (stream_id,)).fetchall()
    found: dict[str, str] = {}
    for o in others:
        path = Path(o["locator"])
        if path.exists():
            for m in read_delta(path, 0, include_sidechains=True).messages:
                found.setdefault(m.external_id, o["id"])
    return found


def tombstone_stream(svc: Services, stream_id: str, reset: bool) -> dict:
    with svc.db.tx() as conn:
        rows = conn.execute("SELECT id, external_id FROM evidence WHERE stream_id = %s AND status = 'active'",
                            (stream_id,)).fetchall()
        elsewhere = _ids_in_other_streams(conn, stream_id) if rows else {}
        ids = []
        for r in rows:
            if r["external_id"] in elsewhere:
                # The message still exists in another session (resume): it was not deleted at the source.
                conn.execute("UPDATE evidence SET stream_id = %s WHERE id = %s", (elsewhere[r["external_id"]], r["id"]))
            else:
                ids.append(r["id"])
        stats = K.tombstone_evidence(conn, "connector", ids, "removed at the source")
        if reset:
            conn.execute("UPDATE streams SET cursor = '{}', status = 'active' WHERE id = %s", (stream_id,))
            # The old evidence stays recorded (without content) with the same external id; free it for re-ingestion.
            conn.execute("UPDATE evidence SET external_id = NULL WHERE stream_id = %s AND status = 'tombstoned'",
                         (stream_id,))
    return stats


# ---------------------------------------------------------------- process


class Daemon:
    def __init__(self, svc: Services, scan_every_s: int = 30, sweep_every_s: int = 600, batch_every_s: int = 60):
        self.svc = svc
        self.batch_every_s = batch_every_s
        self.scan_every_s = scan_every_s
        self.sweep_every_s = sweep_every_s
        self.stop = threading.Event()
        self.threads: list[threading.Thread] = []

    def start(self) -> None:
        bootstrap(self.svc)
        from .quality import check_prompt_version
        check_prompt_version(self.svc)
        workers = [("io", 2), ("cognition", 1), ("derivation", 2)]
        for lane, n in workers:
            for i in range(n):
                t = threading.Thread(target=self._worker, args=(lane, f"{lane}-{i}"), daemon=True, name=f"w-{lane}-{i}")
                t.start()
                self.threads.append(t)
        t = threading.Thread(target=self._scheduler, daemon=True, name="scheduler")
        t.start()
        self.threads.append(t)

    def shutdown(self) -> None:
        self.stop.set()
        for t in self.threads:
            t.join(timeout=5)

    def _worker(self, lane: str, name: str) -> None:
        while not self.stop.is_set():
            try:
                job = self.svc.queue.claim(lane, name)
            except Exception:
                log.exception("failed to fetch job")
                self.stop.wait(5)
                continue
            if job is None:
                self.stop.wait(1.0)
                continue
            try:
                execute(self.svc, job)
            except Exception:  # the worker never dies: the cognition lane has only one
                log.exception("failed to record the result of job %s", job["id"])
                self.stop.wait(5)

    def _scheduler(self) -> None:
        last_scan = last_sweep = last_batch = 0.0
        while not self.stop.is_set():
            now = time.time()
            try:
                if now - last_scan >= self.scan_every_s:
                    self.svc.queue.enqueue("scan_sources", "io", {}, priority=5, dedup_key="scan_sources", max_attempts=1)
                    last_scan = now
                if now - last_batch >= self.batch_every_s:
                    q = self.svc.queue
                    q.enqueue("submit_batches", "io", {}, priority=7, dedup_key="submit_batches", max_attempts=1)
                    q.enqueue("poll_batches", "io", {}, priority=7, dedup_key="poll_batches", max_attempts=1)
                    q.enqueue("check_backfills", "io", {}, priority=9, dedup_key="check_backfills", max_attempts=1)
                    last_batch = now
                if now - last_sweep >= self.sweep_every_s:
                    self.svc.queue.enqueue("derive_sweep", "derivation", {}, priority=95, dedup_key="derive_sweep")
                    # Plumbing: jobs completed more than 2 days ago are of no use.
                    self.svc.db.execute("DELETE FROM jobs WHERE status = 'done' AND finished_at < now() - interval '2 days'")
                    last_sweep = now
            except Exception:
                log.exception("scheduler")
            self.stop.wait(2.0)
