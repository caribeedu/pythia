""""Processing now": what each worker is doing, in which stage, for how long, and the forecast.

Pure plumbing: reads the queue, the units and the call ledger. Forecasts are averages of the measured
durations (per LLM role or per job kind), not judgment.
"""

from __future__ import annotations

from datetime import datetime, timezone

from ..services import Services

WORKERS = {"io": 2, "cognition": 1, "derivation": 2}
LABELS = {
    "process_unit": "process unit", "derive_entity": "entity page", "derive_narratives": "narratives",
    "adjudicate": "adjudicate tension", "integrate_inferences": "integrate inferences",
    "integrate_structured": "integrate catalog claims", "reconcile_entities": "reconcile identities",
    "scan_sources": "scan sources", "scan_github": "read GitHub", "scan_slack": "read Slack", "sync_catalog": "read catalog",
    "submit_batches": "submit batches", "poll_batches": "check batches", "check_backfills": "check backfills",
    "backfill_synthesis": "assemble batch synthesis", "apply_batch_synthesis": "apply batch synthesis",
    "derive_sweep": "derived-items sweep", "golden_run": "golden questions", "reexplain_scan": "measure re-explanation", "estimate_backfill": "estimate backfill",
    "reapply_filters": "reapply filters", "remove_source": "remove source", "apply_correction": "apply correction",
    "tombstone_stream": "erase what vanished at the source", "list_slack_conversations": "list Slack conversations",
}
STAGES = {"triage": "triage", "extraction": "extraction", "resolution": "resolution", "integration": "integration"}
ROLE_OF_JOB = {"derive_entity": "synthesis", "derive_narratives": "synthesis", "adjudicate": "adjudication",
               "integrate_structured": "integration", "integrate_inferences": "integration",
               "reconcile_entities": "reconciliation"}


def _since(ts) -> float:
    if ts is None:
        return 0.0
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    return max(0.0, (datetime.now(timezone.utc) - ts).total_seconds())


def snapshot(svc: Services) -> dict:
    db = svc.db
    role_avg = {r["role"]: float(r["s"]) for r in db.query(
        """SELECT role, avg(duration_ms) / 1000.0 AS s FROM llm_calls
           WHERE NOT batch AND ok AND ts > now() - interval '7 days' GROUP BY role""")}
    job_avg = {r["kind"]: float(r["s"]) for r in db.query(
        """SELECT kind, avg(extract(epoch FROM finished_at - started_at)) AS s FROM jobs
           WHERE status = 'done' AND started_at IS NOT NULL AND finished_at IS NOT NULL GROUP BY kind""")}

    running = []
    for j in db.query("SELECT * FROM jobs WHERE status = 'running' ORDER BY started_at"):
        p = j["payload"] or {}
        what, stage, since, eta = LABELS.get(j["kind"], j["kind"]), None, j["started_at"], job_avg.get(j["kind"])
        if j["kind"] == "process_unit" and p.get("unit_id"):
            u = db.one("""SELECT u.title, u.meta, s.name AS source FROM units u JOIN sources s ON s.id = u.source_id
                          WHERE u.id = %s""", (p["unit_id"],))
            if u:
                what = f"{u['title'] or 'unit'} · {u['source']}"
                now = (u["meta"] or {}).get("now") or {}
                if now.get("stage"):
                    stage, since = STAGES.get(now["stage"], now["stage"]), now.get("since") or since
                    eta = role_avg.get(now["stage"])
        elif j["kind"] == "derive_entity" and p.get("entity_id"):
            e = db.one("SELECT name FROM entities WHERE id = %s", (p["entity_id"],))
            what, stage, eta = f"page of {e['name'] if e else p['entity_id']}", "synthesis", role_avg.get("synthesis")
        elif j["kind"] in ROLE_OF_JOB:
            stage = {"synthesis": "synthesis", "adjudication": "adjudication", "integration": "integration",
                     "reconciliation": "reconciliation"}[ROLE_OF_JOB[j["kind"]]]
        elapsed = _since(since)
        running.append({"lane": j["lane"], "worker": j["worker"], "what": what, "stage": stage or "—",
                        "elapsed": elapsed, "remaining": max(0.0, eta - elapsed) if eta else None})

    lanes = []
    for lane, workers in WORKERS.items():
        rows = db.query("""SELECT kind, count(*) AS n FROM jobs WHERE status = 'queued' AND lane = %s
                           AND run_after <= now() GROUP BY kind""", (lane,))
        later = db.one("SELECT count(*) AS n FROM jobs WHERE status = 'queued' AND lane = %s AND run_after > now()", (lane,))["n"]
        ready = sum(r["n"] for r in rows)
        known = [r for r in rows if r["kind"] in job_avg]
        eta = sum(r["n"] * job_avg[r["kind"]] for r in known) / workers if known else None
        lanes.append({"lane": lane, "workers": workers, "ready": ready, "later": later, "eta": eta,
                      "kinds": sorted(({"kind": LABELS.get(r["kind"], r["kind"]), "n": r["n"]} for r in rows),
                                      key=lambda r: -r["n"])[:4]})

    history = sorted(float(r["s"]) for r in db.query(
        """SELECT extract(epoch FROM ended_at - submitted_at) AS s FROM llm_batches WHERE ended_at IS NOT NULL"""))
    batches = []
    for b in db.query("SELECT * FROM llm_batches WHERE ended_at IS NULL ORDER BY submitted_at"):
        elapsed = _since(b["submitted_at"])
        batches.append({"id": b["id"], "requests": b["request_count"], "elapsed": elapsed,
                        **batch_eta(elapsed, b["request_count"], b.get("progress"), history)})
    staged = db.one("SELECT count(*) AS n FROM batch_requests WHERE status = 'pending'")["n"]

    waiting = db.query("""SELECT kind, count(*) AS n, max(last_error) AS err, min(run_after) AS next FROM jobs
                          WHERE status = 'queued' AND last_error IS NOT NULL GROUP BY kind ORDER BY 2 DESC""")
    for w in waiting:
        w["kind"] = LABELS.get(w["kind"], w["kind"])
        w["in"] = max(0.0, (w["next"] - datetime.now(timezone.utc)).total_seconds()) if w["next"] else None
    # tensions under judgment live here (the Verdicts tab is gone).
    tensions = db.query("""SELECT t.id, t.reason, t.opened_by, t.created_at,
                                  EXISTS (SELECT 1 FROM verdicts v WHERE v.tension_id = t.id AND v.status = 'reverted') AS reopened
                           FROM tensions t WHERE t.status = 'open' ORDER BY t.created_at LIMIT 20""")
    return {"running": running, "lanes": lanes, "batches": batches, "staged": staged, "waiting": waiting,
            "cognition": svc.state.get("cognition"), "tensions": tensions}


def batch_eta(elapsed: float, total: int, progress: dict | None, history: list[float]) -> dict:
    """Forecast of when the batch returns, redone on every read:
    1. from the real pace, when Anthropic already reports completed requests;
    2. otherwise, from the history: the shortest turnaround seen that is still greater than the elapsed time
       (the expired average is no use: the batch has already passed it);
    3. past all those seen: says so, with the limit guaranteed by Anthropic (24 h)."""
    done = sum((progress or {}).get(k, 0) for k in ("succeeded", "errored", "canceled", "expired"))
    if progress and 0 < done < total and elapsed > 0:
        return {"remaining": (total - done) * elapsed / done, "done": done, "basis": "current pace"}
    later = [h for h in history if h > elapsed]
    if later:
        return {"remaining": later[0] - elapsed, "done": done or None, "basis": "batch history"}
    if history:
        return {"remaining": None, "done": done or None,
                "basis": f"already past the slowest seen ({human(history[-1])}); Anthropic guarantees up to 24 h"}
    return {"remaining": None, "done": done or None, "basis": "no history; Anthropic guarantees up to 24 h"}


def human(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    s = int(seconds)
    if s < 60:
        return f"{s} s"
    if s < 3600:
        return f"{s // 60} min {s % 60:02d} s"
    return f"{s // 3600} h {(s % 3600) // 60:02d} min"
