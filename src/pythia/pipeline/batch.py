"""Backfill with the Batch API and deferred synthesis.

A unit is **backfill** when it happened before the source was added and the
source's backfill has not finished yet (structural rule, no judgment). For it:

- triage and extraction become batch requests (50% discount); `process_unit`
  raises `WaitingForBatch` and is resumed when the result arrives — the stages are already
  stored on the unit, so nothing is redone;
- resolution, integration and adjudication stay online and in series (each PR needs to
  see what the previous ones created);
- regeneration of the touched entities' pages is deferred (`deferred_derivations`)
  and happens in a single pass, also as a batch, when the source's backfill ends.

Day-to-day work (units after the source was added) goes through none of this.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from psycopg.types.json import Jsonb

from .. import knowledge as K
from ..ids import new_id
from ..jobs import enqueue
from ..llm.client import CallContext, LLMError, parse_message, usage_of
from ..services import Services

UNIT_KINDS = ("triage", "extraction")
MAX_ATTEMPTS = 3


class WaitingForBatch(Exception):
    """The stage went to (or is already in) a batch. The job ends; the result resumes the unit."""


# ---------------------------------------------------------------- backfill rule


def source_in_backfill(conn, source_id: str | None) -> bool:
    if not source_id:
        return False
    row = conn.execute("SELECT backfill_until, backfill_done_at FROM sources WHERE id = %s", (source_id,)).fetchone()
    return bool(row and row["backfill_until"] and row["backfill_done_at"] is None)


def unit_is_backfill(conn, unit: dict) -> bool:
    row = conn.execute("SELECT backfill_until, backfill_done_at FROM sources WHERE id = %s",
                       (unit["source_id"],)).fetchone()
    return bool(row and row["backfill_until"] and row["backfill_done_at"] is None
                and unit["occurred_at"] < row["backfill_until"])


def mark_scanned(svc: Services, source_id: str) -> None:
    """A full scan finished (precondition for declaring the backfill complete)."""
    svc.state.set(f"scanned:{source_id}", {"t": time.time()})


# ---------------------------------------------------------------- batch stage


def staged_call(svc: Services, *, kind: str, role: str, unit: dict, system: str, user: str, schema: dict,
                max_tokens: int, ctx: CallContext) -> dict:
    """Stage of a backfill unit: returns the batch result, or creates the request and waits."""
    with svc.db.tx() as conn:
        req = conn.execute(
            """SELECT * FROM batch_requests WHERE unit_id = %s AND kind = %s
               ORDER BY created_at DESC LIMIT 1""", (unit["id"], kind)).fetchone()
        if req and req["status"] == "canceled":
            req = None  # canceled by a deletion at the source: starts over as a new request
        if req and req["status"] == "done":
            return req["result"]
        if req and req["status"] in ("pending", "submitted"):
            raise WaitingForBatch(f"{kind} of unit {unit['id']} waiting for a batch")
        attempts = (req["attempts"] + 1) if req else 1
        if req and req["status"] == "failed" and attempts > MAX_ATTEMPTS:
            raise LLMError(f"{kind} failed in the batch {MAX_ATTEMPTS} times: {req['error']}")
        params = svc.llm.structured_params(role, system, user, schema, max_tokens=max_tokens)
        conn.execute(
            """INSERT INTO batch_requests (custom_id, kind, role, unit_id, source_id, params, attempts)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (new_id("br"), kind, role, unit["id"], unit["source_id"], Jsonb(params), attempts))
    raise WaitingForBatch(f"{kind} of unit {unit['id']} sent to a batch")


def reject_result(svc: Services, unit_id: int, kind: str, error: str) -> None:
    """The batch result failed validation: the next attempt asks again."""
    svc.db.execute(
        """UPDATE batch_requests SET status = 'failed', error = %s
           WHERE custom_id = (SELECT custom_id FROM batch_requests WHERE unit_id = %s AND kind = %s
                              ORDER BY created_at DESC LIMIT 1)""", (error[:500], unit_id, kind))


# ---------------------------------------------------------------- submission and collection


def submit_pending(svc: Services, min_age_s: float = 30, min_batch: int = 100, max_batch: int = 5000) -> dict:
    """Gathers pending requests into a batch. Waits to build up volume, but never holds a request longer than `min_age_s`."""
    with svc.db.tx() as conn:
        rows = conn.execute(
            """SELECT custom_id, params, extract(epoch FROM now() - created_at) AS age_s
               FROM batch_requests WHERE status = 'pending'
               ORDER BY created_at LIMIT %s FOR UPDATE SKIP LOCKED""", (max_batch,)).fetchall()
        if not rows:
            return {"submitted": 0}
        oldest = float(rows[0]["age_s"])  # measured by the database clock, so host/DB clock skew does not matter
        if len(rows) < min_batch and oldest < min_age_s:
            return {"submitted": 0, "waiting": len(rows)}
        batch_id = svc.llm.submit_batch([(r["custom_id"], r["params"]) for r in rows])
        ids = [r["custom_id"] for r in rows]
        conn.execute("UPDATE batch_requests SET status = 'submitted', batch_id = %s WHERE custom_id = ANY(%s)",
                     (batch_id, ids))
        conn.execute("INSERT INTO llm_batches (id, status, request_count) VALUES (%s, 'in_progress', %s)",
                     (batch_id, len(ids)))
    return {"submitted": len(ids), "batch_id": batch_id}


def poll(svc: Services) -> dict:
    """Fetches finished batches, records cost (with discount), stores results and resumes whoever was waiting."""
    done = 0
    for b in svc.db.query("SELECT id FROM llm_batches WHERE status = 'in_progress' ORDER BY submitted_at"):
        info = svc.llm.batch_info(b["id"])
        if info.get("counts"):
            svc.db.execute("UPDATE llm_batches SET progress = %s WHERE id = %s",
                           (Jsonb({**info["counts"], "at": datetime.now(timezone.utc).isoformat()}), b["id"]))
        if info["status"] != "ended":
            continue
        for custom_id, message, error in svc.llm.batch_results(b["id"]):
            _store_result(svc, custom_id, message, error)
            done += 1
        # Batch requests without a result (should not happen) do not stay stuck.
        svc.db.execute(
            """UPDATE batch_requests SET status = 'failed', error = 'no result in the batch', params = NULL
               WHERE batch_id = %s AND status = 'submitted'""", (b["id"],))
        # The real end (at Anthropic), not when Pythia saw it: with the machine off, the batch finishes
        # on its own, and the read time would inflate the history the predictions use.
        svc.db.execute("UPDATE llm_batches SET status = 'ended', ended_at = coalesce(%s, now()) WHERE id = %s",
                       (info.get("ended_at"), b["id"]))
    return {"results": done}


def _store_result(svc: Services, custom_id: str, message, error: str | None) -> None:
    with svc.db.tx() as conn:
        req = conn.execute("SELECT * FROM batch_requests WHERE custom_id = %s", (custom_id,)).fetchone()
        if not req or req["status"] != "submitted":
            return  # canceled midway (e.g. the source erased the evidence)
        result, status = None, "failed"
        if message is not None:
            model = req["params"]["model"] if req["params"] else message.model
            svc.llm.record(req["role"], model, usage_of(message), CallContext(source_id=req["source_id"]), 0, True,
                           batch=True)
            try:
                result, status, error = parse_message(message), "done", None
            except LLMError as exc:
                error = str(exc)
        # The request carries source content: it leaves the database as soon as the result arrives.
        conn.execute(
            """UPDATE batch_requests SET status = %s, result = %s, error = %s, params = NULL, done_at = now()
               WHERE custom_id = %s""", (status, Jsonb(result) if result is not None else None, error, custom_id))
        if req["kind"] in UNIT_KINDS:
            enqueue(conn, "process_unit", "cognition", {"unit_id": req["unit_id"]}, priority=900,
                    dedup_key=f"unit:{req['unit_id']}")
        elif req["kind"] == "synthesis":
            enqueue(conn, "apply_batch_synthesis", "derivation", {"custom_id": custom_id}, priority=70)


def cancel_for_units(conn, unit_ids: list[int]) -> None:
    """Evidence erased at the source: in-flight requests of these units are discarded and the stages
    start over (the result cannot bring back content that no longer exists)."""
    if not unit_ids:
        return
    conn.execute(
        """UPDATE batch_requests SET status = 'canceled', params = NULL, result = NULL
           WHERE unit_id = ANY(%s)""", (unit_ids,))
    # The stage cache holds what extraction took from the text: it goes away even on an already processed unit.
    conn.execute("UPDATE units SET meta = meta - 'pipeline' WHERE id = ANY(%s)", (unit_ids,))


# ---------------------------------------------------------------- end of backfill and deferred synthesis


def check_backfills(svc: Services) -> list[str]:
    """Declares complete the backfill of sources with no pending work and triggers the deferred synthesis."""
    finished = []
    for src in svc.db.query("SELECT * FROM sources WHERE backfill_until IS NOT NULL AND backfill_done_at IS NULL"):
        scanned = svc.state.get(f"scanned:{src['id']}")
        if not scanned or scanned["t"] < src["backfill_until"].timestamp():
            continue
        busy = svc.db.one(
            """SELECT
                 (SELECT count(*) FROM units WHERE source_id = %(s)s AND status = 'pending'
                    AND occurred_at < %(u)s) AS units,
                 (SELECT count(*) FROM batch_requests WHERE source_id = %(s)s AND kind = ANY(%(k)s)
                    AND status IN ('pending', 'submitted')) AS requests,
                 (SELECT count(*) FROM jobs WHERE status IN ('queued', 'running') AND kind = 'integrate_structured'
                    AND payload->'candidates'->0->>'source_id' = %(s)s) AS structured,
                 (SELECT count(*) FROM jobs WHERE status IN ('queued', 'running')
                    AND kind IN ('scan_github', 'scan_slack', 'scan_confluence', 'sync_catalog') AND payload->>'source_id' = %(s)s) AS scans""",
            {"s": src["id"], "u": src["backfill_until"], "k": list(UNIT_KINDS)})
        if any(busy.values()):
            continue
        with svc.db.tx() as conn:
            conn.execute("UPDATE sources SET backfill_done_at = now() WHERE id = %s", (src["id"],))
            enqueue(conn, "backfill_synthesis", "derivation", {"source_id": src["id"]}, priority=60,
                    dedup_key=f"backfill_synthesis:{src['id']}")
        finished.append(src["id"])
    return finished


def backfill_synthesis(svc: Services, source_id: str) -> dict:
    """One synthesis pass per entity touched by the backfill, as a batch."""
    from . import derive

    with svc.db.tx() as conn:
        entity_ids = [r["entity_id"] for r in conn.execute(
            "SELECT entity_id FROM deferred_derivations WHERE source_id = %s", (source_id,)).fetchall()]
    created = 0
    for eid in entity_ids:
        with svc.db.tx() as conn:
            prepared = derive.entity_synthesis_input(conn, svc, eid)
            if prepared is not None:
                params = svc.llm.structured_params("synthesis", prepared["system"], prepared["user"],
                                                   prepared["schema"])
                conn.execute(
                    """INSERT INTO batch_requests (custom_id, kind, role, entity_id, source_id, params, meta)
                       VALUES (%s, 'synthesis', 'synthesis', %s, %s, %s, %s)""",
                    (new_id("br"), eid, source_id, Jsonb(params), Jsonb({"watermark": prepared["watermark"]})))
                created += 1
            conn.execute("DELETE FROM deferred_derivations WHERE source_id = %s AND entity_id = %s", (source_id, eid))
    if not created:
        svc.queue.enqueue("derive_narratives", "derivation", {}, priority=80, dedup_key="narratives")
    return {"entities": len(entity_ids), "requests": created}


def apply_batch_synthesis(svc: Services, custom_id: str) -> dict:
    from . import derive

    req = svc.db.one("SELECT * FROM batch_requests WHERE custom_id = %s", (custom_id,))
    if not req:
        return {"skipped": True}
    if req["status"] == "done":
        derive.apply_entity_synthesis(svc, req["entity_id"], req["result"], req["meta"]["watermark"],
                                      narratives_delay_s=None)  # narratives: once, at the end
    else:
        # The batch synthesis failed: the same synthesis is done online (it is the same LLM work, not a shortcut).
        svc.queue.enqueue("derive_entity", "derivation", {"entity_id": req["entity_id"]}, priority=60,
                          dedup_key=f"derive:{req['entity_id']}")
    left = svc.db.one(
        """SELECT count(*) AS n FROM batch_requests WHERE source_id = %s AND kind = 'synthesis'
           AND status IN ('pending', 'submitted')""", (req["source_id"],))["n"]
    if left == 0:
        svc.queue.enqueue("derive_narratives", "derivation", {}, priority=80, dedup_key="narratives")
    return {"entity_id": req["entity_id"], "status": req["status"]}
