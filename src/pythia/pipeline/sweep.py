"""Periodic drift sweep: covers drift with no trigger.

- **What it examines:** one entity per round, in rotation. The priority is plumbing (arithmetic over the
  database, without judging meaning): old documentation about something that had newer activity in another
  source, what LLMs consult most (Push and MCP) and what has gone longest without review.
- **How much it spends:** configurable daily budget; only runs with the cognition queue empty; what is
  left over does not accumulate. Everything the sweep generates (including the judgment) is recorded under the source
  `sweep`.
- **What it does:** the agent detects and collects — what it read live and used as proof becomes evidence —
  and opens a tension with a dossier. The adjudicator decides, treating the hypothesis as an accusation.
"""

from __future__ import annotations

from psycopg.types.json import Jsonb

from ..llm.client import CallContext, schema_object
from ..services import Services
from .. import knowledge as K
from . import prompts
from .adjudicate import tools as adjudication_tools
from .live import LiveReader

STR = {"type": "string"}
STRS = {"type": "array", "items": STR}
SOURCE = "sweep"

SCHEMA = schema_object({
    "summary": STR,
    "suspicions": {"type": "array", "items": schema_object({
        "claim_ids": STRS,
        "hypothesis": STR,
        "reason": STR,
        "evidence": STRS,   # IDs of evidence that already exists (ev_…)
        "proofs": STRS,     # IDs of live reads used as proof (read:N)
    })},
})


def spent_today(svc: Services) -> float:
    return float(svc.db.one("""SELECT coalesce(sum(cost_usd), 0) AS c FROM llm_calls
                               WHERE source_id = %s AND ts >= date_trunc('day', now())""", (SOURCE,))["c"])


def budget_left(svc: Services) -> float:
    cfg = svc.settings.get("sweep")
    if not cfg.get("enabled", True):
        return 0.0
    return float(cfg["daily_budget_usd"]) - spent_today(svc)


def judgement_cost(svc: Services) -> float:
    """Measured cost of judging a sweep suspicion (judgment + proofs); US$0.05 with no history."""
    row = svc.db.one("""SELECT avg(c) AS c FROM (SELECT sum(cost_usd) AS c FROM llm_calls WHERE source_id = %s
                        AND role <> 'sweep' AND job_id IS NOT NULL AND ts > now() - interval '14 days'
                        GROUP BY job_id) j""", (SOURCE,))
    return float(row["c"]) if row and row["c"] else 0.05


def cognition_idle(svc: Services) -> bool:
    return svc.db.one("""SELECT count(*) AS n FROM jobs WHERE lane = 'cognition' AND status IN ('queued', 'running')
                         AND run_after <= now()""")["n"] == 0


def pick(conn, limit: int = 1) -> list[dict]:
    """Rotation by priority (plumbing): documentation older than the newest activity in another
    source (+5), Push/MCP lookups in the last 14 days (+1 each, up to 10) and days without review (up to +10)."""
    return conn.execute(
        """WITH cur AS (
               SELECT c.subject_entity_id AS e, c.source_id, c.recorded_at FROM claims c
               WHERE c.status IN ('current', 'disputed') AND c.subject_entity_id IS NOT NULL),
           doc AS (
               SELECT d.e FROM cur d JOIN cur o ON o.e = d.e AND o.source_id IS DISTINCT FROM d.source_id
               WHERE d.source_id LIKE 'confluence:%%' AND o.recorded_at > d.recorded_at + interval '1 day'
               GROUP BY d.e),
           page AS (SELECT entity_id AS e, id FROM views WHERE kind = 'entity' AND status = 'active'),
           used AS (
               SELECT p.e, count(*) AS n FROM page p
               JOIN (SELECT jsonb_array_elements(refs)->>'id' AS id FROM pushes WHERE ts > now() - interval '14 days'
                     UNION ALL
                     SELECT jsonb_array_elements(refs)->>'id' FROM mcp_calls WHERE ts > now() - interval '14 days') r
                 ON r.id = p.id GROUP BY p.e)
           SELECT e.id, e.name, e.type,
                  (CASE WHEN doc.e IS NOT NULL THEN 5 ELSE 0 END)
                  + least(coalesce(used.n, 0), 10)
                  + least(extract(epoch FROM now() - coalesce(r.reviewed_at, e.created_at)) / 86400, 10) AS score
           FROM entities e
           JOIN (SELECT DISTINCT e FROM cur) has ON has.e = e.id
           LEFT JOIN doc ON doc.e = e.id
           LEFT JOIN used ON used.e = e.id
           LEFT JOIN sweep_reviews r ON r.entity_id = e.id
           WHERE e.status = 'active' AND (r.reviewed_at IS NULL OR r.reviewed_at < now() - interval '1 day')
           ORDER BY score DESC, e.id LIMIT %s""", (limit,)).fetchall()


def run(svc: Services, job_id: int | None = None) -> dict:
    if budget_left(svc) <= 0:
        return {"skipped": "today's budget spent (or sweep turned off)"}
    with svc.db.tx() as conn:
        chosen = pick(conn, 1)
        if not chosen:
            return {"skipped": "nothing to review today"}
        ent = chosen[0]
        claims = K.entity_claims(conn, ent["id"], limit=80)
        history = K.entity_claims(conn, ent["id"], ("superseded",), limit=15)
    user = [f"ENTITY {ent['id']}: {ent['name']} ({ent['type']})", "", "Current claims (with source and date):"]
    user += ["- " + K.format_claim(c) + f" · evidence: {', '.join(c['evidence_ids']) or '-'}" for c in claims]
    if history:
        user += ["", "Already superseded (history):"] + ["- " + K.format_claim(c) for c in history]
    reader = LiveReader(svc, billed_to=SOURCE)
    trace: list = []
    before = spent_today(svc)
    out = svc.llm.agent("sweep", prompts.SWEEP.format(owner=svc.owner_name), "\n".join(user),
                        adjudication_tools(svc) + reader.tools(), SCHEMA, ctx=CallContext(source_id=SOURCE, job_id=job_id),
                        trace=trace)
    opened = []
    with svc.db.tx() as conn:
        for s in out["suspicions"]:
            ids = [r["id"] for r in conn.execute(
                "SELECT id FROM claims WHERE id = ANY(%s) AND status IN ('current', 'disputed')", (s["claim_ids"],)).fetchall()]
            if not ids:
                continue  # cited claims that do not exist (or have already changed): nothing to judge
            evidence = [r["id"] for r in conn.execute("SELECT id FROM evidence WHERE id = ANY(%s)", (s["evidence"],)).fetchall()]
            s["_ids"], s["_ev"] = ids, evidence
    # Each suspicion costs a judgment (and the processing of the proofs): do not open more than the day's
    # budget allows. The ones left out come back in a later review of the entity.
    valid = [x for x in out["suspicions"] if x.get("_ids")]
    room = max(0, int(budget_left(svc) // judgement_cost(svc)))
    deferred = valid[room:]
    for s in valid[:room]:
        proofs = reader.store_proofs(s["proofs"])
        with svc.db.tx() as conn:
            opened.append(K.open_tension(conn, SOURCE, s["_ids"], s["hypothesis"][:300], dossier={
                "detector": "sweep", "hypothesis": s["hypothesis"], "reason": s["reason"],
                "evidence_ids": s["_ev"] + list(proofs.values())}))
    with svc.db.tx() as conn:
        # Audit: the summary and what was investigated (tool and arguments, without the content read).
        tools = [{"tool": t["tool"], "input": {k: v for k, v in t["input"].items() if v is not None},
                  **({"failed": t["error"]} if t.get("error") else {})} for t in trace]
        # a read that did not happen cannot become "nothing found".
        tools += [{"tool": f["tool"], "input": {k: v for k, v in f["input"].items() if v is not None},
                   "failed": f["error"]} for f in reader.failures]
        failed = [t for t in tools if t.get("failed")]
        conn.execute("""INSERT INTO sweep_reviews (entity_id, reviewed_at, suspicions, summary, tools, cost_usd)
                        VALUES (%s, now(), %s, %s, %s, %s)
                        ON CONFLICT (entity_id) DO UPDATE SET reviewed_at = now(), suspicions = EXCLUDED.suspicions,
                            summary = EXCLUDED.summary, tools = EXCLUDED.tools, cost_usd = EXCLUDED.cost_usd""",
                     (ent["id"], len(opened), out["summary"] + (f" [{len(deferred)} suspicion(s) beyond the day's budget; "
                                                                "the entity goes back into the rotation]" if deferred else ""),
                      Jsonb(tools), max(0.0, spent_today(svc) - before)))
        if deferred or (failed and not opened):  # goes back into the rotation as early as the next day
            conn.execute("UPDATE sweep_reviews SET reviewed_at = now() - interval '23 hours' WHERE entity_id = %s",
                         (ent["id"],))
    return {"entity": ent["name"], "suspicions": len(opened), "tensions": opened, "failed_reads": len(failed)}

