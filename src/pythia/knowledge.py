"""Knowledge operations shared by the pipeline, MCP and web.

There is no interpretation here: these are commands that write already-decided events
(by an LLM or by you) and reads of the projections.
"""

from __future__ import annotations

from contextvars import ContextVar
from datetime import datetime
from typing import Any, Iterable

import psycopg

from . import events
from .ids import new_id, sha
from .jobs import enqueue

CLAIM_KINDS = ["profile", "episode", "decision", "state", "causal", "glossary", "procedure"]
ENTITY_TYPES = ["person", "team", "system", "project", "repository", "concept", "organization",
                "document", "incident", "tool", "other"]
CLAIM_STATUSES = ["current", "superseded", "disputed", "retracted"]
VERDICT_KINDS = ["temporal_supersession", "real_conflict", "different_scope", "outdated_source",
                 "duplicate", "human_correction"]
CURRENT = ("current", "disputed")
HUMAN = "human:owner"
# Inside a source's backfill processing, regeneration of derived items is deferred:
# the touched entities are noted and synthesized in a single pass when the backfill ends.
DEFER: ContextVar[str | None] = ContextVar("pythia_defer_derivation", default=None)
# Grouped regeneration: back-to-back changes (backfill) become a single regeneration per entity.
DERIVE_DELAY_S = 120

# ---------------------------------------------------------------- commands


def create_entity(conn, actor: str, *, name: str, type: str, origin: str, aliases: Iterable[str] = (),
                  description: str | None = None, external_ids: dict | None = None,
                  is_owner: bool = False) -> str:
    eid = new_id("en")
    aliases = sorted({a.strip() for a in aliases if a and a.strip() and a.strip() != name})
    events.append(conn, "entity_created", actor, {
        "id": eid, "type": type if type in ENTITY_TYPES else "other", "name": name.strip(),
        "aliases": aliases, "description": description, "origin": origin,
        "external_ids": external_ids or {}, "is_owner": is_owner,
    })
    return eid


def record_mention(conn, actor: str, *, surface: str, context: str | None, type_hint: str | None,
                   entity_id: str | None, confidence: float | None, evidence_id: str | None,
                   unit_id: int | None, candidates: list | None = None) -> str:
    mid = new_id("mn")
    events.append(conn, "mention_recorded", actor, {
        "id": mid, "surface": surface, "context": context, "type_hint": type_hint,
        "entity_id": entity_id, "confidence": confidence, "resolved_by": actor,
        "evidence_id": evidence_id, "unit_id": unit_id, "candidates": candidates or [],
    })
    return mid


def assert_claim(conn, actor: str, **fields) -> str:
    cid = new_id("cl")
    payload = {"id": cid, **fields}
    payload.setdefault("status", "current")
    for key in ("valid_from", "valid_to"):
        if isinstance(payload.get(key), datetime):
            payload[key] = payload[key].isoformat()
    events.append(conn, "claim_asserted", actor, payload)
    return cid


def claim_status(conn, claim_id: str) -> str | None:
    row = conn.execute("SELECT status FROM claims WHERE id = %s", (claim_id,)).fetchone()
    return row["status"] if row else None


def apply_verdict(conn, actor: str, *, kind: str, decided_by: str, changes: list[dict], summary: str,
                  rationale: str, tension_id: str | None = None, cited_evidence: list[str] | None = None,
                  confidence: float | None = None, model: str | None = None,
                  prompt_version: str | None = None, dossier_hash: str | None = None,
                  exploration: list | None = None) -> str:
    """Applies a verdict. `changes` = [{claim_id, to, by?}]; `from` is read from the current state. `by` is the claim
    that took its place, when known (integration and correction): the Evolution trajectory uses this link."""
    full = []
    for ch in changes:
        current = claim_status(conn, ch["claim_id"])
        if current is None:
            continue
        full.append({"claim_id": ch["claim_id"], "from": current, "to": ch["to"],
                     **({"by": ch["by"]} if ch.get("by") else {})})
    claim_ids = sorted({c["claim_id"] for c in full})
    vid = new_id("vd")
    impact = verdict_impact(conn, kind, confidence, claim_ids)
    events.append(conn, "verdict_applied", actor, {
        "id": vid, "tension_id": tension_id, "kind": kind, "decided_by": decided_by,
        "claim_ids": claim_ids, "changes": full, "summary": summary, "rationale": rationale,
        "cited_evidence": cited_evidence or [], "confidence": confidence, "model": model,
        "prompt_version": prompt_version, "dossier_hash": dossier_hash, "impact": impact,
        "exploration": exploration or [],
    })
    invalidate_claims(conn, claim_ids)
    return vid


def verdict_impact(conn, kind: str, confidence: float | None, claim_ids: list[str]) -> float:
    """Feed ordering (presentation): real conflict and low confidence rank higher; more affected derived items rank higher."""
    weight = {"real_conflict": 3.0, "outdated_source": 2.0, "different_scope": 1.5,
              "temporal_supersession": 1.0, "duplicate": 0.2, "human_correction": 0.5}.get(kind, 1.0)
    dependents = conn.execute(
        "SELECT count(*) AS n FROM views WHERE status = 'active' AND cited_claim_ids && %s::text[]",
        (claim_ids,),
    ).fetchone()["n"]
    uncertainty = 1.0 - (confidence if confidence is not None else 0.5)
    return round(weight * (1 + uncertainty) + 0.3 * dependents, 3)


def revert_verdict(conn, actor: str, verdict_id: str, status: str = "reverted") -> bool:
    v = conn.execute("SELECT * FROM verdicts WHERE id = %s", (verdict_id,)).fetchone()
    if not v or v["status"] != "applied":
        return False
    # Only undoes what is still as the verdict left it: later changes (another verdict,
    # a correction of yours) are not trampled.
    inverse = []
    for c in v["changes"]:
        if c["from"] == c["to"]:
            continue
        if claim_status(conn, c["claim_id"]) == c["to"]:
            inverse.append({"claim_id": c["claim_id"], "from": c["to"], "to": c["from"]})
    events.append(conn, "verdict_reverted", actor, {
        "id": verdict_id, "changes": inverse, "status": status, "by": actor,
        "reopen_tension_id": v["tension_id"],
    })
    # The validity the verdict closed goes back to what it was before.
    for ev in conn.execute("""SELECT payload FROM events WHERE type = 'claim_validity_set'
                              AND payload->>'verdict_id' = %s ORDER BY id DESC""", (verdict_id,)).fetchall():
        events.append(conn, "claim_validity_set", actor, {"id": ev["payload"]["id"],
                                                          "valid_to": ev["payload"].get("previous")})
    invalidate_claims(conn, [c["claim_id"] for c in inverse])
    return True


def open_tension(conn, actor: str, claim_ids: list[str], reason: str, dossier: dict | None = None) -> str:
    """`dossier`: what a detector already investigated — hypothesis, reason, evidence by ID. The
    adjudicator starts from it instead of redoing the search, and treats it as an accusation, not a conclusion."""
    tid = new_id("tn")
    events.append(conn, "tension_opened", actor, {"id": tid, "claim_ids": claim_ids, "reason": reason,
                                                  "opened_by": actor, **({"dossier": dossier} if dossier else {})})
    enqueue(conn, "adjudicate", "cognition", {"tension_id": tid}, priority=50, dedup_key=f"adjudicate:{tid}")
    return tid


def rejudge_open_tensions(conn, entity_ids: list[str]) -> None:
    """Schedules (again) the adjudication of open tensions of these entities. The dossier-hash
    cache guarantees nothing is re-judged unless the dossier has changed."""
    entity_ids = [e for e in entity_ids if e]
    if not entity_ids:
        return
    rows = conn.execute(
        """SELECT DISTINCT t.id FROM tensions t JOIN claims c ON c.id = ANY(t.claim_ids)
           WHERE t.status = 'open' AND (c.subject_entity_id = ANY(%s) OR c.object_entity_id = ANY(%s))""",
        (entity_ids, entity_ids)).fetchall()
    for r in rows:
        enqueue(conn, "adjudicate", "cognition", {"tension_id": r["id"]}, priority=50, dedup_key=f"adjudicate:{r['id']}")


def relink_mention(conn, actor: str, mention_id: str, to_entity: str | None, reason: str,
                   confidence: float | None = None, merge_of: str | None = None) -> bool:
    m = conn.execute("SELECT entity_id FROM mentions WHERE id = %s", (mention_id,)).fetchone()
    if not m or m["entity_id"] == to_entity:
        return False
    payload = {"id": mention_id, "from": m["entity_id"], "to": to_entity, "reason": reason, "by": actor,
               "confidence": confidence}
    if merge_of:
        payload["merge_of"] = merge_of
    events.append(conn, "mention_relinked", actor, payload)
    affected = [e for e in (m["entity_id"], to_entity) if e]
    claim_ids = [r["id"] for r in conn.execute(
        "SELECT id FROM claims WHERE subject_mention_id = %s OR object_mention_id = %s",
        (mention_id, mention_id)).fetchall()]
    invalidate(conn, entity_ids=affected, claim_ids=claim_ids)
    return True


def merge_entities(conn, actor: str, from_id: str, into_id: str, reason: str) -> int:
    """Merges entities by relinking each mention (explicit events, reversible one by one)."""
    if from_id == into_id:
        return 0
    mentions = conn.execute("SELECT id FROM mentions WHERE entity_id = %s", (from_id,)).fetchall()
    for m in mentions:
        relink_mention(conn, actor, m["id"], into_id, reason, merge_of=from_id)
    # Claims without a mention (corrections, inferences) also change entity.
    for c in conn.execute(
            """SELECT id, subject_mention_id, object_mention_id, subject_entity_id, object_entity_id FROM claims
               WHERE (subject_entity_id = %s AND subject_mention_id IS NULL)
                  OR (object_entity_id = %s AND object_mention_id IS NULL)""", (from_id, from_id)).fetchall():
        for field in ("subject", "object"):
            if c[f"{field}_entity_id"] == from_id and c[f"{field}_mention_id"] is None:
                events.append(conn, "claim_reassigned", actor, {"id": c["id"], "field": field, "from": from_id,
                                                               "to": into_id, "merge_of": from_id})
    src = conn.execute("SELECT name, aliases, external_ids FROM entities WHERE id = %s", (from_id,)).fetchone()
    dst = conn.execute("SELECT name, aliases, external_ids FROM entities WHERE id = %s", (into_id,)).fetchone()
    added_aliases, added_ext = [], {}
    if src and dst:
        added_aliases = sorted((set(src["aliases"]) | {src["name"]}) - set(dst["aliases"]) - {dst["name"]})
        added_ext = {k: v for k, v in src["external_ids"].items() if k not in dst["external_ids"]}
        change = {}
        if added_aliases:
            change["aliases"] = sorted(set(dst["aliases"]) | set(added_aliases))
        if added_ext:
            change["external_ids"] = {**dst["external_ids"], **added_ext}
        if change:
            events.append(conn, "entity_updated", actor, {"id": into_id, **change})
    # What the merge added to the destination is recorded so the undo can give it back.
    events.append(conn, "entity_merged", actor, {"id": from_id, "into": into_id, "reason": reason,
                                                 "added_aliases": added_aliases, "added_external_ids": added_ext})
    conn.execute("UPDATE views SET status = 'retired' WHERE entity_id = %s", (from_id,))
    invalidate(conn, entity_ids=[into_id])
    return len(mentions)


def unmerge_entity(conn, actor: str, entity_id: str) -> int:
    """Undoes a merge: gives the entity back the mentions and claims the merge moved."""
    ent = entity(conn, entity_id)
    if not ent or ent["status"] != "merged":
        return 0
    into = ent["merged_into"]
    moved = 0
    for ev in conn.execute(
            """SELECT payload FROM events WHERE type = 'mention_relinked' AND payload->>'merge_of' = %s ORDER BY id""",
            (entity_id,)).fetchall():
        if relink_mention(conn, actor, ev["payload"]["id"], entity_id, "undo merge"):
            moved += 1
    for ev in conn.execute(
            """SELECT payload FROM events WHERE type = 'claim_reassigned' AND payload->>'merge_of' = %s ORDER BY id""",
            (entity_id,)).fetchall():
        p = ev["payload"]
        events.append(conn, "claim_reassigned", actor, {"id": p["id"], "field": p["field"], "from": into, "to": entity_id})
        moved += 1
    merge_ev = conn.execute(
        """SELECT payload FROM events WHERE type = 'entity_merged' AND payload->>'id' = %s ORDER BY id DESC LIMIT 1""",
        (entity_id,)).fetchone()
    added_aliases = (merge_ev["payload"].get("added_aliases") if merge_ev else None)
    if added_aliases is None:  # no record of what the merge added: fall back to the entity's own names
        added_aliases = list(ent["aliases"]) + [ent["name"]]
    added_ext = (merge_ev["payload"].get("added_external_ids") if merge_ev else None) or {}
    dst = entity(conn, into)
    change = {}
    if dst and set(added_aliases) & set(dst["aliases"]):
        change["aliases"] = sorted(set(dst["aliases"]) - set(added_aliases))
    if dst and any(k in dst["external_ids"] for k in added_ext):
        change["external_ids"] = {k: v for k, v in dst["external_ids"].items() if k not in added_ext}
    if change:
        events.append(conn, "entity_updated", actor, {"id": into, **change})
    events.append(conn, "entity_unmerged", actor, {"id": entity_id})
    invalidate(conn, entity_ids=[entity_id, into])
    return moved


def update_entity(conn, actor: str, entity_id: str, **fields) -> None:
    fields = {k: v for k, v in fields.items() if v is not None}
    if not fields:
        return
    events.append(conn, "entity_updated", actor, {"id": entity_id, **fields})
    invalidate(conn, entity_ids=[entity_id])


# ---------------------------------------------------------------- cascade


def invalidate_claims(conn, claim_ids: list[str]) -> None:
    if not claim_ids:
        return
    _check_inferences(conn, claim_ids)
    rows = conn.execute(
        "SELECT subject_entity_id, object_entity_id FROM claims WHERE id = ANY(%s)", (claim_ids,)
    ).fetchall()
    entity_ids = {e for r in rows for e in (r["subject_entity_id"], r["object_entity_id"]) if e}
    invalidate(conn, entity_ids=list(entity_ids), claim_ids=claim_ids)


def _check_inferences(conn, claim_ids: list[str]) -> None:
    """Inferences backed by claims that stopped holding become a tension: the adjudicator decides
    whether the inference still stands (judgment belongs to the LLM)."""
    changed = [r["id"] for r in conn.execute(
        "SELECT id FROM claims WHERE id = ANY(%s) AND status NOT IN ('current', 'disputed')", (claim_ids,)).fetchall()]
    if not changed:
        return
    dependents = conn.execute(
        """SELECT id, supporting_claim_ids FROM claims
           WHERE supporting_claim_ids && %s::text[] AND status IN ('current', 'disputed')""", (changed,)).fetchall()
    for d in dependents:
        ids = sorted({d["id"], *[c for c in d["supporting_claim_ids"] if c in changed]})
        exists = conn.execute("SELECT 1 FROM tensions WHERE status = 'open' AND claim_ids @> %s::text[]", (ids,)).fetchone()
        if not exists:
            open_tension(conn, "system", ids, "a claim that supported this inference stopped holding")


def invalidate(conn, entity_ids: list[str] | None = None, claim_ids: list[str] | None = None) -> None:
    """Marks dependent derived items as stale and schedules regeneration of only those."""
    entity_ids = [e for e in (entity_ids or []) if e]
    claim_ids = claim_ids or []
    conn.execute(
        """UPDATE views SET stale = true
           WHERE status = 'active' AND (entity_id = ANY(%s) OR cited_claim_ids && %s::text[]
                                        OR member_entity_ids && %s::text[])""",
        (entity_ids, claim_ids, entity_ids),
    )
    deferred_source = DEFER.get()
    if deferred_source:
        for eid in entity_ids:
            conn.execute("""INSERT INTO deferred_derivations (source_id, entity_id) VALUES (%s, %s)
                            ON CONFLICT DO NOTHING""", (deferred_source, eid))
        return
    for eid in entity_ids:
        enqueue(conn, "derive_entity", "derivation", {"entity_id": eid}, priority=60,
                dedup_key=f"derive:{eid}", delay_s=DERIVE_DELAY_S)
    if entity_ids or claim_ids:
        enqueue(conn, "derive_narratives", "derivation", {}, priority=80, dedup_key="narratives",
                delay_s=narrative_delay_s(conn))


PASSES_PER_HOUR = (0.25, 12.0)   # from one every 4 h to one every 5 min
PASSES_OPTIONS = [(0.25, "Every 4 hours"), (0.5, "Every 2 hours"), (1.0, "Every hour"),
                  (2.0, "Every 30 minutes"), (4.0, "Every 15 minutes"), (12.0, "Every 5 minutes")]


def narrative_delay_s(conn) -> float:
    """Wait between the first change and the narrative pass, from the setting (passes per hour)."""
    from .settings import DEFAULTS
    row = conn.execute("SELECT value FROM settings WHERE key = 'narratives'").fetchone()
    passes = float(((row or {}).get("value") or {}).get("passes_per_hour") or DEFAULTS["narratives"]["passes_per_hour"])
    return 3600.0 / min(max(passes, PASSES_PER_HOUR[0]), PASSES_PER_HOUR[1])


def entity_watermark(conn, entity_id: str) -> str:
    rows = conn.execute(
        """SELECT id, status FROM claims WHERE subject_entity_id = %s OR object_entity_id = %s ORDER BY id""",
        (entity_id, entity_id),
    ).fetchall()
    ent = conn.execute("SELECT name, type, aliases FROM entities WHERE id = %s", (entity_id,)).fetchone()
    basis = "|".join(f"{r['id']}:{r['status']}" for r in rows)
    basis += f"#{ent['name']}:{ent['type']}:{','.join(ent['aliases'])}" if ent else ""
    return sha(basis)


# ---------------------------------------------------------------- tombstone


def tombstone_evidence(conn, actor: str, evidence_ids: list[str], reason: str) -> dict:
    """Really erases the content and retracts claims that depended only on that evidence.

    Erasing at the source also erases derived text: mention context, the statement of the
    retracted claims (in the log and in the projections), unit titles/triage and the text
    of pages that cited those claims (including old versions)."""
    stats = {"evidence": 0, "claims_retracted": 0}
    retracted: list[str] = []
    units: set[int] = set()
    reprocess: set[int] = set()
    for ev in evidence_ids:
        row = conn.execute(
            """UPDATE evidence SET content = NULL, status = 'tombstoned' WHERE id = %s
               AND status IN ('active', 'superseded')
               RETURNING id, unit_id, source_id, stream_id, external_id""",
            (ev,)).fetchone()
        if not row:
            continue
        stats["evidence"] += 1
        if row["unit_id"]:
            units.add(row["unit_id"])
        if (row["source_id"] or "").startswith("slack:"):
            # The message is also context for other units: pending ones have their batch request
            # redone without it; already processed ones are reprocessed (what extraction took from
            # the erased context cannot survive).
            from .connectors.slack import context_units
            units |= context_units(conn, row)
            reprocess |= {r["id"] for r in conn.execute(
                "SELECT id FROM units WHERE meta->'context_ids' ? %s AND status <> 'pending'", (ev,)).fetchall()}
        for m in conn.execute("SELECT id FROM mentions WHERE evidence_id = %s", (ev,)).fetchall():
            events.redact(conn, "mention_recorded", {"id": m["id"]}, ["context"])
        conn.execute("UPDATE mentions SET context = %s WHERE evidence_id = %s", (events.REDACTED, ev))
        for c in conn.execute("SELECT id, evidence_ids, status, supporting_claim_ids FROM claims WHERE %s = ANY(evidence_ids)",
                              (ev,)).fetchall():
            events.append(conn, "claim_evidence_removed", actor, {"id": c["id"], "evidence_id": ev})
            remaining = [e for e in c["evidence_ids"] if e != ev]
            if not remaining and not c["supporting_claim_ids"]:
                if c["status"] != "retracted":
                    events.append(conn, "claim_status_changed", actor, {"id": c["id"], "to": "retracted", "reason": reason})
                    stats["claims_retracted"] += 1
                retracted.append(c["id"])
            invalidate_claims(conn, [c["id"]])
    if retracted:
        for cid in retracted:
            events.redact(conn, "claim_asserted", {"id": cid}, ["statement", "object_value"])
        conn.execute("UPDATE claims SET statement = %s, object_value = %s, embedding = NULL WHERE id = ANY(%s)",
                     (events.REDACTED, events.REDACTED, retracted))
        # Pages that cited the erased content: text goes away now; the cascade regenerates without it.
        conn.execute("""DELETE FROM view_versions vv USING views v
                        WHERE vv.view_id = v.id AND vv.cited_claim_ids && %s::text[]""", (retracted,))
        conn.execute("""UPDATE views SET l0 = %s, l1 = %s, stale = true, embedding = NULL
                        WHERE cited_claim_ids && %s::text[]""",
                     ("(regenerating: part of the content was removed at the source)", "", retracted))
    if units:
        conn.execute("UPDATE units SET title = NULL, triage = NULL WHERE id = ANY(%s)", (list(units),))
        from .pipeline.batch import cancel_for_units
        cancel_for_units(conn, list(units))
    for uid in sorted(reprocess - units):
        reprocess_unit(conn, actor, uid, reason)
    redact_derived_text(conn, list(evidence_ids), retracted)
    return stats


def redact_derived_text(conn, evidence_ids: list[str], retracted_claims: list[str]) -> None:
    """Text written from what was erased goes too: the reason and dossier of tensions that cited
    that evidence or those claims (in the log and the projection), and the sweep summary of the affected entities."""
    if not evidence_ids and not retracted_claims:
        return
    rows = conn.execute(
        """SELECT id FROM tensions WHERE claim_ids && %s::text[]
           OR (dossier IS NOT NULL AND dossier->'evidence_ids' ?| %s::text[])""",
        (retracted_claims, evidence_ids)).fetchall()
    for t in rows:
        events.redact(conn, "tension_opened", {"id": t["id"]}, ["reason", "dossier"])
    if rows:
        conn.execute("UPDATE tensions SET reason = %s, dossier = NULL WHERE id = ANY(%s)",
                     (events.REDACTED, [t["id"] for t in rows]))
    if retracted_claims:
        conn.execute("""UPDATE sweep_reviews SET summary = NULL, tools = '[]' WHERE entity_id IN (
                            SELECT subject_entity_id FROM claims WHERE id = ANY(%s))""", (retracted_claims,))


def reprocess_unit(conn, actor: str, unit_id: int, reason: str) -> None:
    """Redoes a unit from scratch: retracts (and redacts) the claims that came only from it, redacts the context of
    its mentions and puts it back in the queue. Used when the context it read no longer exists."""
    ev_ids = [r["id"] for r in conn.execute("SELECT id FROM evidence WHERE unit_id = %s", (unit_id,)).fetchall()]
    retracted = []
    for c in conn.execute(
            """SELECT id, evidence_ids, status FROM claims WHERE evidence_ids && %s::text[]
               AND cardinality(supporting_claim_ids) = 0""", (ev_ids,)).fetchall():
        if set(c["evidence_ids"]) <= set(ev_ids):
            if c["status"] != "retracted":
                events.append(conn, "claim_status_changed", actor, {"id": c["id"], "to": "retracted", "reason": reason})
            retracted.append(c["id"])
    if retracted:
        for cid in retracted:
            events.redact(conn, "claim_asserted", {"id": cid}, ["statement", "object_value"])
        conn.execute("UPDATE claims SET statement = %s, object_value = %s, embedding = NULL WHERE id = ANY(%s)",
                     (events.REDACTED, events.REDACTED, retracted))
        invalidate_claims(conn, retracted)
        conn.execute("""DELETE FROM view_versions vv USING views v
                        WHERE vv.view_id = v.id AND vv.cited_claim_ids && %s::text[]""", (retracted,))
        conn.execute("""UPDATE views SET l0 = %s, l1 = %s, stale = true, embedding = NULL
                        WHERE cited_claim_ids && %s::text[]""",
                     ("(regenerating: part of the content was removed at the source)", "", retracted))
    for m in conn.execute("SELECT id FROM mentions WHERE unit_id = %s", (unit_id,)).fetchall():
        events.redact(conn, "mention_recorded", {"id": m["id"]}, ["context"])
    conn.execute("UPDATE mentions SET context = %s WHERE unit_id = %s", (events.REDACTED, unit_id))
    conn.execute("""UPDATE units SET status = 'pending', triage = NULL, meta = meta - 'pipeline' - 'context_ids'
                    WHERE id = %s""", (unit_id,))
    from .jobs import enqueue
    enqueue(conn, "process_unit", "cognition", {"unit_id": unit_id}, priority=50, dedup_key=f"unit:{unit_id}")


# ---------------------------------------------------------------- reads


def entity(conn, entity_id: str) -> dict | None:
    return conn.execute("SELECT * FROM entities WHERE id = %s", (entity_id,)).fetchone()


def entity_claims(conn, entity_id: str, statuses: tuple[str, ...] = CURRENT, limit: int = 200) -> list[dict]:
    return conn.execute(
        """SELECT c.*, s.name AS subject_name, o.name AS object_name
           FROM claims c
           LEFT JOIN entities s ON s.id = c.subject_entity_id
           LEFT JOIN entities o ON o.id = c.object_entity_id
           WHERE (c.subject_entity_id = %s OR c.object_entity_id = %s) AND c.status = ANY(%s)
           ORDER BY c.recorded_at DESC LIMIT %s""",
        (entity_id, entity_id, list(statuses), limit),
    ).fetchall()


def owner(conn) -> dict | None:
    return conn.execute("SELECT * FROM entities WHERE is_owner AND status = 'active' LIMIT 1").fetchone()


def format_claim(c: dict) -> str:
    when = ""
    if c.get("valid_from") or c.get("valid_to"):
        vf = c["valid_from"].date().isoformat() if c.get("valid_from") else "?"
        vt = c["valid_to"].date().isoformat() if c.get("valid_to") else "…"
        when = f" [valid {vf} → {vt}]"
    src = c.get("source_label") or c.get("origin")
    return (f"{c['id']} ({c['kind']}, {c['status']}, {c['origin']}, source {src}, "
            f"recorded {c['recorded_at'].date().isoformat()}){when}: {c['statement']}")
