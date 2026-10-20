"""Identity reconciliation: discovered entities × entities anchored in records.

Candidate search is plumbing (similar name, alias, external identifier,
embedding). Deciding whether two entities are the same is judgment: typed decider
(role `reconciliation`, Sonnet by default). Merge applied immediately, reversible (`undo_merge`).
"""

from __future__ import annotations

import numpy as np

from .. import knowledge as K
from ..ids import sha
from ..llm.client import CallContext, schema_object
from ..services import Services
from . import prompts

STR = {"type": "string"}

# Before deciding, the LLM says what the discovered entity is and how it relates to the candidate: "part of" and
# "related" are explicit as non-merges (a small model tends to treat "PR of the repository" as the same thing).
SCHEMA = schema_object({
    "decisions": {"type": "array", "items": {"anyOf": [
        schema_object({"ref": STR, "discovered_is": STR, "decision": {"type": "string", "const": "same"},
                       "entity_id": STR, "confidence": {"type": "number"}, "reason": STR}),
        schema_object({"ref": STR, "discovered_is": STR,
                       "decision": {"type": "string", "enum": ["part_of", "related", "different"]},
                       "confidence": {"type": "number"}, "reason": STR}),
    ]}},
})

PROMPT = prompts.CONTEXT + """

Your task: identity reconciliation. Each DISCOVERED entity (extracted from conversations, PRs etc.) \
comes with candidate ANCHORED entities (coming from official records, such as the incident.io catalog). \
First say in a few words what the discovered entity is (discovered_is: "a PR", "a person", "an \
alert"...). Then decide:
- "same": the discovered entity and one of ITS candidates (listed right below it) are exactly the same \
real-world thing; give the id;
- "part_of": the discovered entity belongs to a candidate or lives inside it (PR, ticket, file, class, \
alert, endpoint of a repository/system; system of a team);
- "related": there is a relation, but it is something else (tool used by, incident that affected...);
- "different": no relation.
Only "same" merges the entities.

"Same" means the two names refer to a single referent: a person and their user, \
a repository written two ways, a team and its nickname. Belonging, being part of, being inside \
or being related is NOT being the same thing. That is a relation, which already becomes a claim by another path. \
Examples of "different":
- a PR, issue, ticket, epic, incident, alert, file, class, endpoint or module × the repository \
or system where it lives;
- a system/service × the team that owns it;
- a tool or library (New Relic, GitPython) × a repository that uses it.
When in doubt, "different": a wrong merge mixes the knowledge of two things, and not merging only \
leaves two entries."""

def reconcile(svc: Services, job_id: int | None = None, batch: int = 25) -> dict:
    with svc.db.tx() as conn:
        # The owner is reconciled too (the catalog's anchored entity merges into him, not the other way around).
        discovered = conn.execute(
            """SELECT * FROM entities WHERE status = 'active' AND (origin = 'discovered' OR is_owner)
               ORDER BY is_owner DESC, created_at""").fetchall()
        blocks, pending = [], []
        for d in discovered:
            cands = conn.execute(
                """SELECT id, name, type, aliases, external_ids, description FROM entities
                   WHERE status = 'active' AND origin = 'anchored' AND id <> %s AND (
                         similarity(lower(name), lower(%s)) > 0.45
                         OR lower(%s) = ANY(SELECT lower(a) FROM unnest(aliases) a)
                         OR EXISTS (SELECT 1 FROM unnest(%s::text[]) da WHERE lower(da) = lower(name)
                                    OR lower(da) = ANY(SELECT lower(a) FROM unnest(aliases) a))
                         OR EXISTS (SELECT 1 FROM jsonb_each_text(external_ids) kv WHERE lower(kv.value) = lower(%s)))
                   ORDER BY similarity(lower(name), lower(%s)) DESC LIMIT 4""",
                (d["id"], d["name"], d["name"], d["aliases"], d["name"], d["name"])).fetchall()
            if d["embedding"] is not None:
                for c in conn.execute(
                        """SELECT id, name, type, aliases, external_ids, description FROM entities
                           WHERE status = 'active' AND origin = 'anchored' AND embedding IS NOT NULL
                           ORDER BY embedding <=> %s LIMIT 2""", (d["embedding"],)).fetchall():
                    if c["id"] not in {x["id"] for x in cands}:
                        cands.append(c)
            if not cands:
                continue
            key = f"reconcile:{d['id']}"
            basis = sha("|".join(sorted(c["id"] for c in cands)) + d["name"] + ",".join(d["aliases"]))
            seen = conn.execute("SELECT value FROM system_state WHERE key = %s", (key,)).fetchone()
            if seen and seen["value"].get("basis") == basis:
                continue  # same candidates already judged
            claims = K.entity_claims(conn, d["id"], limit=6)
            ref = f"D{len(pending) + 1}"
            lines = [f"{ref}: {d['name']} ({d['type']}) aliases: {', '.join(d['aliases']) or '-'}"]
            lines += [f"   known: {c['statement']}" for c in claims]
            lines.append("   anchored candidates:")
            for c in cands:
                ids = ", ".join(f"{k}={v}" for k, v in c["external_ids"].items() if k != "incident_io")
                lines.append(f"   - {c['id']} · {c['name']} ({c['type']}) aliases: {', '.join(c['aliases'][:8]) or '-'} {ids}")
            blocks.append("\n".join(lines))
            pending.append({"ref": ref, "entity": d, "cands": {c["id"] for c in cands}, "key": key, "basis": basis})
            if len(pending) >= batch:
                break
    if not pending:
        return {"checked": 0}
    out = svc.llm.structured("reconciliation", PROMPT.format(owner=svc.owner_name), "\n\n".join(blocks), SCHEMA,
                             max_tokens=4096, ctx=CallContext(job_id=job_id))
    by_ref = {d["ref"]: d for d in out["decisions"]}
    for p in pending:
        d = by_ref.get(p["ref"])
        if d is None:
            p["invalid"] = True
            continue
        if d["decision"] == "same" and d.get("entity_id") not in p["cands"]:
            # Candidate from another block: invalid decision only for this item, which is left for the next
            # pass (not marked as judged); the others go on.
            p["invalid"] = True
    merged = invalid = 0
    with svc.db.tx() as conn:
        for p in pending:
            if p.get("invalid"):
                invalid += 1
                continue
            d = by_ref[p["ref"]]
            if d["decision"] == "same":
                src, dst = p["entity"]["id"], d["entity_id"]
                if p["entity"]["is_owner"]:
                    src, dst = dst, src
                K.merge_entities(conn, "resolution", src, dst, f"reconciliation: {d['reason']}")
                merged += 1
            conn.execute(
                """INSERT INTO system_state (key, value) VALUES (%s, jsonb_build_object('basis', %s::text))
                   ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()""", (p["key"], p["basis"]))
    if len(pending) >= batch:  # there is more to check
        svc.queue.enqueue("reconcile_entities", "cognition", {}, priority=45, dedup_key="reconcile_entities")
    return {"checked": len(pending), "merged": merged, "invalid": invalid}


def apply_team_identities(svc: Services) -> dict:
    """The "Your team" list is the owner's truth about who is who: each person's GitHub login goes
    to their anchored entity (found by email or, if unique, by exact name), as an external
    identifier and alias. It is plumbing for an identifier declared by the owner; merging with discovered entities
    remains the LLM's decision, which now sees this proof on the candidates."""
    stats = {"updated": 0}
    members = svc.settings.get("team")["members"]
    with svc.db.tx() as conn:
        for m in members:
            login = (m.get("github") or "").strip()
            if not login:
                continue
            ent = None
            if m.get("email"):
                ent = conn.execute("""SELECT * FROM entities WHERE status = 'active' AND type = 'person'
                                      AND lower(external_ids->>'email') = lower(%s)
                                      ORDER BY is_owner DESC LIMIT 1""", (m["email"],)).fetchone()
            if ent is None and m.get("name"):
                rows = conn.execute("""SELECT * FROM entities WHERE status = 'active' AND type = 'person'
                                       AND origin = 'anchored' AND lower(name) = lower(%s)""", (m["name"],)).fetchall()
                ent = rows[0] if len(rows) == 1 else None
            if ent is None:
                continue
            ext = dict(ent["external_ids"] or {})
            aliases = sorted(set(ent["aliases"]) | {login})
            if ext.get("github", "").lower() == login.lower() and aliases == sorted(ent["aliases"]):
                continue
            ext.setdefault("github", login)
            K.update_entity(conn, "config", ent["id"], external_ids=ext, aliases=aliases)
            stats["updated"] += 1
    if stats["updated"]:
        svc.queue.enqueue("reconcile_entities", "cognition", {}, priority=45, dedup_key="reconcile_entities")
    return stats
