"""Human corrections and definitions (maximum authority).

- `request_correction` / `request_definition` only record and enqueue: they work without an LLM.
- `apply_correction` uses the LLM to interpret free text into operations.
- `apply_ops` applies already structured operations (coming from the LLM or from web buttons).
"""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
from psycopg.types.json import Jsonb

from .. import knowledge as K
from ..ids import new_id, sha
from ..jobs import enqueue
from ..llm.client import CallContext
from ..services import Services
from . import prompts, schemas

HUMAN_SOURCE = "human:owner"


def ensure_human_source(conn, owner_name: str) -> None:
    conn.execute(
        """INSERT INTO sources (id, kind, name, config) VALUES (%s, 'human', %s, '{}')
           ON CONFLICT (id) DO NOTHING""", (HUMAN_SOURCE, f"{owner_name} (corrections and definitions)"))


def request_correction(svc: Services, target_id: str | None, text: str, via: str) -> str:
    cid = new_id("co")
    with svc.db.tx() as conn:
        conn.execute("INSERT INTO corrections (id, target_id, text, via) VALUES (%s, %s, %s, %s)",
                     (cid, target_id, text, via))
        enqueue(conn, "apply_correction", "cognition", {"correction_id": cid}, priority=20)
    return cid


def request_definition(svc: Services, text: str, via: str) -> int:
    """A definition of yours becomes a unit of the human source, processed by the normal pipeline."""
    now = datetime.now(timezone.utc)
    with svc.db.tx() as conn:
        ensure_human_source(conn, svc.owner_name)
        unit = conn.execute(
            """INSERT INTO units (source_id, kind, title, occurred_at, meta)
               VALUES (%s, 'human_statement', %s, %s, %s) RETURNING id""",
            (HUMAN_SOURCE, text[:120], now, Jsonb({"via": via}))).fetchone()
        conn.execute(
            """INSERT INTO evidence (id, unit_id, source_id, author, role, occurred_at, content, content_hash, meta)
               VALUES (%s, %s, %s, %s, 'human', %s, %s, %s, %s)""",
            (new_id("ev"), unit["id"], HUMAN_SOURCE, svc.owner_name, now, text, sha(text), Jsonb({"via": via})))
        enqueue(conn, "process_unit", "cognition", {"unit_id": unit["id"]}, priority=20, dedup_key=f"unit:{unit['id']}")
    return unit["id"]


def describe_target(conn, target_id: str | None) -> str:
    if not target_id:
        return "No specific target."
    prefix = target_id.split("_")[0]
    if prefix == "cl":
        c = conn.execute("""SELECT c.*, s.name AS subject_name FROM claims c LEFT JOIN entities s ON s.id = c.subject_entity_id
                            WHERE c.id = %s""", (target_id,)).fetchone()
        if not c:
            return f"Target {target_id} not found."
        m = conn.execute("SELECT id, surface, context, entity_id FROM mentions WHERE id = %s",
                         (c["subject_mention_id"],)).fetchone() if c["subject_mention_id"] else None
        text = f"Target claim about {c['subject_name']} ({c['subject_entity_id']}):\n" + K.format_claim(c)
        if m:
            text += f"\nSubject mention {m['id']}: \"{m['surface']}\" context: {m['context']}"
        return text
    if prefix == "en":
        e = K.entity(conn, target_id)
        if not e:
            return f"Target {target_id} not found."
        claims = K.entity_claims(conn, target_id, limit=40)
        mentions = conn.execute("SELECT id, surface, context FROM mentions WHERE entity_id = %s LIMIT 30",
                                (target_id,)).fetchall()
        return (f"Target entity {e['id']}: {e['name']} ({e['type']}), aliases {e['aliases']}\nClaims:\n"
                + "\n".join("- " + K.format_claim(c) for c in claims)
                + "\nMentions:\n" + "\n".join(f"- {m['id']}: \"{m['surface']}\" {m['context']}" for m in mentions))
    if prefix == "mn":
        m = conn.execute("SELECT * FROM mentions WHERE id = %s", (target_id,)).fetchone()
        return f"Target mention {m['id']}: \"{m['surface']}\" context {m['context']} → entity {m['entity_id']}" if m else "not found"
    if prefix == "vd":
        v = conn.execute("SELECT * FROM verdicts WHERE id = %s", (target_id,)).fetchone()
        return (f"Target verdict {v['id']} ({v['kind']}, {v['status']}): {v['summary']}\nRationale: {v['rationale']}\n"
                f"Changes: {v['changes']}") if v else "not found"
    if prefix == "vw":
        v = conn.execute("SELECT * FROM views WHERE id = %s", (target_id,)).fetchone()
        if not v:
            return "not found"
        cited = conn.execute("SELECT * FROM claims WHERE id = ANY(%s)", (v["cited_claim_ids"],)).fetchall()
        return (f"Target page/narrative {v['id']} ({v['kind']}): {v['title']}\nL0: {v['l0']}\nL1:\n{v['l1']}\n"
                "Cited claims:\n" + "\n".join("- " + K.format_claim(c) for c in cited))
    return f"Target {target_id} (unknown type)."


def apply_correction(svc: Services, correction_id: str, job_id: int | None = None) -> dict:
    with svc.db.tx() as conn:
        co = conn.execute("SELECT * FROM corrections WHERE id = %s", (correction_id,)).fetchone()
        if not co or co["status"] != "pending":
            return {"skipped": True}
        target = describe_target(conn, co["target_id"])
        owner = K.owner(conn)
    user = f"Owner: {svc.owner_name} ({owner['id'] if owner else '?'})\n\n{target}\n\nCORRECTION BY {svc.owner_name}:\n{co['text']}"
    out = svc.llm.structured("correction", prompts.CORRECT.format(owner=svc.owner_name), user, schemas.CORRECT,
                             ctx=CallContext(source_id=HUMAN_SOURCE, job_id=job_id))
    ops = out["ops"]
    if co["target_id"] and co["target_id"].startswith("vd_") and not any(o["op"] == "revert_verdict" for o in ops):
        # Overriding a verdict: undoes what it did and applies your truth on top.
        with svc.db.tx() as conn:
            K.revert_verdict(conn, K.HUMAN, co["target_id"], status="overridden")
    result = apply_ops(svc, ops, correction_id=correction_id)
    blamed_source = None
    with svc.db.tx() as conn:
        if co["target_id"] and co["target_id"].startswith("cl_"):
            r = conn.execute("SELECT source_id FROM claims WHERE id = %s", (co["target_id"],)).fetchone()
            blamed_source = r["source_id"] if r else None
        conn.execute(
            """UPDATE corrections SET status = 'applied', ops = %s, error_type = %s, blamed_role = %s,
                      blamed_source = %s, result = %s, applied_at = now() WHERE id = %s""",
            (Jsonb(out["ops"]), out["error_type"], out["blamed_role"], blamed_source,
             out["summary"] + " · " + "; ".join(result), correction_id))
    return {"ops": len(out["ops"]), "result": result}


def apply_ops(svc: Services, ops: list[dict], correction_id: str | None = None) -> list[str]:
    """Applies structured operations as `human:owner`. Deterministic."""
    actor = K.HUMAN
    notes: list[str] = []
    embed_later: list[str] = []
    with svc.db.tx() as conn:
        for op in ops:
            kind = op["op"]
            if kind == "replace_claim" and op.get("claim_id"):
                old = conn.execute("SELECT * FROM claims WHERE id = %s", (op["claim_id"],)).fetchone()
                if not old:
                    notes.append(f"claim {op['claim_id']} does not exist")
                    continue
                cid = K.assert_claim(
                    conn, actor, kind=op.get("kind") or old["kind"], subject_mention_id=old["subject_mention_id"],
                    subject_entity_id=old["subject_entity_id"], predicate=op.get("predicate") or old["predicate"],
                    object_value=op.get("value") or old["object_value"], object_mention_id=old["object_mention_id"],
                    object_entity_id=old["object_entity_id"], statement=op.get("statement") or old["statement"],
                    origin="human", source_id=HUMAN_SOURCE, source_label=K.HUMAN, confidence=1.0,
                    extractor_version="correction", evidence_ids=[], supporting_claim_ids=[])
                embed_later.append(cid)
                K.apply_verdict(conn, actor, kind="human_correction", decided_by=K.HUMAN,
                                changes=[{"claim_id": old["id"], "to": "superseded", "by": cid}],
                                summary=f"Corrected by {svc.owner_name}: {op.get('statement') or ''}"[:300],
                                rationale=f"correction {correction_id}")
                notes.append(f"{old['id']} superseded by {cid}")
            elif kind == "retract_claim" and op.get("claim_id"):
                K.apply_verdict(conn, actor, kind="human_correction", decided_by=K.HUMAN,
                                changes=[{"claim_id": op["claim_id"], "to": "retracted"}],
                                summary=f"Retracted by {svc.owner_name}", rationale=f"correction {correction_id}")
                notes.append(f"{op['claim_id']} retracted")
            elif kind == "relink_mention" and op.get("mention_id"):
                target = op.get("entity_id")
                if not target and op.get("new_entity"):
                    target = K.create_entity(conn, actor, name=op["new_entity"]["name"], type=op["new_entity"]["type"],
                                             origin="anchored")
                if target or op.get("new_entity") is None:
                    K.relink_mention(conn, actor, op["mention_id"], target, f"correction {correction_id}", confidence=1.0)
                    notes.append(f"mention {op['mention_id']} → {target}")
            elif kind == "update_entity" and op.get("entity_id"):
                ent = K.entity(conn, op["entity_id"])
                if not ent:
                    continue
                aliases = sorted(set(ent["aliases"]) | set(op.get("add_aliases") or [])) if op.get("add_aliases") else None
                K.update_entity(conn, actor, op["entity_id"], name=op.get("name"), type=op.get("type"), aliases=aliases)
                notes.append(f"entity {op['entity_id']} updated")
            elif kind == "merge_entities" and op.get("entity_id") and op.get("into_entity_id"):
                n = K.merge_entities(conn, actor, op["entity_id"], op["into_entity_id"], f"correction {correction_id}")
                notes.append(f"{op['entity_id']} merged into {op['into_entity_id']} ({n} mentions)")
            elif kind == "revert_verdict" and op.get("verdict_id"):
                ok = K.revert_verdict(conn, actor, op["verdict_id"], status="reverted")
                notes.append(f"verdict {op['verdict_id']} {'reverted' if ok else 'not reverted'}")
            elif kind == "undo_merge" and op.get("entity_id"):
                n = K.unmerge_entity(conn, actor, op["entity_id"])
                notes.append(f"merge of {op['entity_id']} undone ({n} items given back)")
            elif kind == "new_claim" and op.get("entity_id") and op.get("statement"):
                cid = K.assert_claim(
                    conn, actor, kind=op.get("kind") or "state", subject_entity_id=op["entity_id"],
                    predicate=op.get("predicate") or "fact", object_value=op.get("value"),
                    statement=op["statement"], origin="human", source_id=HUMAN_SOURCE, source_label=K.HUMAN,
                    confidence=1.0, extractor_version="correction", evidence_ids=[], supporting_claim_ids=[])
                embed_later.append(cid)
                K.invalidate(conn, entity_ids=[op["entity_id"]], claim_ids=[cid])
                notes.append(f"new claim {cid}")
    if embed_later:
        with svc.db.tx() as conn:
            rows = conn.execute("SELECT id, statement FROM claims WHERE id = ANY(%s)", (embed_later,)).fetchall()
        vecs = svc.embedder.embed([r["statement"] for r in rows])
        with svc.db.tx() as conn:
            for r, v in zip(rows, vecs):
                conn.execute("UPDATE claims SET embedding = %s WHERE id = %s", (np.array(v, dtype=np.float32), r["id"]))
    return notes


def record_direct(svc: Services, target_id: str, text: str, ops: list[dict], error_type: str, blamed_role: str) -> str:
    """Correction made by a web button: already structured (no LLM), recorded for metrics and applied
    by the cognition queue — the only writer of knowledge."""
    cid = new_id("co")
    with svc.db.tx() as conn:
        blamed_source = None
        if target_id.startswith("cl_"):
            r = conn.execute("SELECT source_id FROM claims WHERE id = %s", (target_id,)).fetchone()
            blamed_source = r["source_id"] if r else None
        conn.execute(
            """INSERT INTO corrections (id, target_id, text, via, status, ops, error_type, blamed_role, blamed_source)
               VALUES (%s, %s, %s, 'web', 'pending', %s, %s, %s, %s)""",
            (cid, target_id, text, Jsonb(ops), error_type, blamed_role, blamed_source))
        enqueue(conn, "apply_ops", "cognition", {"correction_id": cid}, priority=10)
    return cid


def apply_direct(svc: Services, correction_id: str) -> dict:
    with svc.db.tx() as conn:
        co = conn.execute("SELECT * FROM corrections WHERE id = %s", (correction_id,)).fetchone()
    if not co or co["status"] != "pending":
        return {"skipped": True}
    notes = apply_ops(svc, co["ops"], correction_id=correction_id)
    svc.db.execute("UPDATE corrections SET status = 'applied', result = %s, applied_at = now() WHERE id = %s",
                   ("; ".join(notes), correction_id))
    return {"result": notes}
