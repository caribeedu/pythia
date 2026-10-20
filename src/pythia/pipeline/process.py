"""Processing of a settled unit: triage → extraction → resolution → integration.

Each judgment stage is an LLM call with closed output. The code only
assembles what the LLM sees and records what it decided.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from .. import knowledge as K
from psycopg.types.json import Jsonb

from ..llm.client import CallContext, LLMError
from ..services import Services
from . import prompts, schemas

EXTRACTOR_VERSION = f"extraction-{prompts.VERSION}"
_REF = re.compile(r"E(\d+)")


def render_unit(unit: dict, evidence: list[dict], owner: str,
                context: list[dict] | None = None) -> tuple[str, dict[str, str]]:
    """Unit text with [E#] labels and the label → evidence_id map. `context` is the rest of the
    conversation (Slack, chat): it appears first, unlabeled, only for understanding — nothing is extracted or cited from it."""
    labels: dict[str, str] = {}
    lines = [f"Unit: {unit['title'] or '(untitled)'}"]
    meta = unit.get("meta") or {}
    sl = meta.get("slack")
    if sl:
        where = {"channel": "channel", "private": "private channel", "dm": "direct message", "group": "group conversation"}.get(
            sl.get("conversation") or "channel", "channel")
        lines.append(f"Slack · {where} {sl.get('name')} · {'thread' if sl.get('kind') == 'thread' else 'channel messages'}"
                     f" {sl.get('permalink') or ''}"
                     + (" · only what is new or changed since the last read" if meta.get("delta") else ""))
    cf = meta.get("confluence")
    if cf:
        what = "page comments" if cf.get("kind") == "comments" else f"page (version {cf.get('version')})"
        lines.append(f"Confluence · space {cf.get('space', '?')} · {what} «{cf.get('title')}» {cf.get('url', '')}"
                     + (" · only the sections new or changed in this version" if meta.get("delta") else ""))
    if meta.get("live"):
        lines.append(f"Live read of {meta['live'].get('locator')} (made by the sweep or the adjudicator)")
    if context:
        lines.append("\nCONTEXT (already read before; only for understanding — do not extract or cite from here):")
        who = {"user": f"{owner} (user)", "assistant": "AI assistant"}
        for ev in context:
            author = who.get(ev.get("role")) or ev.get("author")
            lines.append(f"- {ev['occurred_at']:%Y-%m-%d %H:%M} · " + (f"{author}: " if author else "") + ev["content"])
        lines.append("\nNEW (extract from here):")
    if meta.get("cwd"):
        lines.append(f"Working directory: {meta['cwd']} (branch {meta.get('branch') or '?'})")
    if meta.get("repo"):
        state = f"merged on {meta['merged_at'][:10]}" if meta.get("merged_at") else meta.get("state")
        lines.append(f"Pull request {meta['repo']}#{meta['number']} ({state}) {meta.get('url', '')}"
                     + (" · archived repository" if meta.get("archived") else "")
                     + (" · only what is new since the last read" if meta.get("delta") else ""))
    lines.append("")
    for i, ev in enumerate(evidence, 1):
        label = f"E{i}"
        labels[label] = ev["id"]
        who = {"user": f"{owner} (user)", "assistant": "AI assistant", "human": f"{owner}",
               "artifact": "PR diff", "catalog": "incident.io catalog",
               "message": "Slack", "reply": "Slack, in the thread", "page": "Confluence page",
               "comment": "Confluence comment", "live_read": "live read"}.get(
            ev["role"], f"{ev['author']} ({ev['role']})")
        lines.append(f"[{label}] {ev['occurred_at']:%Y-%m-%d %H:%M} · {who}:\n{ev['content']}\n")
    return "\n".join(lines), labels


def _refs(refs: list[str], labels: dict[str, str]) -> list[str]:
    out = []
    for r in refs:
        m = _REF.search(r)
        if m and f"E{m.group(1)}" in labels:
            out.append(labels[f"E{m.group(1)}"])
    return sorted(set(out))


def _iso(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).isoformat()
    except ValueError:
        return None


def process_unit(svc: Services, unit_id: int, job_id: int | None = None) -> dict:
    """Processes a unit. In backfill: triage and extraction go to a batch and the
    regeneration of derived items for the touched entities is deferred to the end of the backfill."""
    from . import batch

    with svc.db.tx() as conn:
        unit = conn.execute("SELECT id, source_id, occurred_at FROM units WHERE id = %s", (unit_id,)).fetchone()
        backfill = bool(unit) and batch.unit_is_backfill(conn, unit)
    token = K.DEFER.set(unit["source_id"]) if backfill else None
    try:
        return _process_unit(svc, unit_id, job_id, backfill)
    finally:
        if token is not None:
            K.DEFER.reset(token)


def _process_unit(svc: Services, unit_id: int, job_id: int | None, backfill: bool) -> dict:
    from . import batch

    db = svc.db
    owner_name = svc.owner_name
    with db.tx() as conn:
        unit = conn.execute(
            "SELECT u.*, s.kind AS source_kind FROM units u JOIN sources s ON s.id = u.source_id WHERE u.id = %s",
            (unit_id,)).fetchone()
        if unit is None or unit["status"] != "pending":
            return {"skipped": True}
        evidence = conn.execute(
            "SELECT * FROM evidence WHERE unit_id = %s AND status = 'active' ORDER BY occurred_at, seq",
            (unit_id,)).fetchall()
    if not evidence:
        db.execute("UPDATE units SET status = 'processed', processed_at = now() WHERE id = %s", (unit_id,))
        return {"claims": 0}

    # Proof read live by the sweep: its processing also counts against the sweep budget.
    billed = ((unit["meta"] or {}).get("live") or {}).get("billed_to")
    ctx = CallContext(source_id=billed or unit["source_id"], job_id=job_id)
    context = None
    profile_only = bool((unit["meta"] or {}).get("profile_only"))
    if profile_only:
        from ..connectors.claude_code import profile_context
        context = profile_context(unit, evidence)  # read from the transcript now; neither stored nor citable
    elif (unit["meta"] or {}).get("slack") or (unit["meta"] or {}).get("confluence") or unit["kind"] == "session_delta":
        if (unit["meta"] or {}).get("slack"):
            from ..connectors.slack import unit_context
        elif (unit["meta"] or {}).get("confluence"):
            from ..connectors.confluence import unit_context
        else:
            from ..connectors.claude_code import unit_context
        with db.tx() as conn:
            context = unit_context(conn, unit, evidence)
            # Records what served as context: erasing one of those messages reprocesses this unit.
            conn.execute("UPDATE units SET meta = meta || %s WHERE id = %s",
                         (Jsonb({"context_ids": [c["id"] for c in context]}), unit_id))
    text, labels = render_unit(unit, evidence, owner_name, context)
    human = unit["source_kind"] == "human"
    # Each stage's results stay on the unit: a retry continues where it stopped,
    # without repeating LLM calls or duplicating mentions.
    stage = dict((unit["meta"] or {}).get("pipeline") or {})

    def save_stage(conn, **values):
        stage.update(values)
        conn.execute("UPDATE units SET meta = jsonb_set(meta, '{pipeline}', %s) WHERE id = %s",
                     (Jsonb(stage), unit_id))

    def mark(step: str) -> None:
        """Current stage and since when (for "Processing now"). Plumbing: only records."""
        db.execute("UPDATE units SET meta = meta || %s WHERE id = %s",
                   (Jsonb({"now": {"stage": step, "since": datetime.now(timezone.utc).isoformat()}}), unit_id))

    def call(kind: str, system: str, schema: dict, max_tokens: int) -> dict:
        if backfill:
            return batch.staged_call(svc, kind=kind, role=kind, unit=unit, system=system, user=text, schema=schema,
                                     max_tokens=max_tokens, ctx=ctx)
        return svc.llm.structured(kind, system, text, schema, max_tokens=max_tokens, ctx=ctx)

    # 1. triage (human sources always pass: it is you saying something on purpose)
    if not human and unit["triage"] is None:
        mark("triage")
        triage = call("triage", prompts.TRIAGE.format(owner=owner_name), schemas.TRIAGE, 1024)
        db.execute("UPDATE units SET triage = %s WHERE id = %s", (Jsonb(triage), unit_id))
        if not triage["durable"]:
            db.execute("UPDATE units SET status = 'triaged_out', processed_at = now() WHERE id = %s", (unit_id,))
            return {"triaged_out": True}

    # 2. extraction
    if "extracted" not in stage:
        mark("extraction")
        system = (prompts.EXTRACT_PROFILE if profile_only else prompts.EXTRACT).format(owner=owner_name)
        extracted = call("extraction", system, schemas.EXTRACT, 16000)["claims"]
        if profile_only:
            # Profile source: only the owner's preferences get in (the rest of the chat is not an enabled source).
            extracted = [c for c in extracted if c["kind"] == "profile"]
        for i, c in enumerate(extracted):
            if not _refs(c["evidence_refs"], labels):
                error = f"extraction: claim {i + 1} without valid evidence ({c['evidence_refs']})"
                if backfill:
                    batch.reject_result(svc, unit_id, "extraction", error)
                raise LLMError(error)
        with db.tx() as conn:
            save_stage(conn, extracted=extracted)
    extracted = stage["extracted"]
    if not extracted:
        db.execute("UPDATE units SET status = 'processed', processed_at = now() WHERE id = %s", (unit_id,))
        return {"claims": 0}

    # 3. entity resolution (records mentions and the map in the same transaction)
    if "resolved" not in stage:
        mark("resolution")
        resolve_mentions(svc, extracted, labels, unit, ctx, save_stage)
    resolved = {tuple(k.split(":")[0:1]) + (int(k.split(":")[1]),): v for k, v in stage["resolved"].items()}

    # 4. integration (marks the unit as processed in the same transaction as the writes)
    candidates = []
    for i, c in enumerate(extracted):
        subject = resolved.get(("s", i))
        if not subject or not subject["entity_id"]:
            continue  # the subject is not an entity of the work world → there is no claim
        obj = resolved.get(("o", i))
        candidates.append({
            "ref": f"N{len(candidates) + 1}",
            "kind": c["kind"],
            "subject_mention_id": subject["mention_id"],
            "subject_entity_id": subject["entity_id"],
            "predicate": c["predicate"],
            "object_value": c["object_value"],
            "object_mention_id": obj["mention_id"] if obj else None,
            "object_entity_id": obj["entity_id"] if obj else None,
            "statement": c["statement"],
            "valid_from": _iso(c["valid_from"]),
            "valid_to": _iso(c["valid_to"]),
            "origin": "human" if human else c["origin"],
            "source_id": unit["source_id"],
            "source_label": K.HUMAN if human else unit["source_kind"],
            "confidence": 1.0 if human else c["confidence"],
            "extractor_version": EXTRACTOR_VERSION,
            "evidence_ids": _refs(c["evidence_refs"], labels),
        })
    mark("integration")
    asserted = integrate(svc, candidates, ctx, actor="integration", unit_id=unit_id)
    return {"claims": len(asserted), "candidates": len(candidates)}


# ---------------------------------------------------------------- resolution


def resolve_mentions(svc: Services, extracted: list[dict], labels: dict[str, str], unit: dict,
                     ctx: CallContext, save_stage=None) -> dict[tuple[str, int], dict]:
    """Resolves the subject/object of each claim. Returns {(s|o, i): {mention_id, entity_id}}."""
    owner_name = svc.owner_name
    # Identical mentions (same text, type and context) in the same unit are decided once.
    unique: dict[tuple, dict] = {}
    occurrences: list[tuple[tuple[str, int], tuple, dict]] = []
    for i, c in enumerate(extracted):
        for slot, m in (("s", c["subject"]), ("o", c.get("object_entity"))):
            if not m:
                continue
            key = (m["surface"].strip().lower(), m["type_hint"], m["context"].strip().lower())
            if key not in unique:
                unique[key] = {"ref": f"M{len(unique) + 1}", **m,
                               "evidence_id": (_refs(c["evidence_refs"], labels) or [None])[0]}
            occurrences.append(((slot, i), key, m))
    if not unique:
        if save_stage:
            with svc.db.tx() as conn:
                save_stage(conn, resolved={})
        return {}

    with svc.db.tx() as conn:
        owner = K.owner(conn)
        blocks = []
        cand_map: dict[str, list[dict]] = {}
        for item in unique.values():
            cands = svc.retriever.entity_candidates(conn, item["surface"], item["context"])
            glossary = svc.retriever.glossary_for(conn, item["surface"])
            for g in glossary:  # the glossary is the alias table
                for eid in (g["subject_entity_id"], g["object_entity_id"]):
                    if eid and eid not in {c["id"] for c in cands}:
                        ent = K.entity(conn, eid)
                        if ent and ent["status"] == "active":
                            cands.append(ent)
            if owner and owner["id"] not in {c["id"] for c in cands}:
                cands.append(owner)
            cand_map[item["ref"]] = cands
            lines = [f"{item['ref']}: \"{item['surface']}\" (suggested type: {item['type_hint']})",
                     f"  context: {item['context']}"]
            lines.extend(f"  glossary: {g['statement']}" for g in glossary)
            lines.append("  candidates:")
            for cnd in cands:
                aliases = ", ".join(cnd["aliases"]) if cnd["aliases"] else "-"
                desc = (cnd.get("description") or "")[:160]
                lines.append(f"   - {cnd['id']} · {cnd['name']} ({cnd['type']}) aliases: {aliases} {desc}")
            if not cands:
                lines.append("   (none)")
            blocks.append("\n".join(lines))
    user = f"Owner: {owner_name} ({owner['id'] if owner else '?'})\n\n" + "\n\n".join(blocks)
    decisions = svc.llm.structured("resolution", prompts.RESOLVE.format(owner=owner_name), user, schemas.RESOLVE,
                                   max_tokens=4096, ctx=ctx)["decisions"]
    by_ref = {d["ref"]: d for d in decisions}
    shown_ids = {c["id"] for cands in cand_map.values() for c in cands}
    # No fallback: a missing or invalid decision is an LLM error, not a "default".
    for item in unique.values():
        d = by_ref.get(item["ref"])
        if d is None:
            raise LLMError(f"resolution did not decide {item['ref']}")
        # Any entity the LLM saw as a candidate in the request is valid (another mention may have
        # brought the right entity); an ID it did not see is still an error.
        if d["choice"] == "candidate" and d.get("entity_id") not in shown_ids:
            raise LLMError(f"resolution chose a nonexistent candidate for {item['ref']}: {d.get('entity_id')}")
        if d["choice"] == "new" and not d.get("new_entity"):
            raise LLMError(f"resolution marked {item['ref']} as new without describing the entity")

    result: dict[tuple[str, int], dict] = {}
    with svc.db.tx() as conn:
        entity_for_key: dict[tuple, str | None] = {}
        new_entities: list[str] = []
        created_here: dict[tuple[str, str], str] = {}
        for key, item in unique.items():
            d = by_ref.get(item["ref"])
            valid_ids = shown_ids
            entity_id = None
            if d and d["choice"] == "candidate" and d.get("entity_id") in valid_ids:
                entity_id = d["entity_id"]
            elif d and d["choice"] == "new" and d.get("new_entity"):
                ne = d["new_entity"]
                # Only reuses the new entity created by another mention of THIS unit (same LLM decision).
                key_new = (ne["name"].strip().lower(), ne["type"])
                if key_new in created_here:
                    entity_id = created_here[key_new]
                else:
                    entity_id = K.create_entity(conn, "resolution", name=ne["name"], type=ne["type"],
                                                origin="discovered", aliases=[*ne["aliases"], item["surface"]],
                                                description=ne["description"])
                    created_here[key_new] = entity_id
                    new_entities.append(entity_id)
            entity_for_key[key] = entity_id
        for slot_i, key, m in occurrences:
            item = unique[key]
            d = by_ref.get(item["ref"])
            mid = K.record_mention(
                conn, "resolution", surface=m["surface"], context=m["context"], type_hint=m["type_hint"],
                entity_id=entity_for_key[key], confidence=d["confidence"] if d else None,
                evidence_id=item["evidence_id"], unit_id=unit["id"],
                candidates=[c["id"] for c in cand_map[item["ref"]]])
            result[slot_i] = {"mention_id": mid, "entity_id": entity_for_key[key]}
        if save_stage:
            save_stage(conn, resolved={f"{k[0]}:{k[1]}": v for k, v in result.items()})
    if new_entities:
        embed_entities(svc, new_entities)
    return result


def embed_entities(svc: Services, entity_ids: list[str]) -> None:
    with svc.db.tx() as conn:
        rows = conn.execute("SELECT id, name, type, description FROM entities WHERE id = ANY(%s)",
                            (entity_ids,)).fetchall()
    texts = [f"{r['name']} ({r['type']}): {r['description'] or ''}" for r in rows]
    import numpy as np
    vecs = svc.embedder.embed(texts)
    with svc.db.tx() as conn:
        for r, v in zip(rows, vecs):
            conn.execute("UPDATE entities SET embedding = %s WHERE id = %s", (np.array(v, dtype=np.float32), r["id"]))


# ---------------------------------------------------------------- integration


def integrate(svc: Services, candidates: list[dict], ctx: CallContext, actor: str,
              unit_id: int | None = None) -> list[str]:
    """Confronts new claims with the existing neighborhood and applies the LLM's decision."""
    if not candidates:
        if unit_id is not None:
            svc.db.execute("UPDATE units SET status = 'processed', processed_at = now() WHERE id = %s", (unit_id,))
        return []
    import numpy as np

    owner_name = svc.owner_name
    with svc.db.tx() as conn:
        existing: dict[str, dict] = {}
        subject_blocks = []
        for sid in sorted({c["subject_entity_id"] for c in candidates}):
            ent = K.entity(conn, sid)
            rows = K.entity_claims(conn, sid, limit=40)
            for r in rows:
                existing[r["id"]] = r
            subject_blocks.append(
                f"Subject {sid} · {ent['name']} ({ent['type']}):\n" +
                ("\n".join("  - " + K.format_claim(r) for r in rows) or "  (no claims)"))
        neighbor_lines = []
        for c in candidates:
            for r in svc.retriever.neighbors_for_claim(conn, c["statement"], c["subject_entity_id"], limit=5):
                if r["id"] not in existing:
                    existing[r["id"]] = r
                    neighbor_lines.append(f"  - [{r['subject_name']}] " + K.format_claim(r))
    new_lines = [
        f"{c['ref']} ({c['kind']}, subject {c['subject_entity_id']}, predicate {c['predicate']}, "
        f"origin {c['origin']}, source {c['source_label']}): {c['statement']}"
        + (f" [valid {c['valid_from'] or '?'} → {c['valid_to'] or '…'}]" if c["valid_from"] or c["valid_to"] else "")
        for c in candidates
    ]
    user = ("EXISTING KNOWLEDGE\n" + "\n\n".join(subject_blocks) +
            ("\n\nSemantic neighbors on other entities:\n" + "\n".join(neighbor_lines) if neighbor_lines else "") +
            "\n\nNEW CLAIMS\n" + "\n".join(new_lines))
    system = prompts.INTEGRATE.format(owner=owner_name, authority_policy=svc.settings.get("authority_policy"))
    decisions = {d["ref"]: d for d in svc.llm.structured("integration", system, user, schemas.INTEGRATE,
                                                         ctx=ctx)["decisions"]}
    # Valid target: an existing claim shown or another NEW claim of this batch (by ref, e.g. N3).
    refs = {c["ref"] for c in candidates}

    def targets_of(c: dict) -> list[str]:
        return [t for t in decisions[c["ref"]]["target_claim_ids"] if t in existing or (t in refs and t != c["ref"])]

    for c in candidates:
        d = decisions.get(c["ref"])
        if d is None:
            raise LLMError(f"integration did not decide {c['ref']}")
        # A tension with no target is legitimate: the new claim looks odd by itself (e.g. the source is probably outdated).
        if d["decision"] not in ("new", "tension") and not targets_of(c):
            raise LLMError(f"integration decided '{d['decision']}' for {c['ref']} without a valid existing target "
                           f"(cited {d['target_claim_ids']})")

    vectors = svc.embedder.embed([c["statement"] for c in candidates])
    asserted: list[str] = []
    by_ref: dict[str, str] = {}   # ref of the new claim → claim that represents it (itself, or the one it corroborates)
    from .. import events
    with svc.db.tx() as conn:
        # 1st pass: records the new claims (duplicates of something existing only corroborate).
        pending = []
        for c, vec in zip(candidates, vectors):
            d = decisions[c["ref"]]
            if d["decision"] == "duplicate" and c["origin"] == "human":
                # You said the same as the machine: your claim takes over, with maximum authority.
                d = {**d, "decision": "supersedes"}
            on_existing = [t for t in targets_of(c) if t in existing]
            if d["decision"] == "duplicate" and on_existing:
                events.append(conn, "claim_corroborated", actor, {"id": on_existing[0], "evidence_ids": c["evidence_ids"]})
                by_ref[c["ref"]] = on_existing[0]
                continue
            if d["decision"] == "duplicate":
                pending.append((c, d, None))   # duplicate of another new claim: resolved later
                continue
            fields = {k: v for k, v in c.items() if k != "ref"}
            if d["predicate"]:
                fields["predicate"] = d["predicate"]
            cid = K.assert_claim(conn, actor, **fields)
            conn.execute("UPDATE claims SET embedding = %s WHERE id = %s", (np.array(vec, dtype=np.float32), cid))
            asserted.append(cid)
            by_ref[c["ref"]] = cid
            pending.append((c, d, cid))
        # 2nd pass: relations, now with the batch's new claims resolved.
        for c, d, cid in pending:
            targets = list(dict.fromkeys(by_ref.get(t, t) for t in targets_of(c) if t in existing or t in by_ref))
            targets = [t for t in targets if t != cid]
            if cid is None:
                # Duplicate between new claims: corroborates the other (if it was also a duplicate in a cycle, it has no target and is recorded).
                if targets:
                    events.append(conn, "claim_corroborated", actor, {"id": targets[0], "evidence_ids": c["evidence_ids"]})
                    by_ref[c["ref"]] = targets[0]
                    continue
                fields = {k: v for k, v in c.items() if k != "ref"}
                if d["predicate"]:
                    fields["predicate"] = d["predicate"]
                cid = K.assert_claim(conn, actor, **fields)
                conn.execute("UPDATE claims SET embedding = %s WHERE id = %s",
                             (np.array(vectors[candidates.index(c)], dtype=np.float32), cid))
                asserted.append(cid)
                by_ref[c["ref"]] = cid
            elif d["decision"] == "supersedes" and targets:
                human = c["origin"] == "human"
                K.apply_verdict(conn, K.HUMAN if human else actor,
                                kind="human_correction" if human else "temporal_supersession",
                                decided_by=K.HUMAN if human else "integration",
                                changes=[{"claim_id": t, "to": "superseded", "by": cid} for t in targets],
                                summary=f"Superseded by {cid}: {c['statement'][:160]}",
                                rationale=d["reason"], cited_evidence=c["evidence_ids"],
                                model=svc.settings.model_for("integration"), prompt_version=prompts.VERSION)
            elif d["decision"] == "tension":
                K.open_tension(conn, actor, [cid, *targets], d["reason"])
            K.invalidate(conn, entity_ids=[c["subject_entity_id"], c.get("object_entity_id")], claim_ids=[cid])
            # New evidence about an entity with a reopened tension: the dossier changed, it can be judged again.
            K.rejudge_open_tensions(conn, [c["subject_entity_id"], c.get("object_entity_id")])
        if unit_id is not None:
            conn.execute("UPDATE units SET status = 'processed', processed_at = now() WHERE id = %s", (unit_id,))
    return asserted
