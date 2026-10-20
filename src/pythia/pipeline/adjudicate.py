"""Adjudication of tensions by an agent with rich tools and live access to the source."""

from __future__ import annotations

import json
from pathlib import Path

from .. import knowledge as K
from ..ids import sha
from ..llm.client import AgentTool, CallContext, LLMError, schema_object
from ..services import Services
from . import prompts, schemas

STR = {"type": "string"}


def dossier(conn, tension: dict) -> tuple[str, str]:
    """Initial dossier (deterministic search) and its hash for the verdict cache."""
    claims = conn.execute(
        """SELECT c.*, s.name AS subject_name FROM claims c LEFT JOIN entities s ON s.id = c.subject_entity_id
           WHERE c.id = ANY(%s) ORDER BY c.recorded_at""", (tension["claim_ids"],)).fetchall()
    lines = [f"Tension {tension['id']}: {tension['reason']}", ""]
    basis = []
    d = tension.get("dossier") if isinstance(tension.get("dossier"), dict) else {}
    if d:
        # the detector already investigated. The hypothesis is an ACCUSATION to verify, not a conclusion.
        lines += [f"DETECTOR DOSSIER ({d.get('detector', '?')}) — treat the hypothesis as an accusation to verify, "
                  "not as a conclusion; check the evidence and decide by what it shows.",
                  f"Hypothesis: {d.get('hypothesis', '')}", f"Reason: {d.get('reason', '')}", "Evidence collected:"]
        for ev_id in d.get("evidence_ids") or []:
            ev = conn.execute("SELECT id, occurred_at, role, status, content, meta FROM evidence WHERE id = %s",
                              (ev_id,)).fetchone()
            if not ev or ev["content"] is None:
                lines.append(f"- {ev_id}: (erased at the source)")
                continue
            tag = (f"previous version, superseded in version {ev['meta'].get('superseded_by_version')}"
                   if ev["status"] == "superseded" else "current")
            lines.append(f"\n[{ev['id']} · {ev['occurred_at']:%Y-%m-%d %H:%M} · {ev['role']} · {tag}]\n{ev['content'][:20000]}")
            basis.append(f"{ev['id']}:{ev['status']}")
        lines.append("")
    lines.append("Claims in tension:")
    entity_ids = set()
    for c in claims:
        lines.append(f"- [{c['subject_name']}] " + K.format_claim(c) + f" · evidence: {', '.join(c['evidence_ids'])}")
        basis.append(f"{c['id']}:{c['status']}:{','.join(sorted(c['evidence_ids']))}")
        entity_ids.update(e for e in (c["subject_entity_id"], c["object_entity_id"]) if e)
    for eid in sorted(entity_ids):
        ent = K.entity(conn, eid)
        others = [r for r in K.entity_claims(conn, eid, limit=25) if r["id"] not in tension["claim_ids"]]
        lines.append(f"\nOther current claims of {ent['name']} ({eid}):")
        lines.extend("- " + K.format_claim(r) for r in others)
        basis.extend(f"{r['id']}:{r['status']}" for r in others)
    for c in claims:
        for ev_id in c["evidence_ids"][:3]:
            ev = conn.execute("SELECT id, occurred_at, role, content FROM evidence WHERE id = %s", (ev_id,)).fetchone()
            if ev and ev["content"]:
                lines.append(f"\nEvidence {ev['id']} ({ev['occurred_at']:%Y-%m-%d %H:%M}, {ev['role']}):\n{ev['content'][:1500]}")
    return "\n".join(lines), sha("|".join(sorted(basis)))


def corrected_examples(conn, limit: int = 8) -> str:
    """Automatic verdicts the owner reverted or overrode, with his correction: they become examples."""
    rows = conn.execute(
        """SELECT v.id, v.kind, v.summary, v.rationale, v.status,
                  (SELECT string_agg(c.text, ' | ') FROM corrections c WHERE c.target_id = v.id) AS correction
           FROM verdicts v WHERE v.status <> 'applied' AND v.decided_by <> 'human:owner'
           ORDER BY v.reverted_at DESC NULLS LAST LIMIT %s""", (limit,)).fetchall()
    lines = []
    for r in rows:
        why = r["correction"] or "reverted without comment"
        lines.append(f"- {r['id']} ({r['kind']}): you decided \"{r['summary']}\" because \"{r['rationale']}\" — "
                     f"{'reverted' if r['status'] == 'reverted' else 'overridden'} by the owner: {why}")
    return "\n".join(lines)


def tools(svc: Services) -> list[AgentTool]:
    db = svc.db

    def view_claim(inp):
        with db.tx() as conn:
            c = conn.execute("SELECT * FROM claims WHERE id = %s", (inp["claim_id"],)).fetchone()
            if not c:
                return "not found"
            hist = conn.execute("SELECT status, from_ts, to_ts, verdict_id FROM claim_status_history WHERE claim_id = %s ORDER BY from_ts",
                                (c["id"],)).fetchall()
            return {"claim": K.format_claim(c), "evidence": c["evidence_ids"], "history": hist,
                    "supports": c["supporting_claim_ids"]}

    def view_evidence(inp):
        with db.tx() as conn:
            ev = conn.execute("SELECT * FROM evidence WHERE id = %s", (inp["evidence_id"],)).fetchone()
            if not ev:
                return "not found"
            around = conn.execute(
                """SELECT id, occurred_at, role, left(content, 1200) AS content FROM evidence
                   WHERE stream_id = %s AND status = 'active' AND occurred_at BETWEEN %s - interval '2 hours' AND %s + interval '2 hours'
                   ORDER BY occurred_at, seq LIMIT 12""", (ev["stream_id"], ev["occurred_at"], ev["occurred_at"])).fetchall()
            return {"evidence": {"id": ev["id"], "when": ev["occurred_at"], "role": ev["role"],
                                 "content": ev["content"]}, "context_at_source": around}

    def read_source(inp):
        """Live read of the original source (transcript on disk) around the evidence."""
        with db.tx() as conn:
            ev = conn.execute("SELECT e.*, s.locator FROM evidence e LEFT JOIN streams s ON s.id = e.stream_id WHERE e.id = %s",
                              (inp["evidence_id"],)).fetchone()
        if not ev or not ev["locator"]:
            return "source unavailable"
        if (ev["source_id"] or "").startswith("slack:"):
            return _read_slack(db, ev)
        if (ev["source_id"] or "").startswith("confluence:"):
            return _read_confluence(db, ev)
        path = Path(ev["locator"])
        if not path.exists():
            return "the source no longer exists (removed at the origin)"
        lines = path.read_text(errors="replace").splitlines()
        idx = next((i for i, l in enumerate(lines) if ev["external_id"] and ev["external_id"] in l), None)
        if idx is None:
            return "message not found in the current source"
        out = []
        for l in lines[max(0, idx - 6): idx + 7]:
            try:
                d = json.loads(l)
            except json.JSONDecodeError:
                continue
            msg = d.get("message") or {}
            content = msg.get("content")
            if isinstance(content, list):
                content = " ".join(b.get("text", "") for b in content if b.get("type") == "text")
            if content:
                out.append(f"{d.get('timestamp')} {d.get('type')}: {str(content)[:800]}")
        return "\n".join(out) or "no text around it"

    def entity_claims(inp):
        statuses = tuple(K.CLAIM_STATUSES) if inp["include_history"] else K.CURRENT
        with db.tx() as conn:
            return [K.format_claim(r) for r in K.entity_claims(conn, inp["entity_id"], statuses, limit=80)]

    def verdicts(inp):
        with db.tx() as conn:
            return conn.execute(
                """SELECT id, kind, status, summary, rationale, created_at FROM verdicts
                   WHERE claim_ids && ARRAY(SELECT id FROM claims WHERE subject_entity_id = %s OR object_entity_id = %s)
                   ORDER BY created_at DESC LIMIT 15""", (inp["entity_id"], inp["entity_id"])).fetchall()

    def search(inp):
        return svc.retriever.search(inp["query"], limit=10)

    return [
        AgentTool("view_claim", "Details, status history and evidence of a claim.",
                  schema_object({"claim_id": STR}), view_claim),
        AgentTool("view_evidence", "Content of a piece of evidence and the surrounding messages in the same source.",
                  schema_object({"evidence_id": STR}), view_evidence),
        AgentTool("read_source", "Reads the original source LIVE around a piece of evidence (confirms whether it still exists).",
                  schema_object({"evidence_id": STR}), read_source),
        AgentTool("entity_claims", "Claims of an entity; include_history also brings superseded and retracted ones.",
                  schema_object({"entity_id": STR, "include_history": {"type": "boolean"}}), entity_claims),
        AgentTool("entity_verdicts", "Previous verdicts involving claims of an entity.",
                  schema_object({"entity_id": STR}), verdicts),
        AgentTool("search", "Hybrid search over pages, narratives, claims and entities.",
                  schema_object({"query": STR}), search),
    ]


def adjudicate(svc: Services, tension_id: str, job_id: int | None = None) -> dict:
    from .batch import source_in_backfill

    with svc.db.tx() as conn:
        t = conn.execute("SELECT claim_ids FROM tensions WHERE id = %s", (tension_id,)).fetchone()
        sources = [r["source_id"] for r in conn.execute(
            "SELECT DISTINCT source_id FROM claims WHERE id = ANY(%s) AND source_id IS NOT NULL",
            (t["claim_ids"] if t else [],)).fetchall()]
        in_backfill = next((s for s in sources if source_in_backfill(conn, s)), None)
    token = K.DEFER.set(in_backfill) if in_backfill else None
    try:
        return _adjudicate(svc, tension_id, job_id)
    finally:
        if token is not None:
            K.DEFER.reset(token)


def _adjudicate(svc: Services, tension_id: str, job_id: int | None = None) -> dict:
    with svc.db.tx() as conn:
        tension = conn.execute("SELECT * FROM tensions WHERE id = %s", (tension_id,)).fetchone()
        if not tension or tension["status"] != "open":
            return {"skipped": "tension is not open"}
        text, dossier_hash = dossier(conn, tension)
        # Same dossier already judged by the same model and prompt version: do not re-judge. If you reverted,
        # it holds until the dossier changes, even with another model or prompt.
        cached = conn.execute(
            """SELECT id, status FROM verdicts WHERE tension_id = %s AND dossier_hash = %s
                 AND (status = 'reverted' OR (model = %s AND prompt_version = %s))
               ORDER BY (status = 'reverted') DESC LIMIT 1""",
            (tension_id, dossier_hash, svc.settings.model_for("adjudication"), prompts.VERSION)).fetchone()
        if cached:
            return {"skipped": f"dossier already judged in {cached['id']} ({cached['status']})"}
        first = conn.execute("SELECT source_id FROM claims WHERE id = %s", (tension["claim_ids"][0],)).fetchone()
        lessons = corrected_examples(conn)
    system = prompts.ADJUDICATE.format(owner=svc.owner_name, authority_policy=svc.settings.get("authority_policy"))
    # What the sweep raises counts against its budget, including the judgment.
    ctx = CallContext(source_id="sweep" if tension["opened_by"] == "sweep" else (first["source_id"] if first else None),
                      job_id=job_id)
    user = "INITIAL DOSSIER\n\n" + text
    if lessons:
        user += f"\n\nPREVIOUS JUDGMENTS THAT {svc.owner_name.upper()} CORRECTED (learn his criterion):\n" + lessons
    from .live import LiveReader
    reader = LiveReader(svc, billed_to="sweep" if tension["opened_by"] == "sweep" else None)
    trace: list = []
    result = svc.llm.agent("adjudication", system, user, tools(svc) + reader.tools(), schemas.VERDICT, ctx=ctx, trace=trace)
    # A live read cited as proof becomes stored evidence; the rest of the read is not kept.
    proofs = reader.store_proofs([e for e in result["cited_evidence"] if e.startswith("read:")])
    result["cited_evidence"] = [proofs.get(e, e) for e in result["cited_evidence"] if not e.startswith("read:") or e in proofs]
    # What the agent looked up beyond what the dossier already showed.
    exploration = [t for t in trace if not any(str(v) in text for v in t["input"].values() if isinstance(v, str))]
    valid = set(tension["claim_ids"])
    with svc.db.tx() as conn:
        # the judgment takes minutes. If the dossier changed in the meantime (evidence erased at the
        # source, owner correction), the verdict was thought out over a picture that no longer exists: judge again.
        now = conn.execute("SELECT * FROM tensions WHERE id = %s FOR UPDATE", (tension_id,)).fetchone()
        if not now or now["status"] != "open":
            return {"skipped": "the tension stopped being open during the judgment"}
        if dossier(conn, now)[1] != dossier_hash:
            from ..jobs import enqueue
            enqueue(conn, "adjudicate", "cognition", {"tension_id": tension_id}, priority=50,
                    dedup_key=f"adjudicate:{tension_id}")
            return {"rejudge": "the dossier changed during the judgment; verdict not applied"}
        # The agent may change the status of claims it investigated beyond the tension, as long as they exist.
        ids = [c["claim_id"] for c in result["changes"]]
        by_ids = [c.get("superseded_by") for c in result["changes"] if c.get("superseded_by")]
        known = {r["id"] for r in conn.execute("SELECT id FROM claims WHERE id = ANY(%s)", (ids + by_ids,)).fetchall()}
        # The "superseded by" link only holds for a claim that exists and is not itself.
        changes = [{"claim_id": c["claim_id"], "to": c["to"],
                    **({"by": c["superseded_by"]} if c["to"] == "superseded" and c.get("superseded_by") in known
                       and c.get("superseded_by") != c["claim_id"] else {})}
                   for c in result["changes"] if c["claim_id"] in known]
        unknown = [c["claim_id"] for c in result["changes"] if c["claim_id"] not in known]
        if unknown:
            raise LLMError(f"adjudication cited nonexistent claims: {', '.join(unknown)}")
        vid = K.apply_verdict(conn, "adjudication", kind=result["kind"], decided_by="adjudication", changes=changes,
                              summary=result["summary"], rationale=result["rationale"], tension_id=tension_id,
                              cited_evidence=result["cited_evidence"], confidence=result["confidence"],
                              model=svc.settings.model_for("adjudication"), prompt_version=prompts.VERSION,
                              dossier_hash=dossier_hash, exploration=exploration)
        _close_validity_on_edit(conn, tension, vid, changes)
    return {"verdict_id": vid, "changes": len(changes), "outside_tension": len(set(ids) - valid)}


def _close_validity_on_edit(conn, tension: dict, verdict_id: str, changes: list[dict]) -> None:
    """in a Confluence edit, "evolved" (superseded) means it held until the edit. It is
    plumbing: the date comes from the previous version of the section kept as history."""
    from .. import events
    d = tension.get("dossier") if isinstance(tension.get("dossier"), dict) else {}
    if d.get("detector") != "confluence_edit" or not d.get("evidence_ids"):
        return
    old = conn.execute("SELECT meta FROM evidence WHERE id = %s", (d["evidence_ids"][0],)).fetchone()
    until = (old["meta"] or {}).get("superseded_at") if old else None
    if not until:
        return
    for c in changes:
        if c["to"] != "superseded":
            continue
        prev = conn.execute("SELECT valid_to FROM claims WHERE id = %s", (c["claim_id"],)).fetchone()
        events.append(conn, "claim_validity_set", "adjudication", {
            "id": c["claim_id"], "valid_to": until, "verdict_id": verdict_id,
            "previous": prev["valid_to"].isoformat() if prev and prev["valid_to"] else None})


def _read_slack(db, ev: dict) -> str:
    """Live read of Slack (API + owner token): the thread or surrounding messages, as they are now."""
    from ..connectors import slack
    from ..connectors.common import SourceUnavailable

    if ev["status"] != "active" or not ev["external_id"]:
        return "the evidence was erased at the source"
    _, channel, ts = ev["external_id"].split(":", 2)
    thread_ts = (ev["meta"] or {}).get("thread_ts")
    try:
        client = slack.SlackClient(slack.resolve_token())
        from ..settings import Settings
        return slack.read_live(client, channel, ts, thread_ts, slack.users_cached(db, client), Settings(db).get("slack"))
    except SourceUnavailable as exc:
        return f"Slack unavailable right now: {exc}"


def _read_confluence(db, ev: dict) -> str:
    """Live read of Confluence: the page as it is now (the evidence may be a previous version)."""
    from ..connectors import confluence
    from ..connectors.common import SourceUnavailable
    from ..settings import Settings

    page_id = (ev["meta"] or {}).get("page_id")
    if not page_id:
        return "source unavailable"
    try:
        _url, _title, text = confluence.read_live(confluence.client_from(Settings(db).get("confluence")), page_id)
        return text
    except SourceUnavailable as exc:
        return f"Confluence unavailable right now: {exc}"
