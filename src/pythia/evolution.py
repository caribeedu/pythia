"""Evolution: what changed and why, from most recent to oldest.

Everything here is plumbing over what is already recorded (event log, page versions, units, queue):
group by cause, count, build a text diff. No LLM summary.

- One line per cause: source item, judgment, correction, sweep, source read. A source's backfill
  becomes a single line.
- Regenerated page: its own line, with the L1 diff against the previous version.
- Trajectory of an attribute per entity, through the structured "superseded by" link.
- What produced nothing, with the recorded reason, hidden by default.
- "Updating since T".
"""

from __future__ import annotations

import difflib
import re
from datetime import datetime, timedelta, timezone

from .events import REDACTED

WINDOW = timedelta(days=7)      # each feed page looks at a time window
MAX_ITEMS = 60                  # items shown inside a line (the rest becomes "and N more")
COUNTED = {
    "claim_asserted": "new", "claim_corroborated": "corroborated", "claim_status_changed": "changed status",
    "verdict_applied": "verdicts", "tension_opened": "tensions", "entity_created": "new entities",
    "entity_merged": "merges", "entity_unmerged": "merges undone", "claim_evidence_removed": "evidence removed",
    "claim_validity_set": "validities closed",
}
SOURCE_KINDS = {"claude_code": "chat", "slack": "Slack", "github": "GitHub", "confluence": "Confluence",
                "live": "live read", "incident_io": "incident.io", "human": "you"}
UNIT_KINDS = {
    "session_delta": "chat", "pull_request": "PR", "slack_thread": "thread", "slack_window": "conversation",
    "confluence_page": "page", "confluence_comments": "comments", "live_read": "live read",
}


# ---------------------------------------------------------------- feed


def _group_key_sql() -> str:
    """Line key. Item, judgment, correction and sweep are one line each; a source's backfill
    becomes a single line; periodic reads (source, catalog…) group by hour."""
    return """CASE
        WHEN s.backfill_until IS NOT NULL AND u.occurred_at < s.backfill_until
             AND (s.backfill_done_at IS NULL OR e.ts <= s.backfill_done_at) THEN 'backfill:' || s.id
        WHEN e.eff_cause ~ '^(unit|tension|correction|sweep):' THEN e.eff_cause
        ELSE e.eff_cause || '@' || to_char(date_trunc('hour', e.ts), 'YYYY-MM-DD"T"HH24') END"""


def feed(conn, *, before: datetime | None = None, limit: int = 60, show_noop: bool = False,
         source_id: str | None = None, entity_id: str | None = None, only_verdicts: bool = False) -> dict:
    before = before or datetime.now(timezone.utc) + timedelta(seconds=1)
    since = before - WINDOW
    params: dict = {"before": before, "since": since, "limit": limit}
    filters = []
    if source_id:
        params["src"] = source_id
        filters.append("(s.id = %(src)s OR e.eff_cause IN ('source:' || %(src)s, 'synthesis:' || %(src)s))")
    if entity_id:
        params["refs"] = _entity_refs(conn, entity_id)
        filters.append("(e.payload ->> 'id' = ANY(%(refs)s) OR (e.payload ? 'claim_ids' AND e.payload -> 'claim_ids' ?| %(refs)s))")
    where = "".join(f" AND {f}" for f in filters)
    groups = conn.execute(
        f"""WITH e AS (
                SELECT e.*, {_group_key_sql()} AS gkey, u.id AS unit_id, s.id AS source_id
                FROM events_caused e
                LEFT JOIN units u ON u.id = CASE WHEN e.eff_cause LIKE 'unit:%%' THEN substr(e.eff_cause, 6)::bigint END
                LEFT JOIN sources s ON s.id = u.source_id
                WHERE e.ts < %(before)s AND e.ts >= %(since)s {where})
            SELECT gkey, max(ts) AS ts, min(ts) AS first_ts, min(eff_cause) AS cause,
                   array_agg(DISTINCT unit_id) FILTER (WHERE unit_id IS NOT NULL) AS unit_ids,
                   array_agg(id ORDER BY id) AS event_ids
            FROM e
            GROUP BY gkey
            {"HAVING bool_or(type = 'verdict_applied')" if only_verdicts else ""}
            ORDER BY max(ts) DESC LIMIT %(limit)s""", params).fetchall()
    lines = [_event_line(conn, g) for g in groups]
    if not only_verdicts and not entity_id:
        lines += _page_lines(conn, before, since, source_id, limit)
    elif entity_id:
        lines += _page_lines(conn, before, since, None, limit, entity_id=entity_id)
    if show_noop and not only_verdicts and not entity_id:
        lines += _noop_lines(conn, before, since, source_id, limit)
    lines.sort(key=lambda ln: ln["ts"], reverse=True)
    lines = lines[:limit]
    full = len(lines) == limit
    return {"lines": lines, "names": _names_in(conn, lines),
            "next_before": lines[-1]["ts"] if full else since,
            "window_since": since}


def _entity_refs(conn, entity_id: str) -> list[str]:
    ids = [r["id"] for r in conn.execute(
        "SELECT id FROM claims WHERE subject_entity_id = %s OR object_entity_id = %s", (entity_id, entity_id)).fetchall()]
    return ids + [entity_id]


def _event_line(conn, g: dict) -> dict:
    rows = conn.execute("SELECT id, ts, type, actor, payload FROM events WHERE id = ANY(%s) ORDER BY id",
                        (g["event_ids"],)).fetchall()
    counts: dict[str, int] = {}
    for r in rows:
        counts[r["type"]] = counts.get(r["type"], 0) + 1
    line = {"type": "events", "key": g["gkey"], "ts": g["ts"], "first_ts": g["first_ts"],
            "counts": [(COUNTED[t], n) for t, n in counts.items() if t in COUNTED],
            "items": _items(conn, rows[:MAX_ITEMS * 3]), "more": 0}
    line["more"] = max(0, sum(1 for r in rows if r["type"] in COUNTED) - len(line["items"]))
    line.update(_header(conn, g, rows))
    return line


def _header(conn, g: dict, rows: list[dict]) -> dict:
    key, cause = g["gkey"], g["cause"]
    if key.startswith("backfill:"):
        sid = key.split(":", 1)[1]
        src = conn.execute("SELECT name, kind FROM sources WHERE id = %s", (sid,)).fetchone()
        n = len(g["unit_ids"] or [])
        return {"kind": "backfill", "title": f"Initial read of {src['name'] if src else sid}",
                "detail": f"{n} units processed in this window", "source": src["kind"] if src else None}
    if cause.startswith("unit:"):
        u = conn.execute("""SELECT u.title, u.kind, s.name, s.kind AS skind FROM units u
                            JOIN sources s ON s.id = u.source_id WHERE u.id = %s""", (int(cause[5:]),)).fetchone()
        if not u:
            return {"kind": "source", "title": "Item that no longer exists at the source", "detail": cause, "source": None}
        chip = SOURCE_KINDS.get(u["skind"], u["skind"])
        title = u["title"] or UNIT_KINDS.get(u["kind"], u["kind"])
        # No repetition: in a chat, the unit title is the chat name itself.
        detail = [x for x in (u["name"] if u["name"] != title else None,
                              UNIT_KINDS.get(u["kind"], u["kind"]) if UNIT_KINDS.get(u["kind"]) != chip else None) if x]
        return {"kind": chip, "title": title, "detail": " · ".join(detail), "source": u["skind"]}
    if cause.startswith("tension:"):
        t = conn.execute("SELECT reason, opened_by FROM tensions WHERE id = %s", (cause[8:],)).fetchone()
        return {"kind": "judgment", "title": "Judgment of a tension",
                "detail": (t["reason"] if t else cause[8:]) + (f" · opened by {t['opened_by']}" if t else ""), "source": None}
    if cause.startswith("correction:"):
        c = conn.execute("SELECT text, via FROM corrections WHERE id = %s", (cause[11:],)).fetchone()
        return {"kind": "your correction", "title": "Correction by you", "detail": (c["text"] if c else cause[11:]) +
                (f" · via {c['via']}" if c else ""), "source": None}
    if cause.startswith("sweep:"):
        return {"kind": "sweep", "title": "Drift sweep", "detail": "", "source": None}
    base = cause.split("@")[0]
    if base.startswith("source:"):
        src = conn.execute("SELECT name FROM sources WHERE id = %s", (base[7:],)).fetchone()
        return {"kind": "read", "title": f"Read of {src['name'] if src else base[7:]}",
                "detail": "changes seen at the source (edit, deletion, filters)", "source": None}
    if base.startswith("unattributed:"):
        _, actor, day = base.split(":", 2)
        return {"kind": "no cause", "title": f"Changes by {actor} on {day}",
                "detail": "cause not recorded (made outside a job)", "source": None}
    label = {"startup": "Startup (owner settings)", "sync_catalog": "incident.io catalog", "reconcile_entities": "Identity reconciliation",
             "apply_team_identities": "Team identities", "integrate_inferences": "Narrative inferences",
             "integrate_structured": "Structured facts", "derive_narratives": "Narratives"}
    return {"kind": "routine", "title": label.get(base.split(":")[0], base.split(":")[0]), "detail": "", "source": None}


def _items(conn, rows: list[dict]) -> list[dict]:
    """The items of a line, with the before and after when a claim changed."""
    claim_ids = set()
    for r in rows:
        p = r["payload"]
        if r["type"] in ("claim_corroborated", "claim_status_changed", "claim_validity_set"):
            claim_ids.add(p["id"])
        if r["type"] == "verdict_applied":
            for ch in p.get("changes") or []:
                claim_ids.update(x for x in (ch.get("claim_id"), ch.get("by")) if x)
        if r["type"] == "tension_opened":
            claim_ids.update(p.get("claim_ids") or [])
    claims = {c["id"]: c for c in conn.execute(
        """SELECT c.id, c.statement, c.status, e.name AS subject FROM claims c
           LEFT JOIN entities e ON e.id = c.subject_entity_id WHERE c.id = ANY(%s)""", (list(claim_ids),)).fetchall()}
    entity_ids = set()
    for r in rows:
        if r["type"] == "entity_merged":
            entity_ids.update(x for x in (r["payload"].get("id"), r["payload"].get("into")) if x)
        elif r["type"] == "entity_unmerged":
            entity_ids.add(r["payload"].get("id"))
    entities = {e["id"]: e for e in conn.execute(
        "SELECT id, name, type FROM entities WHERE id = ANY(%s)", (list(entity_ids),)).fetchall()}

    def ent(eid: str | None) -> str:
        e = entities.get(eid)
        return f"{e['name']} ({e['type']})" if e else "entity that no longer exists"

    verdicts = {v["id"]: v for v in conn.execute(
        "SELECT id, status, decided_by FROM verdicts WHERE id = ANY(%s)",
        ([r["payload"]["id"] for r in rows if r["type"] == "verdict_applied"],)).fetchall()}
    out = []
    for r in rows:
        p, t = r["payload"], r["type"]
        if t == "claim_asserted":
            out.append({"t": "new", "id": p["id"], "text": p.get("statement"), "subject_id": p.get("subject_entity_id")})
        elif t == "claim_corroborated":
            c = claims.get(p["id"])
            out.append({"t": "corroborated", "id": p["id"], "text": c["statement"] if c else REDACTED})
        elif t == "claim_status_changed":
            c = claims.get(p["id"])
            out.append({"t": "status", "id": p["id"], "text": c["statement"] if c else REDACTED,
                        "to": p.get("to"), "why": p.get("reason")})
        elif t == "verdict_applied":
            v = verdicts.get(p["id"]) or {}
            changes = []
            for ch in p.get("changes") or []:
                before = claims.get(ch["claim_id"])
                after = claims.get(ch.get("by")) if ch.get("by") else None
                changes.append({"id": ch["claim_id"], "from": ch.get("from"), "to": ch.get("to"),
                                "before": before["statement"] if before else REDACTED,
                                "after": after["statement"] if after else None, "by": ch.get("by")})
            out.append({"t": "verdict", "id": p["id"], "kind": p.get("kind"), "text": p.get("summary"),
                        "changes": changes, "status": v.get("status"),
                        "revertible": v.get("status") == "applied" and v.get("decided_by") != "human:owner"})
        elif t == "tension_opened":
            out.append({"t": "tension", "id": p["id"], "text": p.get("reason"),
                        "claims": [claims[c]["statement"] for c in p.get("claim_ids") or [] if c in claims]})
        elif t == "entity_created":
            out.append({"t": "entity", "id": p["id"], "text": f"{p.get('name')} ({p.get('type')})"})
        elif t == "entity_merged":
            out.append({"t": "merge", "id": p.get("id"), "text": f"{ent(p.get('id'))} merged into {ent(p.get('into'))}",
                        "why": p.get("reason"), "entity_id": p.get("into")})
        elif t == "entity_unmerged":
            out.append({"t": "merge undone", "id": p.get("id"), "text": f"{ent(p.get('id'))} is separate again",
                        "entity_id": p.get("id")})
        elif t == "claim_validity_set":
            c = claims.get(p["id"])
            out.append({"t": "validity", "id": p["id"], "text": c["statement"] if c else REDACTED,
                        "to": f"valid until {p.get('valid_to')}"})
        if len(out) >= MAX_ITEMS:
            break
    return out


# ---------------------------------------------------------------- pages


def _page_lines(conn, before, since, source_id, limit, entity_id: str | None = None) -> list[dict]:
    extra, params = "", {"before": before, "since": since, "limit": limit}
    if entity_id:
        extra = " AND (v.entity_id = %(eid)s OR %(eid)s = ANY(vv.member_entity_ids))"
        params["eid"] = entity_id
    if source_id:
        extra = " AND vv.cause = 'synthesis:' || %(src)s"
        params["src"] = source_id
    rows = conn.execute(
        f"""SELECT vv.view_id, vv.version, vv.title, vv.generated_at, vv.cause, vv.status, v.kind, v.entity_id,
                   v.stale, v.stale_since, v.version AS current_version
            FROM view_versions vv JOIN views v ON v.id = vv.view_id
            WHERE vv.generated_at < %(before)s AND vv.generated_at >= %(since)s {extra}
            ORDER BY vv.generated_at DESC LIMIT %(limit)s""", params).fetchall()
    # Each new claim regenerates pages: one line per page would drown the rest. Without an entity filter, the
    # regenerations become one line per hour (and the backfill synthesis, a single line); each page opens its diff.
    lines, groups = [], {}
    for r in rows:
        page = {"view_id": r["view_id"], "version": r["version"], "title": r["title"], "page_kind": r["kind"],
                "status": r["status"],
                "updating_since": r["stale_since"] if r["stale"] and r["version"] == r["current_version"] else None}
        if entity_id:
            lines.append({"type": "page", "kind": "page", "ts": r["generated_at"], **page})
            continue
        if r["cause"] and r["cause"].startswith("synthesis:"):
            key, title = r["cause"], "Backfill synthesis"
        else:
            key, title = r["generated_at"].strftime("%Y-%m-%d %H"), "Pages regenerated"
        g = groups.setdefault(key, {"type": "pages", "kind": "pages", "ts": r["generated_at"], "title": title,
                                    "pages": [], "cause": r["cause"]})
        g["pages"].append(page)
    for g in groups.values():
        src = None
        if g["cause"] and g["cause"].startswith("synthesis:"):
            src = conn.execute("SELECT name FROM sources WHERE id = %s", (g["cause"].split(":", 1)[1],)).fetchone()
        g["detail"] = f"{len(g['pages'])} version(s)" + (f" · {src['name']}" if src else "")
        g["updating"] = sum(1 for p in g["pages"] if p["updating_since"])
        lines.append(g)
    return lines


def page_diff(conn, view_id: str, version: int) -> dict:
    """Diff of the L1 against the previous version. Redacted text (deletion at the source) shows up as redacted."""
    rows = {r["version"]: r for r in conn.execute(
        "SELECT version, l1, title FROM view_versions WHERE view_id = %s AND version IN (%s, %s)",
        (view_id, version - 1, version)).fetchall()}
    cur, prev = rows.get(version), rows.get(version - 1)
    if not cur:
        return {"lines": [], "first": False}
    if not prev:
        return {"lines": [("=", ln) for ln in cur["l1"].splitlines()], "first": True}
    out = []
    for ln in difflib.unified_diff(prev["l1"].splitlines(), cur["l1"].splitlines(), lineterm="", n=1):
        if ln.startswith(("---", "+++")):
            continue
        out.append(("@" if ln.startswith("@@") else ln[:1] or " ", ln[1:] if not ln.startswith("@@") else "…"))
    return {"lines": out, "first": False}


def updating(conn) -> dict:
    """Pages with a pending regeneration."""
    return conn.execute("""SELECT count(*) AS n, min(stale_since) AS since FROM views
                           WHERE stale AND status = 'active'""").fetchone()


# ---------------------------------------------------------------- no effect


def _noop_lines(conn, before, since, source_id, limit) -> list[dict]:
    extra, params = "", {"before": before, "since": since, "limit": limit}
    if source_id:
        extra = " AND u.source_id = %(src)s"
        params["src"] = source_id
    rows = conn.execute(
        f"""SELECT u.id, u.title, u.kind, u.status, u.triage, coalesce(u.processed_at, u.created_at) AS ts, s.name
            FROM units u JOIN sources s ON s.id = u.source_id
            WHERE coalesce(u.processed_at, u.created_at) < %(before)s AND coalesce(u.processed_at, u.created_at) >= %(since)s
              AND NOT (s.backfill_until IS NOT NULL AND u.occurred_at < s.backfill_until
                       AND (s.backfill_done_at IS NULL OR u.processed_at <= s.backfill_done_at))
              AND (u.status IN ('triaged_out', 'failed')
                   OR (u.status = 'processed' AND NOT EXISTS (
                         SELECT 1 FROM events c WHERE c.cause = 'unit:' || u.id)
                       AND NOT EXISTS (SELECT 1 FROM evidence ev JOIN claims c ON ev.id = ANY(c.evidence_ids)
                                       WHERE ev.unit_id = u.id))) {extra}
            ORDER BY ts DESC LIMIT %(limit)s""", params).fetchall()
    out = []
    for r in rows:
        if r["status"] == "triaged_out":
            why = "triage: " + ((r["triage"] or {}).get("reason") or "no durable knowledge")
        elif r["status"] == "failed":
            why = "failed during processing"
        else:
            why = "extraction found no claims"
        out.append({"type": "no_effect", "kind": "no_effect", "ts": r["ts"], "title": r["title"] or UNIT_KINDS.get(r["kind"], r["kind"]),
                    "detail": f"{r['name']} · {UNIT_KINDS.get(r['kind'], r['kind'])}", "why": why, "failed": r["status"] == "failed"})
    return out


# ---------------------------------------------------------------- trajectory


def trajectory(conn, entity_id: str) -> list[dict]:
    """The history of each attribute of the entity: claims of the same predicate in order, with what each verdict
    changed. The "superseded by" link comes from the verdict (data), never from the summary text."""
    claims = conn.execute(
        """SELECT id, predicate, statement, status, recorded_at, valid_from, valid_to, origin FROM claims
           WHERE subject_entity_id = %s ORDER BY predicate, recorded_at""", (entity_id,)).fetchall()
    ids = [c["id"] for c in claims]
    history: dict[str, list] = {}
    for h in conn.execute(
            """SELECT h.claim_id, h.from_ts, h.status, h.verdict_id, v.kind, v.changes FROM claim_status_history h
               LEFT JOIN verdicts v ON v.id = h.verdict_id WHERE h.claim_id = ANY(%s) ORDER BY h.from_ts""",
            (ids,)).fetchall():
        by = next((ch.get("by") for ch in (h["changes"] or []) if ch.get("claim_id") == h["claim_id"]), None)
        history.setdefault(h["claim_id"], []).append({"ts": h["from_ts"], "status": h["status"],
                                                      "verdict": h["verdict_id"], "kind": h["kind"], "by": by})
    by_ids = [h["by"] for hs in history.values() for h in hs if h["by"]]
    by_text = {r["id"]: r["statement"] for r in conn.execute(
        "SELECT id, statement FROM claims WHERE id = ANY(%s)", (by_ids,)).fetchall()}
    for hs in history.values():
        for h in hs:
            h["by_text"] = by_text.get(h["by"])
    groups: dict[str, list] = {}
    for c in claims:
        groups.setdefault(c["predicate"], []).append({**c, "history": history.get(c["id"], [])})
    # Only the attributes that have a history: more than one claim, or one that is no longer current.
    return [{"predicate": p, "claims": cs} for p, cs in groups.items()
            if len(cs) > 1 or any(c["status"] != "current" for c in cs)]


ID = re.compile(r"\b(?:cl|en|vd)_[0-9a-f]{12}\b")


def _names_in(conn, lines: list) -> dict[str, str]:
    """IDs cited inside texts written by the LLM (tension reason, verdict summary) → something readable:
    the start of the claim or the entity name. Plumbing: swaps an ID for a label."""
    found: set[str] = set()

    def walk(x):
        if isinstance(x, str):
            found.update(ID.findall(x))
        elif isinstance(x, dict):
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
    walk(lines)
    names: dict[str, str] = {}
    ids = sorted(found)
    for r in conn.execute("SELECT id, statement FROM claims WHERE id = ANY(%s)", (ids,)).fetchall():
        st = r["statement"]
        names[r["id"]] = "«" + (st if len(st) <= 80 else st[:77] + "…") + "»"
    for r in conn.execute("SELECT id, name, type FROM entities WHERE id = ANY(%s)", (ids,)).fetchall():
        names[r["id"]] = f"{r['name']} ({r['type']})"
    for r in conn.execute("SELECT id, kind FROM verdicts WHERE id = ANY(%s)", (ids,)).fetchall():
        names[r["id"]] = "verdict: " + r["kind"].replace("_", " ")
    return names
