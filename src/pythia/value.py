"""Value: honest cost. SQL only, no LLM.

- **Maintain × serve × measure:** what it costs to have the knowledge (ingestion, synthesis, sweep), to deliver it (the
  push) and to measure quality (golden questions). By role and by source, for the current and the previous month.
- **Cost per source ÷ what of it reached an LLM:** which sources pay the bill and which only cost. "Reached" =
  claim cited in a page pushed by the push (in the delivered version) or returned by the MCP.
- **Declared coverage:** what these numbers do not count (from gbrain: `get_usage` with `coverage`).
"""

from __future__ import annotations

from datetime import datetime, timezone

SERVE = {"briefing"}          # deliver: the push (the MCP does not call an LLM)
MEASURE = {"golden", "value_triage", "value_judge"}   # measure: golden questions and the re-explanation extractor
BUCKETS = ("maintain", "serve", "measure")
SOURCE_KINDS = {"claude_code": "Claude Code chats", "github": "GitHub", "slack": "Slack", "confluence": "Confluence",
                "incident_io": "incident.io catalog", "live": "Live reads", "human": "Your corrections",
                "sweep": "Sweep", "push": "Push",
                None: "Online synthesis (pages and narratives of all sources)"}


def bucket(role: str) -> str:
    return "serve" if role in SERVE else "measure" if role in MEASURE else "maintain"


def month_bounds(conn, offset: int = 0) -> tuple[datetime, datetime]:
    """Start and end of the month (offset 0 = current, -1 = previous), in the database timezone."""
    r = conn.execute("""SELECT date_trunc('month', now()) + %s * interval '1 month' AS a,
                               date_trunc('month', now()) + (%s + 1) * interval '1 month' AS b""",
                     (offset, offset)).fetchone()
    return r["a"], r["b"]


def costs(conn, start: datetime, end: datetime) -> dict:
    rows = conn.execute(
        """SELECT role, split_part(source_id, ':', 1) AS kind, batch, sum(cost_usd) AS usd, count(*) AS calls
           FROM llm_calls WHERE ts >= %s AND ts < %s GROUP BY 1, 2, 3""", (start, end)).fetchall()
    total = sum(float(r["usd"]) for r in rows)
    by_bucket = {b: 0.0 for b in BUCKETS}
    by_role: dict[str, dict] = {}
    by_source: dict[str | None, float] = {}
    batch = 0.0
    for r in rows:
        usd = float(r["usd"])
        b = bucket(r["role"])
        by_bucket[b] += usd
        role = by_role.setdefault(r["role"], {"role": r["role"], "bucket": b, "usd": 0.0, "calls": 0})
        role["usd"] += usd
        role["calls"] += r["calls"]
        if b == "maintain":
            kind = r["kind"] or None
            by_source[kind] = by_source.get(kind, 0.0) + usd
        if r["batch"]:
            batch += usd
    return {
        "start": start, "end": end, "total": total, "buckets": by_bucket, "batch": batch,
        "roles": sorted(by_role.values(), key=lambda r: -r["usd"]),
        "maintain_by_source": by_source,
    }


def daily(conn, days: int = 14) -> list[dict]:
    """Spend per day and per bucket. No projection: backfills are one-off spends and distort any average."""
    rows = conn.execute(
        """SELECT date_trunc('day', ts)::date AS day, role, sum(cost_usd) AS usd FROM llm_calls
           WHERE ts >= date_trunc('day', now()) - (%s - 1) * interval '1 day' GROUP BY 1, 2 ORDER BY 1""",
        (days,)).fetchall()
    out: dict = {}
    for r in rows:
        d = out.setdefault(r["day"], {"day": r["day"], **{b: 0.0 for b in BUCKETS}})
        d[bucket(r["role"])] += float(r["usd"])
    for d in out.values():
        d["total"] = sum(d[b] for b in BUCKETS)
    return list(out.values())


def delivered_by_source(conn, start: datetime, end: datetime) -> dict[str | None, dict]:
    """Distinct claims that reached an LLM, by source (kind). Push: the claims cited by the page in the
    delivered version (`view_versions`). MCP: claims and pages returned."""
    rows = conn.execute(
        """WITH refs AS (
               SELECT 'push' AS via, r->>'id' AS id, (r->>'version')::int AS version FROM pushes p,
                      jsonb_array_elements(p.refs) r
               WHERE p.ts >= %(a)s AND p.ts < %(b)s AND coalesce(p.skipped, '') = ''
               UNION ALL
               SELECT 'mcp', r->>'id', (r->>'version')::int FROM mcp_calls m, jsonb_array_elements(m.refs) r
               WHERE m.ts >= %(a)s AND m.ts < %(b)s),
           claims_out AS (
               SELECT via, unnest(vv.cited_claim_ids) AS claim_id FROM refs
               JOIN view_versions vv ON vv.view_id = refs.id AND vv.version = refs.version
               UNION ALL
               SELECT via, unnest(v.cited_claim_ids) FROM refs JOIN views v ON v.id = refs.id WHERE refs.version IS NULL
               UNION ALL
               SELECT via, id FROM refs WHERE id LIKE 'cl\\_%%'
               UNION ALL
               SELECT via, unnest(vd.claim_ids) FROM refs JOIN verdicts vd ON vd.id = refs.id)
           SELECT nullif(split_part(c.source_id, ':', 1), '') AS kind,
                  count(DISTINCT co.claim_id) AS claims,
                  count(DISTINCT co.claim_id) FILTER (WHERE co.via = 'push') AS by_push,
                  count(DISTINCT co.claim_id) FILTER (WHERE co.via = 'mcp') AS by_mcp
           FROM claims_out co JOIN claims c ON c.id = co.claim_id GROUP BY 1""",
        {"a": start, "b": end}).fetchall()
    return {r["kind"]: r for r in rows}


def source_table(conn, start: datetime, end: datetime, maintain_by_source: dict) -> list[dict]:
    delivered = delivered_by_source(conn, start, end)
    living = {r["kind"]: r["n"] for r in conn.execute(
        """SELECT nullif(split_part(source_id, ':', 1), '') AS kind, count(*) AS n FROM claims
           WHERE status IN ('current', 'disputed') GROUP BY 1""").fetchall()}
    kinds = set(maintain_by_source) | set(delivered)
    out = []
    for k in kinds:
        usd = maintain_by_source.get(k, 0.0)
        d = delivered.get(k) or {"claims": 0, "by_push": 0, "by_mcp": 0}
        shared = k is None  # online synthesis serves all sources: it is not divided by one source's claims
        out.append({"kind": k, "label": SOURCE_KINDS.get(k, k), "usd": usd, "shared": shared,
                    "living": None if shared else living.get(k, 0),
                    "delivered": None if shared else d["claims"], "by_push": d["by_push"], "by_mcp": d["by_mcp"],
                    "usd_per_delivered": usd / d["claims"] if d["claims"] and not shared else None})
    return sorted(out, key=lambda r: -r["usd"])


def coverage(conn, start: datetime, end: datetime) -> list[str]:
    """What the numbers above do NOT count. Said explicitly, so as not to announce savings without the whole cost."""
    failed = conn.execute("SELECT count(*) AS n FROM llm_calls WHERE NOT ok AND ts >= %s AND ts < %s",
                          (start, end)).fetchone()["n"]
    return [
        "Tokens spent by Claude Code (the consumer) to read what Pythia delivers: they stay on that side. "
        "Measured from the transcripts in step 0.4 of the plan.",
        "The owner's time maintaining Pythia (correcting, reviewing, debugging): it enters the ROI as a cost, it does not show up here.",
        "Embeddings: run locally (fastembed), at no API cost.",
        f"LLM calls that failed: {failed} in the period, with no cost recorded.",
        "Online synthesis (pages and narratives outside the backfill) has no source: it shows up as \"Derived, no source\". "
        "Backfill synthesis is attributed to the source that caused it.",
        "\"Reached an LLM\" counts what was delivered, not what was used: usage is step 0.2 of the plan.",
    ]


def report(conn) -> dict:
    months = []
    for offset in (0, -1):
        a, b = month_bounds(conn, offset)
        c = costs(conn, a, b)
        c["sources"] = source_table(conn, a, b, c["maintain_by_source"])
        c["deliveries"] = conn.execute(
            """SELECT (SELECT count(*) FROM pushes WHERE ts >= %(a)s AND ts < %(b)s AND coalesce(skipped, '') = '') AS pushes,
                      (SELECT count(*) FROM mcp_calls WHERE ts >= %(a)s AND ts < %(b)s) AS mcp""",
            {"a": a, "b": b}).fetchone()
        c["coverage"] = coverage(conn, a, b)
        months.append(c)
    return {"months": months, "daily": daily(conn)}


# ---------------------------------------------------------------- 0.2: was what was delivered used?

import json  # noqa: E402
import re  # noqa: E402
from pathlib import Path  # noqa: E402

ID_RE = re.compile(r"\b(?:vw|cl|vd|en|ev)_[0-9a-f]{12}\b")
PYTHIA_TOOL = "mcp__pythia__"


def _ts(s: str | None) -> datetime | None:
    return datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def read_assistant(path: Path) -> tuple[list[dict], list[dict]]:
    """From the transcript, only what the ASSISTANT produced (text and tool calls), with the time. The context
    injected by the hook is left out (it is an attachment of the event, not the assistant's speech); so is internal reasoning."""
    said, tools = [], []
    with open(path, encoding="utf-8", errors="replace") as f:
        for raw in f:
            try:
                line = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if line.get("type") != "assistant" or line.get("isSidechain"):
                continue
            ts = _ts(line.get("timestamp"))
            for b in (line.get("message") or {}).get("content") or []:
                if b.get("type") == "text":
                    said.append({"ts": ts, "text": b.get("text") or ""})
                elif b.get("type") == "tool_use":
                    inp = b.get("input") or {}
                    said.append({"ts": ts, "text": json.dumps(inp, ensure_ascii=False)})
                    if (b.get("name") or "").startswith(PYTHIA_TOOL):
                        tools.append({"ts": ts, "tool": b["name"][len(PYTHIA_TOOL):], "input": inp})
    return said, tools


def push_usage(conn, projects_root: Path, start: datetime, end: datetime) -> dict:
    """Floor of usage: a pushed reference counts as used if, after the push and in the same session, the assistant
    cites it (the ID, or the ID of a claim the page cited) or opens it through the MCP. Paraphrase is not detected here (left
    to the judge of step 0.4): the number is a FLOOR, not the real precision."""
    pushes = conn.execute(
        """SELECT session_id, ts, refs FROM pushes
           WHERE ts >= %s AND ts < %s AND coalesce(skipped, '') = '' AND session_id IS NOT NULL ORDER BY ts""",
        (start, end)).fetchall()
    by_session: dict[str, list] = {}
    for p in pushes:
        by_session.setdefault(p["session_id"], []).append(p)
    cited_of = {}
    pairs = {(r["id"], r.get("version")) for p in pushes for r in p["refs"] if r.get("id", "").startswith("vw_")}
    for vid, ver in pairs:
        row = conn.execute("SELECT cited_claim_ids FROM view_versions WHERE view_id = %s AND version = %s",
                           (vid, ver)).fetchone() if ver else None
        cited_of[(vid, ver)] = set(row["cited_claim_ids"]) if row else set()
    delivered = used = 0
    sessions = missing = with_mcp = 0
    tool_counts: dict[str, int] = {}
    levels: dict[str, int] = {}
    used_examples: list[dict] = []
    for sid, ps in by_session.items():
        files = list(projects_root.glob(f"*/{sid}.jsonl"))
        if not files:
            missing += 1
            continue
        sessions += 1
        said, tools = read_assistant(files[0])
        if tools:
            with_mcp += 1
        for t in tools:
            tool_counts[t["tool"]] = tool_counts.get(t["tool"], 0) + 1
            if t["tool"] == "open":
                lv = str(t["input"].get("level") or "?")
                levels[lv] = levels.get(lv, 0) + 1
        for p in ps:
            later = [s["text"] for s in said if s["ts"] and s["ts"] > p["ts"]]
            opened = {t["input"].get("id") for t in tools if t["ts"] and t["ts"] > p["ts"]}
            mentioned = {m for txt in later for m in ID_RE.findall(txt)}
            for r in p["refs"]:
                rid = r.get("id")
                if not rid:
                    continue
                delivered += 1
                hit = rid in mentioned or rid in opened or bool(cited_of.get((rid, r.get("version")), set()) & mentioned)
                if hit:
                    used += 1
                    if len(used_examples) < 10:
                        used_examples.append({"session": sid, "id": rid, "ts": p["ts"]})
    return {"sessions": sessions, "missing_transcripts": missing, "sessions_with_mcp": with_mcp,
            "delivered": delivered, "used_explicitly": used,
            "floor": used / delivered if delivered else None, "tools": tool_counts, "open_levels": levels,
            "examples": used_examples}


def usage_cached(svc, max_age_s: int = 1800) -> dict:
    """Reading the transcripts takes a few seconds: the result is cached for 30 min."""
    from .connectors import claude_code
    cached = svc.state.get("value_usage")
    now = datetime.now(timezone.utc)
    if cached and (now - datetime.fromisoformat(cached["at"])).total_seconds() < max_age_s:
        return cached["data"]
    root = claude_code.projects_root(svc.settings.get("claude_code"))
    with svc.db.tx() as conn:
        out = []
        for offset in (0, -1):
            a, b = month_bounds(conn, offset)
            out.append({"start": a, **push_usage(conn, root, a, b)})
    data = json.loads(json.dumps({"months": out}, default=str))
    svc.state.set("value_usage", {"at": now.isoformat(), "data": data})
    return data
