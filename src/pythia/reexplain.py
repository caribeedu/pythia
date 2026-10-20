"""Avoidable re-explanation and the cost of getting oriented, read from the Claude Code transcripts.

Per owner message:
1. separate those that ASSERT something about the work world (Haiku; commands, questions and "ok" are left out);
2. look up what Pythia already knew before it: claims current at that instant (time travel through the status
   history), excluding those that came from the conversation itself (what was said in it is already in the AI's context);
3. a judge (Sonnet) decides whether the message repeats any of those claims. The judge never sees what the push injected;
4. classify: `delivered` (was in the push before the message and was explained anyway), `not_delivered` (was
   in the base and did not reach the AI) or `new` (Pythia did not know it).

Per session, by code (no LLM): input tokens and exploration calls until the first file edit.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from .connectors import claude_code as cc
from .llm.client import CallContext
from .pipeline import prompts, schemas
from .services import Services

SOURCE = "value"
EXPLORE = {"Read", "Grep", "Glob", "Bash", "LS", "NotebookRead", "WebFetch", "WebSearch", "Agent", "Task", "ToolSearch"}
EDIT = {"Edit", "Write", "MultiEdit", "NotebookEdit"}
SPLIT_BATCH = 40
CANDIDATES = 8


def _ts(s: str | None) -> datetime | None:
    return datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def session_cost(path: Path) -> dict:
    """Input tokens (with cache) per API call and tool calls, until the first edit."""
    seen_msgs: set[str] = set()
    total = until_edit = explore = 0
    edited = False
    start = None
    with open(path, encoding="utf-8", errors="replace") as f:
        for raw in f:
            try:
                line = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if start is None and line.get("timestamp"):
                start = _ts(line["timestamp"])
            if line.get("type") != "assistant" or line.get("isSidechain"):
                continue
            msg = line.get("message") or {}
            mid = msg.get("id")
            usage = msg.get("usage") or {}
            if mid and mid not in seen_msgs and usage:
                seen_msgs.add(mid)
                tok = (usage.get("input_tokens") or 0) + (usage.get("cache_read_input_tokens") or 0) \
                    + (usage.get("cache_creation_input_tokens") or 0)
                total += tok
                if not edited:
                    until_edit += tok
            for b in msg.get("content") or []:
                if b.get("type") != "tool_use":
                    continue
                if b.get("name") in EDIT:
                    edited = True
                elif not edited and (b.get("name") in EXPLORE or (b.get("name") or "").startswith("mcp__")):
                    explore += 1
    return {"started_at": start, "tokens_total": total, "tokens_until_edit": until_edit if edited else None,
            "explore_until_edit": explore if edited else None, "had_edit": edited}


def _candidates(conn, svc: Services, text: str, at: datetime, own_streams: list[str]) -> list[dict]:
    vec = np.array(svc.embedder.embed([text[:4000]])[0], dtype=np.float32)
    return conn.execute(
        """SELECT c.id, c.kind, c.statement, e.name AS subject FROM claims c
           LEFT JOIN entities e ON e.id = c.subject_entity_id
           WHERE c.recorded_at < %(t)s AND c.embedding IS NOT NULL
             AND coalesce((SELECT h.status FROM claim_status_history h WHERE h.claim_id = c.id AND h.from_ts <= %(t)s
                           ORDER BY h.from_ts DESC LIMIT 1), 'current') IN ('current', 'disputed')
             AND NOT EXISTS (SELECT 1 FROM evidence ev WHERE ev.id = ANY(c.evidence_ids) AND ev.stream_id = ANY(%(own)s))
           ORDER BY c.embedding <=> %(v)s LIMIT %(n)s""",
        {"t": at, "own": own_streams, "v": vec, "n": CANDIDATES}).fetchall()


def _pushed_claims(conn, session_ids: list[str], before: datetime) -> set[str]:
    rows = conn.execute(
        """SELECT r->>'id' AS id, (r->>'version')::int AS version FROM pushes p, jsonb_array_elements(p.refs) r
           WHERE p.session_id = ANY(%s) AND p.ts < %s AND coalesce(p.skipped, '') = ''""",
        (session_ids, before)).fetchall()
    out: set[str] = set()
    for r in rows:
        if r["id"].startswith("cl_"):
            out.add(r["id"])
        elif r["id"].startswith("vw_"):
            v = conn.execute("SELECT cited_claim_ids FROM view_versions WHERE view_id = %s AND version = %s",
                             (r["id"], r["version"])).fetchone() if r["version"] else \
                conn.execute("SELECT cited_claim_ids FROM views WHERE id = %s", (r["id"],)).fetchone()
            out.update(v["cited_claim_ids"] if v else [])
        elif r["id"].startswith("vd_"):
            v = conn.execute("SELECT claim_ids FROM verdicts WHERE id = %s", (r["id"],)).fetchone()
            out.update(v["claim_ids"] if v else [])
    return out


def _excluded(title: str | None, excluded: list[str]) -> bool:
    t = (title or "").strip().lower()
    return any(t.startswith(x.strip().lower()) for x in excluded if x.strip())


def run(svc: Services, since_days: int = 30, job_id: int | None = None, root: Path | None = None) -> dict:
    cfg = svc.settings.get("claude_code")
    root = root or cc.projects_root(cfg)
    chats = cc.discover_chats(root, cc.app_sessions_root(cfg))
    chat_of: dict[str, dict] = {}
    for c in chats:
        for stem in c["files"]:
            chat_of[stem] = c
    excluded = svc.settings.get("value").get("excluded_chats") or []
    limit = datetime.now(timezone.utc) - timedelta(days=since_days)
    files = sorted((p for p in root.glob("*/*.jsonl") if datetime.fromtimestamp(p.stat().st_mtime, timezone.utc) >= limit),
                   key=lambda p: p.stat().st_mtime)
    ctx = CallContext(source_id=SOURCE, job_id=job_id)
    stats = {"sessions": 0, "skipped_chats": 0, "messages": 0, "judged": 0, "before_base": 0}
    # Before Pythia knew anything, no message can repeat what it knew: measuring that period
    # only dilutes the metric.
    base_start = svc.db.one("SELECT min(recorded_at) AS t FROM claims")["t"]
    for path in files:
        sid = path.stem
        chat = chat_of.get(sid) or {"title": None, "files": [sid]}
        if _excluded(chat.get("title"), excluded):
            stats["skipped_chats"] += 1
            continue
        stats["sessions"] += 1
        delta = cc.read_delta(path, 0)
        user_msgs = [m for m in delta.messages if m.role == "user" and m.text.strip()]
        with svc.db.tx() as conn:
            done = {r["message_id"] for r in conn.execute(
                "SELECT message_id FROM reexplain_messages WHERE message_id = ANY(%s)",
                ([m.external_id for m in user_msgs],)).fetchall()}
        new = [m for m in user_msgs if m.external_id not in done and base_start and m.occurred_at >= base_start]
        stats["before_base"] += sum(1 for m in user_msgs if not base_start or m.occurred_at < base_start)
        streams = [f"{cc.KIND}:{s}" for s in chat["files"]]
        for i in range(0, len(new), SPLIT_BATCH):
            chunk = new[i:i + SPLIT_BATCH]
            listing = "\n\n".join(f"[{n}] {m.text[:3000]}" for n, m in enumerate(chunk, 1))
            out = svc.llm.structured("value_triage", prompts.REEXPLAIN_SPLIT.format(owner=svc.owner_name), listing,
                                     schemas.REEXPLAIN_SPLIT, ctx=ctx)
            flags = {x["n"]: x["asserts"] for x in out["messages"]}
            for n, m in enumerate(chunk, 1):
                stats["messages"] += 1
                asserts = bool(flags.get(n))
                row = {"repeats": None, "claim_ids": [], "candidate_ids": [], "bucket": None, "reason": None}
                if asserts:
                    with svc.db.tx() as conn:
                        cands = _candidates(conn, svc, m.text, m.occurred_at, streams)
                    if cands:
                        listing_c = "\n".join(f"- {c['id']} · [{c['subject'] or '?'}] ({c['kind']}) {c['statement']}"
                                              for c in cands)
                        j = svc.llm.structured("value_judge", prompts.REEXPLAIN_JUDGE.format(owner=svc.owner_name),
                                               f"MESSAGE FROM {svc.owner_name.upper()}:\n{m.text[:6000]}\n\n"
                                               f"WHAT PYTHIA ALREADY KNEW:\n{listing_c}",
                                               schemas.REEXPLAIN_JUDGE, ctx=ctx)
                        stats["judged"] += 1
                        known = {c["id"] for c in cands}
                        repeated = [c for c in j["claim_ids"] if c in known]
                        row.update(repeats=bool(j["repeats"] and repeated), claim_ids=repeated,
                                   candidate_ids=sorted(known), reason=j["reason"])
                    else:
                        row.update(repeats=False)
                    if row["repeats"]:
                        with svc.db.tx() as conn:
                            pushed = _pushed_claims(conn, list(chat["files"]), m.occurred_at)
                        row["bucket"] = "delivered" if pushed & set(row["claim_ids"]) else "not_delivered"
                    else:
                        row["bucket"] = "new"
                svc.db.execute(
                    """INSERT INTO reexplain_messages (message_id, session_id, chat_title, ts, text, asserts, repeats,
                                                       claim_ids, candidate_ids, bucket, reason, prompt_version)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (message_id) DO NOTHING""",
                    (m.external_id, sid, chat.get("title"), m.occurred_at, m.text, asserts, row["repeats"],
                     row["claim_ids"], row["candidate_ids"], row["bucket"], row["reason"], prompts.VERSION))
        cost = session_cost(path)
        svc.db.execute(
            """INSERT INTO session_metrics (session_id, chat_title, started_at, user_messages, asserts, avoidable_delivered,
                                            avoidable_not_delivered, new_context, tokens_total, tokens_until_edit,
                                            explore_until_edit, had_edit, computed_at)
               SELECT %(sid)s, %(title)s, %(start)s, count(*), count(*) FILTER (WHERE asserts),
                      count(*) FILTER (WHERE bucket = 'delivered'), count(*) FILTER (WHERE bucket = 'not_delivered'),
                      count(*) FILTER (WHERE bucket = 'new'), %(tt)s, %(tu)s, %(ex)s, %(ed)s, now()
               FROM reexplain_messages WHERE session_id = %(sid)s
               ON CONFLICT (session_id) DO UPDATE SET chat_title = EXCLUDED.chat_title, started_at = EXCLUDED.started_at,
                   user_messages = EXCLUDED.user_messages, asserts = EXCLUDED.asserts,
                   avoidable_delivered = EXCLUDED.avoidable_delivered, avoidable_not_delivered = EXCLUDED.avoidable_not_delivered,
                   new_context = EXCLUDED.new_context, tokens_total = EXCLUDED.tokens_total,
                   tokens_until_edit = EXCLUDED.tokens_until_edit, explore_until_edit = EXCLUDED.explore_until_edit,
                   had_edit = EXCLUDED.had_edit, computed_at = now()""",
            {"sid": sid, "title": chat.get("title"), "start": cost["started_at"], "tt": cost["tokens_total"],
             "tu": cost["tokens_until_edit"], "ex": cost["explore_until_edit"], "ed": cost["had_edit"]})
    return stats


# ---------------------------------------------------------------- judge validation with the owner

def sample_for_labeling(svc: Services, n: int = 30) -> list[dict]:
    """Sample for the owner to label by hand: half the judge said repeat, half it said do not."""
    half = n // 2
    rows = svc.db.query(
        """(SELECT * FROM reexplain_messages WHERE asserts AND repeats AND owner_label IS NULL ORDER BY md5(message_id) LIMIT %s)
           UNION ALL
           (SELECT * FROM reexplain_messages WHERE asserts AND repeats = false AND owner_label IS NULL
            ORDER BY md5(message_id) LIMIT %s)""", (half, n - half))
    ids = sorted({c for r in rows for c in r["candidate_ids"]})
    claims = {c["id"]: c for c in svc.db.query(
        "SELECT c.id, c.statement, e.name AS subject FROM claims c LEFT JOIN entities e ON e.id = c.subject_entity_id "
        "WHERE c.id = ANY(%s)", (ids,))}
    for r in rows:
        r["candidates"] = [claims[c] for c in r["candidate_ids"] if c in claims]
    rows.sort(key=lambda r: r["message_id"])  # without revealing the judge's order
    return rows


def agreement(svc: Services) -> dict:
    """Agreement of the judge with the owner (percentage and Cohen's kappa)."""
    rows = svc.db.query("SELECT repeats, owner_label FROM reexplain_messages WHERE owner_label IS NOT NULL AND asserts")
    n = len(rows)
    if not n:
        return {"n": 0}
    agree = sum(1 for r in rows if bool(r["repeats"]) == r["owner_label"]) / n
    p_judge = sum(1 for r in rows if r["repeats"]) / n
    p_owner = sum(1 for r in rows if r["owner_label"]) / n
    expected = p_judge * p_owner + (1 - p_judge) * (1 - p_owner)
    kappa = (agree - expected) / (1 - expected) if expected < 1 else None
    return {"n": n, "agree": agree, "kappa": kappa}


def summary(svc: Services) -> dict:
    s = svc.db.one(
        """SELECT count(*) AS sessions, coalesce(sum(user_messages), 0) AS msgs, coalesce(sum(asserts), 0) AS asserts,
                  coalesce(sum(avoidable_delivered), 0) AS delivered, coalesce(sum(avoidable_not_delivered), 0) AS not_delivered,
                  coalesce(sum(new_context), 0) AS new,
                  percentile_cont(0.5) WITHIN GROUP (ORDER BY tokens_until_edit) AS med_tokens_until_edit,
                  percentile_cont(0.5) WITHIN GROUP (ORDER BY explore_until_edit) AS med_explore_until_edit
           FROM session_metrics""")
    sessions = svc.db.query("SELECT * FROM session_metrics ORDER BY started_at DESC NULLS LAST LIMIT 40")
    return {"totals": s, "sessions": sessions, "agreement": agreement(svc)}
