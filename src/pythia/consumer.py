"""Interface for the consuming LLM: the 5 MCP operations and the push briefing.

Reads do not use an LLM (they serve what has already been concluded), so they keep
working with cognition paused — always with the lag notice.
"""

from __future__ import annotations

from typing import Any

from . import knowledge as K
from .llm.client import CallContext, LLMError, LLMUnavailable
from .pipeline import correct, prompts, schemas
from .pipeline.derive import knowledge_map
from .services import Services


def pause_note(svc: Services) -> str:
    st = svc.state.get("cognition")
    if st and st.get("paused_since"):
        return (f"⚠ Cognition paused since {st['paused_since']} ({st.get('reason', '')}); "
                "the knowledge may be lagging.\n")
    return ""


def search(svc: Services, query: str, limit: int = 10) -> str:
    items = svc.retriever.search(query, limit=limit)
    if not items:
        return pause_note(svc) + "Nothing found."
    lines = [f"{it['id']} · [{it['type']}] {it['title']} — {it['l0']}" for it in items]
    return pause_note(svc) + "\n".join(lines) + "\n\nUse open(id, level) to go deeper: 1 = page, 2 = claims, 3 = evidence."


def _evidence_line(ev: dict, full: bool = False) -> str:
    if ev["content"] is None:
        return f"{ev['id']} (removed at the source)"
    meta = ev.get("meta") or {}
    where = f" · {meta.get('cwd')}" if meta.get("cwd") else ""
    content = ev["content"] if full else ev["content"][:500]
    return f"{ev['id']} · {ev['occurred_at']:%Y-%m-%d %H:%M} · {ev['source_id']}{where} · {ev['role']}:\n{content}"


def open_item(svc: Services, item_id: str, level: int = 1) -> str:
    prefix = item_id.split("_")[0]
    with svc.db.tx() as conn:
        if prefix == "en":
            view = conn.execute("SELECT id FROM views WHERE kind = 'entity' AND entity_id = %s AND status = 'active'",
                                (item_id,)).fetchone()
            if view:
                item_id, prefix = view["id"], "vw"
            else:
                ent = K.entity(conn, item_id)
                if not ent:
                    return "Not found."
                claims = K.entity_claims(conn, item_id, limit=50)
                return pause_note(svc) + f"{ent['name']} ({ent['type']}), no page yet.\n" + "\n".join(
                    K.format_claim(c) for c in claims)
        if prefix == "vw":
            v = conn.execute("SELECT * FROM views WHERE id = %s", (item_id,)).fetchone()
            if not v:
                return "Not found."
            head = pause_note(svc) + (f"{v['title']} ({v['kind']}{', stale' if v['stale'] else ''}, "
                                      f"generated {v['generated_at']:%Y-%m-%d})\n")
            if level <= 0:
                return head + v["l0"]
            if level == 1:
                return head + v["l1"] + f"\n\nCited claims: {', '.join(v['cited_claim_ids'])}"
            claims = conn.execute("SELECT * FROM claims WHERE id = ANY(%s) ORDER BY recorded_at", (v["cited_claim_ids"],)).fetchall()
            if level == 2:
                return head + "\n".join(K.format_claim(c) for c in claims)
            evs = conn.execute("SELECT * FROM evidence WHERE id = ANY(%s) ORDER BY occurred_at, seq",
                               ([e for c in claims for e in c["evidence_ids"]],)).fetchall()
            return head + "\n\n".join(_evidence_line(e) for e in evs)
        if prefix == "cl":
            c = conn.execute("""SELECT c.*, s.name AS subject_name, o.name AS object_name FROM claims c
                                LEFT JOIN entities s ON s.id = c.subject_entity_id
                                LEFT JOIN entities o ON o.id = c.object_entity_id WHERE c.id = %s""", (item_id,)).fetchone()
            if not c:
                return "Not found."
            text = pause_note(svc) + f"Subject: {c['subject_name']} ({c['subject_entity_id']})\n" + K.format_claim(c)
            if c["supporting_claim_ids"]:
                text += f"\nInferred from: {', '.join(c['supporting_claim_ids'])}"
            if level < 3:
                return text + f"\nEvidence: {', '.join(c['evidence_ids']) or '-'}"
            evs = conn.execute("SELECT * FROM evidence WHERE id = ANY(%s) ORDER BY occurred_at, seq", (c["evidence_ids"],)).fetchall()
            return text + "\n\n" + "\n\n".join(_evidence_line(e, full=True) for e in evs)
        if prefix == "ev":
            ev = conn.execute("SELECT * FROM evidence WHERE id = %s", (item_id,)).fetchone()
            return _evidence_line(ev, full=True) if ev else "Not found."
        if prefix == "vd":
            v = conn.execute("SELECT * FROM verdicts WHERE id = %s", (item_id,)).fetchone()
            if not v:
                return "Not found."
            changes = "; ".join(f"{c['claim_id']}: {c['from']} → {c['to']}" for c in v["changes"])
            return (f"Verdict {v['id']} ({v['kind']}, {v['status']}, by {v['decided_by']}, {v['created_at']:%Y-%m-%d %H:%M})\n"
                    f"{v['summary']}\nRationale: {v['rationale']}\nChanges: {changes}")
    return "ID not recognized."


def timeline(svc: Services, item_id: str) -> str:
    prefix = item_id.split("_")[0]
    with svc.db.tx() as conn:
        if prefix == "vw":
            v = conn.execute("SELECT entity_id, cited_claim_ids FROM views WHERE id = %s", (item_id,)).fetchone()
            if not v:
                return "Not found."
            if v["entity_id"]:
                item_id, prefix = v["entity_id"], "en"
            else:
                claim_ids = v["cited_claim_ids"]
        if prefix == "en":
            claim_ids = [r["id"] for r in conn.execute(
                "SELECT id FROM claims WHERE subject_entity_id = %s OR object_entity_id = %s", (item_id, item_id)).fetchall()]
        elif prefix == "cl":
            claim_ids = [item_id]
        elif prefix != "vw":
            return "Timeline available for entities, pages, narratives and claims."
        rows = conn.execute(
            """SELECT h.from_ts, h.status, h.verdict_id, c.id, c.statement, v.summary, v.kind AS vkind
               FROM claim_status_history h JOIN claims c ON c.id = h.claim_id
               LEFT JOIN verdicts v ON v.id = h.verdict_id
               WHERE h.claim_id = ANY(%s) ORDER BY h.from_ts""", (claim_ids,)).fetchall()
    lines = []
    for r in rows:
        if r["verdict_id"]:
            lines.append(f"{r['from_ts']:%Y-%m-%d %H:%M} · {r['id']} → {r['status']} "
                         f"(verdict {r['verdict_id']}, {r['vkind']}: {r['summary']})")
        else:
            lines.append(f"{r['from_ts']:%Y-%m-%d %H:%M} · {r['id']} {r['status']}: {r['statement']}")
    return pause_note(svc) + ("\n".join(lines) or "No history.")


def request_correction(svc: Services, item_id: str | None, text: str) -> str:
    cid = correct.request_correction(svc, item_id, text, via="mcp")
    return f"Correction {cid} recorded with maximum authority; it will be applied shortly." + (
        "\n" + pause_note(svc) if pause_note(svc) else "")


def request_definition(svc: Services, text: str) -> str:
    unit_id = correct.request_definition(svc, text, via="mcp")
    return f"Definition recorded (unit {unit_id}); it will be incorporated shortly." + (
        "\n" + pause_note(svc) if pause_note(svc) else "")


# ---------------------------------------------------------------- push


def briefing(svc: Services, *, event: str, prompt: str | None, cwd: str | None, session_id: str | None,
             source: str | None = None) -> str:
    """What the hook injects: the rules (at the start of the conversation) and the briefing chosen by an LLM."""
    rules = rules_block(svc, event, source)
    text = _briefing(svc, event=event, prompt=prompt, cwd=cwd, session_id=session_id, source=source)
    return "\n\n".join(t for t in (rules, text) if t)


def rules_block(svc: Services, event: str, source: str | None) -> str:
    """Rules for using Pythia: fixed text from the settings, at the start of each conversation and after /clear or
    compaction (a resume keeps the history, so it does not repeat). It is not judgment: it is the owner's text."""
    cfg = svc.settings.get("push")
    if event != "SessionStart" or source == "resume" or not cfg.get("enabled", True) or not cfg.get("rules_enabled", True):
        return ""
    return (cfg.get("rules") or "").replace("{owner}", svc.owner_name).strip()


def _briefing(svc: Services, *, event: str, prompt: str | None, cwd: str | None, session_id: str | None,
              source: str | None = None) -> str:
    """Briefing chosen by an LLM. The LLM only chooses IDs; the text is their L0, with no free text.
    Without an LLM, it injects nothing (there is no heuristically assembled briefing)."""
    cfg = svc.settings.get("push")
    if not cfg.get("enabled", True):
        _log_push(svc, session_id, event, [], 0, "disabled", cwd=cwd)
        return ""
    with svc.db.tx() as conn:
        kmap = knowledge_map(conn)
        own = K.owner(conn)
        own_view = conn.execute("SELECT l0 FROM views WHERE kind = 'entity' AND entity_id = %s AND status = 'active'",
                                (own["id"],)).fetchone() if own else None
        valid_ids = {r["id"] for r in conn.execute("SELECT id FROM views WHERE status = 'active'").fetchall()}
        recent_verdicts = conn.execute(
            """SELECT v.id, v.kind, v.summary FROM verdicts v WHERE v.status = 'applied'
               ORDER BY v.created_at DESC LIMIT 30""").fetchall()
    if not kmap:
        _log_push(svc, session_id, event, [], 0, "no knowledge", cwd=cwd)
        return ""
    user = f"Event: {event}\nWorking directory: {cwd or '?'}\n"
    user += f"Current request from {svc.owner_name}:\n{prompt}" if prompt else "Session start (no request yet)."
    if recent_verdicts:
        user += "\n\nRecent verdicts:\n" + "\n".join(f"{v['id']} ({v['kind']}): {v['summary']}" for v in recent_verdicts)
    try:
        out = svc.llm.structured("briefing", prompts.BRIEFING.format(owner=svc.owner_name), user, schemas.BRIEFING,
                                 max_tokens=1500, cached_context="KNOWLEDGE MAP\n" + kmap,
                                 ctx=CallContext(source_id="push"))
    except (LLMUnavailable, LLMError) as exc:
        _log_push(svc, session_id, event, [], 0, f"no LLM: {exc}"[:200], cwd=cwd)
        return ""
    selected = [i for i in out["relevant_ids"] if i in valid_ids][:8]
    from .usage import view_ref
    with svc.db.tx() as conn:
        own_ref = conn.execute("SELECT id FROM views WHERE kind = 'entity' AND entity_id = %s AND status = 'active'",
                               (own["id"],)).fetchone() if own else None
        resets = event == "SessionStart" and source != "resume"
        delivered = set() if resets else already_delivered(conn, chat_sessions(svc, session_id))
        candidates = ([("own", view_ref(conn, own_ref["id"]))] if own_view and own_ref else []) + \
            [("view", view_ref(conn, vid)) for vid in selected]
        rows = {r["id"]: r for r in conn.execute(
            "SELECT id, title, l0 FROM views WHERE id = ANY(%s)", (selected,)).fetchall()} if selected else {}
    # Only what has not yet been delivered in this conversation (same ID and version): what is already in the
    # LLM's context is not repeated on every request. Conversation start (or resume after compaction) delivers everything.
    # The owner profile goes once per conversation (it is background; a page rewrite does not justify
    # repeating it); the subject pages come back when they change version.
    delivered_ids = {i for i, _v in delivered}
    fresh = [(k, r) for k, r in candidates
             if (r["id"] not in delivered_ids if k == "own" else (r["id"], r.get("version")) not in delivered)]
    by_id = {v["id"]: v for v in recent_verdicts}
    verdicts = [by_id[v] for v in out["verdict_ids"][:2] if v in by_id and (v, None) not in delivered]
    if not fresh and not verdicts:
        _log_push(svc, session_id, event, selected, 0, "already delivered in this conversation", cwd=cwd, source=source)
        return ""
    parts = [f"[Pythia — context about {svc.owner_name}. Go deeper with the pythia MCP tools: open(id, level).]"]
    for kind, ref in fresh:
        if kind == "own":
            parts.append(f"About {svc.owner_name}: {own_view['l0']}")
        elif ref["id"] in rows:
            r = rows[ref["id"]]
            parts.append(f"- {r['id']} · {r['title']}: {r['l0']}")
    for v in verdicts:
        parts.append(f"Attention: recent verdict {v['id']} ({v['kind']}): {v['summary']}")
    note = pause_note(svc)
    if note:
        parts.append(note.strip())
    text = "\n".join(parts)[: int(cfg.get("max_chars", 6000))]
    # Usage: references only, with the delivered version of each page/narrative.
    refs = [r for _, r in fresh] + [{"id": v["id"]} for v in verdicts]
    _log_push(svc, session_id, event, [r["id"] for k, r in fresh if k == "view"], len(text), None, refs=refs, cwd=cwd,
              source=source)
    return text


def chat_sessions(svc: Services, session_id: str | None) -> list[str]:
    """All session files of this session's chat: resuming creates a new file, but it is the same
    conversation, with the same history in the LLM."""
    if not session_id:
        return []
    from .connectors import claude_code as cc
    cfg = svc.settings.get("claude_code")
    for chat in cc.discover_chats(cc.projects_root(cfg), cc.app_sessions_root(cfg)):
        if session_id in chat["files"]:
            return chat["files"]
    return [session_id]


def already_delivered(conn, sessions: list[str]) -> set[tuple[str, int | None]]:
    """(ID, version) already injected in this chat since the last restart of the LLM's context (SessionStart of
    type startup, /clear or compaction; a resume does not count, the history continues)."""
    if not sessions:
        return set()
    start = conn.execute("""SELECT max(ts) AS t FROM pushes WHERE session_id = ANY(%s) AND event = 'SessionStart'
                            AND coalesce(source, 'startup') <> 'resume'""", (sessions,)).fetchone()["t"]
    rows = conn.execute("""SELECT refs FROM pushes WHERE session_id = ANY(%s) AND skipped IS NULL
                           AND ts >= coalesce(%s, '-infinity'::timestamptz)""", (sessions, start)).fetchall()
    return {(r["id"], r.get("version")) for row in rows for r in row["refs"] if r.get("id")}


def _log_push(svc: Services, session_id, event, selected, chars, skipped, refs=None, cwd=None, source=None) -> None:
    from psycopg.types.json import Jsonb
    svc.db.execute("""INSERT INTO pushes (session_id, event, selected_ids, chars, skipped, refs, cwd, source)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                   (session_id, event, selected, chars, skipped, Jsonb(refs or []), cwd, source))
