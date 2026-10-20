"""Usage: what was delivered to the LLMs, by Pull (MCP) and Push (hook).

Only references are stored — pages/narratives with the delivered version, claims, evidence,
entities, verdicts and corrections —, in the order they appeared. The screen rebuilds the text from the
history; what was erased at the source shows up as erased (deletion keeps holding here).
Extracting IDs from the returned text is plumbing: IDs have a fixed format.
"""

from __future__ import annotations

import json
import re

from psycopg.types.json import Jsonb

from .events import REDACTED
from .services import Services

_ID = re.compile(r"\b(vw|cl|ev|en|vd|co)_[0-9a-f]{12}\b")
_UNIT = re.compile(r"\(unit (\d+)\)")


def refs_in(conn, text: str) -> list[dict]:
    """IDs in the text, in order, without repetition; pages and narratives carry the current version (the delivered one)."""
    seen, out = set(), []
    for m in _ID.finditer(text or ""):
        rid = m.group(0)
        if rid in seen:
            continue
        seen.add(rid)
        ref = {"id": rid}
        if rid.startswith("vw_"):
            row = conn.execute("SELECT version FROM views WHERE id = %s", (rid,)).fetchone()
            if row:
                ref["version"] = row["version"]
        out.append(ref)
    for m in _UNIT.finditer(text or ""):
        out.append({"unit": int(m.group(1))})
    return out


def log_call(svc: Services, tool: str, args: dict, client: str | None, session: str | None,
             output: str | None, error: Exception | None) -> None:
    with svc.db.tx() as conn:
        refs = refs_in(conn, output or "")
        if tool in ("open", "timeline") and output and args.get("id"):
            # The opened item comes first (the output does not always repeat its own ID); an entity with a page
            # is opened through the page, as in consumer.open_item.
            target = args["id"]
            if target.startswith("en_"):
                v = conn.execute("SELECT id FROM views WHERE kind = 'entity' AND entity_id = %s AND status = 'active'",
                                 (target,)).fetchone()
                target = v["id"] if v else target
            refs = [r for r in refs if r.get("id") != target]
            refs.insert(0, view_ref(conn, target) if target.startswith("vw_") else {"id": target})
        conn.execute(
            """INSERT INTO mcp_calls (tool, args, client, session, refs, chars, ok, error)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (tool, Jsonb(args), client, session, Jsonb(refs), len(output or ""),
             error is None, str(error)[:300] if error else None))


def view_ref(conn, view_id: str) -> dict:
    row = conn.execute("SELECT version FROM views WHERE id = %s", (view_id,)).fetchone()
    return {"id": view_id, **({"version": row["version"]} if row else {})}


# ---------------------------------------------------------------- rebuilding for the screen


def render_ref(conn, ref: dict, ts, *, full: bool = False) -> dict:
    """What a reference showed when it was delivered. `full`: the whole page (L1), not just the L0."""
    if "unit" in ref:
        return {"id": f"unit {ref['unit']}", "label": "definition recorded", "text": ""}
    rid = ref["id"]
    gone = {"id": rid, "label": "(erased at the source)", "text": "", "gone": True}
    kind = rid.split("_")[0]
    if kind == "vw":
        if ref.get("version"):
            row = conn.execute("SELECT * FROM view_versions WHERE view_id = %s AND version = %s",
                               (rid, ref["version"])).fetchone()
        else:  # recorded without a version: the version in force at delivery time (version history)
            row = conn.execute("""SELECT * FROM view_versions WHERE view_id = %s AND generated_at <= %s
                                  ORDER BY version DESC LIMIT 1""", (rid, ts)).fetchone()
        if not row:
            return gone
        return {"id": rid, "label": f"{row['title']} · version {row['version']}", "text": row["l1"] if full else row["l0"]}
    if kind == "cl":
        c = conn.execute("SELECT statement, status FROM claims WHERE id = %s", (rid,)).fetchone()
        if not c or c["statement"] == REDACTED:
            return gone
        st = conn.execute("""SELECT status FROM claim_status_history WHERE claim_id = %s AND from_ts <= %s
                             ORDER BY from_ts DESC LIMIT 1""", (rid, ts)).fetchone()
        return {"id": rid, "label": f"claim ({st['status'] if st else c['status']} at the time)", "text": c["statement"]}
    if kind == "ev":
        e = conn.execute("SELECT content, occurred_at, role FROM evidence WHERE id = %s", (rid,)).fetchone()
        if not e or e["content"] is None:
            return gone
        return {"id": rid, "label": f"evidence · {e['role']} · {e['occurred_at']:%Y-%m-%d %H:%M}",
                "text": e["content"] if full else e["content"][:500]}
    if kind == "en":
        e = conn.execute("SELECT name, type FROM entities WHERE id = %s", (rid,)).fetchone()
        return {"id": rid, "label": f"entity · {e['name']} ({e['type']})" if e else "entity", "text": ""}
    if kind == "vd":
        v = conn.execute("SELECT kind, summary FROM verdicts WHERE id = %s", (rid,)).fetchone()
        return {"id": rid, "label": f"verdict · {v['kind']}" if v else "verdict", "text": v["summary"] if v else ""}
    if kind == "co":
        c = conn.execute("SELECT text, status FROM corrections WHERE id = %s", (rid,)).fetchone()
        return {"id": rid, "label": f"correction · {c['status']}" if c else "correction", "text": c["text"] if c else ""}
    return {"id": rid, "label": "", "text": ""}


def pull_calls(svc: Services, limit: int = 100) -> list[dict]:
    with svc.db.tx() as conn:
        rows = conn.execute("SELECT * FROM mcp_calls ORDER BY ts DESC LIMIT %s", (limit,)).fetchall()
        for r in rows:
            opened = r["refs"][0].get("id") if r["tool"] == "open" and r["refs"] else None
            level = int(r["args"].get("level", 1)) if r["tool"] == "open" else 0
            r["entries"] = [render_ref(conn, ref, r["ts"], full=(ref.get("id") == opened and level >= 1) or level >= 3)
                          for ref in r["refs"]]
            r["args_text"] = ", ".join(f"{k}={json.dumps(v, ensure_ascii=False)}" for k, v in r["args"].items())
    return rows


def push_by_chat(svc: Services, chat_titles: dict[str, str], limit: int = 200) -> list[dict]:
    """Pushes grouped by chat (the hook's session_id is the Claude Code session file)."""
    groups: dict[str, dict] = {}
    with svc.db.tx() as conn:
        for p in conn.execute("SELECT * FROM pushes ORDER BY ts DESC LIMIT %s", (limit,)).fetchall():
            title = chat_titles.get(p["session_id"] or "") or f"session {(p['session_id'] or '?')[:8]}"
            g = groups.setdefault(title, {"title": title, "pushes": [], "last": p["ts"]})
            refs = p["refs"]
            p["entries"] = [render_ref(conn, ref, p["ts"]) for ref in refs]
            g["pushes"].append(p)
    return sorted(groups.values(), key=lambda g: g["last"], reverse=True)
