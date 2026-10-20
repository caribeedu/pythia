"""Connector for Claude Code sessions (local JSONL transcripts, append-only).

Each session file is a stream with a cursor (byte offset). A stream is
*settled* when it grew past the cursor and has not changed for `debounce_minutes`
(each LLM reply restarts the clock). The delta becomes a unit; each
message in the delta becomes a piece of evidence.

Everything here is deterministic plumbing: read, split, strip harness
markup and store. No interpretation.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from psycopg.types.json import Jsonb

from ..db import Database
from ..ids import new_id, sha
from ..jobs import enqueue

KIND = "claude_code"
MAX_UNIT_CHARS = 60_000
_HARNESS_TAGS = re.compile(r"<system-reminder>.*?</system-reminder>", re.DOTALL)


def projects_root(settings_value: dict) -> Path:
    return Path(os.path.expanduser(settings_value.get("projects_root", "~/.claude/projects")))


def source_id_for(project_dir: Path) -> str:
    """Source for a whole project folder (chats are the usual unit; see `chat_source_id`)."""
    return f"{KIND}:{project_dir}"


PROFILE_SOURCE = f"{KIND}:profile"   # owner profile from ALL chats (only his messages)


def profile_files(chats: list[dict], enabled_chat_ids: set[str], excluded_titles: list[str]) -> list[str]:
    """Files of chats that are neither fully enabled nor excluded (the "Pythia" chat stays out)."""
    out = []
    for c in chats:
        title = (c.get("title") or "").strip().lower()
        if c["id"] in enabled_chat_ids or any(title.startswith(x.strip().lower()) for x in excluded_titles if x.strip()):
            continue
        out.extend(c["files"])
    return sorted(set(out))


def chat_source_id(chat_id: str) -> str:
    return f"{KIND}:chat:{chat_id}"


def app_sessions_root(settings_value: dict) -> Path:
    return Path(os.path.expanduser(settings_value.get(
        "app_sessions_root", "~/Library/Application Support/Claude/claude-code-sessions")))


_HEAD_CACHE: dict[str, tuple[float, dict]] = {}


def _file_info(path: Path) -> dict:
    """First message ID (identity of the resume chain), last title, cwd and mtime.
    Cached in memory by mtime: the file is only re-read when it changes."""
    st = path.stat()
    cached = _HEAD_CACHE.get(str(path))
    if cached and cached[0] == st.st_mtime:
        return cached[1]
    info = {"first": None, "title": None, "cwd": None, "mtime": st.st_mtime}
    with open(path, "rb") as f:
        for raw in f:
            try:
                d = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if d.get("type") == "custom-title":
                info["title"] = d.get("customTitle") or info["title"]
            elif d.get("type") in ("user", "assistant"):
                info["first"] = info["first"] or d.get("uuid")
                info["cwd"] = info["cwd"] or d.get("cwd")
    _HEAD_CACHE[str(path)] = (st.st_mtime, info)
    return info


def discover_chats(projects: Path, app_root: Path) -> list[dict]:
    """Chats for the allowlist. The chat identity is the Claude app's (fixed ID, current title,
    current and previous files): renaming changes nothing and resuming continues the same chat. A chat without that
    record (terminal) is the chain of files that start with the same message (each resume copies the
    history with the same IDs). App-internal format: if it cannot be read, falls back to the chain."""
    files = {p.stem: p for p in projects.glob("*/*.jsonl")} if projects.is_dir() else {}
    chats: dict[str, dict] = {}
    covered: set[str] = set()
    if app_root.is_dir():
        for f in app_root.glob("*/*/*.json"):
            try:
                d = json.loads(f.read_text())
            except (OSError, json.JSONDecodeError):
                continue
            stems = [x for x in [d.get("cliSessionId"), *(d.get("priorCliSessionIds") or [])] if x and x in files]
            if not stems or not d.get("sessionId"):
                continue
            last = d.get("lastActivityAt")
            chats[d["sessionId"]] = {
                "id": d["sessionId"], "title": d.get("title") or "(untitled)", "files": stems,
                "cwd": d.get("cwd"), "archived": bool(d.get("isArchived")), "origin": "app",
                "last_activity": datetime.fromtimestamp(last / 1000, tz=timezone.utc) if last else
                datetime.fromtimestamp(max(files[x].stat().st_mtime for x in stems), tz=timezone.utc)}
            covered |= set(stems)
    chains: dict[str, list[tuple[str, dict]]] = {}
    for stem, path in files.items():
        if stem in covered:
            continue
        try:
            info = _file_info(path)
        except OSError:
            continue
        chains.setdefault(info["first"] or stem, []).append((stem, info))
    for members in chains.values():
        members.sort(key=lambda m: m[1]["mtime"])
        cid = f"cli-{members[0][0]}"
        latest = members[-1][1]
        chats[cid] = {"id": cid, "title": latest["title"] or next((m[1]["title"] for m in reversed(members) if m[1]["title"]), None)
                      or "(untitled)", "files": [m[0] for m in members], "cwd": latest["cwd"], "archived": False,
                      "origin": "terminal", "last_activity": datetime.fromtimestamp(latest["mtime"], tz=timezone.utc)}
    return sorted(chats.values(), key=lambda c: c["last_activity"], reverse=True)


def session_paths(projects: Path) -> dict[str, Path]:
    return {p.stem: p for p in projects.glob("*/*.jsonl")} if projects.is_dir() else {}


def discover_projects(root: Path) -> list[dict]:
    """Lists the project directories available for the allowlist."""
    out = []
    if not root.is_dir():
        return out
    for d in sorted(root.iterdir()):
        if not d.is_dir():
            continue
        files = list(d.glob("*.jsonl"))
        if not files:
            continue
        last = max(f.stat().st_mtime for f in files)
        out.append({
            "path": str(d),
            "name": d.name,
            "sessions": len(files),
            "last_activity": datetime.fromtimestamp(last, tz=timezone.utc),
        })
    return out


# ---------------------------------------------------------------- reading


@dataclass
class Message:
    external_id: str
    role: str
    text: str
    occurred_at: datetime
    meta: dict = field(default_factory=dict)


@dataclass
class Delta:
    messages: list[Message]
    title: str | None
    session_id: str | None
    cwd: str | None
    branch: str | None
    end_offset: int


def _ts(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _clean(text: str) -> str:
    return _HARNESS_TAGS.sub("", text).strip()


def _summarize_tool(block: dict) -> str:
    name = block.get("name", "?")
    inp = block.get("input") or {}
    detail = inp.get("description") or inp.get("command") or inp.get("query") or inp.get("file_path") \
        or inp.get("prompt") or inp.get("pattern") or ""
    detail = str(detail).replace("\n", " ")[:200]
    return f"[tool {name}: {detail}]" if detail else f"[tool {name}]"


def read_delta(path: Path, offset: int, include_sidechains: bool = False) -> Delta:
    """Reads complete lines from `offset` and groups them into messages."""
    with open(path, "rb") as f:
        f.seek(offset)
        data = f.read()
    last_newline = data.rfind(b"\n")
    if last_newline < 0:
        return Delta([], None, None, None, None, offset)
    data = data[: last_newline + 1]
    end_offset = offset + len(data)

    messages: list[Message] = []
    by_assistant_id: dict[str, Message] = {}
    title = session_id = cwd = branch = None
    for raw in data.splitlines():
        try:
            line = json.loads(raw)
        except json.JSONDecodeError:
            continue
        kind = line.get("type")
        if kind == "custom-title":
            title = line.get("customTitle") or title
            continue
        if kind not in ("user", "assistant"):
            continue
        if line.get("isSidechain") and not include_sidechains:
            continue
        if line.get("isMeta") or line.get("isCompactSummary"):
            continue  # compaction summary: written by the AI, recorded as if from the user
        session_id = line.get("sessionId") or session_id
        cwd = line.get("cwd") or cwd
        branch = line.get("gitBranch") or branch
        msg = line.get("message") or {}
        content = msg.get("content")
        when = _ts(line.get("timestamp"))
        meta = {"cwd": line.get("cwd"), "branch": line.get("gitBranch")}
        if kind == "user":
            if isinstance(content, str):
                text = _clean(content)
            else:
                text = _clean("\n".join(b.get("text", "") for b in content or [] if b.get("type") == "text"))
            if text:
                messages.append(Message(line.get("uuid") or sha(text)[:16], "user", text, when, meta))
            continue
        # assistant: blocks of the same message arrive on separate lines
        parts = []
        for b in content or []:
            if b.get("type") == "text" and b.get("text"):
                parts.append(b["text"].strip())
            elif b.get("type") == "tool_use":
                parts.append(_summarize_tool(b))
        if not parts:
            continue
        mid = msg.get("id") or line.get("uuid")
        existing = by_assistant_id.get(mid)
        if existing:
            existing.text += "\n" + "\n".join(parts)
        else:
            m = Message(mid, "assistant", "\n".join(parts), when, meta)
            by_assistant_id[mid] = m
            messages.append(m)
    return Delta(messages, title, session_id, cwd, branch, end_offset)


def split_messages(messages: list[Message], limit: int = MAX_UNIT_CHARS) -> list[list[Message]]:
    """Splits into units on message boundaries, without truncating content."""
    chunks: list[list[Message]] = []
    current: list[Message] = []
    size = 0
    for m in messages:
        if current and size + len(m.text) > limit:
            chunks.append(current)
            current, size = [], 0
        current.append(m)
        size += len(m.text)
    if current:
        chunks.append(current)
    return chunks


# ---------------------------------------------------------------- scan


class ClaudeCodeConnector:
    def __init__(self, db: Database, owner_name: str):
        self.db = db
        self.owner_name = owner_name

    def scan(self, source: dict, debounce_s: float, include_sidechains: bool = False,
             now: float | None = None, paths: dict[str, Path] | None = None) -> dict:
        """Scans a chat (its session files). Returns counters. Idempotent.

        A chat file that vanished from disk becomes a tombstone only if its folder still exists (the
        folder listing is complete); a missing folder (unmounted disk, changed root) deletes nothing."""
        now = now if now is not None else time.time()
        stats = {"streams": 0, "units": 0, "deleted": 0}
        if "path" in source["config"]:  # source pointing at a project folder
            project = Path(source["config"]["path"])
            try:
                files = {str(p): p for p in project.glob("*.jsonl")}
            except OSError:
                return stats
        else:
            paths = paths or {}
            files = {str(paths[stem]): paths[stem] for stem in source["config"].get("files") or [] if stem in paths}
        with self.db.tx() as conn:
            known = {r["locator"]: r for r in conn.execute(
                "SELECT * FROM streams WHERE source_id = %s", (source["id"],)).fetchall()}
        for locator, path in files.items():
            stats["streams"] += 1
            try:
                st = path.stat()
            except FileNotFoundError:
                continue
            stream = known.get(locator)
            profile = bool(source["config"].get("profile_only"))
            stream_id = f"{PROFILE_SOURCE}:{path.stem}" if profile else f"{KIND}:{path.stem}"
            if stream is None:
                with self.db.tx() as conn:
                    conn.execute(
                        """INSERT INTO streams (id, source_id, locator, observed_size, observed_mtime, last_change_at)
                           VALUES (%s, %s, %s, %s, %s, now()) ON CONFLICT (id) DO NOTHING""",
                        (stream_id, source["id"], locator, st.st_size, st.st_mtime),
                    )
                stream = {"id": stream_id, "cursor": {}, "observed_size": st.st_size,
                          "observed_mtime": st.st_mtime, "status": "active"}
            elif st.st_size != stream["observed_size"]:
                with self.db.tx() as conn:
                    conn.execute(
                        """UPDATE streams SET observed_size = %s, observed_mtime = %s, last_change_at = now(),
                                  status = 'active', updated_at = now() WHERE id = %s""",
                        (st.st_size, st.st_mtime, stream["id"]),
                    )
            offset = int((stream.get("cursor") or {}).get("offset", 0))
            if st.st_size < offset:
                # File rewritten to a smaller size: the old content no longer exists at the source.
                with self.db.tx() as conn:
                    enqueue(conn, "tombstone_stream", "cognition", {"stream_id": stream["id"], "reset": True},
                            priority=10, dedup_key=f"tombstone:{stream['id']}")
                continue
            settled = st.st_size > offset and (now - st.st_mtime) >= debounce_s
            if settled:
                stats["units"] += self._settle(source, stream["id"], path, offset, st.st_mtime, include_sidechains)
        for locator, stream in known.items():
            gone = locator not in files and not Path(locator).exists() and Path(locator).parent.is_dir()
            if gone and stream["status"] == "active":
                stats["deleted"] += 1
                with self.db.tx() as conn:
                    conn.execute("UPDATE streams SET status = 'deleted', updated_at = now() WHERE id = %s",
                                 (stream["id"],))
                    enqueue(conn, "tombstone_stream", "cognition", {"stream_id": stream["id"]},
                            priority=10, dedup_key=f"tombstone:{stream['id']}")
        return stats

    def _settle(self, source: dict, stream_id: str, path: Path, offset: int, mtime: float,
                include_sidechains: bool) -> int:
        delta = read_delta(path, offset, include_sidechains)
        created = 0
        with self.db.tx() as conn:
            if delta.title:
                conn.execute("UPDATE streams SET title = %s WHERE id = %s", (delta.title, stream_id))
            title = delta.title or conn.execute("SELECT title FROM streams WHERE id = %s",
                                                (stream_id,)).fetchone()["title"]
            profile = bool(source["config"].get("profile_only"))
            # Profile from all chats: only the owner's messages become evidence; the AI reply he reacts to is
            # read from the transcript at processing time, as context, and is never stored.
            messages = [m for m in delta.messages if m.role == "user"] if profile else delta.messages
            for chunk in split_messages(messages):
                fresh = []
                for m in chunk:
                    # IDs are global within the source: a resumed session repeats another one's history.
                    # Also across chats: dedup holds for all Claude Code sessions. The profile source
                    # does not count for enabled chats (if a chat is enabled later, it reads everything).
                    exists = conn.execute(
                        "SELECT 1 FROM evidence WHERE source_id LIKE %s AND external_id = %s"
                        + ("" if profile else " AND source_id <> %s"),
                        (f"{KIND}:%", m.external_id) + (() if profile else (PROFILE_SOURCE,))).fetchone()
                    if not exists:
                        fresh.append(m)
                if not fresh:
                    continue
                first_user = next((m.text for m in fresh if m.role == "user"), fresh[0].text)
                unit_title = title or first_user[:120]
                # Priority: most recent first (backfill from new to old).
                recency = int(fresh[-1].occurred_at.timestamp())
                unit = conn.execute(
                    """INSERT INTO units (source_id, stream_id, kind, title, occurred_at, meta)
                       VALUES (%s, %s, 'session_delta', %s, %s, %s) RETURNING id""",
                    (source["id"], stream_id, unit_title, fresh[-1].occurred_at,
                     Jsonb({"session_id": delta.session_id, "cwd": delta.cwd, "branch": delta.branch,
                            "transcript": str(path), "offset_from": offset, "offset_to": delta.end_offset,
                            **({"profile_only": True} if profile else {})})),
                ).fetchone()
                for m in fresh:
                    conn.execute(
                        """INSERT INTO evidence (id, unit_id, source_id, stream_id, external_id, author, role,
                                                 occurred_at, content, content_hash, meta)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (new_id("ev"), unit["id"], source["id"], stream_id, m.external_id,
                         self.owner_name if m.role == "user" else "assistant", m.role, m.occurred_at,
                         m.text, sha(m.text), Jsonb(m.meta)),
                    )
                enqueue(conn, "process_unit", "cognition", {"unit_id": unit["id"]},
                        priority=_recency_priority(recency), dedup_key=f"unit:{unit['id']}")
                created += 1
            conn.execute(
                """UPDATE streams SET cursor = %s, last_settled_at = now(), updated_at = now() WHERE id = %s""",
                (Jsonb({"offset": delta.end_offset}), stream_id),
            )
        return created


def _recency_priority(epoch: int) -> int:
    """More recent = lower number = processed sooner. A window of ~10 years in days."""
    days_ago = max(0, int((time.time() - epoch) // 86400))
    return 1000 + min(days_ago, 4000)


CONTEXT_CHARS = 20_000


def unit_context(conn, unit: dict, evidence: list[dict]) -> list[dict]:
    """what came right before the delta in the same chat (the assistant reply the owner reacts to), so
    extraction understands short reactions like "shorten this". Context only: it neither becomes evidence nor is cited.
    Looks up by chat (the source), not by file, because resuming the session opens another file. The message
    immediately before always goes in whole; the others, up to `CONTEXT_CHARS`."""
    if not evidence:
        return []
    first = min(e["occurred_at"] for e in evidence)
    before = conn.execute(
        """SELECT * FROM evidence WHERE source_id = %s AND status = 'active' AND NOT (id = ANY(%s))
           AND occurred_at < %s ORDER BY occurred_at DESC LIMIT 20""",
        (unit["source_id"], [e["id"] for e in evidence], first)).fetchall()
    picked, size = [], 0
    for e in before:
        if picked and size + len(e["content"]) > CONTEXT_CHARS:
            break
        picked.append(e)
        size += len(e["content"])
    return list(reversed(picked))


PROFILE_REPLY_CHARS = 1500   # the end of the reply the owner reacts to (what he usually comments on)


def profile_context(unit: dict, evidence: list[dict]) -> list[dict]:
    """Profile from all chats: for each owner message, the AI reply that came right before it, read from the
    transcript now (not stored). File gone = no context."""
    meta = unit.get("meta") or {}
    path = Path(meta.get("transcript") or "")
    if not evidence or not path.is_file():
        return []
    try:
        msgs = read_delta(path, 0).messages
    except OSError:
        return []
    wanted = {e["external_id"] for e in evidence}
    out, last_reply = [], None
    for m in msgs:
        if m.role == "assistant":
            last_reply = m
        elif m.external_id in wanted and last_reply is not None:
            text = last_reply.text
            out.append({"occurred_at": last_reply.occurred_at, "role": "assistant",
                        "content": ("…" + text[-PROFILE_REPLY_CHARS:]) if len(text) > PROFILE_REPLY_CHARS else text})
            last_reply = None
    return out
