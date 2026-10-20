"""Slack connector: allowlisted channels and DMs, read with the owner's user token (no bot).

Units (§6.1):
- **thread**: the replies to a message. Settles `settle_minutes` after the last reply;
  a new reply after that becomes a delta (only what is new, with the rest of the thread as context);
- **window**: loose channel messages (no thread), grouped while the gap between one and
  the next is less than `settle_minutes`. The window settles when the channel stays idle that long.

Each message is a piece of evidence. On every read, the last `revision_days` are re-read in full:
an edited message retracts the previous version; a message that vanished becomes a tombstone, but only when the
listing covers the whole stream (never delete based on an incomplete scan). Everything here is
plumbing: read, group by time, format Slack markup and store.

Bots: every message Slack flags as from a bot (`bot_id` or `subtype=bot_message`) is ignored,
and so is the whole thread that starts with a bot; `read_bots` are the exceptions. Ignored authors and subtypes
are owner lists.
"""

from __future__ import annotations

import html
import re
import time
from datetime import datetime, timedelta, timezone

import httpx
from psycopg.types.json import Jsonb

from .. import vault
from ..db import Database
from ..ids import sha
from .common import Item, SourceUnavailable, create_unit, sync_stream

KIND = "slack"
API = "https://slack.com/api/"
# User token scopes: read history and list conversations of each type, and user names.
REQUIRED_SCOPES = {
    "channel": {"channels:history", "channels:read"},
    "private": {"groups:history", "groups:read"},
    "dm": {"im:history", "im:read"},
    "group": {"mpim:history", "mpim:read"},
}
BASE_SCOPES = {"users:read"}
MAX_UNIT_CHARS = 300_000
_USER = re.compile(r"<@([UW][A-Z0-9]+)(?:\|[^>]*)?>")
_CHANNEL = re.compile(r"<#([CG][A-Z0-9]+)(?:\|([^>]*))?>")
_SPECIAL = re.compile(r"<!(here|channel|everyone)(?:\|[^>]*)?>")
_SUBTEAM = re.compile(r"<!subteam\^([A-Z0-9]+)(?:\|([^>]*))?>")
_LINK = re.compile(r"<((?:https?|mailto):[^|>]+)(?:\|([^>]*))?>")


def source_id_for(channel_id: str) -> str:
    return f"{KIND}:{channel_id}"


def _dt(ts: str | float) -> datetime:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc)


def resolve_token() -> str:
    """API + owner token only, stored in the Keychain."""
    token = vault.get_secret(vault.SLACK_TOKEN)
    if not token:
        raise SourceUnavailable("Slack token not configured (Settings → Slack)")
    return token


class NotFound(SourceUnavailable):
    """Slack states that the object does not exist (e.g. deleted thread): it is a complete answer, not a failure."""


class SlackClient:
    def __init__(self, token: str, transport: httpx.BaseTransport | None = None, max_wait_s: float = 60):
        self.http = httpx.Client(base_url=API, transport=transport, timeout=30,
                                 headers={"Authorization": f"Bearer {token}", "User-Agent": "pythia"})
        self.max_wait_s = max_wait_s
        self.scopes: set[str] = set()

    def call(self, method: str, params: dict | None = None) -> dict:
        """One Web API call. Rate limit: waits for Retry-After (up to `max_wait_s` in
        total per call) and tries again; beyond that, the source is unavailable and the job goes back."""
        waited = 0.0
        while True:
            try:
                r = self.http.get(method, params=params)
            except httpx.HTTPError as exc:
                raise SourceUnavailable(f"Slack unreachable: {exc}") from exc
            if r.status_code == 429:
                wait = float(r.headers.get("retry-after") or 30)
                if waited + wait > self.max_wait_s:
                    raise SourceUnavailable(f"Slack rate limit ({method}); will retry later")
                time.sleep(wait)
                waited += wait
                continue
            if r.status_code >= 500:
                raise SourceUnavailable(f"Slack {r.status_code}")
            r.raise_for_status()
            if r.headers.get("x-oauth-scopes") is not None:
                self.scopes = {s.strip() for s in r.headers["x-oauth-scopes"].split(",") if s.strip()}
            body = r.json()
            if not body.get("ok"):
                err = body.get("error", "unknown error")
                if err in ("invalid_auth", "not_authed", "token_revoked", "token_expired", "account_inactive"):
                    raise SourceUnavailable(f"Slack rejected the token ({err})")
                if err == "missing_scope":
                    raise SourceUnavailable(f"Slack token without the required scope: {body.get('needed')}")
                if err in ("thread_not_found", "message_not_found"):
                    raise NotFound(err)
                if err in ("channel_not_found", "not_in_channel"):
                    raise SourceUnavailable(f"no access to the conversation ({err})")
                if err == "ratelimited":
                    raise SourceUnavailable("Slack rate limit")
                raise SourceUnavailable(f"Slack: {err}")
            return body

    def pages(self, method: str, key: str, params: dict | None = None):
        """Iterates a cursor listing; ends only when Slack says it is over (complete listing)."""
        cursor = None
        while True:
            body = self.call(method, {**(params or {}), "limit": 200, **({"cursor": cursor} if cursor else {})})
            yield from body.get(key) or []
            cursor = (body.get("response_metadata") or {}).get("next_cursor")
            if not cursor:
                return


def verify_token(client: SlackClient) -> dict:
    """Checks the token before reading: it must be a user token (no bot) and have the read scopes."""
    me = client.call("auth.test")
    if me.get("bot_id"):
        raise SourceUnavailable("the token is a bot token; Pythia reads with the owner's user token (xoxp-)")
    missing = BASE_SCOPES - client.scopes if client.scopes else set()
    if missing:
        raise SourceUnavailable(f"Slack token without the scopes: {', '.join(sorted(missing))}")
    return me


def conversation_kind(c: dict) -> str:
    if c.get("is_im"):
        return "dm"
    if c.get("is_mpim"):
        return "group"
    return "private" if c.get("is_private") else "channel"


def k_scope(kind: str) -> str:
    return {"channel": "channels", "private": "groups", "dm": "im", "group": "mpim"}[kind]


def group_name(mpdm: str, users: dict[str, dict], me_id: str | None = None) -> str:
    """`mpdm-alex.moreira--ana.lima--bruno.reis-1` (Slack's format for group conversations) →
    "Ana Lima, Bruno Reis": logins become names, and the owner is dropped from the list."""
    if not mpdm.startswith("mpdm-"):
        return ""
    by_login = {u.get("login"): u for u in users.values() if u.get("login")}
    body = re.sub(r"-\d+$", "", mpdm[len("mpdm-"):])
    names = []
    for login in body.split("--"):
        u = by_login.get(login)
        if u and u.get("id") == me_id:
            continue
        names.append((u or {}).get("name") or login)
    return ", ".join(names)


def list_conversations(client: SlackClient, users: dict[str, dict], me_id: str | None = None) -> list[dict]:
    """Conversations the owner takes part in (to choose the allowlist on the web)."""
    out = []
    api_types = {"channel": "public_channel", "private": "private_channel", "dm": "im", "group": "mpim"}
    kinds = [k for k in api_types if not client.scopes or f"{k_scope(k)}:read" in client.scopes]
    if not kinds:
        return []
    for c in client.pages("users.conversations", "channels",
                          {"types": ",".join(api_types[k] for k in kinds), "exclude_archived": "false"}):
        kind = conversation_kind(c)
        if kind == "dm":
            other = users.get(c.get("user") or "", {})
            name = f"DM with {display_name(other) or c.get('user')}"
        elif kind == "group":
            name = group_name(c.get("name") or "", users, me_id) or c.get("name") or c["id"]
        else:
            name = f"#{c.get('name') or c['id']}"
        out.append({"id": c["id"], "name": name, "kind": kind, "archived": bool(c.get("is_archived"))})
    order = {"channel": 0, "private": 1, "dm": 2, "group": 3}
    return sorted(out, key=lambda c: (order[c["kind"]], c["name"].lower()))


def load_users(client: SlackClient) -> dict[str, dict]:
    users = {}
    for u in client.pages("users.list", "members"):
        users[u["id"]] = {"id": u["id"], "name": display_name(u), "is_bot": bool(u.get("is_bot")),
                          "deleted": bool(u.get("deleted")), "login": u.get("name") or ""}
    return users


def display_name(u: dict) -> str:
    if not u:
        return ""
    if "profile" not in u:
        return u.get("name") or ""
    p = u.get("profile") or {}
    return p.get("real_name") or p.get("display_name") or u.get("real_name") or u.get("name") or ""


def raw_hash(msg: dict) -> str:
    """Only what the person wrote or attached (not the formatting nor the resolved names)."""
    import json
    basis = {"text": msg.get("text") or "", "subtype": msg.get("subtype"), "user": msg.get("user"),
             "files": [[f.get("id"), f.get("name"), f.get("title")] for f in msg.get("files") or []],
             "attachments": [[a.get("title"), a.get("text"), a.get("fallback")] for a in msg.get("attachments") or []]}
    return sha(json.dumps(basis, sort_keys=True, ensure_ascii=False))


def is_bot(msg: dict) -> bool:
    """A bot is whatever Slack itself flags as a bot (never inferred from the name)."""
    return bool(msg.get("bot_id")) or msg.get("subtype") == "bot_message"


def bot_name(msg: dict) -> str:
    return (msg.get("bot_profile") or {}).get("name") or msg.get("username") or msg.get("bot_id") or "bot"


class SlackConnector:
    def __init__(self, db: Database, client: SlackClient, cfg: dict, owner_name: str,
                 users: dict[str, dict] | None = None, me: dict | None = None):
        self.db = db
        self.api = client
        self.cfg = cfg
        self.owner_name = owner_name
        self.users = users if users is not None else {}
        self.me = me or {}

    # ------------------------------------------------------------ filters (structural, owner lists)

    def _bot_allowed(self, msg: dict) -> bool:
        allowed = {b.lower() for b in self.cfg.get("read_bots") or []}
        return bot_name(msg).lower() in allowed or (msg.get("bot_id") or "").lower() in allowed

    def _excluded_author(self, msg: dict) -> bool:
        excluded = {a.lower() for a in self.cfg.get("exclude_authors") or []}
        uid = msg.get("user") or ""
        u = self.users.get(uid, {})
        return bool(excluded & {uid.lower(), (u.get("name") or "").lower(), (u.get("login") or "").lower()})

    def skip(self, msg: dict) -> bool:
        if msg.get("subtype") in set(self.cfg.get("ignore_subtypes") or []):
            return True
        if msg.get("subtype") == "tombstone":  # "This message was deleted" (kept because it has replies)
            return True
        if is_bot(msg) and not self._bot_allowed(msg):
            return True
        return self._excluded_author(msg)

    # ------------------------------------------------------------ formatting (Slack markup → text)

    def who(self, msg: dict) -> str:
        if is_bot(msg):
            return f"{bot_name(msg)} (bot)"
        uid = msg.get("user") or "?"
        name = self.users.get(uid, {}).get("name") or uid
        if uid == self.me.get("user_id"):
            return f"{name} ({self.owner_name}, the owner) [slack:{uid}]"
        return f"{name} [slack:{uid}]"

    def render_text(self, text: str) -> str:
        def user(m):
            uid = m.group(1)
            name = self.users.get(uid, {}).get("name") or uid
            return f"@{name} [slack:{uid}]"

        text = _USER.sub(user, text or "")
        text = _CHANNEL.sub(lambda m: f"#{m.group(2) or m.group(1)}", text)
        text = _SPECIAL.sub(lambda m: f"@{m.group(1)}", text)
        text = _SUBTEAM.sub(lambda m: f"@{m.group(2) or m.group(1)}".replace("@@", "@"), text)
        text = _LINK.sub(lambda m: f"{m.group(2)} ({m.group(1)})" if m.group(2) else m.group(1), text)
        return html.unescape(text)

    def item(self, channel: str, msg: dict, *, reply: bool = False) -> Item:
        parts = [self.render_text(msg.get("text") or "")]
        for a in msg.get("attachments") or []:
            body = a.get("text") or a.get("fallback") or ""
            title = a.get("title") or ""
            if body or title:
                parts.append(f"[attachment: {title}] {self.render_text(body)}".strip())
        for f in msg.get("files") or []:
            parts.append(f"[file: {f.get('title') or f.get('name') or '?'} ({f.get('filetype') or f.get('mimetype') or '?'})]")
        edited = " (edited)" if msg.get("edited") else ""
        head = f"{'Reply' if reply else 'Message'}{edited} by {self.who(msg)}:"
        return Item(external_id=f"msg:{channel}:{msg['ts']}", author=msg.get("user") or bot_name(msg),
                    role="reply" if reply else "message", occurred_at=_dt(msg["ts"]),
                    content=head + "\n" + "\n".join(p for p in parts if p),
                    meta={"kind": "slack", "ts": msg["ts"], "thread_ts": msg.get("thread_ts"),
                          "user": msg.get("user"), "bot": is_bot(msg), "subtype": msg.get("subtype"),
                          "bot_name": bot_name(msg) if is_bot(msg) else None, "bot_id": msg.get("bot_id"),
                          "raw_hash": raw_hash(msg)})

    def permalink(self, channel: str, ts: str, thread_ts: str | None = None) -> str:
        base = (self.me.get("url") or "https://slack.com/").rstrip("/")
        link = f"{base}/archives/{channel}/p{ts.replace('.', '')}"
        return link + (f"?thread_ts={thread_ts}&cid={channel}" if thread_ts and thread_ts != ts else "")

    # ------------------------------------------------------------ reading

    def scan(self, source: dict, now: datetime | None = None) -> dict:
        now = now or datetime.now(timezone.utc)
        cfg = source["config"]
        channel = cfg["channel"]
        settle = float(self.cfg["settle_minutes"]) * 60
        horizon = (now - timedelta(days=int(cfg.get("horizon_days") or self.cfg["horizon_days"]))).timestamp()
        revision = (now - timedelta(days=float(self.cfg["revision_days"]))).timestamp()
        state = dict(cfg.get("state") or {})
        full = not state.get("initialized") or bool(state.get("full_rescan"))
        frontier = float(state.get("last_settled_ts") or 0)
        # First read (or re-read requested by a filter/horizon): the whole horizon. Afterwards, the
        # revision period re-read in full (edits and deletions), going back to the last settled
        # window if the source sat idle longer than that — nothing from the gap is lost.
        start = horizon if full else max(horizon, min(revision, frontier or revision))
        history = list(self.api.pages("conversations.history", "messages",
                                      {"channel": channel, "oldest": f"{start:.6f}", "inclusive": "true"}))
        # Channel level only; replies "also sent to the channel" belong to the thread.
        top = sorted((m for m in history
                      if (not m.get("thread_ts") or m["thread_ts"] == m["ts"]) and m.get("subtype") != "thread_broadcast"),
                     key=lambda m: float(m["ts"]))
        stats = {"messages": len(top), "windows": 0, "threads": 0, "units": 0}
        stats["units"] += self._windows(source, channel, top, start, now.timestamp(), settle, frontier, stats)
        stats["units"] += self._threads(source, channel, top, start, now.timestamp(), settle, full, stats)
        self._save_state(source["id"], {"initialized": True}, drop=("full_rescan",))
        return stats

    def _save_state(self, source_id: str, updates: dict, drop: tuple[str, ...] = ()) -> None:
        """State written by key, right away (checkpoint): an interrupted scan neither loses what it did nor
        erases what another job marked."""
        with self.db.tx() as conn:
            conn.execute(
                """UPDATE sources SET config = jsonb_set(config, '{state}',
                       (coalesce(config->'state', '{}'::jsonb) - %s::text[]) || %s, true) WHERE id = %s""",
                (list(drop), Jsonb(updates), source_id))

    def _windows(self, source, channel, top, start, now, settle, frontier, stats) -> int:
        """Channel-level messages → windows (the one that opens a thread too: the thread keeps only the
        replies). Already settled windows are checked (edit/deletion); new messages are
        grouped while the gap between one and the next is less than `settle` (measured only with what
        passes the filters) and the window lasts at most `window_max_hours`."""
        units = 0
        max_span = float(self.cfg.get("window_max_hours") or 6) * 3600
        ext = lambda m: f"msg:{channel}:{m['ts']}"  # noqa: E731
        with self.db.tx() as conn:
            known = {r["external_id"]: r["stream_id"] for r in conn.execute(
                """SELECT e.external_id, e.stream_id FROM evidence e JOIN streams s ON s.id = e.stream_id
                   WHERE e.source_id = %s AND e.status = 'active' AND s.meta->>'slack' = 'window'
                     AND e.occurred_at >= %s""", (source["id"], _dt(start))).fetchall()}
            windows = conn.execute(
                """SELECT * FROM streams WHERE source_id = %s AND status = 'active' AND meta->>'slack' = 'window'
                   AND (meta->>'last_ts')::float >= %s""", (source["id"], start)).fetchall()
        spans = [(float(w["meta"]["first_ts"]), float(w["meta"]["last_ts"]), w["id"]) for w in windows]

        def window_of(m):
            t = float(m["ts"])
            return next((wid for lo, hi, wid in spans if lo <= t <= hi), None)

        # 1) Already settled windows: what changed comes in as a delta (edit, or a message that passes
        #    the filters again) and what vanished goes out (deletion) — only if the re-read covered the whole window.
        for lo, _hi, wid in spans:
            msgs = [m for m in top if known.get(ext(m)) == wid or (ext(m) not in known and window_of(m) == wid)]
            items = [self.item(channel, m) for m in msgs if not self.skip(m)]
            units += self._settle_stream(source, wid, items, complete=lo >= start, delta=True)
        # 2) Messages not yet in any window: group them and settle the idle ones.
        pending = [m for m in top if not self.skip(m) and ext(m) not in known and window_of(m) is None]
        group: list[dict] = []
        for m in pending:
            t = float(m["ts"])
            if group and (t - float(group[-1]["ts"]) >= settle or t - float(group[0]["ts"]) >= max_span):
                units += self._new_window(source, channel, group, stats)
                frontier = max(frontier, float(group[-1]["ts"]))
                self._save_state(source["id"], {"last_settled_ts": f"{frontier:.6f}"})
                group = []
            group.append(m)
        if group and (now - float(group[-1]["ts"]) >= settle or now - float(group[0]["ts"]) >= max_span):
            units += self._new_window(source, channel, group, stats)
            frontier = max(frontier, float(group[-1]["ts"]))
            self._save_state(source["id"], {"last_settled_ts": f"{frontier:.6f}"})
        return units

    def _new_window(self, source, channel, msgs, stats) -> int:
        stats["windows"] += 1
        first, last = msgs[0]["ts"], msgs[-1]["ts"]
        sid = f"{source['id']}:w:{first}"
        meta = {"slack": "window", "channel": channel, "first_ts": first, "last_ts": last}
        with self.db.tx() as conn:
            conn.execute(
                """INSERT INTO streams (id, source_id, locator, title, meta, last_change_at)
                   VALUES (%s, %s, %s, %s, %s, now()) ON CONFLICT (id) DO UPDATE SET meta = streams.meta || EXCLUDED.meta""",
                (sid, source["id"], self.permalink(channel, first), f"{source['name']} · {_dt(first):%Y-%m-%d %H:%M}",
                 Jsonb(meta)))
        return self._settle_stream(source, sid, [self.item(channel, m) for m in msgs], True, delta=False)

    def _replies(self, channel: str, ts: str, oldest: str | None = None) -> list[dict] | None:
        """Thread replies (complete listing, without `oldest`). None = Slack says the thread does not exist."""
        params = {"channel": channel, "ts": ts, **({"oldest": oldest} if oldest else {})}
        try:
            return list(self.api.pages("conversations.replies", "messages", params))
        except NotFound:
            return None

    def _threads(self, source, channel, top, start, now, settle, full, stats) -> int:
        """Threads rooted in the period read, already known threads whose root is in the period (the root may
        have lost its replies or vanished) and older threads still watched (new reply)."""
        units = 0
        revision_s = float(self.cfg["revision_hours"]) * 3600
        watch_from = now - float(self.cfg.get("thread_watch_days") or 30) * 86400
        parents = {m["ts"]: m for m in top if m.get("reply_count")}
        with self.db.tx() as conn:
            known = {r["meta"]["thread_ts"]: r for r in conn.execute(
                """SELECT * FROM streams WHERE source_id = %s AND status = 'active' AND meta->>'slack' = 'thread'
                   AND ((meta->>'thread_ts')::float >= %s OR (meta->>'latest_reply_seen')::float >= %s)""",
                (source["id"], start, watch_from)).fetchall()}
        for ts in sorted(set(parents) | set(known), key=float):
            st = known.get(ts)
            seen = (st["meta"] if st else {}).get("latest_reply_seen")
            # Per thread (checkpoint): an interrupted full read does not re-read threads already
            # done. The filter re-read erases `latest_reply_seen`, and that is enough to re-read.
            due = not st or now - float(st["meta"].get("revised_at") or 0) >= revision_s
            parent = parents.get(ts)
            if parent is not None:
                latest = parent.get("latest_reply") or ts
                if now - float(latest) < settle:
                    continue  # still active: wait for it to settle
                with self.db.tx() as conn:
                    has_root = root_ready(conn, source["id"], channel, ts)
                if not has_root and not self.skip(parent):
                    continue  # the message that opens the thread has not been stored yet (its window has not settled)
                if seen == latest and not due:
                    continue
            elif float(ts) >= start:
                if not due:
                    continue  # root in the period, but no replies in the listing: check at revision
            else:
                # Old watched thread: only checks whether there is a new reply (cheap); if there is, re-reads it whole.
                if not due:
                    continue
                probe = self._replies(channel, ts, oldest=seen)
                if probe is not None and not any(float(r["ts"]) > float(seen or 0) for r in probe if r["ts"] != ts):
                    self._mark_thread(st["id"], {"revised_at": now})
                    continue
            listing = self._replies(channel, ts)
            root = parent or next((r for r in listing or [] if r["ts"] == ts), None)
            replies = [r for r in listing or [] if r["ts"] != ts]
            latest = parent.get("latest_reply") if parent else (max((r["ts"] for r in replies), key=float, default=ts))
            if replies and now - float(latest) < settle:
                continue
            # A thread that starts with a bot leaves whole, including human replies.
            root_skipped = root is not None and is_bot(root) and not self._bot_allowed(root)
            if (root_skipped or not replies) and st is None:
                continue  # never read and nothing to read
            sid = st["id"] if st else f"{source['id']}:t:{ts}"
            meta = {"slack": "thread", "channel": channel, "thread_ts": ts, "latest_reply_seen": latest,
                    "revised_at": now}
            with self.db.tx() as conn:
                conn.execute(
                    """INSERT INTO streams (id, source_id, locator, title, meta, last_change_at)
                       VALUES (%s, %s, %s, %s, %s, now())
                       ON CONFLICT (id) DO UPDATE SET meta = streams.meta || EXCLUDED.meta, updated_at = now()""",
                    (sid, source["id"], self.permalink(channel, ts),
                     f"{source['name']} · thread of {_dt(ts):%Y-%m-%d %H:%M}", Jsonb(meta)))
            items = [] if root_skipped else [self.item(channel, r, reply=True) for r in replies if not self.skip(r)]
            stats["threads"] += 0 if root_skipped else 1
            # Complete listing (or Slack said the thread does not exist): what vanished goes out.
            units += self._settle_stream(source, sid, items, True, delta=bool(seen))
        return units

    def _mark_thread(self, stream_id: str, updates: dict) -> None:
        with self.db.tx() as conn:
            conn.execute("UPDATE streams SET meta = meta || %s WHERE id = %s", (Jsonb(updates), stream_id))

    def _settle_stream(self, source, stream_id, items, complete, delta) -> int:
        created = 0
        with self.db.tx() as conn:
            fresh = sync_stream(conn, source_id=source["id"], stream_id=stream_id, items=items, complete=complete,
                                reason="edited, deleted on Slack or outside the filters")
            st = conn.execute("SELECT meta, title FROM streams WHERE id = %s", (stream_id,)).fetchone()
            kind = st["meta"].get("slack")
            meta = {"slack": {"channel": source["config"]["channel"], "name": source["name"], "kind": kind,
                              "thread_ts": st["meta"].get("thread_ts"), "conversation": source["config"].get("kind"),
                              "permalink": self.permalink(source["config"]["channel"], st["meta"].get("thread_ts") or st["meta"].get("first_ts") or "0")},
                    "delta": delta and bool(fresh)}
            for i, part in enumerate(_split(fresh)):
                uid = create_unit(conn, source_id=source["id"], stream_id=stream_id,
                                  kind="slack_thread" if kind == "thread" else "slack_window",
                                  title=st["title"] + (f" (part {i + 1})" if i else ""), items=part,
                                  meta={**meta, "part": i + 1})
                if uid:
                    created += 1
            conn.execute("UPDATE streams SET last_settled_at = now() WHERE id = %s", (stream_id,))
        return created


def _split(items: list[Item], limit: int = MAX_UNIT_CHARS) -> list[list[Item]]:
    """A conversation larger than the context becomes several units, in order, without cutting or discarding anything."""
    parts, current, size = [], [], 0
    for it in items:
        if current and size + len(it.content) > limit:
            parts.append(current)
            current, size = [], 0
        current.append(it)
        size += len(it.content)
    if current:
        parts.append(current)
    return parts


# ---------------------------------------------------------------- context, live read, filters, estimate


CONTEXT_CHARS = 60_000


def unit_context(conn, unit: dict, evidence: list[dict]) -> list[dict]:
    """The rest of the conversation, so extraction understands the unit: in a thread, the message that opened it and
    the earlier replies; in a window (edit), its earlier messages. Only what came BEFORE the
    unit, the closest first up to `CONTEXT_CHARS` (the root always goes in). Read at processing
    time and only from what is still active — what was erased does not come back as context."""
    m = (unit.get("meta") or {}).get("slack") or {}
    if not m or not evidence:
        return []
    first = min(e["occurred_at"] for e in evidence)
    ids = [e["id"] for e in evidence]
    before = conn.execute(
        """SELECT * FROM evidence WHERE stream_id = %s AND status = 'active' AND NOT (id = ANY(%s))
           AND occurred_at < %s ORDER BY occurred_at DESC""", (unit["stream_id"], ids, first)).fetchall()
    root = None
    if m.get("kind") == "thread" and m.get("thread_ts"):
        root = conn.execute("SELECT * FROM evidence WHERE source_id = %s AND external_id = %s AND status = 'active'",
                            (unit["source_id"], f"msg:{m['channel']}:{m['thread_ts']}")).fetchone()
    picked, size = [], len(root["content"]) if root else 0
    for e in before:
        if size + len(e["content"]) > CONTEXT_CHARS:
            break
        picked.append(e)
        size += len(e["content"])
    return ([root] if root else []) + list(reversed(picked))


def context_units(conn, ev: dict) -> set[int]:
    """Pending units that use this message as context: those of the same stream and, if it opens a
    thread, those of the thread."""
    ts = (ev["external_id"] or "").rsplit(":", 1)[-1]
    rows = conn.execute(
        """SELECT id FROM units WHERE status = 'pending' AND source_id = %s
           AND (stream_id = %s OR meta->'slack'->>'thread_ts' = %s)""",
        (ev["source_id"], ev["stream_id"], ts)).fetchall()
    return {r["id"] for r in rows}


def root_ready(conn, source_id: str, channel: str, thread_ts: str) -> bool:
    return bool(conn.execute("SELECT 1 FROM evidence WHERE source_id = %s AND external_id = %s",
                             (source_id, f"msg:{channel}:{thread_ts}")).fetchone())


def read_live(client: SlackClient, channel: str, ts: str, thread_ts: str | None, users: dict[str, dict],
              cfg: dict | None = None) -> str:
    """Live read for the adjudicator: the thread (or the surrounding messages) as they are now on Slack,
    with the owner's same lists (ignored bots and authors do not appear)."""
    reader = SlackConnector(None, client, cfg or {}, "", users=users)
    if thread_ts:
        try:
            msgs = list(client.pages("conversations.replies", "messages", {"channel": channel, "ts": thread_ts}))
        except NotFound:
            msgs = []
        exists = any(m["ts"] == ts for m in msgs)
    else:
        target = client.call("conversations.history", {"channel": channel, "oldest": ts, "latest": ts,
                                                        "inclusive": "true", "limit": 1}).get("messages") or []
        exists = any(m["ts"] == ts for m in target)
        around = []
        for m in client.pages("conversations.history", "messages", {"channel": channel, "inclusive": "true",
                                                                    "oldest": f"{float(ts) - 3600:.6f}",
                                                                    "latest": f"{float(ts) + 3600:.6f}"}):
            around.append(m)
            if len(around) >= 200:
                break
        msgs = sorted({m["ts"]: m for m in around + target}.values(), key=lambda x: float(x["ts"]))
    if not exists:
        return "the message no longer exists on Slack (deleted at the origin)"
    out = []
    for m in msgs:
        if m["ts"] != ts and reader.skip(m):
            continue
        mark = " ← this one" if m["ts"] == ts else ""
        out.append(f"{_dt(m['ts']):%Y-%m-%d %H:%M} {reader.who(m)}{mark}: {reader.render_text(m.get('text') or '')}")
    return "\n".join(out)


def reapply_filters(db, source: dict, cfg: dict, users: dict[str, dict]) -> dict:
    """The owner's lists changed: retracts what fell outside and re-reads the horizon (what holds again
    comes in; what did not change is deduplicated by hash, at no LLM cost)."""
    from .. import knowledge as K

    excluded = {a.lower() for a in cfg.get("exclude_authors") or []}
    read_bots = {b.lower() for b in cfg.get("read_bots") or []}
    subtypes = set(cfg.get("ignore_subtypes") or [])
    stats = {"retracted": 0}
    with db.tx() as conn:
        ids = []
        for ev in conn.execute("SELECT id, author, meta FROM evidence WHERE source_id = %s AND status = 'active'",
                               (source["id"],)).fetchall():
            m = ev["meta"]
            author = (ev["author"] or "").lower()
            u = users.get(m.get("user") or "", {})
            out = not m.get("bot") and bool(excluded & {(m.get("user") or "").lower(), (u.get("name") or "").lower(),
                                                         (u.get("login") or "").lower()})
            out |= bool(m.get("bot")) and not ({(m.get("bot_name") or "").lower(), (m.get("bot_id") or "").lower(),
                                                author} & read_bots)
            out |= (m.get("subtype") or "") in subtypes
            if out:
                ids.append(ev["id"])
        if ids:
            stats.update(K.tombstone_evidence(conn, "connector", ids, "outside the owner's filters"))
            conn.execute("UPDATE evidence SET external_id = NULL WHERE id = ANY(%s)", (ids,))
            stats["retracted"] = len(ids)
        conn.execute("""UPDATE sources SET config = jsonb_set(config, '{state,full_rescan}', 'true'::jsonb, true)
                        WHERE id = %s""", (source["id"],))
        conn.execute("UPDATE streams SET meta = meta - 'latest_reply_seen' WHERE source_id = %s", (source["id"],))
    return stats


def estimate_backfill(client: SlackClient, source: dict, cfg: dict, usd_per_char: float,
                      known: set[str] | None = None) -> dict:
    """Estimate before turning on: reads the horizon's history only to count (no LLM), estimates the text
    of the replies by the average message size and multiplies by the measured average cost per character."""
    days = int(source["config"].get("horizon_days") or cfg["horizon_days"])
    oldest = datetime.now(timezone.utc) - timedelta(days=days)
    n = chars = replies = 0
    new_n = new_chars = new_replies = 0  # what has not been read yet
    known = known or set()
    reader = SlackConnector(None, client, cfg, "")
    for m in client.pages("conversations.history", "messages",
                          {"channel": source["config"]["channel"], "oldest": f"{oldest.timestamp():.6f}"}):
        if m.get("subtype") == "thread_broadcast" or reader.skip(m):
            continue
        n += 1
        chars += len(m.get("text") or "")
        replies += int(m.get("reply_count") or 0)
        if f"msg:{source['config']['channel']}:{m['ts']}" not in known:
            new_n += 1
            new_chars += len(m.get("text") or "")
            new_replies += int(m.get("reply_count") or 0)
    avg = chars / n if n else 0
    total_chars = chars + replies * avg
    new_chars += new_replies * avg  # same computation as the total: replies by the average message size
    return {"messages": n, "replies": replies, "chars": int(total_chars), "usd_per_char": usd_per_char,
            "usd": round(total_chars * usd_per_char, 2), "new": new_n,
            "usd_new": round(new_chars * usd_per_char, 2), "horizon_days": days,
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}


def required_scopes(kind: str) -> set[str]:
    return BASE_SCOPES | REQUIRED_SCOPES.get(kind, set())


def missing_scopes(client: SlackClient, kind: str) -> set[str]:
    return required_scopes(kind) - client.scopes if client.scopes else set()


def users_cached(db, client: SlackClient, max_age_h: float = 24) -> dict[str, dict]:
    """User names (to format authors and mentions), refreshed once a day."""
    row = db.one("SELECT value, updated_at FROM system_state WHERE key = 'slack_users'")
    if row and (datetime.now(timezone.utc) - row["updated_at"]).total_seconds() < max_age_h * 3600:
        return row["value"]["users"]
    users = load_users(client)
    db.execute("""INSERT INTO system_state (key, value) VALUES ('slack_users', %s)
                  ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()""",
               (Jsonb({"users": users}),))
    return users

