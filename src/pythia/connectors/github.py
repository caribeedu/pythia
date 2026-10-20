"""GitHub connector: PRs of the repository allowlist.

A PR is processed when it SETTLES: merged, closed, or open and idle for more
than `stale_days`. Activity after that becomes a delta (only what is new). Each
description, review, comment and the diff summary is a piece of evidence; editing a
description/comment retracts the previous version; a deleted comment (in a complete
listing) becomes a tombstone. Backfill of `horizon_days`, from most recent to
oldest. Everything deterministic: read, split and store.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import httpx
from psycopg.types.json import Jsonb

from .. import vault
from ..db import Database
from .common import Item, NotFound, SourceUnavailable, create_unit, sync_stream

KIND = "github"
API = "https://api.github.com"
_NEXT = re.compile(r'<([^>]+)>;\s*rel="next"')


def source_id_for(repo: str) -> str:
    return f"{KIND}:{repo.lower()}"


def _ts(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def resolve_token(cfg: dict) -> str:
    """API-only integration: personal token stored in the Keychain (nothing from the user's local environment)."""
    token = vault.get_secret(vault.GITHUB_TOKEN)
    if not token:
        raise SourceUnavailable("GitHub token not configured (Settings → GitHub)")
    return token


class GitHubClient:
    def __init__(self, token: str, transport: httpx.BaseTransport | None = None):
        self.http = httpx.Client(base_url=API, transport=transport, timeout=30, headers={
            "Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "pythia"})

    def get(self, path: str, params: dict | None = None) -> Any:
        try:
            r = self.http.get(path, params=params)
        except httpx.HTTPError as exc:
            raise SourceUnavailable(f"GitHub unreachable: {exc}") from exc
        self._check(r)
        return r.json()

    def pages(self, path: str, params: dict | None = None, stop: Callable[[dict], bool] | None = None):
        """Iterates paginated items (Link: rel=next). `stop(item)` interrupts pagination."""
        url, query = path, {"per_page": 100, **(params or {})}
        while url:
            try:
                r = self.http.get(url, params=query)
            except httpx.HTTPError as exc:
                raise SourceUnavailable(f"GitHub unreachable: {exc}") from exc
            self._check(r)
            for item in r.json():
                if stop and stop(item):
                    return
                yield item
            m = _NEXT.search(r.headers.get("link", ""))
            url, query = (m.group(1), None) if m else (None, None)

    @staticmethod
    def _check(r: httpx.Response) -> None:
        if r.status_code in (401,):
            raise SourceUnavailable("GitHub rejected the credential (401)")
        if r.status_code in (403, 429) and (r.headers.get("x-ratelimit-remaining") == "0" or r.status_code == 429):
            raise SourceUnavailable(f"GitHub rate limit (reset {r.headers.get('x-ratelimit-reset')})")
        if r.status_code == 404:
            raise NotFound(f"not found or no access: {r.request.url.path}")
        if r.status_code >= 500:
            raise SourceUnavailable(f"GitHub {r.status_code}")
        r.raise_for_status()


def owner_login(cfg: dict, client: GitHubClient) -> str:
    return cfg.get("owner_login") or client.get("/user")["login"]


def verify_token(client: GitHubClient, cfg: dict) -> None:
    """Checks the token before reading. Classic: needs the `repo` scope (GitHub has no read-only
    scope for private repositories). Fine-grained: permissions are per repository and GitHub
    does not report them here; a repository without access shows up as an error of that source."""
    try:
        r = client.http.get("/user")
    except httpx.HTTPError as exc:
        raise SourceUnavailable(f"GitHub unreachable: {exc}") from exc
    client._check(r)
    if (cfg.get("token_type") or "classic") != "classic":
        return
    scopes = {x.strip() for x in (r.headers.get("x-oauth-scopes") or "").split(",") if x.strip()}
    if "repo" not in scopes:
        raise SourceUnavailable("classic token without the `repo` scope (required to read PRs of private repositories)")


class GitHubConnector:
    def __init__(self, db: Database, client: GitHubClient, cfg: dict, owner_name: str, team: list[dict] | None = None):
        self.db = db
        self.gh = client
        self.cfg = cfg
        self.owner_name = owner_name
        self.team = team or []
        self._owner_login: str | None = None

    @property
    def me(self) -> str:
        if self._owner_login is None:
            self._owner_login = owner_login(self.cfg, self.gh)
        return self._owner_login

    def scan(self, source: dict, now: datetime | None = None) -> dict:
        now = now or datetime.now(timezone.utc)
        repo = source["config"]["repo"]
        stats = {"listed": 0, "settled": 0, "units": 0}
        state_key = f"github_cursor:{repo.lower()}"
        row = self.db.one("SELECT value FROM system_state WHERE key = %s", (state_key,))
        cursor = _ts(row["value"]["updated_at"]) if row else None
        horizon = now - timedelta(days=int(source["config"].get("horizon_days") or self.cfg["horizon_days"]))
        floor = max(horizon, cursor - timedelta(hours=1)) if cursor else horizon
        info = self.gh.get(f"/repos/{repo}")
        newest = cursor
        for pr in self.gh.pages(f"/repos/{repo}/pulls", {"state": "all", "sort": "updated", "direction": "desc"},
                                stop=lambda p: _ts(p["updated_at"]) < floor):
            stats["listed"] += 1
            updated = _ts(pr["updated_at"])
            newest = max(newest, updated) if newest else updated
            self._upsert_stream(source, repo, pr, info.get("archived", False))
        if newest:
            self.db.execute(
                """INSERT INTO system_state (key, value) VALUES (%s, %s)
                   ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()""",
                (state_key, Jsonb({"updated_at": newest.isoformat()})))
        # Settles what is eligible (including open PRs that went idle without new activity).
        stale_before = now - timedelta(days=int(self.cfg["stale_days"]))
        for st in self.db.query("SELECT * FROM streams WHERE source_id = %s AND status = 'active'", (source["id"],)):
            m = st["meta"]
            updated = _ts(m.get("updated_at"))
            if updated is None or updated < horizon:
                continue
            if m.get("settled_updated_at") == m.get("updated_at"):
                continue
            done = m.get("merged_at") or m.get("state") == "closed"
            if not done and updated > stale_before:
                continue
            stats["settled"] += 1
            if self._settle(source, repo, st):
                stats["units"] += 1
        return stats

    def _upsert_stream(self, source: dict, repo: str, pr: dict, archived: bool) -> None:
        sid = f"{KIND}:{repo.lower()}#{pr['number']}"
        meta = {"number": pr["number"], "state": pr["state"], "merged_at": pr.get("merged_at"),
                "updated_at": pr["updated_at"], "title": pr["title"], "author": pr["user"]["login"],
                "url": pr["html_url"], "archived": archived}
        self.db.execute(
            """INSERT INTO streams (id, source_id, locator, title, meta, last_change_at)
               VALUES (%s, %s, %s, %s, %s, now())
               ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title,
                   meta = streams.meta || EXCLUDED.meta, last_change_at = CASE
                     WHEN streams.meta->>'updated_at' IS DISTINCT FROM EXCLUDED.meta->>'updated_at' THEN now()
                     ELSE streams.last_change_at END, updated_at = now()""",
            (sid, source["id"], pr["html_url"], pr["title"], Jsonb(meta)))

    # ------------------------------------------------------------ who and what is read

    def _excluded_author(self, user: dict | None) -> bool:
        return bool(user) and user.get("login", "").lower() in {a.lower() for a in self.cfg.get("exclude_authors") or []}

    def _is_bot(self, user: dict | None) -> bool:
        """A bot is whatever GitHub itself flags as a bot (never inferred from the name). Exceptions: bots the owner reads."""
        if not user or user.get("type") != "Bot":
            return False
        return user.get("login", "").lower() not in {b.lower() for b in self.cfg.get("read_bots") or []}

    def _skip(self, user: dict | None) -> bool:
        return self._excluded_author(user) or self._is_bot(user)

    def _excluded_path(self, path: str) -> bool:
        return excluded_path(path, self.cfg.get("exclude_paths") or [])

    def _who(self, login: str) -> str:
        return f"{login} ({self.owner_name}, the owner)" if login.lower() == self.me.lower() else login

    def collect(self, repo: str, number: int, source: dict | None = None) -> tuple[dict, list[Item], list[str]]:
        """Fetches everything of the PR and assembles the evidence. Returns (pr, items, participants).

        Listings are complete or raise an error. If the PR does not pass the repository mode, the
        files are not even fetched (empty items)."""
        pr = self.gh.get(f"/repos/{repo}/pulls/{number}")
        reviews = list(self.gh.pages(f"/repos/{repo}/pulls/{number}/reviews"))
        rcomments = list(self.gh.pages(f"/repos/{repo}/pulls/{number}/comments"))
        icomments = list(self.gh.pages(f"/repos/{repo}/issues/{number}/comments"))
        participants = pr_participants(pr, reviews, rcomments, icomments)
        if source is not None and not passes_mode(source, participants, self.me, self.team_logins()):
            return pr, [], participants

        status = ("merged on " + pr["merged_at"][:10]) if pr.get("merged_at") else (
            "closed without merge on " + (pr.get("closed_at") or "")[:10] if pr["state"] == "closed" else "open")
        author = pr["user"]["login"]
        items: list[Item] = []
        if not self._excluded_author(pr["user"]):
            items.append(Item(
                external_id=f"pr:{pr['id']}", author=author, role="author", occurred_at=_ts(pr["created_at"]),
                content=(f"PR {repo}#{number}: {pr['title']}\nAuthor: {self._who(author)} · {status} · "
                         f"{pr['base']['ref']} ← {pr['head']['ref']}\n\n{pr.get('body') or '(no description)'}"),
                meta={"url": pr["html_url"], "kind": "description"}))
        for rv in reviews:
            if self._skip(rv.get("user")):
                continue
            if not (rv.get("body") or "").strip() and rv["state"] not in ("APPROVED", "CHANGES_REQUESTED"):
                continue
            login = rv["user"]["login"] if rv.get("user") else "?"
            items.append(Item(
                external_id=f"review:{rv['id']}", author=login, role="reviewer",
                occurred_at=_ts(rv.get("submitted_at")) or _ts(pr["updated_at"]),
                content=f"Review by {self._who(login)} ({rv['state']}):\n{rv.get('body') or ''}",
                meta={"url": rv.get("html_url"), "kind": "review", "bot": (rv.get("user") or {}).get("type") == "Bot"}))
        # A thread that starts with a bot leaves whole (including human replies).
        by_id = {c["id"]: c for c in rcomments}

        def root(c):
            seen = set()
            while c.get("in_reply_to_id") and c["in_reply_to_id"] in by_id and c["id"] not in seen:
                seen.add(c["id"])
                c = by_id[c["in_reply_to_id"]]
            return c

        for c in rcomments:
            if self._skip(c.get("user")) or self._is_bot(root(c).get("user")):
                continue
            if self._excluded_path(c.get("path") or ""):
                continue
            where = f"{c.get('path')}:{c.get('line') or c.get('original_line') or '?'}"
            hunk = c.get("diff_hunk") or ""
            reply = " (reply)" if c.get("in_reply_to_id") else ""
            items.append(Item(
                external_id=f"rcomment:{c['id']}", author=c["user"]["login"], role="reviewer",
                occurred_at=_ts(c["created_at"]),
                content=(f"Comment{reply} by {self._who(c['user']['login'])} on {where}:\n"
                         + (f"Commented excerpt:\n```diff\n{hunk}\n```\n" if hunk else "") + c["body"]),
                meta={"url": c.get("html_url"), "kind": "code_comment", "path": c.get("path"),
                      "thread": root(c)["id"], "bot": c["user"].get("type") == "Bot"}))
        for c in icomments:
            if self._skip(c.get("user")):
                continue
            items.append(Item(
                external_id=f"icomment:{c['id']}", author=c["user"]["login"], role="comment",
                occurred_at=_ts(c["created_at"]),
                content=f"Comment by {self._who(c['user']['login'])}:\n{c['body']}",
                meta={"url": c.get("html_url"), "kind": "comment", "bot": c["user"].get("type") == "Bot"}))
        items.extend(self._file_items(repo, number, pr))
        return pr, items, participants

    def _file_items(self, repo: str, number: int, pr: dict) -> list[Item]:
        """Whole diff, one piece of evidence per file. Nothing is cut; a missing patch is recorded."""
        when = _ts(pr.get("merged_at") or pr.get("closed_at") or pr["updated_at"])
        out = []
        for f in self.gh.pages(f"/repos/{repo}/pulls/{number}/files"):
            if self._excluded_path(f["filename"]):
                continue
            renamed = f" (before: {f['previous_filename']})" if f.get("previous_filename") else ""
            patch = f.get("patch")
            body = patch if patch else "(GitHub did not provide this file's patch: binary or too large)"
            out.append(Item(
                external_id=f"file:{pr['id']}:{f['filename']}", author=pr["user"]["login"], role="artifact",
                occurred_at=when,
                content=f"File {f['filename']}{renamed} — {f['status']}, +{f['additions']}/-{f['deletions']}\n```diff\n{body}\n```",
                meta={"kind": "file", "path": f["filename"], "patch_missing": not patch}))
        return out

    def team_logins(self) -> set[str]:
        return {m["github"].lower() for m in self.team if m.get("github")}

    def _settle(self, source: dict, repo: str, stream: dict) -> bool:
        number = stream["meta"]["number"]
        pr, items, participants = self.collect(repo, number, source)
        passes = bool(items) or passes_mode(source, participants, self.me, self.team_logins())
        created = []
        with self.db.tx() as conn:
            # Outside the repository mode: what had already been read from this PR is retracted.
            fresh = sync_stream(conn, source_id=source["id"], stream_id=stream["id"], items=items, complete=True,
                                reason="edited, removed on GitHub or outside the filters")
            meta = {"repo": repo, "number": number, "url": pr["html_url"], "state": pr["state"],
                    "merged_at": pr.get("merged_at"), "archived": stream["meta"].get("archived", False),
                    "delta": bool(stream["meta"].get("settled_updated_at"))}
            for i, part in enumerate(split_items(fresh)):
                uid = create_unit(conn, source_id=source["id"], stream_id=stream["id"], kind="pull_request",
                                  title=f"{repo}#{number}: {pr['title']}" + (f" (part {i + 1})" if i else ""),
                                  items=part, meta={**meta, "part": i + 1})
                if uid:
                    created.append(uid)
            conn.execute("UPDATE streams SET meta = meta || %s, last_settled_at = now() WHERE id = %s",
                         (Jsonb({"settled_updated_at": stream["meta"]["updated_at"], "participants": participants,
                                 "filtered": not passes}), stream["id"]))
        return bool(created)


# ---------------------------------------------------------------- filters (structural, configured by the owner)

MODES = {"all": "all PRs", "owner": "only those involving you", "team": "those involving you or your team"}
DEFAULT_MODE = "team"
_MENTION = re.compile(r"(?<![\w@])@([A-Za-z0-9](?:[A-Za-z0-9-]{0,38}))")
MAX_UNIT_CHARS = 300_000


def pr_participants(pr: dict, reviews: list, rcomments: list, icomments: list) -> list[str]:
    """Who takes part in the PR, per what GitHub itself records (author, reviewers, commenters,
    review requests, assignees and @mentions in the description)."""
    logins = {pr["user"]["login"]}
    logins |= {r["user"]["login"] for r in reviews if r.get("user")}
    logins |= {c["user"]["login"] for c in rcomments + icomments if c.get("user")}
    logins |= {u["login"] for u in pr.get("requested_reviewers") or []}
    logins |= {u["login"] for u in pr.get("assignees") or []}
    logins |= set(_MENTION.findall(pr.get("body") or ""))
    return sorted(l.lower() for l in logins)


def passes_mode(source: dict, participants: list[str], owner_login: str, team_logins: set[str]) -> bool:
    mode = source["config"].get("mode") or DEFAULT_MODE
    if mode == "all":
        return True
    people = {owner_login.lower()} | (team_logins if mode == "team" else set())
    return bool(people & set(participants))


def excluded_path(path: str, patterns: list[str]) -> bool:
    from fnmatch import fnmatch
    return any(fnmatch(path, p) or fnmatch(path.rsplit("/", 1)[-1], p) for p in patterns)


def split_items(items: list[Item], limit: int = MAX_UNIT_CHARS) -> list[list[Item]]:
    """A PR larger than the context becomes several units (by file), without cutting or discarding anything."""
    head = [i for i in items if i.meta.get("kind") != "file"]
    files = [i for i in items if i.meta.get("kind") == "file"]
    parts: list[list[Item]] = []
    current, size = list(head), sum(len(i.content) for i in head)
    for f in files:
        if current and size + len(f.content) > limit:
            parts.append(current)
            current, size = [], 0
        current.append(f)
        size += len(f.content)
    if current:
        parts.append(current)
    return parts


# ---------------------------------------------------------------- reapply filters, remove, estimate


def reapply_filters(db, source: dict, cfg: dict, owner_login: str, team_logins: set[str]) -> dict:
    """The filters changed: retracts what fell outside and forces a re-read of the rest (what passes
    again comes in as new evidence; what did not change is deduplicated by hash, at no LLM cost)."""
    from .. import knowledge as K

    stats = {"retracted": 0, "reread": 0}
    excluded = {a.lower() for a in cfg.get("exclude_authors") or []}
    read_bots = {b.lower() for b in cfg.get("read_bots") or []}
    paths = cfg.get("exclude_paths") or []
    with db.tx() as conn:
        ids = []
        for ev in conn.execute(
                """SELECT e.id, e.author, e.meta, s.meta AS stream_meta FROM evidence e JOIN streams s ON s.id = e.stream_id
                   WHERE e.source_id = %s AND e.status = 'active'""", (source["id"],)).fetchall():
            m, sm = ev["meta"], ev["stream_meta"]
            out = (ev["author"] or "").lower() in excluded
            out |= bool(m.get("bot")) and (ev["author"] or "").lower() not in read_bots
            out |= bool(m.get("path")) and excluded_path(m["path"], paths)
            out |= bool(sm.get("participants") is not None) and not passes_mode(source, sm["participants"],
                                                                                 owner_login, team_logins)
            if out:
                ids.append(ev["id"])
        if ids:
            K.tombstone_evidence(conn, "connector", ids, "outside the owner's filters")
            conn.execute("UPDATE evidence SET external_id = NULL WHERE id = ANY(%s)", (ids,))
            stats["retracted"] = len(ids)
        # Re-read: on the next scan every already settled PR is collected again.
        stats["reread"] = conn.execute(
            "UPDATE streams SET meta = meta - 'settled_updated_at' WHERE source_id = %s", (source["id"],)).rowcount
    return stats


def remove_source(db, source_id: str) -> dict:
    """Remove: stops reading and retracts everything that came only from this source. Pausing is just turning off."""
    from .. import knowledge as K

    with db.tx() as conn:
        src = conn.execute("SELECT config FROM sources WHERE id = %s", (source_id,)).fetchone()
        repo = ((src or {}).get("config") or {}).get("repo")
        ids = [r["id"] for r in conn.execute(  # including the version history and the live-read proofs for it
            """SELECT id FROM evidence WHERE (source_id = %s AND status IN ('active', 'superseded'))
               OR (source_id = 'live:github' AND status = 'active' AND meta->>'repo' = %s)""",
            (source_id, (repo or "").lower())).fetchall()]
        stats = K.tombstone_evidence(conn, "connector", ids, "source removed by the owner") if ids else {"evidence": 0}
        conn.execute("UPDATE evidence SET external_id = NULL WHERE source_id = %s", (source_id,))
        conn.execute("DELETE FROM batch_requests WHERE source_id = %s", (source_id,))
        conn.execute("DELETE FROM deferred_derivations WHERE source_id = %s", (source_id,))
        conn.execute("DELETE FROM system_state WHERE key LIKE %s", (f"%{source_id}%",))
        src = conn.execute("SELECT config FROM sources WHERE id = %s", (source_id,)).fetchone()
        if src and src["config"].get("repo"):
            conn.execute("DELETE FROM system_state WHERE key = %s", (f"github_cursor:{src['config']['repo'].lower()}",))
        conn.execute("DELETE FROM sources WHERE id = %s", (source_id,))
    return stats


def estimate_backfill(client: GitHubClient, source: dict, cfg: dict, owner_login: str, team_logins: set[str],
                      avg_cost: float, already_read: set[int] | None = None) -> dict:
    """Estimate: PRs in the horizon that pass the mode × measured average cost per PR. `already_read`
    (already settled PRs) is left out: after the backfill, only what remains to be read matters."""
    import time as _t

    repo = source["config"]["repo"]
    days = int(source["config"].get("horizon_days") or cfg["horizon_days"])
    since = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    mode = source["config"].get("mode") or DEFAULT_MODE
    base = f"repo:{repo} is:pr updated:>={since}"
    queries = [base] if mode == "all" else [
        f"{base} involves:{person}" for person in sorted({owner_login.lower()} | (team_logins if mode == "team" else set()))]
    seen, exact = set(), True
    for q in queries:
        for page in range(1, 11):  # GitHub search returns at most 1000 results
            r = client.get("/search/issues", {"q": q, "per_page": 100, "page": page})
            seen |= {i["number"] for i in r["items"]}
            _t.sleep(2.1)  # GitHub search limit: 30 per minute
            if page * 100 >= r["total_count"]:
                break
        else:
            exact = False
    new = seen - (already_read or set())
    return {"prs": len(seen), "new": len(new), "exact": exact, "avg_cost": avg_cost,
            "usd": round(len(seen) * avg_cost, 2), "usd_new": round(len(new) * avg_cost, 2), "mode": mode,
            "horizon_days": days, "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
