"""Confluence connector: allowlisted spaces and page trees, read through the v2 API with the owner's
token.

- **Page:** split by headings; each section is a piece of evidence. Backfill reads only the current version.
- **Edit:** an edit is not a retraction. A new or changed section is extracted with the rest of the page as
  context. The section's previous version is kept as history (`superseded`), and the claims that came from it
  go to the adjudicator with both versions, in a tension with a dossier: still valid, evolved or
  wrong. A section that did not change costs nothing.
- **Comments:** footer and inline (with the commented excerpt), one stream per page; editing a
  comment is a retraction, as in chat.
- **Deletion:** a deleted page really erases everything; an archived one is only marked; a page that left the
  enabled tree leaves the base (like changing a filter). Never deletes on an incomplete listing.

Everything here is plumbing: read, convert the storage format to text, split by headings
(the structure comes from the page itself), compare hashes and store.
"""

from __future__ import annotations

import html
import re
import time
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from typing import Any

import httpx
from psycopg.types.json import Jsonb

from .. import vault
from ..db import Database
from ..ids import new_id, sha
from .common import Item, SourceUnavailable, create_unit, sync_stream
from .common import NotFound as common_NotFound

KIND = "confluence"
SCOPES = ["read:page:confluence", "read:comment:confluence", "read:space:confluence",
          "read:hierarchical-content:confluence", "read:user:confluence"]
MAX_UNIT_CHARS = 120_000
CONTEXT_CHARS = 60_000


class NotFound(common_NotFound):
    """Confluence states that the page does not exist (deleted): a complete answer, not a failure."""


def source_id_for(space_key: str, root_id: str | None = None) -> str:
    return f"{KIND}:tree:{root_id}" if root_id else f"{KIND}:space:{space_key}"


def _dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


# ---------------------------------------------------------------- client


def resolve_credentials(cfg: dict) -> tuple[str, str]:
    token = vault.get_secret(vault.CONFLUENCE_TOKEN)
    email = (cfg.get("email") or "").strip()
    if not token or not email or not cfg.get("site"):
        raise SourceUnavailable("Confluence without site, email or token (Settings → Confluence)")
    return email, token


class ConfluenceClient:
    def __init__(self, site: str, email: str, token: str, token_type: str = "scoped",
                 transport: httpx.BaseTransport | None = None, cloud_id: str | None = None, max_wait_s: float = 60):
        self.site = site.removeprefix("https://").strip("/")
        self.token_type = token_type
        self.transport = transport
        self.max_wait_s = max_wait_s
        if token_type == "scoped":
            cloud_id = cloud_id or self._cloud_id()
            base = f"https://api.atlassian.com/ex/confluence/{cloud_id}/wiki"
        else:
            base = f"https://{self.site}/wiki"
        # Scoped and classic tokens both use basic authentication (email + token).
        self.http = httpx.Client(base_url=base, transport=transport, timeout=30, auth=(email, token),
                                 headers={"Accept": "application/json", "User-Agent": "pythia"})

    def _cloud_id(self) -> str:
        try:
            r = httpx.Client(transport=self.transport, timeout=15).get(f"https://{self.site}/_edge/tenant_info")
            r.raise_for_status()
            return r.json()["cloudId"]
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            raise SourceUnavailable(f"could not discover the cloudId of {self.site}: {exc}") from exc

    def get(self, path: str, params: dict | None = None) -> Any:
        waited = 0.0
        while True:
            try:
                r = self.http.get(path, params=params)
            except httpx.HTTPError as exc:
                raise SourceUnavailable(f"Confluence unreachable: {exc}") from exc
            if r.status_code == 429:
                wait = float(r.headers.get("retry-after") or 30)
                if waited + wait > self.max_wait_s:
                    raise SourceUnavailable("Confluence rate limit; will retry later")
                time.sleep(wait)
                waited += wait
                continue
            if r.status_code in (401, 403):
                raise SourceUnavailable(f"Confluence rejected the token ({r.status_code}); check email, token and scopes")
            if r.status_code == 404:
                raise NotFound(path)
            if r.status_code >= 500:
                raise SourceUnavailable(f"Confluence {r.status_code}")
            r.raise_for_status()
            return r.json()

    def post(self, path: str, body: dict) -> Any:
        waited = 0.0
        while True:
            try:
                r = self.http.post(path, json=body)
            except httpx.HTTPError as exc:
                raise SourceUnavailable(f"Confluence unreachable: {exc}") from exc
            if r.status_code != 429:
                break
            wait = float(r.headers.get("retry-after") or 30)
            if waited + wait > self.max_wait_s:
                raise SourceUnavailable("Confluence rate limit; will retry later")
            time.sleep(wait)
            waited += wait
        if r.status_code in (401, 403):
            raise SourceUnavailable(f"Confluence rejected the token ({r.status_code})")
        r.raise_for_status()
        return r.json()

    def pages(self, path: str, params: dict | None = None):
        """Iterates a v2 cursor listing to the end (complete listing or error)."""
        query = {"limit": 250, **(params or {})}
        while path:
            body = self.get(path, query)
            yield from body.get("results") or []
            nxt = (body.get("_links") or {}).get("next")
            path, query = (nxt.removeprefix("/wiki"), None) if nxt else (None, None)


def verify(client: ConfluenceClient) -> None:
    """A cheap read that requires the right token (and, with a scoped token, the space scope)."""
    client.get("/api/v2/spaces", {"limit": 1})


def list_spaces(client: ConfluenceClient) -> list[dict]:
    out = [{"id": s["id"], "key": s["key"], "name": s.get("name") or s["key"], "type": s.get("type"),
            "status": s.get("status")} for s in client.pages("/api/v2/spaces", {"status": "current"})]
    return sorted(out, key=lambda s: (s["type"] == "personal", s["name"].lower()))


def space_pages(client: ConfluenceClient, space_id: str, with_body: bool = False) -> list[dict]:
    params = {"status": "current", **({"body-format": "storage"} if with_body else {})}
    return list(client.pages(f"/api/v2/spaces/{space_id}/pages", params))


DESCENDANTS_DEPTH = 10  # maximum the API accepts; where the tree is deeper, fetch again from there


def space_nodes(client: "ConfluenceClient", space_id: str, pages: list[dict]) -> dict[str, dict]:
    """Pages and folders of the space with each one's parent: {id: {"title", "parent", "type", "position"}}.
    Folders do not come in the page listing, and the folders API needs another scope; the descendants of the
    home page (hierarchy scope) bring both."""
    nodes = {str(p["id"]): {"title": p["title"], "parent": str(p.get("parentId") or ""), "type": "page",
                            "position": p.get("position") or 0} for p in pages}
    home = str(client.get(f"/api/v2/spaces/{space_id}").get("homepageId") or "")
    stack = [home] if home else []
    while stack:
        root = stack.pop()
        for d in client.pages(f"/api/v2/pages/{root}/descendants", {"depth": DESCENDANTS_DEPTH}):
            nid = str(d["id"])
            nodes[nid] = {"title": d.get("title") or nid, "parent": str(d.get("parentId") or ""),
                          "type": d.get("type") or "page", "position": d.get("childPosition") or 0}
            if (d.get("depth") or 0) >= DESCENDANTS_DEPTH and (d.get("type") or "page") == "page":
                stack.append(nid)
    return nodes


def tree_of(pages: list[dict], root_id: str, nodes: dict[str, dict] | None = None) -> list[dict]:
    """The root (page or folder) and all pages below it. `nodes` brings the folders, which do not come in the
    page listing."""
    parent_of = {nid: n["parent"] for nid, n in (nodes or {}).items()}
    parent_of.update({str(p["id"]): str(p.get("parentId") or "") for p in pages})

    def ancestors(node: str):
        seen = set()
        while node and node not in seen:
            seen.add(node)
            yield node
            node = parent_of.get(node, "")

    root = str(root_id)
    return [p for p in pages if root in ancestors(str(p["id"]))]


def page_outline(nodes: dict[str, dict]) -> list[dict]:
    """Pages and folders of the space in tree order, with depth (to choose roots on the web)."""
    by_parent: dict[str, list[tuple[str, dict]]] = {}
    for nid, n in nodes.items():
        by_parent.setdefault(n["parent"] if n["parent"] in nodes else "", []).append((nid, n))
    out = []

    def walk(parent: str, depth: int):
        for nid, n in sorted(by_parent.get(parent, []), key=lambda x: (x[1]["position"], x[1]["title"].lower())):
            out.append({"id": nid, "title": n["title"], "depth": depth, "folder": n["type"] == "folder"})
            walk(nid, depth + 1)

    walk("", 0)
    return out


# ---------------------------------------------------------------- storage format → text per section


class _Storage(HTMLParser):
    """Confluence storage format (XHTML with `ac:`/`ri:` macros) → text, split on
    headings. Formatting only: headings, paragraphs, lists, tables, code, mentions, links and macros.
    Nothing is discarded: code blocks (CDATA) become text, and each macro's parameters appear
    next to it (e.g. `[status: title=OBSOLETE]`, `[jira: key=PROJ-1]`)."""

    BLOCK = {"p", "div", "li", "tr", "br", "pre", "blockquote", "ac:task", "hr"}

    def __init__(self, users: dict[str, str]):
        super().__init__(convert_charrefs=True)
        self.users = users
        self.sections: list[tuple[list[str], list[str]]] = [([], [])]
        self.path: list[tuple[int, str]] = []
        self.heading: int | None = None
        self.heading_text: list[str] = []
        self.cell = False
        self.in_cell = 0  # inside a cell, paragraphs and lists do not break the table row
        self.macros: list[dict] = []  # stack: {"name", "params": [(name, value)], "emitted"}
        self.param: tuple[str, list[str]] | None = None

    def _out(self) -> list[str]:
        return self.sections[-1][1]

    def _emit_macro_head(self) -> None:
        m = self.macros[-1] if self.macros else None
        if m and not m["emitted"]:
            m["emitted"] = True
            params = ", ".join(f"{k}={v}" for k, v in m["params"] if v)
            self._write(f"[{m['name']}{': ' + params if params else ''}] ")

    def _write(self, text: str) -> None:
        (self.heading_text if self.heading is not None else self._out()).append(text)

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "ac:structured-macro":
            self.macros.append({"name": a.get("ac:name") or "macro", "params": [], "emitted": False})
        elif tag == "ac:parameter":
            self.param = (a.get("ac:name") or "", [])
        elif tag in ("ac:rich-text-body", "ac:plain-text-body", "ac:plain-text-link-body"):
            self._emit_macro_head()
        elif tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            self.heading, self.heading_text = int(tag[1]), []
        elif tag == "ri:user":
            aid = a.get("ri:account-id", "")
            self._write(f"@{self.users.get(aid, aid or 'someone')}")
        elif tag == "ri:page":
            self._write(f"[page: {a.get('ri:content-title', '?')}]")
        elif tag == "ri:attachment":
            self._write(f"[attachment: {a.get('ri:filename', '?')}]")
        elif tag == "a" and a.get("href"):
            self._pending_href = a["href"]
        elif tag in ("td", "th"):
            self._out().append(" | " if self.cell else "")
            self.cell = True
            self.in_cell += 1
        elif tag == "tr":
            self.cell = False
            self._out().append("\n")
        elif tag == "li":
            self._out().append(" · " if self.in_cell else "\n- ")
        elif tag in self.BLOCK:
            self._out().append(" " if self.in_cell else "\n")

    def handle_endtag(self, tag):
        if tag == "ac:parameter" and self.param is not None:
            name, parts = self.param
            if self.macros:
                self.macros[-1]["params"].append((name or "value", " ".join("".join(parts).split())))
            self.param = None
        elif tag == "ac:structured-macro" and self.macros:
            self._emit_macro_head()
            self.macros.pop()
        elif tag == "a" and getattr(self, "_pending_href", None):
            self._write(f" ({self._pending_href})")
            self._pending_href = None
        elif tag in ("h1", "h2", "h3", "h4", "h5", "h6") and self.heading is not None:
            title = " ".join("".join(self.heading_text).split()) or "(untitled)"
            level = self.heading
            self.path = [(lvl, t) for lvl, t in self.path if lvl < level] + [(level, title)]
            self.sections.append(([t for _, t in self.path], []))
            self.heading = None
        elif tag in ("td", "th"):
            self.in_cell = max(0, self.in_cell - 1)
        elif tag in ("p", "pre", "blockquote", "ac:plain-text-body"):
            self._out().append(" " if self.in_cell else "\n")
        elif tag == "tr":
            self._out().append("\n")

    def handle_data(self, data):
        if self.param is not None:
            self.param[1].append(data)
            return
        self._write(data)

    def unknown_decl(self, data):
        # Code blocks and `noformat` come in CDATA: they are content, not markup.
        if data.startswith("CDATA["):
            self._emit_macro_head()
            self._write(data[len("CDATA["):])


def sections_of(storage: str, users: dict[str, str] | None = None) -> list[tuple[str, str]]:
    """[(heading path, text)] in page order. A section without text is omitted."""
    parser = _Storage(users or {})
    parser.feed(storage or "")
    parser.close()
    out = []
    for path, parts in parser.sections:
        text = re.sub(r"[ \t]+", " ", "".join(parts))
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n\s*\n+", "\n", text).strip()
        if text:
            out.append((" > ".join(path) or "(start)", html.unescape(text)))
    return out


def sections_with_raw(storage: str, users: dict[str, str]) -> list[tuple[str, str, str]]:
    """(path, text with names, raw text with account IDs). The edit hash uses the raw one: renaming a
    person (or failing to get the names on a read) is not an edit of the page."""
    named, raw = sections_of(storage, users), sections_of(storage, {})
    return [(p, t, raw[i][1] if i < len(raw) else t) for i, (p, t) in enumerate(named)]


def users_names(client: ConfluenceClient, account_ids: set[str], cache: dict[str, str]) -> dict[str, str]:
    missing = [a for a in account_ids if a and a not in cache]
    for i in range(0, len(missing), 100):
        try:
            for u in client.post("/api/v2/users-bulk", {"accountIds": missing[i:i + 100]}).get("results") or []:
                cache[u["accountId"]] = u.get("publicName") or u.get("displayName") or u["accountId"]
        except (SourceUnavailable, httpx.HTTPError):
            break  # without the user scope: the account ID stays (resolution still finds it through the catalog)
    return cache


_MENTION = re.compile(r'ri:account-id="([^"]+)"')


# ---------------------------------------------------------------- reading


class ConfluenceConnector:
    def __init__(self, db: Database, client: ConfluenceClient, cfg: dict, owner_name: str,
                 owner_aliases: list[str] | None = None):
        self.db = db
        self.api = client
        self.cfg = cfg
        self.owner_name = owner_name
        self.owner_names = {n.lower() for n in [owner_name, *(owner_aliases or [])]}
        self._users: dict[str, str] = {}

    def _who(self, account_id: str | None) -> str:
        name = self._users.get(account_id or "", account_id or "?")
        # The owner is marked by the name he declared himself (Settings → Owner), as in the other sources.
        return f"{name} ({self.owner_name}, the owner)" if name.lower() in self.owner_names else name

    def _excluded(self, account_id: str | None) -> bool:
        excluded = {a.lower() for a in self.cfg.get("exclude_authors") or []}
        return bool(excluded & {(account_id or "").lower(), self._users.get(account_id or "", "").lower()})

    def in_scope(self, source: dict) -> list[dict]:
        cfg = source["config"]
        pages = space_pages(self.api, cfg["space_id"])
        if cfg.get("root_id"):
            return tree_of(pages, cfg["root_id"], space_nodes(self.api, cfg["space_id"], pages))
        blogs = list(self.api.pages(f"/api/v2/spaces/{cfg['space_id']}/blogposts", {"status": "current"}))
        return pages + [{**b, "_blog": True} for b in blogs]

    def scan(self, source: dict, now: datetime | None = None) -> dict:
        now = now or datetime.now(timezone.utc)
        settle = timedelta(minutes=float(self.cfg["settle_minutes"]))
        revision = float(self.cfg["comment_revision_hours"]) * 3600
        stats = {"pages": 0, "read": 0, "units": 0, "removed": 0, "archived": 0}
        listing = self.in_scope(source)  # complete or error
        stats["pages"] = len(listing)
        live = {str(p["id"]): p for p in listing}
        if self.cfg.get("exclude_authors"):  # the owner's list may contain names: resolve the authors before filtering
            users_names(self.api, {str(p.get("authorId") or "") for p in listing}, self._users)
        with self.db.tx() as conn:
            known = {r["id"]: r for r in conn.execute(
                "SELECT * FROM streams WHERE source_id = %s AND meta->>'confluence' = 'page'", (source["id"],)).fetchall()}
            # Page read by another enabled source (space × tree): stays with it, unless it has
            # released the page (left its tree) or is turned off.
            elsewhere = {r["id"]: r for r in conn.execute(
                """SELECT st.*, src.enabled AS owner_enabled FROM streams st JOIN sources src ON src.id = st.source_id
                   WHERE st.source_id LIKE 'confluence:%%' AND st.source_id <> %s
                   AND st.meta->>'confluence' = 'page' AND st.status = 'active'""", (source["id"],)).fetchall()}
        for pid, p in live.items():
            sid = f"{KIND}:page:{pid}"
            other = elsewhere.get(sid)
            if other and other["owner_enabled"] and not other["meta"].get("orphan_since"):
                continue
            if self._excluded(p.get("authorId")):
                if sid in known:
                    self._remove_page(source, pid, "author ignored by the owner")
                continue
            if other:
                self._take_over(source, pid)  # the other source released the page: it now belongs to this one
            st = known.get(sid) or (other and {**other, "meta": {**other["meta"], "version_seen": None}})
            version = (p.get("version") or {}).get("number")
            changed_at = _dt((p.get("version") or {}).get("createdAt")) or now
            restored = st is not None and st["status"] != "active"
            if st is not None and st["meta"].get("archived"):
                self._mark(sid, {"archived": False})  # came back from the archived list
            if st is None or restored or st["meta"].get("version_seen") != version:
                if now - changed_at < settle and not restored:
                    continue  # still being edited: wait for it to settle
                stats["read"] += 1
                stats["units"] += self._read_page(source, pid, p.get("_blog", False))
                stats["units"] += self._read_comments(source, pid, now, p.get("_blog", False))
            elif now.timestamp() - float(st["meta"].get("comments_at") or 0) >= revision:
                stats["units"] += self._read_comments(source, pid, now, p.get("_blog", False))
        # Known pages that vanished from the complete listing: deleted, archived or out of scope.
        gone = [(sid, st) for sid, st in known.items() if sid.split(":")[-1] not in live and st["status"] == "active"]
        if gone:
            self._space_readable(source)  # a 404 from lack of access to the space is not a deletion
        for sid, st in gone:
            pid = sid.split(":")[-1]
            if st["meta"].get("archived"):
                continue
            fate = self._fate(pid, st["meta"].get("blog"))
            if fate == "archived":
                self._mark(sid, {"archived": True})
                stats["archived"] += 1
            elif fate == "deleted":
                self._remove_page(source, pid, "deleted in Confluence")
                stats["removed"] += 1
            elif not st["meta"].get("orphan_since"):
                # Left this source's scope: another enabled source may take over; otherwise it goes on the next read.
                self._mark(sid, {"orphan_since": now.isoformat()})
            elif now - _dt(st["meta"]["orphan_since"]) >= timedelta(minutes=float(self.cfg["poll_minutes"]) * 2):
                self._remove_page(source, pid, "left the enabled scope")
                stats["removed"] += 1
        return stats

    def _mark(self, stream_id: str, meta: dict) -> None:
        with self.db.tx() as conn:
            conn.execute("UPDATE streams SET meta = meta || %s WHERE id = %s", (Jsonb(meta), stream_id))

    def _take_over(self, source: dict, page_id: str) -> None:
        """The page moves to this source, with what has already been read from it (nothing is re-read or reprocessed)."""
        ids = (f"{KIND}:page:{page_id}", f"{KIND}:comments:{page_id}")
        with self.db.tx() as conn:
            conn.execute("UPDATE streams SET source_id = %s, meta = meta - 'orphan_since' WHERE id IN (%s, %s)",
                         (source["id"], *ids))
            conn.execute("UPDATE evidence SET source_id = %s WHERE stream_id IN (%s, %s)", (source["id"], *ids))
            conn.execute("UPDATE units SET source_id = %s WHERE stream_id IN (%s, %s)", (source["id"], *ids))

    def _space_readable(self, source: dict) -> None:
        try:
            self.api.get(f"/api/v2/spaces/{source['config']['space_id']}")
        except NotFound as exc:
            raise SourceUnavailable("the space is no longer accessible with this token; nothing was deleted") from exc

    def _fate(self, page_id: str, blog: bool) -> str:
        try:
            p = self.api.get(f"/api/v2/{'blogposts' if blog else 'pages'}/{page_id}")
        except NotFound:
            return "deleted"  # the space is still readable (checked before): the page no longer exists
        status = p.get("status")
        if status in ("trashed", "deleted", "purged"):
            return "deleted"
        return "archived" if status == "archived" else "moved"

    def _remove_page(self, source: dict, page_id: str, reason: str) -> None:
        from .. import knowledge as K
        streams = (f"{KIND}:page:{page_id}", f"{KIND}:comments:{page_id}")
        with self.db.tx() as conn:
            ids = [r["id"] for r in conn.execute(
                """SELECT id FROM evidence WHERE (stream_id IN (%s, %s) AND status IN ('active', 'superseded'))
                   OR (source_id = 'live:confluence' AND status = 'active' AND meta->>'page_id' = %s)""",
                (*streams, page_id)).fetchall()]
            if ids:
                # The version history and the live reads of the page go away too.
                K.tombstone_evidence(conn, "connector", ids, reason)
                conn.execute("UPDATE evidence SET external_id = NULL WHERE id = ANY(%s)", (ids,))
            # The page title is also text derived from it.
            conn.execute("UPDATE streams SET status = 'deleted', title = NULL, locator = '' WHERE id IN (%s, %s)", streams)

    # ------------------------------------------------------------ page

    def _read_page(self, source: dict, page_id: str, blog: bool) -> int:
        from .. import knowledge as K
        p = self.api.get(f"/api/v2/{'blogposts' if blog else 'pages'}/{page_id}", {"body-format": "storage"})
        storage = ((p.get("body") or {}).get("storage") or {}).get("value") or ""
        users_names(self.api, set(_MENTION.findall(storage)) | {(p.get("version") or {}).get("authorId") or "",
                                                                 p.get("authorId") or ""}, self._users)
        version = (p.get("version") or {}).get("number")
        when = _dt((p.get("version") or {}).get("createdAt")) or datetime.now(timezone.utc)
        url = f"https://{self.api.site}/wiki{(p.get('_links') or {}).get('webui', '')}"
        title = p.get("title") or "(untitled)"
        seen: dict[str, int] = {}
        items = []
        for path, text, raw in sections_with_raw(storage, self._users):
            seen[path] = seen.get(path, 0) + 1
            key = sha(f"{path}#{seen[path]}")[:16]
            items.append(Item(
                external_id=f"sec:{page_id}:{key}", author=(p.get("version") or {}).get("authorId") or "",
                role="page", occurred_at=when,
                content=f"Page «{title}» · section: {path}\n{text}",
                meta={"kind": "confluence", "page_id": page_id, "path": path, "version": version, "url": url,
                      "section_key": key, "text_hash": sha(raw), "raw_hash": sha(f"{path}\n{raw}")}))
        sid = f"{KIND}:page:{page_id}"
        with self.db.tx() as conn:
            st = conn.execute("SELECT * FROM streams WHERE id = %s", (sid,)).fetchone()
            conn.execute(
                """INSERT INTO streams (id, source_id, locator, title, meta, last_change_at)
                   VALUES (%s, %s, %s, %s, %s, now())
                   ON CONFLICT (id) DO UPDATE SET source_id = EXCLUDED.source_id, title = EXCLUDED.title,
                       locator = EXCLUDED.locator, meta = streams.meta || EXCLUDED.meta, status = 'active', updated_at = now()""",
                (sid, source["id"], url, title, Jsonb({"confluence": "page", "page_id": page_id, "blog": blog,
                                                       "version_seen": version, "space": source["config"]["space_key"],
                                                       "author_id": p.get("authorId"),
                                                       "author_name": self._users.get(p.get("authorId") or "", "")})))
            existing = {r["external_id"]: r for r in conn.execute(
                "SELECT id, external_id, content_hash, meta FROM evidence WHERE stream_id = %s AND status = 'active'",
                (sid,)).fetchall()}
            current = {it.external_id: it for it in items}
            changed = [ext for ext, it in current.items() if ext in existing
                       and existing[ext]["content_hash"] != it.meta["raw_hash"]]
            removed = [ext for ext in existing if ext not in current]
            added = [ext for ext in current if ext not in existing]
            # A section only renamed or moved (same text, another key): not a content change. The old
            # version becomes history, the new one keeps supporting the same claims, and nothing is re-extracted.
            by_text = {current[e].meta["text_hash"]: e for e in added}
            moved = {old: by_text[existing[old]["meta"].get("text_hash")] for old in removed
                     if existing[old]["meta"].get("text_hash") in by_text}
            removed = [e for e in removed if e not in moved]
            quiet = set(moved.values())
            old_ids = [existing[e]["id"] for e in changed + removed + list(moved)]
            if old_ids:
                # Previous version of the section: kept as history (not a retraction).
                conn.execute(
                    """UPDATE evidence SET status = 'superseded', external_id = NULL,
                           meta = meta || %s WHERE id = ANY(%s)""",
                    (Jsonb({"superseded_at": when.isoformat(), "superseded_by_version": version}), old_ids))
            fresh = [it for ext, it in current.items() if ext in added or ext in changed]
            delta = st is not None and bool(st["meta"].get("version_seen")) and st["status"] == "active"
            meta = {"confluence": {"page_id": page_id, "title": title, "url": url, "version": version,
                                   "space": source["config"]["space_key"], "kind": "page"}, "delta": delta}
            for it in [it for it in fresh if it.external_id in quiet]:
                _store_quiet(conn, source["id"], sid, it)
            to_extract = [it for it in fresh if it.external_id not in quiet]
            created = 0
            for i, part in enumerate(_split(to_extract)):
                if create_unit(conn, source_id=source["id"], stream_id=sid, kind="confluence_page",
                               title=f"{title} (v{version})" + (f" · part {i + 1}" if i else ""), items=part,
                               meta={**meta, "part": i + 1}, priority=40 if delta else None):
                    created += 1
            for old_ext, new_ext in moved.items():
                new_ev = conn.execute("SELECT id FROM evidence WHERE stream_id = %s AND external_id = %s",
                                      (sid, new_ext)).fetchone()
                for c in _section_claims(conn, sid, existing[old_ext]["meta"].get("section_key")):
                    from .. import events
                    events.append(conn, "claim_corroborated", "connector", {"id": c, "evidence_ids": [new_ev["id"]]})
            # Claims of the section (from any previous version) → adjudicator, with both versions.
            for ext in changed + removed:
                claim_ids = _section_claims(conn, sid, existing[ext]["meta"].get("section_key"))
                if not claim_ids:
                    continue
                new_ev = conn.execute("SELECT id FROM evidence WHERE stream_id = %s AND external_id = %s",
                                      (sid, ext)).fetchone() if ext in changed else None
                what = "changed" if ext in changed else "no longer appears with this title"
                hint = ("" if ext in changed else " It may have been deleted, or only renamed/moved with changed text: "
                        "read the current page (read_confluence) before deciding.")
                K.open_tension(
                    conn, "connector", claim_ids,
                    f"A section of the page «{title}» {what} in version {version} of Confluence.",
                    dossier={"detector": "confluence_edit",
                             "hypothesis": (f"The section {what}.{hint} Decide, claim by claim: still holds (current), "
                                            f"evolved (superseded: it was true until the edit) or was wrong (retracted). "
                                            f"The edit may be a correction or an evolution; the new text is not absolute truth, "
                                            f"and neither is the status declared on the page."),
                             "evidence_ids": [existing[ext]["id"]] + ([new_ev["id"]] if new_ev else []),
                             "page_id": page_id,
                             "reason": f"edit of the page {url} (version {version})"})
        return created

    # ------------------------------------------------------------ comments (retraction on edit)

    def _read_comments(self, source: dict, page_id: str, now: datetime, blog: bool = False) -> int:
        items = []
        base = "blogposts" if blog else "pages"
        for kind in ("footer", "inline"):
            roots = list(self.api.pages(f"/api/v2/{base}/{page_id}/{kind}-comments", {"body-format": "storage"}))
            for c in roots:
                for cm, reply in [(c, False)] + [(r, True) for r in self.api.pages(
                        f"/api/v2/{kind}-comments/{c['id']}/children", {"body-format": "storage"})]:
                    items.append((kind, cm, reply, c))
        ids = {(cm.get("version") or {}).get("authorId") or "" for _k, cm, _r, _c in items}
        for _k, cm, _r, _c in items:
            ids |= set(_MENTION.findall(((cm.get("body") or {}).get("storage") or {}).get("value") or ""))
        users_names(self.api, ids, self._users)
        evid = []
        for kind, cm, reply, root in items:
            author = (cm.get("version") or {}).get("authorId")
            if self._excluded(author):
                continue
            storage = ((cm.get("body") or {}).get("storage") or {}).get("value") or ""
            body = "\n".join(t for _p, t in sections_of(storage, self._users))
            anchor = ((root.get("properties") or {}).get("inlineOriginalSelection") or "") if kind == "inline" else ""
            resolved = (root.get("resolutionStatus") or "") if kind == "inline" else ""
            head = (f"{'Reply' if reply else 'Comment'} {'inline' if kind == 'inline' else ''} by {self._who(author)}"
                    + (" (resolved)" if resolved == "resolved" else "") + ":").replace("  ", " ")
            evid.append(Item(
                external_id=f"comment:{cm['id']}", author=author or "", role="comment",
                occurred_at=_dt((cm.get("version") or {}).get("createdAt")) or now,
                content=head + (f"\nCommented excerpt: «{anchor}»" if anchor else "") + f"\n{body}",
                meta={"kind": "confluence_comment", "page_id": page_id, "root": str(root["id"]),
                      "author_name": self._users.get(author or "", ""), "raw_hash": sha(storage + anchor)}))
        sid = f"{KIND}:comments:{page_id}"
        created = 0
        with self.db.tx() as conn:
            page = conn.execute("SELECT title, locator FROM streams WHERE id = %s", (f"{KIND}:page:{page_id}",)).fetchone()
            title = page["title"] if page else page_id
            conn.execute(
                """INSERT INTO streams (id, source_id, locator, title, meta, last_change_at)
                   VALUES (%s, %s, %s, %s, %s, now())
                   ON CONFLICT (id) DO UPDATE SET source_id = EXCLUDED.source_id, status = 'active', updated_at = now()""",
                (sid, source["id"], page["locator"] if page else "", f"Comments · {title}",
                 Jsonb({"confluence": "comments", "page_id": page_id})))
            fresh = sync_stream(conn, source_id=source["id"], stream_id=sid, items=evid, complete=True,
                                reason="comment edited, deleted in Confluence or outside the filters")
            for i, part in enumerate(_split(fresh)):
                if create_unit(conn, source_id=source["id"], stream_id=sid, kind="confluence_comments",
                               title=f"Comments · {title}" + (f" · part {i + 1}" if i else ""), items=part,
                               meta={"confluence": {"page_id": page_id, "title": title, "kind": "comments",
                                                    "url": page["locator"] if page else ""}}):
                    created += 1
            conn.execute("UPDATE streams SET meta = meta || %s WHERE id = %s",
                         (Jsonb({"comments_at": now.timestamp()}), f"{KIND}:page:{page_id}"))
        return created


def _section_claims(conn, stream_id: str, section_key: str | None) -> list[str]:
    """Current claims supported by any version of this section (the current one or those kept as history):
    a claim kept through an edit keeps following the later edits."""
    if not section_key:
        return []
    return [r["id"] for r in conn.execute(
        """SELECT DISTINCT c.id FROM claims c JOIN evidence e ON e.id = ANY(c.evidence_ids)
           WHERE e.stream_id = %s AND e.meta->>'section_key' = %s AND e.status IN ('active', 'superseded')
             AND c.status IN ('current', 'disputed')""", (stream_id, section_key)).fetchall()]


def _store_quiet(conn, source_id: str, stream_id: str, it: Item) -> None:
    """Evidence without a unit: the section only changed place or title (the text is the same)."""
    from .common import content_hash
    conn.execute(
        """INSERT INTO evidence (id, unit_id, source_id, stream_id, external_id, author, role, occurred_at,
                                 content, content_hash, meta)
           VALUES (%s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (new_id("ev"), source_id, stream_id, it.external_id, it.author, it.role, it.occurred_at, it.content,
         content_hash(it), Jsonb(it.meta)))


def _split(items: list[Item], limit: int = MAX_UNIT_CHARS) -> list[list[Item]]:
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


# ---------------------------------------------------------------- context, estimate, live read


def reapply_filters(db, source: dict, cfg: dict) -> dict:
    """The ignored-authors list changed: what came only from them goes away (pages created by them and
    their comments); what holds again is re-read on the next read (the comments of all
    pages are re-read, and removed pages come back as restored)."""
    from .. import knowledge as K
    excluded = {a.lower() for a in cfg.get("exclude_authors") or []}
    out = {"retracted": 0, "pages": 0}
    with db.tx() as conn:
        pages = conn.execute(
            """SELECT * FROM streams WHERE source_id = %s AND meta->>'confluence' = 'page' AND status = 'active'""",
            (source["id"],)).fetchall()
    connector = ConfluenceConnector(db, None, cfg, "")
    for st in pages:
        if excluded & {(st["meta"].get("author_id") or "").lower(), (st["meta"].get("author_name") or "").lower()}:
            connector._remove_page(source, st["meta"]["page_id"], "author ignored by the owner")
            out["pages"] += 1
    with db.tx() as conn:
        ids = [r["id"] for r in conn.execute(
            """SELECT id FROM evidence WHERE source_id = %s AND status = 'active' AND role = 'comment'
               AND (lower(author) = ANY(%s) OR lower(meta->>'author_name') = ANY(%s))""",
            (source["id"], list(excluded), list(excluded))).fetchall()]
        if ids:
            K.tombstone_evidence(conn, "connector", ids, "outside the owner's filters")
            conn.execute("UPDATE evidence SET external_id = NULL WHERE id = ANY(%s)", (ids,))
            out["retracted"] = len(ids)
        conn.execute("""UPDATE streams SET meta = meta - 'comments_at' WHERE source_id = %s""", (source["id"],))
    return out


def unit_context(conn, unit: dict, evidence: list[dict]) -> list[dict]:
    """A changed section arrives with the rest of the page as context; a comment arrives with its thread (the
    comment it replies to and the earlier replies) and with the page. Only what is active, up to
    CONTEXT_CHARS."""
    m = (unit.get("meta") or {}).get("confluence") or {}
    if not m:
        return []
    ids = [e["id"] for e in evidence]
    thread = []
    if m.get("kind") == "comments":
        roots = {(e.get("meta") or {}).get("root") for e in evidence} - {None}
        first = min(e["occurred_at"] for e in evidence)
        thread = conn.execute(
            """SELECT * FROM evidence WHERE stream_id = %s AND status = 'active' AND NOT (id = ANY(%s))
               AND meta->>'root' = ANY(%s) AND occurred_at <= %s ORDER BY occurred_at""",
            (unit["stream_id"], ids, list(roots), first)).fetchall()
    page = conn.execute(
        """SELECT * FROM evidence WHERE stream_id = %s AND status = 'active' AND NOT (id = ANY(%s))
           ORDER BY seq""", (f"{KIND}:page:{m['page_id']}", ids)).fetchall()
    out, size = [], 0
    for r in list(thread) + list(page):
        if size + len(r["content"]) > CONTEXT_CHARS:
            break
        out.append(r)
        size += len(r["content"])
    return out


def estimate_backfill(client: ConfluenceClient, source: dict, usd_per_char: float, read_pages: set[str]) -> dict:
    """Before turning on: the text of the pages in scope (no LLM) × measured average cost per character.
    Pages already read are left out of what remains."""
    cfg = source["config"]
    pages = space_pages(client, cfg["space_id"], with_body=True)
    if cfg.get("root_id"):
        pages = tree_of(pages, cfg["root_id"], space_nodes(client, cfg["space_id"], pages))
    chars = new_chars = new = 0
    for p in pages:
        n = sum(len(t) for _path, t in sections_of(((p.get("body") or {}).get("storage") or {}).get("value") or ""))
        chars += n
        if str(p["id"]) not in read_pages:
            new += 1
            new_chars += n
    return {"pages": len(pages), "chars": chars, "usd": round(chars * usd_per_char, 2), "new": new,
            "usd_new": round(new_chars * usd_per_char, 2), "usd_per_char": usd_per_char,
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}


def page_ref(ref: str) -> tuple[str | None, bool]:
    """Page ID (and whether it is a blog post) from an ID, a page URL, a blog URL or `viewpage.action`."""
    ref = ref.strip()
    if ref.isdigit():
        return ref, False
    for pattern, blog in ((r"/blog/(?:\d{4}/\d{2}/\d{2}/)?(\d+)", True), (r"/pages/(\d+)", False),
                          (r"[?&]pageId=(\d+)", False)):
        m = re.search(pattern, ref)
        if m:
            return m.group(1), blog
    return None, False


def read_live(client: ConfluenceClient, ref: str) -> tuple[str, str, str]:
    """Current page (ID or URL), as it is now: (locator, title, text). "Not found" is not "deleted":
    it may be lack of access or a link I cannot read."""
    page_id, blog = page_ref(ref)
    if not page_id:
        return ref, "", "cannot read this link (use the page ID or the full URL; short /x/ links are not supported)"
    p = None
    for kind in (("blogposts", "pages") if blog else ("pages", "blogposts")):
        try:
            p = client.get(f"/api/v2/{kind}/{page_id}", {"body-format": "storage"})
            break
        except NotFound:
            continue
    if p is None:
        return ref, "", "could not find the page with this token (it may have been deleted or be restricted)"
    if p.get("status") in ("trashed", "deleted", "purged"):
        return ref, p.get("title") or page_id, "the page is in the Confluence trash (deleted)"
    storage = ((p.get("body") or {}).get("storage") or {}).get("value") or ""
    text = "\n\n".join(f"## {path}\n{t}" for path, t in sections_of(storage))
    url = f"https://{client.site}/wiki{(p.get('_links') or {}).get('webui', '')}"
    return url, p.get("title") or page_id, f"{p.get('title')} (version {(p.get('version') or {}).get('number')})\n\n{text}"


def client_from(cfg: dict) -> ConfluenceClient:
    email, token = resolve_credentials(cfg)
    return ConfluenceClient(cfg["site"], email, token, cfg.get("token_type") or "scoped", cloud_id=cfg.get("cloud_id"))

