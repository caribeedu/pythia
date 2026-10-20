"""Plumbing shared by the connectors: syncing a stream's evidence and creating units.

Rules of §6.4: new item → new evidence; content changed → the old version is
retracted (tombstone) and the new one comes in; gone from a COMPLETE listing → tombstone.
Never deletes based on an incomplete listing.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime

from psycopg.types.json import Jsonb

from .. import knowledge as K
from ..ids import new_id, sha
from ..jobs import enqueue


class SourceUnavailable(Exception):
    """Source down, without credentials or rate limited: try later."""


class NotFound(SourceUnavailable):
    """The source answered that the item does not exist (or is not visible): it is an answer, not an access failure."""


@dataclass
class Item:
    external_id: str
    author: str
    role: str
    occurred_at: datetime
    content: str
    meta: dict = field(default_factory=dict)


def content_hash(it: Item) -> str:
    """Hash that decides whether the item changed at the source. Connectors that format the text (names, links) pass
    `raw_hash` over the raw fields: renaming a person does not turn into an "edit" of each of their messages."""
    return it.meta.get("raw_hash") or sha(it.content)


def sync_stream(conn, *, source_id: str, stream_id: str, items: list[Item], complete: bool, reason: str) -> list[Item]:
    """Compares the source's current items with the stream's evidence. Returns the items that become new evidence."""
    existing = {r["external_id"]: r for r in conn.execute(
        """SELECT id, external_id, content_hash FROM evidence
           WHERE source_id = %s AND stream_id = %s AND status = 'active' AND external_id IS NOT NULL""",
        (source_id, stream_id)).fetchall()}
    fresh: list[Item] = []
    to_tombstone: list[str] = []
    seen = set()
    for it in items:
        seen.add(it.external_id)
        old = existing.get(it.external_id)
        if old is None:
            # It may also already exist in another stream of the same source (ids are global within the source).
            if conn.execute("SELECT 1 FROM evidence WHERE source_id = %s AND external_id = %s",
                            (source_id, it.external_id)).fetchone():
                continue
            fresh.append(it)
        elif old["content_hash"] != content_hash(it):
            to_tombstone.append(old["id"])  # edit = retraction of the previous version
            fresh.append(it)
    if complete:
        to_tombstone.extend(r["id"] for ext, r in existing.items() if ext not in seen)
    if to_tombstone:
        K.tombstone_evidence(conn, "connector", to_tombstone, reason)
        conn.execute("UPDATE evidence SET external_id = NULL WHERE id = ANY(%s)", (to_tombstone,))
    return fresh


def create_unit(conn, *, source_id: str, stream_id: str | None, kind: str, title: str | None, items: list[Item],
                meta: dict, priority: int | None = None) -> int | None:
    if not items:
        return None
    occurred = max(it.occurred_at for it in items)
    unit = conn.execute(
        """INSERT INTO units (source_id, stream_id, kind, title, occurred_at, meta)
           VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
        (source_id, stream_id, kind, title, occurred, Jsonb(meta))).fetchone()
    for it in sorted(items, key=lambda i: i.occurred_at):
        conn.execute(
            """INSERT INTO evidence (id, unit_id, source_id, stream_id, external_id, author, role, occurred_at,
                                     content, content_hash, meta)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (new_id("ev"), unit["id"], source_id, stream_id, it.external_id, it.author, it.role, it.occurred_at,
             it.content, content_hash(it), Jsonb(it.meta)))
    enqueue(conn, "process_unit", "cognition", {"unit_id": unit["id"]},
            priority=priority if priority is not None else recency_priority(occurred.timestamp()),
            dedup_key=f"unit:{unit['id']}")
    return unit["id"]


def recency_priority(epoch: float) -> int:
    """More recent = lower number = processed sooner (backfill from new to old)."""
    days_ago = max(0, int((time.time() - epoch) // 86400))
    return 1000 + min(days_ago, 4000)
