"""Event sourcing: append-only log + projections applied in the same transaction.

All knowledge state (entities, mentions, claims, tensions, verdicts) is a
projection of the `events` log. Reverting means writing a compensating event.
`rebuild` rebuilds the projections from scratch out of the log.

Evidence content does NOT go into the log: it stays in the `evidence` table, so
that an edit/deletion at the source really erases the content. Text *derived*
from a piece of evidence (mention context, statement of a claim that only it
supported) is redacted in the log itself by `redact` — the only exception to
append-only, because erasing at the source must erase here too. `rebuild` keeps
reproducing the same state, since the projection reads the redacted payload.
"""

from __future__ import annotations

from contextvars import ContextVar
from datetime import datetime
from typing import Any, Callable

import psycopg
from psycopg.types.json import Jsonb

REDACTED = "[removed at the source]"

PROJECTION_TABLES = [
    "entities",
    "mentions",
    "mention_link_history",
    "claims",
    "claim_status_history",
    "tensions",
    "verdicts",
]

_APPLY: dict[str, Callable[[psycopg.Connection, dict, datetime, int], None]] = {}


def _handler(event_type: str):
    def register(fn):
        _APPLY[event_type] = fn
        return fn

    return register


# What caused the changes in progress (unit:<id>, tension:<id>, correction:<id>…), for Evolution.
# Set by whoever runs the work (the job); it is event metadata, not part of the payload.
CAUSE: ContextVar[str | None] = ContextVar("pythia_event_cause", default=None)


def append(conn: psycopg.Connection, event_type: str, actor: str, payload: dict[str, Any]) -> int:
    """Writes the event and applies the projection. Returns the event id."""
    if event_type not in _APPLY:
        raise ValueError(f"unknown event type: {event_type}")
    row = conn.execute(
        "INSERT INTO events (type, actor, payload, cause) VALUES (%s, %s, %s, %s) RETURNING id, ts",
        (event_type, actor, Jsonb(payload), CAUSE.get()),
    ).fetchone()
    _APPLY[event_type](conn, payload, row["ts"], row["id"])
    return row["id"]


def redact(conn: psycopg.Connection, event_type: str, match: dict[str, str], fields: list[str]) -> int:
    """Replaces text fields in the payloads matching `match` (e.g. {"id": "cl_x"})."""
    where = " AND ".join(f"payload->>'{k}' = %s" for k in match)
    sets = "payload"
    params: list = []
    for f in fields:
        sets = f"jsonb_set({sets}, '{{{f}}}', to_jsonb(%s::text))"
        params.append(REDACTED)
    cur = conn.execute(f"UPDATE events SET payload = {sets} WHERE type = %s AND {where}",
                       (*params, event_type, *match.values()))
    return cur.rowcount


def rebuild(conn: psycopg.Connection) -> int:
    """Wipes the projections and replays the whole log. Returns how many events were applied."""
    for table in PROJECTION_TABLES:
        conn.execute(f"TRUNCATE {table}")
    count = 0
    cur = conn.execute("SELECT id, ts, type, payload FROM events ORDER BY id")
    for ev in cur.fetchall():
        _APPLY[ev["type"]](conn, ev["payload"], ev["ts"], ev["id"])
        count += 1
    return count


# ---------------------------------------------------------------- entities


@_handler("entity_created")
def _entity_created(conn, p, ts, eid):
    conn.execute(
        """INSERT INTO entities (id, type, name, aliases, external_ids, description, origin, is_owner, created_at, updated_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            p["id"],
            p["type"],
            p["name"],
            p.get("aliases", []),
            Jsonb(p.get("external_ids", {})),
            p.get("description"),
            p["origin"],
            p.get("is_owner", False),
            ts,
            ts,
        ),
    )


@_handler("entity_updated")
def _entity_updated(conn, p, ts, eid):
    fields = {k: p[k] for k in ("type", "name", "aliases", "description") if k in p}
    if "external_ids" in p:
        fields["external_ids"] = Jsonb(p["external_ids"])
    if not fields:
        return
    sets = ", ".join(f"{k} = %s" for k in fields)
    conn.execute(
        f"UPDATE entities SET {sets}, updated_at = %s WHERE id = %s",
        (*fields.values(), ts, p["id"]),
    )


@_handler("entity_merged")
def _entity_merged(conn, p, ts, eid):
    conn.execute(
        "UPDATE entities SET status = 'merged', merged_into = %s, updated_at = %s WHERE id = %s",
        (p["into"], ts, p["id"]),
    )


@_handler("entity_unmerged")
def _entity_unmerged(conn, p, ts, eid):
    conn.execute(
        "UPDATE entities SET status = 'active', merged_into = NULL, updated_at = %s WHERE id = %s",
        (ts, p["id"]),
    )


# ---------------------------------------------------------------- mentions


@_handler("mention_recorded")
def _mention_recorded(conn, p, ts, eid):
    conn.execute(
        """INSERT INTO mentions (id, evidence_id, unit_id, surface, context, type_hint, entity_id,
                                 confidence, resolved_by, candidates, created_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            p["id"],
            p.get("evidence_id"),
            p.get("unit_id"),
            p["surface"],
            p.get("context"),
            p.get("type_hint"),
            p.get("entity_id"),
            p.get("confidence"),
            p.get("resolved_by"),
            Jsonb(p.get("candidates", [])),
            ts,
        ),
    )
    conn.execute(
        "INSERT INTO mention_link_history (mention_id, entity_id, from_ts, event_id) VALUES (%s, %s, %s, %s)",
        (p["id"], p.get("entity_id"), ts, eid),
    )


@_handler("mention_relinked")
def _mention_relinked(conn, p, ts, eid):
    mid, to = p["id"], p.get("to")
    conn.execute(
        "UPDATE mentions SET entity_id = %s, resolved_by = %s, confidence = %s WHERE id = %s",
        (to, p.get("by"), p.get("confidence"), mid),
    )
    conn.execute(
        "UPDATE mention_link_history SET to_ts = %s WHERE mention_id = %s AND to_ts IS NULL",
        (ts, mid),
    )
    conn.execute(
        "INSERT INTO mention_link_history (mention_id, entity_id, from_ts, event_id) VALUES (%s, %s, %s, %s)",
        (mid, to, ts, eid),
    )
    conn.execute("UPDATE claims SET subject_entity_id = %s WHERE subject_mention_id = %s", (to, mid))
    conn.execute("UPDATE claims SET object_entity_id = %s WHERE object_mention_id = %s", (to, mid))


# ---------------------------------------------------------------- claims


def _set_claim_status(conn, claim_id: str, status: str, ts, eid: int, verdict_id: str | None) -> None:
    conn.execute("UPDATE claims SET status = %s WHERE id = %s", (status, claim_id))
    conn.execute(
        "UPDATE claim_status_history SET to_ts = %s WHERE claim_id = %s AND to_ts IS NULL",
        (ts, claim_id),
    )
    conn.execute(
        """INSERT INTO claim_status_history (claim_id, status, from_ts, event_id, verdict_id)
           VALUES (%s, %s, %s, %s, %s)""",
        (claim_id, status, ts, eid, verdict_id),
    )


@_handler("claim_asserted")
def _claim_asserted(conn, p, ts, eid):
    conn.execute(
        """INSERT INTO claims (id, kind, subject_mention_id, subject_entity_id, predicate, object_value,
                               object_mention_id, object_entity_id, statement, valid_from, valid_to,
                               recorded_at, origin, source_id, source_label, status, confidence,
                               extractor_version, evidence_ids, supporting_claim_ids)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            p["id"],
            p["kind"],
            p.get("subject_mention_id"),
            p.get("subject_entity_id"),
            p["predicate"],
            p.get("object_value"),
            p.get("object_mention_id"),
            p.get("object_entity_id"),
            p["statement"],
            p.get("valid_from"),
            p.get("valid_to"),
            ts,
            p["origin"],
            p.get("source_id"),
            p.get("source_label"),
            p.get("status", "current"),
            p.get("confidence"),
            p.get("extractor_version"),
            p.get("evidence_ids", []),
            p.get("supporting_claim_ids", []),
        ),
    )
    conn.execute(
        """INSERT INTO claim_status_history (claim_id, status, from_ts, event_id)
           VALUES (%s, %s, %s, %s)""",
        (p["id"], p.get("status", "current"), ts, eid),
    )


@_handler("claim_corroborated")
def _claim_corroborated(conn, p, ts, eid):
    conn.execute(
        """UPDATE claims SET evidence_ids = ARRAY(SELECT DISTINCT unnest(evidence_ids || %s::text[]))
           WHERE id = %s""",
        (p["evidence_ids"], p["id"]),
    )


@_handler("claim_evidence_removed")
def _claim_evidence_removed(conn, p, ts, eid):
    conn.execute(
        "UPDATE claims SET evidence_ids = array_remove(evidence_ids, %s) WHERE id = %s",
        (p["evidence_id"], p["id"]),
    )


@_handler("claim_validity_set")
def _claim_validity_set(conn, p, ts, eid):
    """a claim that evolved in a Confluence edit was valid until the edit date."""
    conn.execute("UPDATE claims SET valid_to = %s WHERE id = %s", (p.get("valid_to"), p["id"]))


@_handler("claim_reassigned")
def _claim_reassigned(conn, p, ts, eid):
    column = "subject_entity_id" if p["field"] == "subject" else "object_entity_id"
    conn.execute(f"UPDATE claims SET {column} = %s WHERE id = %s", (p["to"], p["id"]))


@_handler("claim_status_changed")
def _claim_status_changed(conn, p, ts, eid):
    _set_claim_status(conn, p["id"], p["to"], ts, eid, p.get("verdict_id"))


# ---------------------------------------------------------------- tensions and verdicts


@_handler("tension_opened")
def _tension_opened(conn, p, ts, eid):
    conn.execute(
        """INSERT INTO tensions (id, claim_ids, reason, opened_by, created_at, dossier)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (p["id"], p["claim_ids"], p["reason"], p["opened_by"], ts,
         Jsonb(p["dossier"]) if isinstance(p.get("dossier"), dict) else None),  # redacted = no dossier
    )


@_handler("verdict_applied")
def _verdict_applied(conn, p, ts, eid):
    conn.execute(
        """INSERT INTO verdicts (id, tension_id, kind, decided_by, claim_ids, changes, summary, rationale,
                                 cited_evidence, confidence, model, prompt_version, dossier_hash, impact, exploration,
                                 created_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            p["id"],
            p.get("tension_id"),
            p["kind"],
            p["decided_by"],
            p["claim_ids"],
            Jsonb(p["changes"]),
            p["summary"],
            p["rationale"],
            p.get("cited_evidence", []),
            p.get("confidence"),
            p.get("model"),
            p.get("prompt_version"),
            p.get("dossier_hash"),
            p.get("impact", 0),
            Jsonb(p.get("exploration", [])),
            ts,
        ),
    )
    for ch in p["changes"]:
        if ch["from"] != ch["to"]:
            _set_claim_status(conn, ch["claim_id"], ch["to"], ts, eid, p["id"])
    if p.get("tension_id"):
        conn.execute(
            "UPDATE tensions SET status = 'resolved', verdict_id = %s WHERE id = %s",
            (p["id"], p["tension_id"]),
        )


@_handler("verdict_reverted")
def _verdict_reverted(conn, p, ts, eid):
    conn.execute(
        "UPDATE verdicts SET status = %s, reverted_at = %s WHERE id = %s",
        (p.get("status", "reverted"), ts, p["id"]),
    )
    for ch in p["changes"]:
        _set_claim_status(conn, ch["claim_id"], ch["to"], ts, eid, p["id"])
    if p.get("reopen_tension_id"):
        conn.execute(
            "UPDATE tensions SET status = 'open', verdict_id = NULL WHERE id = %s",
            (p["reopen_tension_id"],),
        )
