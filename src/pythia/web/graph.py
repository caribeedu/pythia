"""Reads for Explore: ego graph and side panel, with support for "how it was at T"."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .. import knowledge as K

MAX_NODES = 50


def _claims_at(conn, entity_ids: list[str], at: datetime | None) -> list[dict]:
    """Claims that touch the entities, with status and endpoints as they were at `at` (or now)."""
    if at is None:
        return conn.execute(
            """SELECT c.*, c.status AS status_at, c.subject_entity_id AS subj_at, c.object_entity_id AS obj_at
               FROM claims c WHERE (c.subject_entity_id = ANY(%s) OR c.object_entity_id = ANY(%s))""",
            (entity_ids, entity_ids)).fetchall()
    return conn.execute(
        """WITH base AS (
             SELECT c.*,
               (SELECT h.status FROM claim_status_history h WHERE h.claim_id = c.id AND h.from_ts <= %(t)s
                  AND (h.to_ts IS NULL OR h.to_ts > %(t)s) LIMIT 1) AS status_at,
               coalesce((SELECT l.entity_id FROM mention_link_history l WHERE l.mention_id = c.subject_mention_id
                  AND l.from_ts <= %(t)s AND (l.to_ts IS NULL OR l.to_ts > %(t)s) LIMIT 1),
                  CASE WHEN c.subject_mention_id IS NULL THEN c.subject_entity_id END) AS subj_at,
               coalesce((SELECT l.entity_id FROM mention_link_history l WHERE l.mention_id = c.object_mention_id
                  AND l.from_ts <= %(t)s AND (l.to_ts IS NULL OR l.to_ts > %(t)s) LIMIT 1),
                  CASE WHEN c.object_mention_id IS NULL THEN c.object_entity_id END) AS obj_at
             FROM claims c WHERE c.recorded_at <= %(t)s)
           SELECT * FROM base WHERE status_at IS NOT NULL AND (subj_at = ANY(%(ids)s) OR obj_at = ANY(%(ids)s))""",
        {"t": at, "ids": entity_ids}).fetchall()


def _view_at(conn, view_id: str, at: datetime | None) -> dict | None:
    if at is None:
        return conn.execute("SELECT * FROM views WHERE id = %s", (view_id,)).fetchone()
    return conn.execute(
        """SELECT v.id, v.kind, v.entity_id, vv.title, vv.l0, vv.l1, vv.cited_claim_ids, vv.member_entity_ids,
                  vv.status, vv.generated_at, false AS stale
           FROM views v JOIN view_versions vv ON vv.view_id = v.id
           WHERE v.id = %s AND vv.generated_at <= %s ORDER BY vv.version DESC LIMIT 1""", (view_id, at)).fetchone()


def _entities_at(conn, ids: list[str], at: datetime | None) -> dict[str, dict]:
    rows = conn.execute("SELECT * FROM entities WHERE id = ANY(%s)", (ids,)).fetchall()
    out = {}
    for r in rows:
        if at is not None and r["created_at"] > at:
            continue
        if at is None and r["status"] != "active":
            continue
        out[r["id"]] = r
    return out


def ego_graph(conn, center: str | None, depth: int, at: datetime | None) -> dict[str, Any]:
    if not center:
        own = K.owner(conn)
        center = own["id"] if own else None
    if not center:
        return {"center": None, "nodes": [], "edges": []}
    if center.startswith("vw_"):
        v = _view_at(conn, center, at)
        if v and v["kind"] == "entity":
            center = v["entity_id"]
        elif v:
            return _narrative_graph(conn, v, at)
    nodes: dict[str, dict] = {}
    edges: dict[str, dict] = {}
    frontier = [center]
    seen = {center}
    for _ in range(max(1, depth)):
        claims = _claims_at(conn, frontier, at)
        nxt = []
        for c in claims:
            if c["status_at"] not in ("current", "disputed"):
                continue
            s, o = c["subj_at"], c["obj_at"]
            if s and o and s != o:
                edges[c["id"]] = {"id": c["id"], "source": s, "target": o, "label": c["predicate"],
                                  "status": c["status_at"]}
                for e in (s, o):
                    if e not in seen and len(seen) < MAX_NODES:
                        seen.add(e)
                        nxt.append(e)
        frontier = nxt
        if not frontier:
            break
    ents = _entities_at(conn, list(seen), at)
    counts = {}
    for c in _claims_at(conn, list(ents), at):
        if c["status_at"] in ("current", "disputed") and c["subj_at"] in ents:
            counts[c["subj_at"]] = counts.get(c["subj_at"], 0) + 1
    views = {r["entity_id"]: r for r in conn.execute(
        "SELECT id, entity_id FROM views WHERE kind = 'entity' AND entity_id = ANY(%s)", (list(ents),)).fetchall()}
    for eid, e in ents.items():
        l0 = None
        if eid in views:
            vv = _view_at(conn, views[eid]["id"], at)
            l0 = vv["l0"] if vv else None
        nodes[eid] = {"id": eid, "label": e["name"], "kind": e["type"], "group": "entity", "l0": l0 or "",
                      "claims": counts.get(eid, 0), "owner": e["is_owner"], "center": eid == center}
    # Narratives that contain the center.
    narr = conn.execute(
        "SELECT id FROM views WHERE kind = 'narrative' AND %s = ANY(member_entity_ids)", (center,)).fetchall()
    for r in narr:
        v = _view_at(conn, r["id"], at)
        if not v or v["status"] != "active" or len(nodes) >= MAX_NODES:
            continue
        nodes[v["id"]] = {"id": v["id"], "label": v["title"], "kind": "narrative", "group": "narrative",
                          "l0": v["l0"], "claims": len(v["cited_claim_ids"]), "owner": False, "center": False}
        for m in v["member_entity_ids"]:
            if m in nodes:
                edges[f"{v['id']}:{m}"] = {"id": f"{v['id']}:{m}", "source": v["id"], "target": m,
                                           "label": "", "status": "member"}
    edges = {k: e for k, e in edges.items() if e["source"] in nodes and e["target"] in nodes}
    return {"center": center, "nodes": list(nodes.values()), "edges": list(edges.values())}


def _narrative_graph(conn, v: dict, at: datetime | None) -> dict[str, Any]:
    ents = _entities_at(conn, v["member_entity_ids"], at)
    nodes = [{"id": v["id"], "label": v["title"], "kind": "narrative", "group": "narrative", "l0": v["l0"],
              "claims": len(v["cited_claim_ids"]), "owner": False, "center": True}]
    edges = []
    for eid, e in list(ents.items())[:MAX_NODES]:
        nodes.append({"id": eid, "label": e["name"], "kind": e["type"], "group": "entity", "l0": e["description"] or "",
                      "claims": 0, "owner": e["is_owner"], "center": False})
        edges.append({"id": f"{v['id']}:{eid}", "source": v["id"], "target": eid, "label": "", "status": "member"})
    return {"center": v["id"], "nodes": nodes, "edges": edges}


def node_detail(conn, node_id: str, at: datetime | None) -> dict[str, Any]:
    if node_id.startswith("vw_"):
        v = _view_at(conn, node_id, at)
        if not v:
            return {"error": "not found"}
        claims = conn.execute("SELECT * FROM claims WHERE id = ANY(%s) ORDER BY recorded_at", (v["cited_claim_ids"],)).fetchall()
        members = conn.execute("SELECT id, name, type FROM entities WHERE id = ANY(%s)", (v["member_entity_ids"],)).fetchall()
        return {"kind": "narrative" if v["kind"] == "narrative" else "page", "id": v["id"], "title": v["title"],
                "l0": v["l0"], "l1": v["l1"], "stale": v["stale"], "generated_at": v["generated_at"],
                "members": members, "claims": [_claim_row(c, c["status"]) for c in claims]}
    ent = K.entity(conn, node_id)
    if not ent:
        return {"error": "not found"}
    view = conn.execute("SELECT id FROM views WHERE kind = 'entity' AND entity_id = %s", (node_id,)).fetchone()
    vv = _view_at(conn, view["id"], at) if view else None
    claims = [c for c in _claims_at(conn, [node_id], at)]
    claims.sort(key=lambda c: c["recorded_at"], reverse=True)
    ids = [c["id"] for c in claims]
    verdicts = conn.execute(
        """SELECT id, kind, status, summary, created_at FROM verdicts WHERE claim_ids && %s::text[]
           ORDER BY created_at DESC LIMIT 20""", (ids,)).fetchall()
    mentions = conn.execute(
        "SELECT id, surface, context, confidence FROM mentions WHERE entity_id = %s ORDER BY created_at DESC LIMIT 30",
        (node_id,)).fetchall()
    return {"kind": "entity", "id": ent["id"], "title": ent["name"], "type": ent["type"], "aliases": ent["aliases"],
            "origin": ent["origin"], "owner": ent["is_owner"], "view_id": view["id"] if view else None,
            "l0": vv["l0"] if vv else "", "l1": vv["l1"] if vv else "", "stale": bool(vv and vv["stale"]),
            "claims": [_claim_row(c, c["status_at"]) for c in claims], "verdicts": verdicts, "mentions": mentions}


def _claim_row(c: dict, status: str) -> dict:
    return {"id": c["id"], "kind": c["kind"], "statement": c["statement"], "status": status, "origin": c["origin"],
            "source": c["source_label"], "recorded_at": c["recorded_at"], "evidence_ids": c["evidence_ids"],
            "predicate": c["predicate"], "subject_mention_id": c["subject_mention_id"]}
