"""Hybrid search (vector + text) with rank fusion (RRF).

It is plumbing: it assembles what the LLM sees (initial dossier, `search` result).
It never concludes anything.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from .db import Database
from .embed import Embedder

RRF_K = 60


def _rrf(rankings: list[list[str]]) -> dict[str, float]:
    scores: dict[str, float] = {}
    for ranking in rankings:
        for pos, key in enumerate(ranking):
            scores[key] = scores.get(key, 0.0) + 1.0 / (RRF_K + pos + 1)
    return scores


def _tsquery(text: str) -> str:
    words = [w for w in "".join(ch if ch.isalnum() else " " for ch in text.lower()).split() if len(w) > 1]
    return " | ".join(words[:12])


class Retriever:
    def __init__(self, db: Database, embedder: Embedder):
        self.db = db
        self.embedder = embedder

    def vector(self, text: str) -> np.ndarray:
        return np.array(self.embedder.embed([text])[0], dtype=np.float32)

    def search(self, query: str, *, limit: int = 12, kinds: tuple[str, ...] = ("view", "claim", "entity"),
               statuses: tuple[str, ...] = ("current", "disputed")) -> list[dict[str, Any]]:
        vec = self.vector(query)
        tsq = _tsquery(query)
        rankings: list[list[str]] = []
        items: dict[str, dict] = {}
        n = limit * 3
        with self.db.tx() as conn:
            if "view" in kinds:
                vrows = conn.execute(
                    """SELECT id, kind, title, l0, entity_id FROM views WHERE status = 'active' AND embedding IS NOT NULL
                       ORDER BY embedding <=> %s LIMIT %s""", (vec, n)).fetchall()
                rankings.append([r["id"] for r in vrows])
                trows = conn.execute(
                    """SELECT id, kind, title, l0, entity_id FROM views WHERE status = 'active' AND tsv @@ to_tsquery('simple', %s)
                       ORDER BY ts_rank(tsv, to_tsquery('simple', %s)) DESC LIMIT %s""", (tsq, tsq, n)).fetchall() if tsq else []
                rankings.append([r["id"] for r in trows])
                for r in [*vrows, *trows]:
                    items[r["id"]] = {"id": r["id"], "type": "page" if r["kind"] == "entity" else "narrative",
                                      "title": r["title"], "l0": r["l0"], "entity_id": r["entity_id"]}
            if "claim" in kinds:
                crows = conn.execute(
                    """SELECT c.id, c.statement, c.kind, c.status, c.subject_entity_id FROM claims c
                       WHERE c.status = ANY(%s) AND c.embedding IS NOT NULL
                       ORDER BY c.embedding <=> %s LIMIT %s""", (list(statuses), vec, n)).fetchall()
                rankings.append([r["id"] for r in crows])
                ctrows = conn.execute(
                    """SELECT c.id, c.statement, c.kind, c.status, c.subject_entity_id FROM claims c
                       WHERE c.status = ANY(%s) AND c.tsv @@ to_tsquery('simple', %s)
                       ORDER BY ts_rank(c.tsv, to_tsquery('simple', %s)) DESC LIMIT %s""",
                    (list(statuses), tsq, tsq, n)).fetchall() if tsq else []
                rankings.append([r["id"] for r in ctrows])
                for r in [*crows, *ctrows]:
                    items[r["id"]] = {"id": r["id"], "type": "claim", "title": f"{r['kind']} ({r['status']})",
                                      "l0": r["statement"], "entity_id": r["subject_entity_id"]}
            if "entity" in kinds:
                erows = conn.execute(
                    """SELECT id, name, type, description FROM entities WHERE status = 'active'
                       AND (lower(name) %% lower(%s) OR lower(%s) LIKE '%%' || lower(name) || '%%'
                            OR EXISTS (SELECT 1 FROM unnest(aliases) a WHERE lower(%s) LIKE '%%' || lower(a) || '%%'))
                       ORDER BY similarity(lower(name), lower(%s)) DESC LIMIT %s""",
                    (query, query, query, query, n)).fetchall()
                rankings.append([r["id"] for r in erows])
                for r in erows:
                    items.setdefault(r["id"], {"id": r["id"], "type": "entity", "title": f"{r['name']} ({r['type']})",
                                               "l0": r["description"] or "", "entity_id": r["id"]})
        scores = _rrf(rankings)
        ranked = sorted(scores, key=lambda k: -scores[k])
        out = []
        for key in ranked[:limit]:
            item = dict(items[key])
            item["score"] = round(scores[key], 4)
            out.append(item)
        return out

    def neighbors_for_claim(self, conn, statement: str, exclude_entity: str | None, limit: int = 8) -> list[dict]:
        """Semantically close claims on other entities (so integration sees beyond the slot)."""
        vec = self.vector(statement)
        return conn.execute(
            """SELECT c.*, s.name AS subject_name FROM claims c LEFT JOIN entities s ON s.id = c.subject_entity_id
               WHERE c.status IN ('current', 'disputed') AND c.embedding IS NOT NULL
                 AND (c.subject_entity_id IS DISTINCT FROM %s)
               ORDER BY c.embedding <=> %s LIMIT %s""",
            (exclude_entity, vec, limit),
        ).fetchall()

    def entity_candidates(self, conn, surface: str, context: str, limit: int = 6) -> list[dict]:
        """Candidates for a mention: exact alias, trigram on the name and embedding similarity."""
        exact = conn.execute(
            """SELECT id, type, name, aliases, description FROM entities
               WHERE status = 'active' AND (lower(name) = lower(%s) OR lower(%s) = ANY(SELECT lower(a) FROM unnest(aliases) a)
                     OR EXISTS (SELECT 1 FROM jsonb_each_text(external_ids) kv WHERE lower(kv.value) = lower(%s)))""",
            (surface, surface, surface)).fetchall()
        fuzzy = conn.execute(
            """SELECT id, type, name, aliases, description FROM entities
               WHERE status = 'active' AND similarity(lower(name), lower(%s)) > 0.3
               ORDER BY similarity(lower(name), lower(%s)) DESC LIMIT %s""", (surface, surface, limit)).fetchall()
        vec = self.vector(f"{surface}: {context[:300]}")
        semantic = conn.execute(
            """SELECT id, type, name, aliases, description FROM entities
               WHERE status = 'active' AND embedding IS NOT NULL ORDER BY embedding <=> %s LIMIT %s""",
            (vec, limit)).fetchall()
        seen: dict[str, dict] = {}
        for r in [*exact, *fuzzy, *semantic]:
            seen.setdefault(r["id"], r)
        return list(seen.values())[: limit + len(exact)]

    def glossary_for(self, conn, surface: str, limit: int = 3) -> list[dict]:
        """Current glossary claims that mention the term (they serve as an alias table)."""
        return conn.execute(
            """SELECT c.statement, c.subject_entity_id, c.object_entity_id FROM claims c
               LEFT JOIN entities s ON s.id = c.subject_entity_id
               WHERE c.kind = 'glossary' AND c.status IN ('current', 'disputed')
                 AND (lower(s.name) = lower(%s) OR lower(%s) = ANY(SELECT lower(a) FROM unnest(s.aliases) a)
                      OR lower(c.object_value) = lower(%s) OR c.statement ILIKE '%%' || %s || '%%')
               ORDER BY c.recorded_at DESC LIMIT %s""",
            (surface, surface, surface, surface, limit),
        ).fetchall()
