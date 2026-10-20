"""The incident.io catalog as an entity seed (deterministic, no LLM).

Each entry of the configured types becomes an `anchored` entity with its
external identifiers (catalog id, GitHub login, Slack, email...).
Attributes that point to other synced entities become relation claims
(`state`, source `incident_io_catalog`); useful literal attributes become
simple claims. These claims do NOT skip integration: they go through the LLM, which
confronts them with what is already known — this is where conflict between sources is born.

Each entry is a stream with one piece of evidence (the entry's rendered snapshot):
changed → the previous version is retracted and the new one generates claims; gone from the
complete listing → tombstone (claims that depended only on it are retracted).
"""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone
from typing import Any

import httpx

from .. import events
from .. import knowledge as K
from .. import vault
from ..db import Database
from ..ids import new_id, sha
from ..jobs import enqueue
from .common import Item, SourceUnavailable, sync_stream

KIND = "incident_io"
SOURCE_ID = "incident_io:catalog"
LABEL = "incident_io_catalog"
PRIMITIVES = {"String", "Text", "Number", "Bool"}
# Identity, not knowledge: they go to external_ids instead of becoming a claim.
IDENTITY_ATTRS = {"email": "email", "slack": "slack", "slack user": "slack", "github": "github",
                  "github user": "github", "jira": "jira", "jira user": "jira", "zoom": "zoom", "zoom user": "zoom",
                  "google workspace": "google", "google workspace user": "google", "incident.io user": "incident_io_user"}
SKIP_ATTRS = {"avatar", "timezone", "phone number", "state", "role"}


def _slug(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_") or "attribute"


def _text(v: dict | None) -> str:
    if not v:
        return ""
    if isinstance(v, str):
        return v
    return (v.get("label") or v.get("literal") or "").strip()


def store_available_types(conn, types: list[dict]) -> None:
    """Stores the types the catalog has, so Settings can list all of them (not just the default ones)."""
    from psycopg.types.json import Jsonb
    conn.execute(
        """INSERT INTO system_state (key, value) VALUES ('catalog_types_available', %s)
           ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()""",
        (Jsonb({"types": [{"type_name": t["type_name"], "name": t["name"]} for t in types]}),))


def type_options(conn, mapping: dict[str, str]) -> dict[str, str]:
    """Types for Settings: those in the mapping and those the catalog has, in stable order."""
    row = conn.execute("SELECT value FROM system_state WHERE key = 'catalog_types_available'").fetchone()
    available = [t["type_name"] for t in (row["value"]["types"] if row else [])]
    return {t: mapping.get(t, "") for t in sorted(set(mapping) | set(available))}


class IncidentIOClient:
    def __init__(self, api_key: str, base_url: str = "https://api.incident.io", transport: httpx.BaseTransport | None = None):
        self.http = httpx.Client(base_url=base_url, transport=transport, timeout=30,
                                 headers={"Authorization": f"Bearer {api_key}", "User-Agent": "pythia"})

    def get(self, path: str, params: dict | None = None) -> dict:
        try:
            r = self.http.get(path, params=params)
        except httpx.HTTPError as exc:
            raise SourceUnavailable(f"incident.io unreachable: {exc}") from exc
        if r.status_code in (401, 403):
            raise SourceUnavailable(f"incident.io rejected the key ({r.status_code})")
        if r.status_code == 429 or r.status_code >= 500:
            raise SourceUnavailable(f"incident.io {r.status_code}")
        r.raise_for_status()
        return r.json()

    def types(self) -> list[dict]:
        return self.get("/v3/catalog_types")["catalog_types"]

    def entries(self, type_id: str) -> list[dict]:
        out, after = [], None
        while True:
            params = {"catalog_type_id": type_id, "page_size": 250}
            if after:
                params["after"] = after
            page = self.get("/v3/catalog_entries", params)
            out.extend(page["catalog_entries"])
            after = (page.get("pagination_meta") or {}).get("after")
            if not after or not page["catalog_entries"]:
                return out


def client_from(cfg: dict) -> IncidentIOClient:
    key = vault.get_secret(vault.INCIDENT_IO_KEY)
    if not key:
        raise SourceUnavailable("incident.io API key not configured")
    return IncidentIOClient(key, cfg.get("base_url") or "https://api.incident.io")


class CatalogConnector:
    def __init__(self, db: Database, client: IncidentIOClient, cfg: dict):
        self.db = db
        self.api = client
        self.cfg = cfg
        self._names: dict[str, str] = {}

    def sync(self) -> dict:
        mapping: dict[str, str] = {k: v for k, v in self.cfg["types"].items() if v}
        available = self.api.types()
        types = [t for t in available if t["type_name"] in mapping]
        # Complete listings first: nothing is deleted if one of them fails.
        data = {t["id"]: (t, [e for e in self.api.entries(t["id"])]) for t in types}
        stats = {"types": len(types), "entities_created": 0, "entities_updated": 0, "claims": 0, "removed": 0}
        now = datetime.now(timezone.utc)
        with self.db.tx() as conn:
            store_available_types(conn, available)
            conn.execute("""INSERT INTO sources (id, kind, name, config, backfill_until)
                            VALUES (%s, %s, 'incident.io catalog', '{}', now())
                            ON CONFLICT (id) DO NOTHING""", (SOURCE_ID, KIND))
            entity_of: dict[str, str] = {}            # entry id → entity
            by_name: dict[tuple[str, str], str] = {}  # (type_name, name) → entity
            # Step 1: entities and identities.
            for type_id, (t, entries) in data.items():
                attrs = {a["id"]: a for a in t["schema"]["attributes"]}
                for e in entries:
                    if e.get("archived_at"):
                        continue
                    ext = {"incident_io": e["id"]}
                    if e.get("external_id"):
                        ext["incident_io_external"] = e["external_id"]
                    for aid, val in (e.get("attribute_values") or {}).items():
                        a = attrs.get(aid)
                        key = IDENTITY_ATTRS.get(a["name"].lower()) if a else None
                        if key and _text(val.get("value")):
                            ext[key] = _text(val.get("value"))
                    eid, created, updated = self._upsert_entity(conn, e, mapping[t["type_name"]], ext, t["name"])
                    entity_of[e["id"]] = eid
                    by_name[(t["type_name"], e["name"].lower())] = eid
                    self._names[eid] = e["name"]
                    stats["entities_created"] += created
                    stats["entities_updated"] += updated
            # Step 1b: the SCIM group is a Team's member list (the Team itself points to it):
            # it is the same thing per the record, so the group is merged into the Team (the members go to
            # it). A group cited by more than one Team, or by none, stays as its own entity.
            self._fold_scim_groups(conn, data, entity_of, by_name)
            # Step 2: evidence per entry and claims for the new/changed entries.
            candidates: list[dict] = []
            live_streams = set()
            for type_id, (t, entries) in data.items():
                attrs = {a["id"]: a for a in t["schema"]["attributes"]}
                for e in entries:
                    if e.get("archived_at"):
                        continue
                    stream_id = f"{KIND}:{e['id']}"
                    live_streams.add(stream_id)
                    rendered, facts = self._render(t, attrs, e, entity_of, by_name)
                    conn.execute(
                        """INSERT INTO streams (id, source_id, locator, title, meta) VALUES (%s, %s, %s, %s, '{}')
                           ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, status = 'active', updated_at = now()""",
                        (stream_id, SOURCE_ID, f"incident.io catalog {t['type_name']} {e['id']}", e["name"]))
                    item = Item(external_id=f"entry:{e['id']}", author="incident.io", role="catalog",
                                occurred_at=_ts(e.get("updated_at")) or now, content=rendered,
                                meta={"catalog_type": t["type_name"]})
                    ev_id = new_id("ev")
                    facts = self._carry_over(conn, stream_id, item, entity_of[e["id"]], facts, ev_id)
                    fresh = sync_stream(conn, source_id=SOURCE_ID, stream_id=stream_id, items=[item], complete=True,
                                        reason="entry changed or removed in the catalog")
                    if not fresh:
                        continue
                    self._store_evidence(conn, stream_id, fresh[0], ev_id)
                    for f in facts:
                        candidates.append({
                            "kind": "state", "subject_mention_id": None, "subject_entity_id": entity_of[e["id"]],
                            "predicate": f["predicate"], "object_value": f["value"], "object_mention_id": None,
                            "object_entity_id": f.get("object_entity_id"), "statement": f["statement"],
                            "valid_from": None, "valid_to": None, "origin": "observed", "source_id": SOURCE_ID,
                            "source_label": LABEL, "confidence": 1.0, "extractor_version": "catalog-v1",
                            "evidence_ids": [ev_id], "supporting_claim_ids": []})
            # Entries that vanished (complete listing of all types): the record stopped saying that.
            gone = conn.execute(
                "SELECT id FROM streams WHERE source_id = %s AND status = 'active' AND NOT (id = ANY(%s))",
                (SOURCE_ID, list(live_streams))).fetchall()
            for g in gone:
                ids = [r["id"] for r in conn.execute(
                    "SELECT id FROM evidence WHERE stream_id = %s AND status = 'active'", (g["id"],)).fetchall()]
                K.tombstone_evidence(conn, "connector", ids, "entry removed from the catalog")
                conn.execute("UPDATE streams SET status = 'deleted' WHERE id = %s", (g["id"],))
                stats["removed"] += 1
            # Structured claims go to integration (LLM) in batches.
            for i in range(0, len(candidates), 40):
                batch = [{"ref": f"N{j + 1}", **c} for j, c in enumerate(candidates[i:i + 40])]
                enqueue(conn, "integrate_structured", "cognition", {"candidates": batch}, priority=40)
            stats["claims"] = len(candidates)
            enqueue(conn, "reconcile_entities", "cognition", {}, priority=45, dedup_key="reconcile_entities")
        return stats

    def _upsert_entity(self, conn, e: dict, etype: str, ext: dict, type_label: str) -> tuple[str, int, int]:
        aliases = [a for a in (e.get("aliases") or []) if a and len(a) <= 60][:40]
        if ext.get("github"):
            aliases.append(ext["github"])
        description = f"{type_label} in the incident.io catalog."
        row = conn.execute("""SELECT * FROM entities WHERE external_ids @> %s::jsonb
                              ORDER BY (status = 'active') DESC, is_owner DESC LIMIT 1""",
                           (f'{{"incident_io": "{e["id"]}"}}',)).fetchone()
        if row is None:
            eid = K.create_entity(conn, "connector", name=e["name"], type=etype, origin="anchored", aliases=aliases,
                                  description=description, external_ids=ext)
            return eid, 1, 0
        if row["status"] == "merged":  # merged (into the owner, into the Team…): the entry points to the final entity
            target = row
            while target and target["status"] == "merged" and target["merged_into"]:
                target = conn.execute("SELECT * FROM entities WHERE id = %s", (target["merged_into"],)).fetchone()
            if target:
                return target["id"], 0, 0
        changes: dict[str, Any] = {}
        if row["name"] != e["name"] and not row["is_owner"]:
            changes["name"] = e["name"]
        merged_aliases = sorted(set(row["aliases"]) | {a for a in aliases if a != e["name"]})
        if merged_aliases != sorted(row["aliases"]):
            changes["aliases"] = merged_aliases
        merged_ext = {**row["external_ids"], **ext}
        if merged_ext != row["external_ids"]:
            changes["external_ids"] = merged_ext
        if changes:
            K.update_entity(conn, "connector", row["id"], **changes)
        return row["id"], 0, int(bool(changes))

    def _render(self, t: dict, attrs: dict, e: dict, entity_of: dict, by_name: dict) -> tuple[str, list[dict]]:
        lines = [f"incident.io catalog · {t['name']}: {e['name']}"]
        if e.get("aliases"):
            lines.append("Aliases: " + ", ".join(e["aliases"][:40]))
        facts = []
        for aid, val in sorted((e.get("attribute_values") or {}).items(), key=lambda kv: attrs.get(kv[0], {}).get("name", "")):
            a = attrs.get(aid)
            if not a:
                continue
            name = a["name"]
            if self._excluded(t["type_name"], name):
                continue
            values = val.get("array_value") or ([val["value"]] if val.get("value") else [])
            ref = a.get("type") not in PRIMITIVES

            def target_of(v):
                if not ref:
                    return None
                return entity_of.get((v or {}).get("literal") or "") or by_name.get((a["type"], _text(v).lower()))

            def shown(v):
                # Reference: name of the synced entity + id at the source, so that changing the target changes the snapshot.
                t = target_of(v)
                literal = (v or {}).get("literal") if isinstance(v, dict) else None
                label = self._names.get(t) or _text(v)
                return f"{label} [{literal}]" if ref and literal else label

            labels = [shown(v) for v in values if _text(v)]
            if not labels:
                continue
            lines.append(f"{name}: {', '.join(labels)}")
            low = name.lower()
            if low in IDENTITY_ATTRS or low in SKIP_ATTRS or a.get("type", "").startswith("Image"):
                continue
            for v in values:
                label = _text(v)
                if not label:
                    continue
                target = target_of(v)
                if ref:
                    if target is None:
                        continue  # points to something that is not a synced entity (e.g. escalation path)
                    if target == entity_of.get(e["id"]):
                        continue  # points to itself (e.g. the SCIM group merged into the Team)
                    label = self._names.get(target, label)
                facts.append({"predicate": _slug(name), "value": label, "object_entity_id": target,
                              "statement": f"According to the incident.io catalog, {name} of {e['name']}: {label}."})
        return "\n".join(lines), facts

    def _carry_over(self, conn, stream_id: str, item: Item, entity_id: str, facts: list[dict], ev_id: str) -> list[dict]:
        """Changed entry: a fact identical to that of a current claim from the previous version now cites the new
        evidence (corroboration) instead of being retracted and reintegrated by the LLM. Only what changed goes on."""
        old = conn.execute(
            """SELECT id, content_hash FROM evidence WHERE stream_id = %s AND external_id = %s AND status = 'active'""",
            (stream_id, item.external_id)).fetchone()
        if not old or old["content_hash"] == sha(item.content):
            return facts
        current = {r["statement"]: r["id"] for r in conn.execute(
            """SELECT id, statement FROM claims WHERE %s = ANY(evidence_ids) AND subject_entity_id = %s
               AND status IN ('current', 'disputed')""", (old["id"], entity_id)).fetchall()}
        rest = []
        for f in facts:
            cid = current.get(f["statement"])
            if cid:
                events.append(conn, "claim_corroborated", "connector", {"id": cid, "evidence_ids": [ev_id]})
            else:
                rest.append(f)
        return rest

    def _fold_scim_groups(self, conn, data: dict, entity_of: dict[str, str], by_name: dict) -> None:
        owners: dict[str, list[str]] = {}
        # The reference may come by the entry's ID or by its external_id (as is the case for SCIM).
        by_external = {e["external_id"]: e["id"] for _t, (_ty, es) in data.items() for e in es if e.get("external_id")}
        for _type_id, (t, entries) in data.items():
            scim_attrs = {a["id"] for a in t["schema"]["attributes"] if a.get("type") == "SCIMGroup"}
            if not scim_attrs:
                continue
            for e in entries:
                if e.get("archived_at") or e["id"] not in entity_of:
                    continue
                for aid in scim_attrs:
                    val = (e.get("attribute_values") or {}).get(aid) or {}
                    for v in val.get("array_value") or ([val["value"]] if val.get("value") else []):
                        lit = (v or {}).get("literal")
                        lit = lit if lit in entity_of else by_external.get(lit)
                        if lit:
                            owners.setdefault(lit, []).append(entity_of[e["id"]])
        for scim_entry, teams in owners.items():
            teams = sorted(set(teams))
            group = entity_of.get(scim_entry)
            if len(teams) != 1 or not group or group == teams[0]:
                continue
            row = conn.execute("SELECT status FROM entities WHERE id = %s", (group,)).fetchone()
            if row and row["status"] == "active":
                K.merge_entities(conn, "connector", group, teams[0],
                                 "catalog: the Team points to this SCIM group (its member list)")
                # The "Team → SCIM group" claim became a relation of the Team with itself: it says nothing.
                for c in conn.execute(
                        """SELECT id FROM claims WHERE subject_entity_id = %s AND object_entity_id = %s
                           AND status IN ('current', 'disputed')""", (teams[0], teams[0])).fetchall():
                    from .. import events
                    events.append(conn, "claim_status_changed", "connector",
                                  {"id": c["id"], "to": "retracted", "reason": "Team relation to its own SCIM group"})
                    K.invalidate_claims(conn, [c["id"]])
            entity_of[scim_entry] = teams[0]
            for k, v in list(by_name.items()):  # references by name also become the Team
                if v == group:
                    by_name[k] = teams[0]

    def _excluded(self, type_name: str, attr: str) -> bool:
        rules = {r.strip().lower() for r in self.cfg.get("exclude_attributes") or []}
        return attr.lower() in rules or f"{type_name}: {attr}".lower() in rules

    def _store_evidence(self, conn, stream_id: str, item: Item, ev: str) -> str:
        from psycopg.types.json import Jsonb
        conn.execute(
            """INSERT INTO evidence (id, source_id, stream_id, external_id, author, role, occurred_at, content,
                                     content_hash, meta)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (ev, SOURCE_ID, stream_id, item.external_id, item.author, item.role, item.occurred_at, item.content,
             sha(item.content), Jsonb(item.meta)))
        return ev


def _ts(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None
