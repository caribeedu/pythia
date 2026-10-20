from __future__ import annotations

import re
from datetime import datetime, timezone

from psycopg.types.json import Jsonb

from pythia.ids import new_id, sha
from pythia.jobs import enqueue

SOURCE = "claude_code:/tmp/-repo-atlas"


def add_unit(svc, *messages: tuple[str, str], title: str = "session") -> int:
    """Creates a session unit with evidence and enqueues processing."""
    now = datetime.now(timezone.utc)
    with svc.db.tx() as conn:
        conn.execute("""INSERT INTO sources (id, kind, name, config) VALUES (%s, 'claude_code', 'atlas', %s)
                        ON CONFLICT DO NOTHING""", (SOURCE, Jsonb({"path": "/tmp/-repo-atlas"})))
        unit = conn.execute("""INSERT INTO units (source_id, kind, title, occurred_at, meta)
                               VALUES (%s, 'session_delta', %s, %s, %s) RETURNING id""",
                            (SOURCE, title, now, Jsonb({"cwd": "/repo/atlas"}))).fetchone()
        for role, text in messages:
            conn.execute("""INSERT INTO evidence (id, unit_id, source_id, external_id, author, role, occurred_at, content, content_hash)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                         (new_id("ev"), unit["id"], SOURCE, new_id("x"), "Alex" if role == "user" else "assistant",
                          role, now, text, sha(text)))
        enqueue(conn, "process_unit", "cognition", {"unit_id": unit["id"]}, dedup_key=f"unit:{unit['id']}")
    return unit["id"]


def mention(surface, type_hint="person", context="said in the session"):
    return {"surface": surface, "type_hint": type_hint, "context": context}


def claim(kind, subject, predicate, value, statement, obj=None, refs=("E1",), origin="observed"):
    return {"kind": kind, "subject": subject, "predicate": predicate, "object_value": value, "object_entity": obj,
            "statement": statement, "valid_from": None, "valid_to": None, "evidence_refs": list(refs),
            "confidence": 0.9, "origin": origin}


def refs_in(user: str) -> list[str]:
    return re.findall(r"^(M\d+): \"([^\"]+)\"", user, re.M)


def resolver(owner_surfaces=("Alex",)):
    """Resolves: owner by name; existing entity if it appears as a candidate with the same name; otherwise new."""
    def handler(user, **_):
        decisions = []
        blocks = user.split("\n\n")
        for block in blocks:
            m = re.match(r"(M\d+): \"([^\"]+)\" \(suggested type: (\w+)\)", block)
            if not m:
                continue
            ref, surface, etype = m.groups()
            cands = re.findall(r"- (en_[0-9a-f]+) · ([^(]+) \((\w+)\)", block)
            match = next((c[0] for c in cands if c[1].strip().lower() == surface.lower()), None)
            if surface in owner_surfaces:
                match = next((c[0] for c in cands if c[1].strip() == "Alex"), match)
            if surface.startswith("fixture:"):
                decisions.append({"ref": ref, "choice": "not_entity", "entity_id": None, "new_entity": None,
                                  "confidence": 0.9, "reason": "test value"})
            elif match:
                decisions.append({"ref": ref, "choice": "candidate", "entity_id": match, "new_entity": None,
                                  "confidence": 0.9, "reason": "same name"})
            else:
                decisions.append({"ref": ref, "choice": "new", "entity_id": None,
                                  "new_entity": {"name": surface, "type": etype, "aliases": [], "description": f"{surface} ({etype})"},
                                  "confidence": 0.8, "reason": "new"})
        return {"decisions": decisions}
    return handler


def integrator(rule=None):
    """By default everything is new; `rule(ref, line, existing_ids)` may return another decision."""
    def handler(user, **_):
        existing = re.findall(r"(cl_[0-9a-f]+) \(", user.split("NEW CLAIMS")[0])
        out = []
        for m in re.finditer(r"^(N\d+) \(([^)]*)\): (.*)$", user.split("NEW CLAIMS")[1], re.M):
            ref, meta, statement = m.groups()
            pred = re.search(r"predicate (\w+)", meta).group(1)
            decision = {"ref": ref, "decision": "new", "target_claim_ids": [], "predicate": pred, "reason": "ok"}
            if rule:
                decision.update(rule(ref, statement, existing) or {})
            out.append(decision)
        return {"decisions": out}
    return handler


def synth_entity(user, **_):
    ids = re.findall(r"^- (cl_[0-9a-f]+)", user, re.M)
    name = re.search(r"Entity en_[0-9a-f]+: ([^(]+)\(", user).group(1).strip()
    return {"l0": f"L0 of {name}", "l1": f"Page of {name} " + " ".join(f"[{i}]" for i in ids), "cited_claim_ids": ids}


def default_handlers(svc, extract):
    svc.llm.handlers.update({
        "triage": lambda **_: {"durable": True, "reason": "has knowledge", "kinds": ["profile"]},
        "extraction": lambda **_: {"claims": extract.pop(0) if extract else []},
        "resolution": resolver(),
        "integration": integrator(),
        "synthesis": lambda user, **kw: synth_entity(user) if user.startswith("Entity") else {"narratives": [], "inferences": []},
    })
