"""Closed JSON Schemas for each role's outputs."""

from ..knowledge import CLAIM_KINDS, CLAIM_STATUSES, ENTITY_TYPES, VERDICT_KINDS
from ..llm.client import schema_object


def nullable(schema: dict) -> dict:
    return {"anyOf": [schema, {"type": "null"}]}


STR = {"type": "string"}
NUM = {"type": "number"}
STRS = {"type": "array", "items": STR}

TRIAGE = schema_object({
    "durable": {"type": "boolean"},
    "reason": STR,
    "kinds": {"type": "array", "items": {"type": "string", "enum": CLAIM_KINDS}},
})

MENTION = schema_object({
    "surface": STR,
    "type_hint": {"type": "string", "enum": ENTITY_TYPES},
    "context": STR,
})

EXTRACT = schema_object({
    "claims": {"type": "array", "items": schema_object({
        "kind": {"type": "string", "enum": CLAIM_KINDS},
        "subject": MENTION,
        "predicate": STR,
        "object_value": STR,
        "object_entity": nullable(MENTION),
        "statement": STR,
        "valid_from": nullable(STR),
        "valid_to": nullable(STR),
        "evidence_refs": STRS,
        "confidence": NUM,
        "origin": {"type": "string", "enum": ["observed", "inferred"]},
    })},
})

# Each choice is a variant with its own required fields: "new" without an entity is impossible.
RESOLVE = schema_object({
    "decisions": {"type": "array", "items": {"anyOf": [
        schema_object({"ref": STR, "choice": {"type": "string", "const": "candidate"}, "entity_id": STR,
                       "confidence": NUM, "reason": STR}),
        schema_object({"ref": STR, "choice": {"type": "string", "const": "new"},
                       "new_entity": schema_object({
                           "name": STR,
                           "type": {"type": "string", "enum": ENTITY_TYPES},
                           "aliases": STRS,
                           "description": STR,
                       }),
                       "confidence": NUM, "reason": STR}),
        schema_object({"ref": STR, "choice": {"type": "string", "const": "not_entity"},
                       "confidence": NUM, "reason": STR}),
    ]}},
})

INTEGRATE = schema_object({
    "decisions": {"type": "array", "items": schema_object({
        "ref": STR,
        "decision": {"type": "string", "enum": ["new", "duplicate", "supersedes", "tension"]},
        "target_claim_ids": STRS,
        "predicate": STR,
        "reason": STR,
    })},
})

VERDICT = schema_object({
    "kind": {"type": "string", "enum": [k for k in VERDICT_KINDS if k != "human_correction"]},
    "changes": {"type": "array", "items": schema_object({
        "claim_id": STR,
        "to": {"type": "string", "enum": CLAIM_STATUSES},
        # who took the place of a "superseded" claim (the Evolution trajectory uses this link).
        "superseded_by": {"anyOf": [STR, {"type": "null"}]},
    })},
    "summary": STR,
    "rationale": STR,
    "cited_evidence": STRS,
    "confidence": NUM,
})

SYNTH_ENTITY = schema_object({
    "l0": STR,
    "l1": STR,
    "cited_claim_ids": STRS,
})

SYNTH_NARRATIVES = schema_object({
    "narratives": {"type": "array", "items": schema_object({
        "op": {"type": "string", "enum": ["create", "update", "retire"]},
        "id": nullable(STR),
        "title": STR,
        "l0": STR,
        "l1": STR,
        "member_entity_ids": STRS,
        "cited_claim_ids": STRS,
    })},
    "inferences": {"type": "array", "items": schema_object({
        "subject_entity_id": STR,
        "kind": {"type": "string", "enum": CLAIM_KINDS},
        "predicate": STR,
        "value": STR,
        "statement": STR,
        "supporting_claim_ids": STRS,
    })},
})

CORRECT = schema_object({
    "ops": {"type": "array", "items": schema_object({
        "op": {"type": "string", "enum": ["replace_claim", "retract_claim", "relink_mention",
                                          "update_entity", "merge_entities", "undo_merge",
                                          "revert_verdict", "new_claim"]},
        "claim_id": nullable(STR),
        "mention_id": nullable(STR),
        "entity_id": nullable(STR),
        "into_entity_id": nullable(STR),
        "verdict_id": nullable(STR),
        "new_entity": nullable(schema_object({"name": STR, "type": {"type": "string", "enum": ENTITY_TYPES}})),
        "name": nullable(STR),
        "type": nullable({"type": "string", "enum": ENTITY_TYPES}),
        "add_aliases": STRS,
        "kind": nullable({"type": "string", "enum": CLAIM_KINDS}),
        "predicate": nullable(STR),
        "value": nullable(STR),
        "statement": nullable(STR),
    })},
    "error_type": {"type": "string", "enum": ["entity", "claim", "verdict", "synthesis", "other"]},
    "blamed_role": {"type": "string", "enum": ["extraction", "resolution", "integration", "adjudication", "synthesis",
                                               "none"]},
    "summary": STR,
})

BRIEFING = schema_object({
    "relevant_ids": STRS,
    "verdict_ids": STRS,
})

GOLDEN_ANSWER = schema_object({"answer": STR, "used_ids": STRS})
GOLDEN_JUDGE = schema_object({"passed": {"type": "boolean"}, "rationale": STR})


REEXPLAIN_SPLIT = schema_object({"messages": {"type": "array", "items": schema_object({
    "n": {"type": "integer"},
    "asserts": {"type": "boolean"},
})}})

REEXPLAIN_JUDGE = schema_object({
    "repeats": {"type": "boolean"},
    "claim_ids": STRS,
    "reason": STR,
})
