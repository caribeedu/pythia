from pythia import events
from pythia import knowledge as K

from .conftest import owner_id

TABLES = ["entities", "mentions", "mention_link_history", "claims", "claim_status_history", "tensions", "verdicts"]
IGNORE = {"embedding", "tsv", "description"}


def snapshot(conn) -> dict:
    out = {}
    for t in TABLES:
        rows = conn.execute(f"SELECT * FROM {t}").fetchall()
        norm = sorted((tuple(sorted((k, str(v)) for k, v in r.items() if k not in IGNORE)) for r in rows))
        out[t] = norm
    return out


def test_rebuild_reproduces_projections(svc):
    own = owner_id(svc)
    with svc.db.tx() as conn:
        atlas = K.create_entity(conn, "t", name="atlas", type="system", origin="discovered")
        qp = K.create_entity(conn, "t", name="Zephyr", type="team", origin="discovered")
        m1 = K.record_mention(conn, "t", surface="atlas", context="repo", type_hint="system", entity_id=atlas,
                              confidence=0.9, evidence_id=None, unit_id=None)
        m2 = K.record_mention(conn, "t", surface="QP", context="team", type_hint="team", entity_id=own,
                              confidence=0.4, evidence_id=None, unit_id=None)
        c1 = K.assert_claim(conn, "t", kind="state", subject_mention_id=m1, subject_entity_id=atlas, predicate="owner",
                            object_value="Zephyr", object_mention_id=m2, object_entity_id=own,
                            statement="atlas belongs to Zephyr", origin="observed", evidence_ids=["ev_1"])
        c2 = K.assert_claim(conn, "t", kind="state", subject_mention_id=m1, subject_entity_id=atlas, predicate="owner",
                            object_value="Order Mgmt", statement="atlas belongs to Order Mgmt", origin="observed",
                            evidence_ids=["ev_2"])
        vid = K.apply_verdict(conn, "t", kind="temporal_supersession", decided_by="integration",
                              changes=[{"claim_id": c1, "to": "superseded"}], summary="s", rationale="r")
        K.revert_verdict(conn, "t", vid)
        K.relink_mention(conn, "t", m2, qp, "correction")
        tid = K.open_tension(conn, "t", [c1, c2], "two owners")
        before = snapshot(conn)
    with svc.db.tx() as conn:
        n = events.rebuild(conn)
        after = snapshot(conn)
    assert n > 5
    assert before == after
    with svc.db.tx() as conn:
        c = conn.execute("SELECT status, object_entity_id FROM claims WHERE id = %s", (c1,)).fetchone()
    assert c == {"status": "current", "object_entity_id": qp}


def test_as_of_history_tracks_status(svc):
    with svc.db.tx() as conn:
        e = K.create_entity(conn, "t", name="atlas", type="system", origin="discovered")
        c = K.assert_claim(conn, "t", kind="state", subject_entity_id=e, predicate="status",
                           statement="atlas is active", origin="observed")
        K.apply_verdict(conn, "t", kind="outdated_source", decided_by="adjudication",
                        changes=[{"claim_id": c, "to": "retracted"}], summary="s", rationale="r")
        hist = conn.execute("SELECT status, to_ts IS NULL AS open FROM claim_status_history WHERE claim_id = %s ORDER BY from_ts",
                            (c,)).fetchall()
    assert hist == [{"status": "current", "open": False}, {"status": "retracted", "open": True}]
