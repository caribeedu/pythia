"""backfill with the Batch API and deferred synthesis."""

from pythia import knowledge as K
from pythia.daemon import drain
from pythia.pipeline import batch

from .helpers import SOURCE, add_unit, claim, default_handlers, mention


def start_backfill(svc):
    """The source was 'added' 1h from now: everything that exists now is backfill."""
    svc.db.execute("UPDATE sources SET backfill_until = now() + interval '1 hour', backfill_done_at = NULL WHERE id = %s",
                   (SOURCE,))


def run_batches(svc):
    batch.submit_pending(svc, min_age_s=0)
    batch.poll(svc)


def extraction():
    return [[claim("profile", mention("Alex"), "prefers_writing_style", "short sentences", "Alex prefers short sentences."),
             claim("state", mention("atlas", "system"), "owner", "Zephyr", "atlas belongs to Zephyr.",
                   obj=mention("Zephyr", "team"))]]


def test_backfill_unit_uses_batch_for_triage_and_extraction_and_defers_synthesis(svc):
    default_handlers(svc, extraction())
    uid = add_unit(svc, ("user", "shorten this; atlas belongs to Zephyr"))
    start_backfill(svc)

    drain(svc, lanes=("cognition",))
    reqs = svc.db.query("SELECT kind, status FROM batch_requests")
    assert reqs == [{"kind": "triage", "status": "pending"}]
    assert not any(r == "triage" for r, _ in svc.llm.calls), "triage was not online"
    assert svc.db.one("SELECT status FROM units WHERE id = %s", (uid,))["status"] == "pending"

    run_batches(svc)
    assert svc.db.one("SELECT params FROM batch_requests WHERE kind = 'triage'")["params"] is None, \
        "the request (with source content) leaves the database when the result arrives"
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT status FROM batch_requests WHERE kind = 'extraction'")["status"] == "pending"

    run_batches(svc)
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT status FROM units WHERE id = %s", (uid,))["status"] == "processed"
    assert svc.db.one("SELECT count(*) AS n FROM claims")["n"] == 2
    assert {r for r, _ in svc.llm.calls} >= {"batch:triage", "batch:extraction", "resolution", "integration"}
    # Deferred synthesis: no regeneration during the backfill.
    assert svc.db.one("SELECT count(*) AS n FROM jobs WHERE kind IN ('derive_entity', 'derive_narratives') AND status = 'queued'")["n"] == 0
    deferred = {r["entity_id"] for r in svc.db.query("SELECT entity_id FROM deferred_derivations")}
    assert len(deferred) >= 2

    # Batch cost with a 50% discount.
    calls = svc.db.query("SELECT role, batch, cost_usd FROM llm_calls WHERE role IN ('triage', 'extraction')")
    assert calls and all(c["batch"] for c in calls)
    online = svc.db.one("SELECT cost_usd FROM llm_calls WHERE role = 'integration' LIMIT 1")["cost_usd"]
    tri = next(c for c in calls if c["role"] == "triage")["cost_usd"]
    assert float(tri) < float(online)  # haiku in batch < sonnet online

    # End of the backfill: one batch synthesis pass, then narratives.
    batch.mark_scanned(svc, SOURCE)
    svc.db.execute("UPDATE sources SET backfill_until = now() - interval '1 second' WHERE id = %s", (SOURCE,))
    assert batch.check_backfills(svc) == [SOURCE]
    drain(svc, lanes=("derivation",))
    assert svc.db.one("SELECT count(*) AS n FROM batch_requests WHERE kind = 'synthesis' AND status = 'pending'")["n"] == len(deferred)
    assert svc.db.one("SELECT count(*) AS n FROM deferred_derivations")["n"] == 0
    run_batches(svc)
    svc.db.execute("UPDATE jobs SET run_after = now() WHERE status = 'queued'")
    drain(svc, lanes=("derivation",))
    pages = {r["title"] for r in svc.db.query("SELECT title FROM views WHERE kind = 'entity' AND status = 'active'")}
    assert {"Alex", "atlas"} <= pages
    assert any(r == "batch:synthesis" for r, _ in svc.llm.calls)
    assert not any(r == "synthesis" and "Entity" in u for r, u in svc.llm.calls), "entity synthesis was not online"


def test_live_unit_after_backfill_start_is_processed_immediately(svc):
    default_handlers(svc, extraction())
    add_unit(svc, ("user", "something"))
    svc.db.execute("UPDATE sources SET backfill_until = now() - interval '1 hour', backfill_done_at = NULL WHERE id = %s",
                   (SOURCE,))
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT count(*) AS n FROM batch_requests")["n"] == 0
    assert any(r == "triage" for r, _ in svc.llm.calls)
    assert svc.db.one("SELECT count(*) AS n FROM jobs WHERE kind = 'derive_entity'")["n"] >= 1


def test_invalid_batch_result_is_retried_then_fails(svc):
    default_handlers(svc, [])
    svc.llm.handlers["extraction"] = lambda **_: {"claims": [claim("state", mention("atlas", "system"), "x", "y",
                                                                  "atlas y", refs=("E9",))]}
    uid = add_unit(svc, ("user", "atlas"))
    start_backfill(svc)
    for _ in range(12):
        drain(svc, lanes=("cognition",))
        run_batches(svc)
        svc.db.execute("UPDATE jobs SET run_after = now() WHERE status = 'queued'")
    drain(svc, lanes=("cognition",))
    attempts = [r["attempts"] for r in svc.db.query("SELECT attempts FROM batch_requests WHERE kind = 'extraction' ORDER BY attempts")]
    assert attempts == [1, 2, 3]
    assert svc.db.one("SELECT status FROM units WHERE id = %s", (uid,))["status"] == "failed"


def test_deletion_cancels_pending_batch_and_restarts_stages(svc):
    default_handlers(svc, extraction())
    uid = add_unit(svc, ("user", "atlas belongs to Zephyr"), ("user", "and shorten this"))
    start_backfill(svc)
    drain(svc, lanes=("cognition",))
    run_batches(svc)                      # triage ready
    drain(svc, lanes=("cognition",))      # pending extraction
    batch.submit_pending(svc, min_age_s=0)  # extraction sent
    ev = svc.db.query("SELECT id FROM evidence WHERE unit_id = %s ORDER BY seq", (uid,))[0]["id"]
    with svc.db.tx() as conn:
        K.tombstone_evidence(conn, "connector", [ev], "erased at the source")
    assert svc.db.one("SELECT status FROM batch_requests WHERE kind = 'extraction'")["status"] == "canceled"
    batch.poll(svc)                       # the result arrives, but is ignored
    assert svc.db.one("SELECT result FROM batch_requests WHERE kind = 'extraction'")["result"] is None
    unit = svc.db.one("SELECT meta, triage FROM units WHERE id = %s", (uid,))
    assert "pipeline" not in unit["meta"] and unit["triage"] is None


def test_budget_halts_batch_submission(svc):
    from pythia.llm.client import BudgetExceeded
    import pytest
    default_handlers(svc, extraction())
    add_unit(svc, ("user", "x"))
    start_backfill(svc)
    drain(svc, lanes=("cognition",))
    svc.settings.set("monthly_cap_usd", 0.0)
    with pytest.raises(BudgetExceeded):
        batch.submit_pending(svc, min_age_s=0)
    assert svc.db.one("SELECT status FROM batch_requests")["status"] == "pending"


def test_sweep_and_online_derive_respect_deferred_backfill_entities(svc):
    from pythia.pipeline import derive
    default_handlers(svc, extraction())
    add_unit(svc, ("user", "shorten this; atlas belongs to Zephyr"))
    start_backfill(svc)
    for _ in range(2):
        drain(svc, lanes=("cognition",))
        run_batches(svc)
    drain(svc, lanes=("cognition",))
    deferred = [r["entity_id"] for r in svc.db.query("SELECT entity_id FROM deferred_derivations")]
    assert deferred
    assert derive.sweep(svc) == 0, "the periodic sweep does not schedule deferred entities"
    assert derive.derive_entity(svc, deferred[0]) == {"skipped": "synthesis deferred by the backfill"}
    assert not any(r == "synthesis" for r, _ in svc.llm.calls)


def test_sweep_and_online_derive_skip_entities_waiting_for_batch_synthesis(svc):
    """After the deferred synthesis becomes a batch request, the entity leaves deferred_derivations; until the batch
    returns, neither the sweep nor online derivation may regenerate it."""
    from psycopg.types.json import Jsonb
    from pythia.pipeline import derive
    default_handlers(svc, extraction())
    add_unit(svc, ("user", "shorten this; atlas belongs to Zephyr"))
    drain(svc, lanes=("cognition",))
    eid = svc.db.one("SELECT id FROM entities WHERE name = 'atlas'")["id"]
    svc.db.execute("UPDATE views SET stale = true WHERE entity_id = %s", (eid,))
    svc.db.execute("""INSERT INTO batch_requests (custom_id, kind, role, entity_id, source_id, params, meta, status)
                      VALUES ('br_t', 'synthesis', 'synthesis', %s, NULL, %s, '{}', 'submitted')""", (eid, Jsonb({})))
    svc.db.execute("DELETE FROM jobs WHERE kind = 'derive_entity'")
    derive.sweep(svc)
    queued = [r["payload"]["entity_id"] for r in svc.db.query("SELECT payload FROM jobs WHERE kind = 'derive_entity'")]
    assert eid not in queued
    assert derive.derive_entity(svc, eid) == {"skipped": "batch synthesis in progress"}
