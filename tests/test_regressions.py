"""bugs from other projects (Graphiti, gbrain, OpenViking) as regression tests.

The test LLM is fake: here the plumbing is locked down. Whether the judgment is right (the LLM deciding well) is
a matter for the golden questions."""

from pythia import knowledge as K
from pythia.daemon import bootstrap, drain
from pythia.jobs import enqueue
from pythia.pipeline import correct

from .helpers import add_unit, claim, default_handlers, integrator, mention


def test_unrelated_facts_on_the_same_attribute_do_not_supersede_each_other(svc):
    """Different invoices, paid on different days, do not supersede each other.
    No rule-based path (same subject and predicate, similar text, date) changes status: only the LLM."""
    default_handlers(svc, [
        [claim("episode", mention("Alex", "person"), "paid_invoice", "INV-1", "Alex paid invoice INV-1 on 10/01.")],
        [claim("episode", mention("Alex", "person"), "paid_invoice", "INV-2", "Alex paid invoice INV-2 on 10/03.")],
    ])
    add_unit(svc, ("user", "I paid INV-1"))
    drain(svc, lanes=("cognition",))
    add_unit(svc, ("user", "I paid INV-2"))
    drain(svc, lanes=("cognition",))
    assert [r["status"] for r in svc.db.query("SELECT status FROM claims ORDER BY recorded_at")] == ["current", "current"]
    assert svc.db.one("SELECT count(*) AS n FROM verdicts")["n"] == 0


def test_bare_first_name_offers_the_owner_among_namesakes(svc):
    """gbrain #2723: "Victor" alone always went to the contact, never to the owner. Here there are Alexandre Vale
    and Alexandre Garden; the resolver always receives the owner as a candidate and knows who the owner is."""
    with svc.db.tx() as conn:
        for name in ("Alexandre Vale", "Alexandre Garden"):
            K.create_entity(conn, "t", name=name, type="person", origin="anchored")
        owner = K.owner(conn)
    seen = []
    default_handlers(svc, [[claim("profile", mention("Alexandre", "person", "Alexandre, take a look at this, please"),
                                  "prefers", "small PRs", "Alexandre prefers small PRs.")]])
    original = svc.llm.handlers["resolution"]

    def spy(user, **kw):
        seen.append(user)
        return original(user=user, **kw)

    svc.llm.handlers["resolution"] = spy
    add_unit(svc, ("user", "Alexandre, take a look at this, please"))
    drain(svc, lanes=("cognition",))
    assert seen and f"Owner: {svc.owner_name} ({owner['id']})" in seen[0]
    assert owner["id"] in seen[0].split("candidates:")[1], "the owner is among the first name's candidates"


def test_owner_correction_survives_reprocessing_the_same_unit(svc):
    """gbrain #4696: a forgotten fact resurfaced in the next extraction. The owner's correction does not depend
    on the unit's evidence, stays current and is shown to integration as human."""
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "team X", "Atlas belongs to team X.")]] * 2)
    uid = add_unit(svc, ("user", "atlas belongs to team X"))
    drain(svc, lanes=("cognition",))
    wrong = svc.db.one("SELECT id FROM claims")["id"]
    correct.apply_ops(svc, [{"op": "replace_claim", "claim_id": wrong, "statement": "Atlas belongs to Zephyr.",
                             "value": "Zephyr"}])
    human = svc.db.one("SELECT id FROM claims WHERE origin = 'human'")["id"]
    seen = []
    base = integrator()
    svc.llm.handlers["integration"] = lambda user, **kw: (seen.append(user), base(user=user, **kw))[1]
    with svc.db.tx() as conn:  # the same unit is processed again (edit, erased context…)
        conn.execute("UPDATE units SET status = 'pending' WHERE id = %s", (uid,))
        enqueue(conn, "process_unit", "cognition", {"unit_id": uid}, dedup_key=f"unit:{uid}")
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT status FROM claims WHERE id = %s", (human,))["status"] == "current"
    assert svc.db.one("SELECT status FROM claims WHERE id = %s", (wrong,))["status"] == "superseded"
    assert seen and human in seen[0] and "human" in seen[0].split(human)[1][:80]


def test_deleting_one_source_item_regenerates_only_what_it_touched_and_restart_adds_nothing(svc):
    """Erasing something must not regenerate the whole base, and
    restarting the daemon must not requeue a regeneration per item."""
    systems = ["atlas", "nimbus", "zephyr", "orders-py"]
    default_handlers(svc, [[claim("state", mention(s, "system"), "language", "Go", f"{s} is written in Go.")]
                           for s in systems])
    for s in systems:
        add_unit(svc, ("user", f"{s} is in Go"))
    drain(svc)
    svc.db.execute("UPDATE jobs SET run_after = now() WHERE status = 'queued'")
    drain(svc)
    assert svc.db.one("SELECT count(*) AS n FROM jobs WHERE status = 'queued'")["n"] == 0
    target = svc.db.one("SELECT c.subject_entity_id, c.evidence_ids FROM claims c JOIN entities e "
                        "ON e.id = c.subject_entity_id WHERE e.name = 'atlas'")
    with svc.db.tx() as conn:
        K.tombstone_evidence(conn, "t", target["evidence_ids"], "erased at the source")
    queued = svc.db.query("SELECT kind, payload FROM jobs WHERE status = 'queued' AND kind = 'derive_entity'")
    assert [q["payload"]["entity_id"] for q in queued] == [target["subject_entity_id"]], "only the touched entity"
    before = svc.db.one("SELECT count(*) AS n FROM jobs WHERE status = 'queued'")["n"]
    bootstrap(svc, requeue=True)  # daemon restart
    assert svc.db.one("SELECT count(*) AS n FROM jobs WHERE status = 'queued'")["n"] == before


def test_restart_does_not_rewrite_the_owner_when_other_sources_added_aliases(svc):
    """Finding in every restart wrote the owner's `entity_updated` (and regenerated his page) because the
    aliases coming from the catalog are not in the settings."""
    with svc.db.tx() as conn:
        own = K.owner(conn)
        K.update_entity(conn, "catalog", own["id"], aliases=sorted(set(own["aliases"]) | {"slack:U123"}))
    bootstrap(svc, requeue=False)
    n = svc.db.one("SELECT count(*) AS n FROM events WHERE type = 'entity_updated'")["n"]
    bootstrap(svc, requeue=False)
    assert svc.db.one("SELECT count(*) AS n FROM events WHERE type = 'entity_updated'")["n"] == n
