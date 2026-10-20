import re
import time

from pythia import consumer
from pythia import knowledge as K
from pythia.daemon import drain

from .conftest import owner_id
from .helpers import add_unit, claim, default_handlers, integrator, mention


def test_unit_to_claims_pages_and_narrative(svc):
    extract = [[
        claim("profile", mention("Alex"), "prefers_writing_style", "short sentences",
              "Alex prefers short sentences with no fluff."),
        claim("state", mention("atlas", "system", "repository Alex works on"), "owner", "Zephyr",
              "The atlas system belongs to the Zephyr team.", obj=mention("Zephyr", "team", "Alex's team")),
        claim("state", mention("fixture:john", "person", "john inside a test JSON"), "email", "john@x",
              "john has email john@x"),
    ]]
    default_handlers(svc, extract)
    narratives_seen = []

    def synth(user, **_):
        if user.startswith("Entity"):
            from .helpers import synth_entity
            return synth_entity(user)
        narratives_seen.append(user)
        own = re.search(r"(en_[0-9a-f]+) · Alex", user).group(1)
        ids = re.findall(r"(cl_[0-9a-f]+) \(profile", user)
        return {"narratives": [{"op": "create", "id": None, "title": "How Alex writes", "l0": "Alex writes short.",
                                "l1": f"Short and direct [{ids[0]}]", "member_entity_ids": [own], "cited_claim_ids": ids}],
                "inferences": [{"subject_entity_id": own, "kind": "profile", "predicate": "avoids",
                                 "value": "wordiness", "statement": "Alex avoids wordy texts.",
                                 "supporting_claim_ids": ids}]}

    svc.llm.handlers["synthesis"] = synth
    unit_id = add_unit(svc, ("user", "shorten this, it is wordy. Atlas belongs to Zephyr."), ("assistant", "Ok."))
    drain(svc, lanes=("cognition", "derivation"))
    # derive_narratives has a 60s delay: bring it forward for the test.
    svc.db.execute("UPDATE jobs SET run_after = now() WHERE status = 'queued'")
    drain(svc)

    assert svc.db.one("SELECT status FROM units WHERE id = %s", (unit_id,))["status"] == "processed"
    claims = svc.db.query("SELECT statement, origin, status, subject_entity_id FROM claims ORDER BY recorded_at")
    statements = [c["statement"] for c in claims]
    assert "Alex prefers short sentences with no fluff." in statements
    assert "The atlas system belongs to the Zephyr team." in statements
    assert not any("john" in s for s in statements), "a fixture is not an entity → does not become a claim"
    assert any(c["origin"] == "inferred" for c in claims), "the narrative's inference became a claim"

    names = {r["name"] for r in svc.db.query("SELECT name FROM entities")}
    assert {"Alex", "atlas", "Zephyr"} <= names
    assert "fixture:john" not in names

    pages = svc.db.query("SELECT title, l0, cited_claim_ids FROM views WHERE kind = 'entity' AND status = 'active'")
    assert {p["title"] for p in pages} >= {"Alex", "atlas"}
    narr = svc.db.one("SELECT * FROM views WHERE kind = 'narrative'")
    assert narr["title"] == "How Alex writes" and narr["cited_claim_ids"]

    # Zoom: search → open levels 0..3 → timeline.
    found = consumer.search(svc, "how Alex writes short sentences")
    assert narr["id"] in found or "Alex" in found
    assert "Alex writes short." in consumer.open_item(svc, narr["id"], 0)
    assert "Short and direct" in consumer.open_item(svc, narr["id"], 1)
    lvl2 = consumer.open_item(svc, narr["id"], 2)
    assert "Alex prefers short sentences" in lvl2
    assert "shorten this" in consumer.open_item(svc, narr["id"], 3)
    own = owner_id(svc)
    assert "current" in consumer.timeline(svc, own)

    # The ledger recorded cost per role.
    roles = {r["role"] for r in svc.db.query("SELECT DISTINCT role FROM llm_calls")}
    assert {"triage", "extraction", "resolution", "integration", "synthesis"} <= roles


def test_triage_discards_noise(svc):
    default_handlers(svc, [])
    svc.llm.handlers["triage"] = lambda **_: {"durable": False, "reason": "greeting", "kinds": []}
    uid = add_unit(svc, ("user", "good morning"))
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT status FROM units WHERE id = %s", (uid,))["status"] == "triaged_out"
    assert not any(r == "extraction" for r, _ in svc.llm.calls)


def test_supersession_creates_reversible_verdict(svc):
    extract = [
        [claim("state", mention("atlas", "system"), "owner", "Order Mgmt", "atlas belongs to Order Mgmt.")],
        [claim("state", mention("atlas", "system"), "owner", "Zephyr", "atlas now belongs to Zephyr.")],
    ]
    default_handlers(svc, extract)
    add_unit(svc, ("user", "atlas belongs to Order Mgmt"))
    drain(svc, lanes=("cognition",))
    svc.llm.handlers["integration"] = integrator(
        lambda ref, st, existing: {"decision": "supersedes", "target_claim_ids": existing[:1], "reason": "transfer"})
    add_unit(svc, ("user", "atlas moved to Zephyr"))
    drain(svc, lanes=("cognition",))
    old = svc.db.one("SELECT id, status FROM claims WHERE statement LIKE 'atlas belongs%%'")
    new = svc.db.one("SELECT id, status FROM claims WHERE statement LIKE 'atlas now%%'")
    assert old["status"] == "superseded" and new["status"] == "current"
    v = svc.db.one("SELECT * FROM verdicts")
    assert v["kind"] == "temporal_supersession" and v["decided_by"] == "integration"
    with svc.db.tx() as conn:
        assert K.revert_verdict(conn, K.HUMAN, v["id"])
    assert svc.db.one("SELECT status FROM claims WHERE id = %s", (old["id"],))["status"] == "current"
    assert svc.db.one("SELECT status FROM verdicts WHERE id = %s", (v["id"],))["status"] == "reverted"


def test_tension_goes_to_adjudicator_agent_with_tools(svc):
    extract = [
        [claim("decision", mention("atlas", "system"), "database", "Postgres", "atlas uses Postgres.")],
        [claim("decision", mention("atlas", "system"), "database", "MySQL", "atlas uses MySQL.")],
    ]
    default_handlers(svc, extract)
    add_unit(svc, ("user", "we decided on Postgres for atlas"))
    drain(svc, lanes=("cognition",))
    svc.llm.handlers["integration"] = integrator(
        lambda ref, st, existing: {"decision": "tension", "target_claim_ids": existing[:1], "reason": "different databases"})
    used_tools = []

    def adjudicator(user, tools, **_):
        ids = re.findall(r"(cl_[0-9a-f]+) \(decision", user)
        pg = next(i for i in ids if "Postgres" in tools["view_claim"].run({"claim_id": i})["claim"])
        my = next(i for i in ids if i != pg)
        ev = tools["view_claim"].run({"claim_id": my})["evidence"][0]
        used_tools.append(tools["view_evidence"].run({"evidence_id": ev}))
        used_tools.append(tools["search"].run({"query": "atlas database"}))
        return {"kind": "real_conflict", "changes": [{"claim_id": pg, "to": "current"}, {"claim_id": my, "to": "disputed"}],
                "summary": "atlas uses Postgres", "rationale": "an explicit decision is older but stronger",
                "cited_evidence": [ev], "confidence": 0.55}

    svc.llm.handlers["adjudication"] = adjudicator
    add_unit(svc, ("user", "I think atlas uses MySQL"))
    drain(svc, lanes=("cognition",))
    t = svc.db.one("SELECT * FROM tensions")
    v = svc.db.one("SELECT * FROM verdicts WHERE decided_by = 'adjudication'")
    assert t["status"] == "resolved" and t["verdict_id"] == v["id"]
    assert v["kind"] == "real_conflict" and v["dossier_hash"]
    assert svc.db.one("SELECT status FROM claims WHERE statement = 'atlas uses MySQL.'")["status"] == "disputed"
    assert used_tools and used_tools[0]["evidence"]["content"] == "I think atlas uses MySQL"

    # Reverting reopens the tension, but the same dossier is not judged again.
    with svc.db.tx() as conn:
        K.revert_verdict(conn, K.HUMAN, v["id"])
        from pythia.jobs import enqueue
        enqueue(conn, "adjudicate", "cognition", {"tension_id": t["id"]})
    before = len([r for r, _ in svc.llm.calls if r == "adjudication"])
    drain(svc, lanes=("cognition",))
    after = len([r for r, _ in svc.llm.calls if r == "adjudication"])
    assert before == after


def test_relink_cascades_only_dependents_without_reextraction(svc):
    extract = [[claim("state", mention("QP", "team"), "owner_of", "atlas", "QP owns atlas.",
                      obj=mention("atlas", "system"))]]
    default_handlers(svc, extract)
    add_unit(svc, ("user", "QP owns atlas"))
    drain(svc)
    svc.db.execute("UPDATE jobs SET run_after = now() WHERE status = 'queued'")
    drain(svc)
    qp = svc.db.one("SELECT id FROM entities WHERE name = 'QP'")["id"]
    c = svc.db.one("SELECT * FROM claims")
    extractions = len([r for r, _ in svc.llm.calls if r == "extraction"])
    with svc.db.tx() as conn:
        zephyr = K.create_entity(conn, K.HUMAN, name="Zephyr", type="team", origin="anchored")
        K.relink_mention(conn, K.HUMAN, c["subject_mention_id"], zephyr, "QP = Zephyr")
        derive_jobs = conn.execute("SELECT payload->>'entity_id' AS e FROM jobs WHERE kind = 'derive_entity' AND status = 'queued'").fetchall()
    assert {r["e"] for r in derive_jobs} >= {qp, zephyr}
    svc.db.execute("UPDATE jobs SET run_after = now() WHERE status = 'queued'")
    drain(svc)
    assert svc.db.one("SELECT subject_entity_id FROM claims")["subject_entity_id"] == zephyr
    assert len([r for r, _ in svc.llm.calls if r == "extraction"]) == extractions
    assert svc.db.one("SELECT status FROM views WHERE entity_id = %s", (qp,))["status"] == "retired"
    assert svc.db.one("SELECT l0 FROM views WHERE entity_id = %s", (zephyr,))["l0"] == "L0 of Zephyr"


def test_tombstone_retracts_claims_supported_only_by_deleted_evidence(svc):
    extract = [[claim("state", mention("atlas", "system"), "color", "green", "atlas's sky is green")]]
    default_handlers(svc, extract)
    add_unit(svc, ("user", "the sky is green"))
    drain(svc, lanes=("cognition",))
    ev = svc.db.one("SELECT id FROM evidence")["id"]
    with svc.db.tx() as conn:
        stats = K.tombstone_evidence(conn, "connector", [ev], "edited at the source")
    assert stats == {"evidence": 1, "claims_retracted": 1}
    assert svc.db.one("SELECT status FROM claims")["status"] == "retracted"
    assert svc.db.one("SELECT content FROM evidence")["content"] is None


def test_missing_llm_decision_is_an_error_not_a_default(svc):
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "Zephyr", "atlas belongs to Zephyr.")]])
    svc.llm.handlers["integration"] = lambda **_: {"decisions": []}
    uid = add_unit(svc, ("user", "atlas belongs to Zephyr"))
    drain(svc, lanes=("cognition",))
    job = svc.db.one("SELECT * FROM jobs WHERE kind = 'process_unit'")
    assert job["status"] == "queued" and "did not decide N1" in job["last_error"] and job["attempts"] == 1
    assert svc.db.one("SELECT count(*) AS n FROM claims")["n"] == 0
    assert svc.db.one("SELECT status FROM units WHERE id = %s", (uid,))["status"] == "pending"


def test_glossary_feeds_entity_candidates(svc):
    default_handlers(svc, [
        [claim("glossary", mention("ZEPH", "concept"), "means", "Zephyr", "ZEPH is the acronym of the Zephyr team.",
               obj=mention("Zephyr", "team"))],
        [claim("state", mention("ZEPH", "team"), "owner_of", "atlas", "ZEPH takes care of atlas.")],
    ])
    add_unit(svc, ("user", "ZEPH = Zephyr"))
    drain(svc, lanes=("cognition",))
    add_unit(svc, ("user", "ZEPH takes care of atlas"))
    drain(svc, lanes=("cognition",))
    resolution_prompts = [u for r, u in svc.llm.calls if r == "resolution"]
    last = resolution_prompts[-1]
    assert "glossary: ZEPH is the acronym of the Zephyr team." in last
    assert "· Zephyr (team)" in last


def test_corrected_verdicts_become_examples_and_exploration_is_recorded(svc):
    extract = [
        [claim("decision", mention("atlas", "system"), "queue", "NSQ", "atlas uses NSQ.")],
        [claim("decision", mention("atlas", "system"), "queue", "Kafka", "atlas uses Kafka.")],
        [claim("decision", mention("atlas", "system"), "queue", "SQS", "atlas uses SQS.")],
    ]
    default_handlers(svc, extract)
    add_unit(svc, ("user", "atlas usa NSQ"))
    drain(svc, lanes=("cognition",))
    svc.llm.handlers["integration"] = integrator(
        lambda ref, st, existing: {"decision": "tension", "target_claim_ids": existing[:1], "reason": "different queues"})

    def adjudicator(user, tools, **_):
        ids = re.findall(r"(cl_[0-9a-f]+) \(decision", user)
        tools["search"].run({"query": "queues used by atlas"})
        return {"kind": "temporal_supersession", "changes": [{"claim_id": ids[0], "to": "superseded"}],
                "summary": "queue changed", "rationale": "more recent wins", "cited_evidence": [], "confidence": 0.7}

    svc.llm.handlers["adjudication"] = adjudicator
    add_unit(svc, ("user", "atlas usa Kafka"))
    drain(svc, lanes=("cognition",))
    v = svc.db.one("SELECT * FROM verdicts WHERE decided_by = 'adjudication'")
    assert v["exploration"] == [{"tool": "search", "input": {"query": "queues used by atlas"}}]
    from pythia.pipeline import correct
    correct.request_correction(svc, v["id"], "queue decisions only change with an ADR", via="web")
    svc.llm.handlers["correction"] = lambda **_: {"ops": [{"op": "revert_verdict", "claim_id": None, "mention_id": None,
        "entity_id": None, "into_entity_id": None, "verdict_id": v["id"], "new_entity": None, "name": None, "type": None,
        "add_aliases": [], "kind": None, "predicate": None, "value": None, "statement": None}],
        "error_type": "verdict", "blamed_role": "adjudication", "summary": "reverter"}
    drain(svc, lanes=("cognition",))
    add_unit(svc, ("user", "atlas usa SQS"))
    drain(svc, lanes=("cognition",))
    last = [u for r, u in svc.llm.calls if r == "adjudication"][-1]
    assert "PREVIOUS JUDGMENTS THAT ALEX CORRECTED" in last and "queue decisions only change with an ADR" in last


def test_resolution_accepts_entity_shown_for_another_mention_but_not_unseen_ids(svc):
    with svc.db.tx() as conn:
        atlas = K.create_entity(conn, "t", name="atlas", type="system", origin="anchored")
    default_handlers(svc, [[claim("state", mention("the Go backend", "system"), "language", "Go", "The backend is in Go."),
                            claim("state", mention("atlas", "system"), "owner", "Zephyr", "atlas belongs to Zephyr.")]])

    def resolve(user, **_):
        refs = re.findall(r"^(M\d+): \"([^\"]+)\"", user, re.M)
        return {"decisions": [{"ref": r, "choice": "candidate", "entity_id": atlas, "confidence": 0.8, "reason": "same system"}
                              for r, _ in refs]}

    svc.llm.handlers["resolution"] = resolve
    uid = add_unit(svc, ("user", "the Go backend is atlas, owned by Zephyr"))
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT status FROM units WHERE id = %s", (uid,))["status"] == "processed"
    assert {r["subject_entity_id"] for r in svc.db.query("SELECT subject_entity_id FROM claims")} == {atlas}
    svc.llm.handlers["resolution"] = lambda user, **_: {"decisions": [
        {"ref": r, "choice": "candidate", "entity_id": "en_unseen", "confidence": 0.8, "reason": "x"}
        for r, _ in re.findall(r"^(M\d+): \"([^\"]+)\"", user, re.M)]}
    svc.llm.handlers["extraction"] = lambda **_: {"claims": [claim("state", mention("fraud", "system"), "x", "y", "fraud y")]}
    uid2 = add_unit(svc, ("user", "fraud"))
    drain(svc, lanes=("cognition",))
    assert "nonexistent candidate" in svc.db.one("SELECT last_error FROM jobs WHERE payload->>'unit_id' = %s", (str(uid2),))["last_error"]


def test_integration_targets_may_be_other_new_claims_of_the_same_unit(svc):
    """Regression (Confluence unit 536): integration pointed the tension at another new claim (N1)."""
    extract = [[
        claim("decision", mention("atlas", "system"), "database", "Postgres", "atlas uses Postgres."),
        claim("decision", mention("atlas", "system"), "database", "MySQL", "atlas uses MySQL."),
        claim("decision", mention("atlas", "system"), "database", "Postgres", "Atlas runs on Postgres."),
    ]]
    default_handlers(svc, extract)
    rules = {"N2": {"decision": "tension", "target_claim_ids": ["N1"], "reason": "different databases"},
             "N3": {"decision": "duplicate", "target_claim_ids": ["N1"], "reason": "same thing"}}
    svc.llm.handlers["integration"] = integrator(lambda ref, st, existing: rules.get(ref))
    svc.llm.handlers["adjudication"] = lambda **_: {"kind": "different_scope", "changes": [], "summary": "s",
                                                   "rationale": "r", "cited_evidence": [], "confidence": 0.5}
    add_unit(svc, ("user", "atlas: Postgres or MySQL?"))
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT count(*) AS n FROM jobs WHERE status = 'failed'")["n"] == 0
    pg = svc.db.one("SELECT id, evidence_ids FROM claims WHERE statement = 'atlas uses Postgres.'")
    my = svc.db.one("SELECT id FROM claims WHERE statement = 'atlas uses MySQL.'")["id"]
    assert svc.db.one("SELECT count(*) AS n FROM claims WHERE statement = 'Atlas runs on Postgres.'")["n"] == 0
    assert sorted(svc.db.one("SELECT claim_ids FROM tensions")["claim_ids"]) == sorted([my, pg["id"]])


def test_tension_without_target_is_opened_on_the_new_claim_alone(svc):
    """Regression (unit 536): 'tension' with empty target_claim_ids brought down the whole unit."""
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "team X", "Atlas belongs to team X.")]])
    svc.llm.handlers["integration"] = integrator(
        lambda ref, st, existing: {"decision": "tension", "target_claim_ids": [], "reason": "looks outdated"})
    svc.llm.handlers["adjudication"] = lambda **_: {"kind": "different_scope", "changes": [], "summary": "s",
                                                   "rationale": "r", "cited_evidence": [], "confidence": 0.5}
    add_unit(svc, ("user", "atlas belongs to team X"))
    drain(svc, lanes=("cognition",))
    cid = svc.db.one("SELECT id FROM claims WHERE predicate = 'owner'")["id"]
    assert svc.db.one("SELECT claim_ids FROM tensions")["claim_ids"] == [cid]


def test_adjudication_cache_keys_on_model_and_prompt_version_but_owner_revert_holds(svc, monkeypatch):
    """the same dossier with another prompt/model is judged again; what the owner reverted is not."""
    from pythia.jobs import enqueue
    from pythia.pipeline import prompts
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "team X", "Atlas belongs to team X.")]])
    add_unit(svc, ("user", "atlas belongs to team X"))
    drain(svc, lanes=("cognition",))
    cid = svc.db.one("SELECT id FROM claims")["id"]
    svc.llm.handlers["adjudication"] = lambda **_: {"kind": "different_scope", "changes": [], "summary": "s",
                                                   "rationale": "r", "cited_evidence": [], "confidence": 0.5}
    with svc.db.tx() as conn:
        tid = K.open_tension(conn, "integration", [cid], "odd")
    drain(svc, lanes=("cognition",))
    judged = lambda: len([r for r, _ in svc.llm.calls if r == "adjudication"])
    assert judged() == 1

    def rejudge():
        with svc.db.tx() as conn:
            conn.execute("UPDATE tensions SET status = 'open' WHERE id = %s", (tid,))
            enqueue(conn, "adjudicate", "cognition", {"tension_id": tid})
        drain(svc, lanes=("cognition",))

    rejudge()
    assert judged() == 1, "same dossier, same model and prompt: cache"
    monkeypatch.setattr(prompts, "VERSION", prompts.VERSION + "-new")
    rejudge()
    assert judged() == 2, "new prompt: judges again"
    v = svc.db.one("SELECT id FROM verdicts WHERE tension_id = %s ORDER BY created_at DESC LIMIT 1", (tid,))
    with svc.db.tx() as conn:
        K.revert_verdict(conn, K.HUMAN, v["id"])
    monkeypatch.setattr(prompts, "VERSION", prompts.VERSION + "-other")
    rejudge()
    assert judged() == 2, "reverted by the owner holds until the dossier changes"


def test_verdict_is_not_applied_when_the_dossier_changed_during_judgement(svc):
    """the picture changed while the agent was judging → does not apply, judges again."""
    from pythia.pipeline.adjudicate import dossier
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "team X", "Atlas belongs to team X.")]])
    add_unit(svc, ("user", "atlas belongs to team X"))
    drain(svc, lanes=("cognition",))
    c = svc.db.one("SELECT id, subject_entity_id, evidence_ids FROM claims")
    with svc.db.tx() as conn:
        tid = K.open_tension(conn, "integration", [c["id"]], "odd")
    calls = []

    def judge(**_):
        calls.append(1)
        if len(calls) == 1:  # during the first judgment, another claim about the same entity arrives
            with svc.db.tx() as conn:
                K.assert_claim(conn, K.HUMAN, kind="state", subject_entity_id=c["subject_entity_id"],
                               predicate="owner", object_value="Zephyr", statement="Atlas belongs to Zephyr.",
                               origin="human", source_id=None, confidence=1.0, evidence_ids=c["evidence_ids"])
        return {"kind": "different_scope", "changes": [], "summary": "s", "rationale": "r",
                "cited_evidence": [], "confidence": 0.5}

    svc.llm.handlers["adjudication"] = judge
    drain(svc, lanes=("cognition",))
    verdicts = svc.db.query("SELECT dossier_hash FROM verdicts WHERE tension_id = %s", (tid,))
    with svc.db.tx() as conn:
        current = dossier(conn, conn.execute("SELECT * FROM tensions WHERE id = %s", (tid,)).fetchone())[1]
    assert len(calls) == 2, "judged again"
    assert [v["dossier_hash"] for v in verdicts] == [current], "only the judgment over the current picture was applied"


def test_chat_delta_gets_the_previous_assistant_reply_as_context(svc):
    """the owner's short reaction ("shorten this") reaches extraction with the reply he reacts to, as
    context that cannot be cited."""
    from pythia.connectors.claude_code import unit_context
    from pythia.pipeline.process import render_unit
    seen = []
    default_handlers(svc, [[], [claim("profile", mention("Alex"), "prefers_replies", "short",
                                       "Alex asked \"shorten this\": prefers short replies.")]])
    base_extract = svc.llm.handlers["extraction"]
    svc.llm.handlers["extraction"] = lambda user, **kw: (seen.append(user), base_extract(user=user, **kw))[1]
    add_unit(svc, ("user", "explain event_timer"), ("assistant", "event_timer evaluates the window 60 s after the last datum..."))
    # The helper records the whole unit at one instant; in real life the reply comes after the question.
    svc.db.execute("UPDATE evidence SET occurred_at = occurred_at - interval '1 minute' WHERE role = 'user'")
    drain(svc, lanes=("cognition",))
    uid = add_unit(svc, ("user", "shorten this"))
    with svc.db.tx() as conn:
        unit = conn.execute("SELECT u.*, s.kind AS source_kind FROM units u JOIN sources s ON s.id = u.source_id WHERE u.id = %s",
                            (uid,)).fetchone()
        evidence = conn.execute("SELECT * FROM evidence WHERE unit_id = %s", (uid,)).fetchall()
        ctx = unit_context(conn, unit, evidence)
    assert [c["content"] for c in ctx][-1].startswith("event_timer evaluates")
    text, labels = render_unit(unit, evidence, "Alex", ctx)
    assert "AI assistant: event_timer evaluates" in text.split("NEW")[0] and list(labels) == ["E1"]
    drain(svc, lanes=("cognition",))
    assert "CONTEXT" in seen[-1] and "AI assistant: event_timer" in seen[-1]
    c = svc.db.one("SELECT evidence_ids FROM claims WHERE kind = 'profile'")
    assert len(c["evidence_ids"]) == 1, "cites only the owner's message"


def test_rules_go_at_the_start_of_each_conversation_only(svc):
    """the usage rules go at the start of the conversation (and after /clear or compaction), not on resume nor on every
    request, and even without knowledge or an LLM (it is the owner's text)."""
    from pythia import consumer
    start = consumer.briefing(svc, event="SessionStart", prompt=None, cwd="/r", session_id="s1", source="startup")
    assert "Pythia rules" in start and svc.owner_name in start and "{owner}" not in start
    assert "Pythia rules" not in consumer.briefing(svc, event="SessionStart", prompt=None, cwd="/r", session_id="s1",
                                                       source="resume")
    assert "Pythia rules" not in consumer.briefing(svc, event="UserPromptSubmit", prompt="hi", cwd="/r", session_id="s1")
    assert "Pythia rules" in consumer.briefing(svc, event="SessionStart", prompt=None, cwd="/r", session_id="s1",
                                                   source="compact")
    cfg = svc.settings.get("push")
    svc.settings.set("push", {**cfg, "rules_enabled": False})
    assert consumer.briefing(svc, event="SessionStart", prompt=None, cwd="/r", session_id="s2", source="startup") == ""
