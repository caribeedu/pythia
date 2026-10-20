"""Golden questions: imported from the sources, battery (3× Pythia + baseline without
Pythia), evidence recall, judge check and the sealed gate."""

from fastapi.testclient import TestClient

from pythia import quality
from pythia.daemon import drain
from pythia.web.app import create_app

from .helpers import add_unit, claim, default_handlers, mention


def _seed(svc):
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "Zephyr", "Atlas belongs to Zephyr.")]])
    add_unit(svc, ("user", "atlas belongs to Zephyr"))
    drain(svc)
    svc.db.execute("UPDATE jobs SET run_after = now() WHERE status = 'queued'")
    drain(svc)
    return svc.db.one("SELECT evidence_ids FROM claims")["evidence_ids"][0]


def _ouro(svc, wrong_judge_passes=False):
    def answer(user, tools, **_):
        if "search" in tools:  # Pythia arm
            found = tools["search"].run({"query": "atlas"})
            ids = [w for w in found.split() if w.startswith("vw_")]
            if ids:
                tools["open"].run({"id": ids[0], "level": 2})
            return {"answer": "Zephyr" if "atlas" in user and "atlas" in found else "I don't know", "used_ids": []}
        found = tools["search_text"].run({"terms": "atlas Zephyr"})  # baseline
        return {"answer": "Zephyr" if "ev_" in found and "atlas" in user else "I don't know", "used_ids": []}

    def judge(user, **_):
        given = user.split("Answer given:")[1]
        expected = user.split("Expected answer:")[1].split("Answer given:")[0]
        if "Order Mgmt" in given:  # plausible wrong answer
            return {"passed": wrong_judge_passes, "rationale": "r"}
        return {"passed": ("I don't know" in given) if "not in the sources" in expected else ("Zephyr" in given), "rationale": "r"}

    svc.llm.handlers["golden"] = lambda **kw: answer(**kw) if "tools" in kw else judge(**kw)


def test_battery_reports_by_kind_against_the_baseline_with_recall_and_judge_check(svc):
    ev = _seed(svc)
    quality.add_question(svc, "old question", "x")
    n = quality.import_questions(svc, [
        {"question": "Who owns atlas?", "expected": "Zephyr.", "kind": "fact", "evidence_ids": [ev],
         "wrong_answers": ["O Order Mgmt."]},
        {"question": "What is the team budget in 2027?", "expected": "That information is not in the sources.",
         "kind": "abstention", "evidence_ids": []},
    ])
    assert n == 2
    assert not svc.db.one("SELECT active FROM golden_questions WHERE question = 'old question'")["active"]
    _ouro(svc)
    battery = quality.start_battery(svc)
    drain(svc, lanes=("derivation",))
    rep = quality.battery_report(svc, battery)
    assert rep["done"] and len(rep["runs"]) == quality.REPEATS + 1
    kinds = {k["kind"]: k for k in rep["kinds"]}
    assert kinds["fact"]["pythia"]["mean"] == 1.0 and kinds["fact"]["pythia"]["n"] == quality.REPEATS
    assert kinds["fact"]["baseline"]["mean"] == 1.0
    assert kinds["abstention"]["pythia"]["mean"] == 1.0, "saying 'I don't know' passes when the answer does not exist"
    assert kinds["fact"]["recall"]["mean"] == 1.0, "Pythia's search brought the expected evidence"
    assert rep["judge"]["n"] == 1 and rep["judge"]["fooled"] == 0
    assert rep["regressions"] == []


def test_judge_that_accepts_wrong_answers_is_reported_and_sealed_regressions_gate(svc):
    ev = _seed(svc)
    quality.import_questions(svc, [{"question": "Who owns atlas?", "expected": "Zephyr.", "kind": "fact",
                                "evidence_ids": [ev], "wrong_answers": ["O Order Mgmt."]}])
    svc.db.execute("UPDATE golden_questions SET sealed = true")
    _ouro(svc, wrong_judge_passes=True)
    first = quality.start_battery(svc)
    drain(svc, lanes=("derivation",))
    assert quality.battery_report(svc, first)["judge"]["fooled"] == 1, "fooled judge shows up"
    svc.llm.handlers["golden"] = lambda **kw: ({"answer": "I don't know", "used_ids": []} if "tools" in kw
                                           else {"passed": False, "rationale": "r"})
    second = quality.start_battery(svc)
    drain(svc, lanes=("derivation",))
    regs = quality.battery_report(svc, second)["regressions"]
    assert [r["question"] for r in regs] == ["Who owns atlas?"]
    page = TestClient(create_app(svc, with_mcp=False)).get("/quality").text
    assert "Regression on the sealed questions" in page and "Run the full battery" in page


def test_sealed_half_is_stable_by_hash(svc):
    assert quality._sealed("Who owns atlas?") == quality._sealed("  who owns atlas?  ")


def test_partial_battery_runs_only_the_requested_kinds_once(svc):
    ev = _seed(svc)
    quality.import_questions(svc, [
        {"question": "Who owns atlas?", "expected": "Zephyr.", "kind": "fact", "evidence_ids": [ev]},
        {"question": "Who owns atlas today, really?", "expected": "Zephyr.", "kind": "conflict", "evidence_ids": [ev],
         "wrong_answers": ["O Order Mgmt."]},
    ])
    _ouro(svc)
    b = quality.start_battery(svc, kinds=["conflict"], repeats=1)
    drain(svc, lanes=("derivation",))
    rep = quality.battery_report(svc, b)
    assert len(rep["runs"]) == 2 and {k["kind"] for k in rep["kinds"]} == {"conflict", "_total"}
    assert rep["kinds"][0]["n"] == 1
