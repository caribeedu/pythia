"""Quality: correction metrics and golden questions."""

from __future__ import annotations

import re

from psycopg.types.json import Jsonb

from .ids import new_id, sha
from .llm.client import AgentTool, CallContext, schema_object
from .pipeline import prompts, schemas
from .services import Services
from . import consumer


def correction_metrics(svc: Services) -> dict:
    by_type = svc.db.query(
        "SELECT coalesce(error_type, 'pending') AS k, count(*) AS n FROM corrections GROUP BY 1 ORDER BY 2 DESC")
    by_role = svc.db.query(
        "SELECT coalesce(blamed_role, 'pending') AS k, count(*) AS n FROM corrections GROUP BY 1 ORDER BY 2 DESC")
    by_source = svc.db.query(
        "SELECT coalesce(blamed_source, '-') AS k, count(*) AS n FROM corrections GROUP BY 1 ORDER BY 2 DESC")
    reverted = svc.db.one(
        "SELECT count(*) FILTER (WHERE status <> 'applied') AS rev, count(*) AS total FROM verdicts WHERE decided_by <> 'human:owner'")
    claims = svc.db.one("SELECT count(*) FILTER (WHERE origin <> 'human') AS n FROM claims")
    total = sum(r["n"] for r in by_type)
    return {
        "by_type": by_type,
        "by_role": by_role,
        "by_source": by_source,
        "total": total,
        "machine_claims": claims["n"],
        "rate": (total / claims["n"]) if claims["n"] else 0.0,
        "verdicts_reverted": reverted["rev"],
        "verdicts_total": reverted["total"],
    }


# ---------------------------------------------------------------- golden questions

KINDS = {"conflict": "conflict between sources", "fact": "simple fact", "multi_source": "multiple sources", "temporal": "time", "update": "update",
         "abstention": "don't know (abstention)", "profile": "owner profile", None: "no kind"}
REPEATS = 3   # Pythia runs per battery: the score varies between runs, so mean and range are reported
_IDS = re.compile(r"\b(?:vw|cl|ev)_[0-9a-f]{12}\b")


def add_question(svc: Services, question: str, expected: str, from_correction: str | None = None,
                 kind: str | None = None) -> str:
    gid = new_id("gq")
    svc.db.execute("""INSERT INTO golden_questions (id, question, expected, from_correction, kind, sealed)
                      VALUES (%s, %s, %s, %s, %s, %s)""",
                   (gid, question, expected, from_correction, kind, _sealed(question)))
    return gid


def _sealed(question: str) -> bool:
    """Sealed half by hash of the question (stable): nothing is tuned by looking at it; it only decides."""
    return int(sha(question.strip().lower())[:8], 16) % 2 == 1


def import_questions(svc: Services, items: list[dict]) -> int:
    """Imports questions written from the sources (never from what Pythia extracted); the ones active until now are retired."""
    with svc.db.tx() as conn:
        conn.execute("UPDATE golden_questions SET active = false")
        for it in items:
            conn.execute(
                """INSERT INTO golden_questions (id, question, expected, kind, sealed, evidence_ids,
                                                 evidence_note, wrong_answers)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (new_id("gq"), it["question"], it["expected"], it.get("kind"), _sealed(it["question"]),
                 it.get("evidence_ids") or [], it.get("evidence_note"), it.get("wrong_answers") or []))
    return len(items)


def _pythia_tools(svc: Services, seen: set[str]) -> list[AgentTool]:
    STR = {"type": "string"}

    def search(inp):
        out = consumer.search(svc, inp["query"])
        seen.update(_IDS.findall(out))
        return out

    def open_(inp):
        out = consumer.open_item(svc, inp["id"], inp["level"])
        seen.add(inp["id"])
        seen.update(_IDS.findall(out))
        return out

    return [AgentTool("search", "Searches Pythia; returns IDs and L0.", schema_object({"query": STR}), search),
            AgentTool("open", "Opens an ID at level 1 (page), 2 (claims) or 3 (evidence).",
                      schema_object({"id": STR, "level": {"type": "integer", "enum": [0, 1, 2, 3]}}), open_)]


def _baseline_tools(svc: Services, seen: set[str]) -> list[AgentTool]:
    """Dumb baseline (no Pythia): text search over the raw content of the sources, like a grep over a dump."""
    STR = {"type": "string"}

    def search_text(inp):
        terms = [t for t in inp["terms"].split() if len(t) > 1][:6]
        if not terms:
            return "Give search terms."
        rows = svc.db.query(
            """SELECT e.id, e.source_id, e.occurred_at, e.content FROM evidence e
               WHERE e.status = 'active' AND e.content ILIKE ALL(%s) ORDER BY e.occurred_at DESC LIMIT 15""",
            ([f"%{t}%" for t in terms],))
        if not rows:
            return "Nothing found with all those terms; try fewer terms or synonyms."
        lines = []
        for r in rows:
            seen.add(r["id"])
            text = r["content"]
            at = text.lower().find(terms[0].lower())
            snippet = text[max(0, at - 150): at + 250].replace("\n", " ")
            lines.append(f"{r['id']} · {r['source_id']} · {r['occurred_at']:%Y-%m-%d} · …{snippet}…")
        return "\n".join(lines) + "\n\nUse read(id) for the whole text."

    def read(inp):
        r = svc.db.one("SELECT content, source_id, occurred_at FROM evidence WHERE id = %s AND status = 'active'",
                       (inp["id"],))
        if not r:
            return "not found"
        seen.add(inp["id"])
        return f"{r['source_id']} · {r['occurred_at']:%Y-%m-%d %H:%M}\n{r['content'][:40000]}"

    return [AgentTool("search_text", "Text search (all words) over the original text of the sources.",
                      schema_object({"terms": STR}), search_text),
            AgentTool("read", "Reads the original text of an excerpt (ID ev_…).", schema_object({"id": STR}), read)]


def answer(svc: Services, question: str, ctx: CallContext, arm: str = "pythia") -> tuple[dict, set[str]]:
    """Answers through the same operations the consuming LLM has (Pythia arm) or only with text search over the
    sources (baseline). Also returns the IDs the retrieval brought back."""
    seen: set[str] = set()
    if arm == "baseline":
        out = svc.llm.agent("golden", prompts.GOLDEN_BASELINE.format(owner=svc.owner_name), question,
                            _baseline_tools(svc, seen), schemas.GOLDEN_ANSWER, max_steps=8, ctx=ctx)
    else:
        out = svc.llm.agent("golden", prompts.GOLDEN_ANSWER.format(owner=svc.owner_name), question,
                            _pythia_tools(svc, seen), schemas.GOLDEN_ANSWER, max_steps=8, ctx=ctx)
    return out, seen


def _evidence_of(svc: Services, ids: set[str]) -> set[str]:
    """From the references seen (pages, claims, excerpts) to the source excerpts behind them."""
    ev = {i for i in ids if i.startswith("ev_")}
    claims = {i for i in ids if i.startswith("cl_")}
    for r in svc.db.query("SELECT cited_claim_ids FROM views WHERE id = ANY(%s)", ([i for i in ids if i.startswith("vw_")],)):
        claims.update(r["cited_claim_ids"])
    for r in svc.db.query("SELECT evidence_ids FROM claims WHERE id = ANY(%s)", (list(claims),)):
        ev.update(r["evidence_ids"])
    return ev


def _judge(svc: Services, question: str, expected: str, given: str, ctx: CallContext) -> dict:
    return svc.llm.structured("golden", prompts.GOLDEN_JUDGE,
                              f"Question: {question}\n\nExpected answer: {expected}\n\nAnswer given: {given}",
                              schemas.GOLDEN_JUDGE, max_tokens=2048, ctx=ctx)


def run_golden(svc: Services, run_id: str, job_id: int | None = None) -> dict:
    run = svc.db.one("SELECT * FROM golden_runs WHERE id = %s", (run_id,))
    arm = run["arm"]
    questions = svc.db.query("SELECT * FROM golden_questions WHERE active ORDER BY created_at")
    kinds = (run["config"] or {}).get("kinds")
    if kinds:  # partial battery: only some kinds (to fit the budget)
        questions = [q for q in questions if q["kind"] in kinds]
    ctx = CallContext(job_id=job_id)
    done = {r["question_id"] for r in svc.db.query("SELECT question_id FROM golden_results WHERE run_id = %s", (run_id,))}
    for q in questions:
        if q["id"] in done:
            continue
        ans, seen = answer(svc, q["question"], ctx, arm)
        judge = _judge(svc, q["question"], q["expected"], ans["answer"], ctx)
        expected_ev = set(q["evidence_ids"] or [])
        got = _evidence_of(svc, seen)
        recall = len(expected_ev & got) / len(expected_ev) if expected_ev else None
        svc.db.execute(
            """INSERT INTO golden_results (run_id, question_id, answer, passed, rationale, retrieved_evidence_ids, recall)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (run_id, q["id"], ans["answer"], judge["passed"], judge["rationale"], sorted(got), recall))
    if (run["config"] or {}).get("judge_check"):
        checked = {(r["question_id"], r["wrong_answer"]) for r in svc.db.query(
            "SELECT question_id, wrong_answer FROM golden_judge_checks WHERE run_id = %s", (run_id,))}
        for q in questions:
            for wrong in q["wrong_answers"] or []:
                if (q["id"], wrong) in checked:
                    continue
                j = _judge(svc, q["question"], q["expected"], wrong, ctx)
                svc.db.execute("INSERT INTO golden_judge_checks (run_id, question_id, wrong_answer, passed, rationale) "
                               "VALUES (%s, %s, %s, %s, %s)", (run_id, q["id"], wrong, j["passed"], j["rationale"]))
    row = svc.db.one("SELECT count(*) FILTER (WHERE passed) AS p, count(*) AS t FROM golden_results WHERE run_id = %s",
                     (run_id,))
    cost = svc.db.one("SELECT coalesce(sum(cost_usd), 0) AS c FROM llm_calls WHERE job_id = %s", (job_id,))["c"] \
        if job_id else None
    svc.db.execute("""UPDATE golden_runs SET finished_at = now(), passed = %s, total = %s, status = 'done',
                      cost_usd = coalesce(cost_usd, 0) + coalesce(%s, 0) WHERE id = %s""",
                   (row["p"], row["t"], cost, run_id))
    return {"passed": row["p"], "total": row["t"]}


def start_run(svc: Services, reason: str = "manual", arm: str = "pythia", battery: str | None = None,
              judge_check: bool = False, kinds: list[str] | None = None) -> str:
    rid = new_id("gr")
    config = {"models": svc.settings.get("models"), "effort": svc.settings.get("effort"),
              "prompt_version": prompts.VERSION, "reason": reason, "judge_check": judge_check,
              **({"kinds": kinds} if kinds else {})}
    svc.db.execute("INSERT INTO golden_runs (id, config, arm, battery) VALUES (%s, %s, %s, %s)",
                   (rid, Jsonb(config), arm, battery))
    svc.queue.enqueue("golden_run", "derivation", {"run_id": rid}, priority=40, max_attempts=5)
    return rid


def start_battery(svc: Services, reason: str = "manual battery", kinds: list[str] | None = None,
                  repeats: int = REPEATS) -> str:
    """Battery: Pythia `repeats` times (mean and range), the no-Pythia baseline once, and the judge check with
    plausible wrong answers. `kinds` restricts to some kinds (partial battery, cheaper)."""
    battery = new_id("gb")
    for i in range(repeats):
        start_run(svc, reason, "pythia", battery, judge_check=(i == 0), kinds=kinds)
    start_run(svc, reason, "baseline", battery, kinds=kinds)
    return battery


def run_if_questions(svc: Services, reason: str) -> str | None:
    """Golden questions are mandatory when changing prompt, model or schema (one Pythia run, so as not to pay for the
    whole battery on every change)."""
    if not svc.db.one("SELECT 1 AS x FROM golden_questions WHERE active LIMIT 1"):
        return None
    return start_run(svc, reason)


def check_prompt_version(svc: Services) -> str | None:
    """At startup: if the prompt version changed since the last run, runs again."""
    last = svc.db.one("SELECT config FROM golden_runs ORDER BY started_at DESC LIMIT 1")
    if last and last["config"].get("prompt_version") == prompts.VERSION:
        return None
    return run_if_questions(svc, f"prompt version {prompts.VERSION}")


def _stats(values: list[float]) -> dict:
    return {"mean": sum(values) / len(values), "min": min(values), "max": max(values), "n": len(values)} if values else None


def battery_report(svc: Services, battery: str | None = None) -> dict | None:
    """Score per kind (mean and range across runs), Pythia × baseline, recall, judge and cost."""
    if battery is None:
        row = svc.db.one("SELECT battery FROM golden_runs WHERE battery IS NOT NULL ORDER BY started_at DESC LIMIT 1")
        if not row:
            return None
        battery = row["battery"]
    runs = svc.db.query("SELECT * FROM golden_runs WHERE battery = %s ORDER BY started_at", (battery,))
    by_kind: dict = {}
    for run in runs:
        rows = svc.db.query("""SELECT q.kind, r.passed, r.recall FROM golden_results r
                               JOIN golden_questions q ON q.id = r.question_id WHERE r.run_id = %s""", (run["id"],))
        per: dict = {}
        for r in rows:
            k = per.setdefault(r["kind"], {"ok": 0, "n": 0, "recall": []})
            k["n"] += 1
            k["ok"] += 1 if r["passed"] else 0
            if r["recall"] is not None:
                k["recall"].append(float(r["recall"]))
        for kind, k in list(per.items()) + [("_total", {"ok": sum(v["ok"] for v in per.values()),
                                                        "n": sum(v["n"] for v in per.values()),
                                                        "recall": [x for v in per.values() for x in v["recall"]]})]:
            slot = by_kind.setdefault(kind, {"kind": kind, "label": KINDS.get(kind, "total"), "pythia": [],
                                             "baseline": [], "recall": [], "n": k["n"]})
            if k["n"]:
                slot[run["arm"]].append(k["ok"] / k["n"])
            if run["arm"] == "pythia" and k["recall"]:
                slot["recall"].append(sum(k["recall"]) / len(k["recall"]))
    kinds = []
    for k in by_kind.values():
        kinds.append({**k, "pythia": _stats(k["pythia"]), "baseline": _stats(k["baseline"]), "recall": _stats(k["recall"])})
    kinds.sort(key=lambda k: (k["kind"] == "_total", str(k["kind"])))
    checks = svc.db.one("""SELECT count(*) AS n, count(*) FILTER (WHERE c.passed) AS fooled FROM golden_judge_checks c
                           JOIN golden_runs r ON r.id = c.run_id WHERE r.battery = %s""", (battery,))
    costs = {arm: sum(float(r["cost_usd"] or 0) for r in runs if r["arm"] == arm) for arm in ("pythia", "baseline")}
    correct = svc.db.one("""SELECT count(*) FILTER (WHERE res.passed) AS ok FROM golden_results res
                            JOIN golden_runs r ON r.id = res.run_id WHERE r.battery = %s AND r.arm = 'pythia'""",
                         (battery,))["ok"]
    return {"battery": battery, "runs": runs, "kinds": kinds, "judge": checks,
            "done": all(r["status"] == "done" for r in runs), "costs": costs,
            "cost_per_correct": costs["pythia"] / correct if correct else None,
            "regressions": sealed_regressions(svc, battery)}


def sealed_regressions(svc: Services, battery: str) -> list[dict]:
    """Gate (gbrain): SEALED questions that passed in all Pythia runs of the previous battery and
    failed in some run of this one. A change that causes a regression only gets in with a written justification."""
    prev = svc.db.one("""SELECT battery FROM golden_runs WHERE battery IS NOT NULL AND battery <> %s AND status = 'done'
                           AND started_at < (SELECT min(started_at) FROM golden_runs WHERE battery = %s)
                         ORDER BY started_at DESC LIMIT 1""", (battery, battery))
    if not prev:
        return []

    def always_passed(b):
        return {r["question_id"] for r in svc.db.query(
            """SELECT res.question_id FROM golden_results res JOIN golden_runs r ON r.id = res.run_id
               JOIN golden_questions q ON q.id = res.question_id
               WHERE r.battery = %s AND r.arm = 'pythia' AND q.sealed GROUP BY 1 HAVING bool_and(res.passed)""", (b,))}

    before, now = always_passed(prev["battery"]), always_passed(battery)
    lost = before - now
    return svc.db.query("SELECT id, question, kind FROM golden_questions WHERE id = ANY(%s)", (list(lost),)) if lost else []


DETECTORS = {"integration": "integration (new claim × neighbors)", "synthesis": "inference integration",
             "system": "inference that lost support", "sweep": "drift sweep",
             "connector": "Confluence page edit"}


def detector_precision(svc: Services) -> list[dict]:
    """of each suspicion raised, how many the adjudicator confirmed (changed some claim to
    superseded, retracted or disputed). Whoever accuses is not whoever judges, so it can be measured."""
    # One line per tension (its latest verdict). Confirmed = the current verdict actually changed some
    # claim to superseded, retracted or disputed; a verdict you reverted does not count.
    rows = svc.db.query(
        """WITH last AS (
               SELECT DISTINCT ON (t.id) t.id, t.opened_by, v.id AS vid, v.status, v.changes
               FROM tensions t LEFT JOIN verdicts v ON v.tension_id = t.id
               ORDER BY t.id, v.created_at DESC NULLS LAST)
           SELECT opened_by AS detector, count(*) AS total, count(vid) AS judged,
                  count(vid) FILTER (WHERE status = 'applied' AND EXISTS (
                      SELECT 1 FROM jsonb_array_elements(changes) c
                      WHERE c->>'to' IN ('superseded', 'retracted', 'disputed') AND c->>'from' IS DISTINCT FROM c->>'to'))
                      AS confirmed,
                  count(vid) FILTER (WHERE status <> 'applied') AS reverted
           FROM last GROUP BY 1 ORDER BY 2 DESC""")
    for r in rows:
        r["label"] = DETECTORS.get(r["detector"], r["detector"])
        r["precision"] = r["confirmed"] / r["judged"] if r["judged"] else None
        r["low"], r["high"] = wilson(r["confirmed"], r["judged"])
        r["few"] = r["judged"] < 30
    return rows


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float | None, float | None]:
    """95% confidence interval (Wilson) of a k/n proportion. With few cases it is wide."""
    if not n:
        return None, None
    p = k / n
    den = 1 + z * z / n
    mid = (p + z * z / (2 * n)) / den
    half = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / den
    return max(0.0, mid - half), min(1.0, mid + half)


def recent_sweeps(svc: Services, limit: int = 15) -> list[dict]:
    rows = svc.db.query(
        """SELECT r.*, e.name, e.type FROM sweep_reviews r JOIN entities e ON e.id = r.entity_id
           ORDER BY r.reviewed_at DESC LIMIT %s""", (limit,))
    for r in rows:
        r["investigated"] = ", ".join(
            t["tool"] + "(" + ", ".join(str(v) for v in t["input"].values()) + ")" + (" ✗" if t.get("failed") else "")
            for t in r["tools"]) or "nothing (only the base)"
        r["failed"] = [t for t in r["tools"] if t.get("failed")]
        r["inconclusive"] = bool(r["failed"]) and not r["suspicions"]
    return rows
