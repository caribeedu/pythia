"""Avoidable re-explanation and cost of getting oriented, from transcripts."""

import json
import re
from datetime import datetime, timedelta, timezone

from psycopg.types.json import Jsonb

from pythia import reexplain
from pythia.daemon import drain

from .helpers import add_unit, claim, default_handlers, mention


def _transcript(path, sid, lines):
    path.mkdir(parents=True, exist_ok=True)
    (path / f"{sid}.jsonl").write_text("\n".join(json.dumps(x) for x in lines) + "\n")


def test_repeated_known_fact_is_classified_by_whether_the_push_had_delivered_it(svc, tmp_path):
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "Zephyr", "Atlas belongs to Zephyr.")]])
    add_unit(svc, ("user", "atlas belongs to Zephyr"))
    drain(svc, lanes=("cognition",))
    cid = svc.db.one("SELECT id FROM claims")["id"]
    t0 = datetime.now(timezone.utc) + timedelta(minutes=5)
    sid = "sess-a"
    svc.db.execute("INSERT INTO pushes (session_id, event, refs, ts) VALUES (%s, 'x', %s, %s)",
                   (sid, Jsonb([{"id": cid}]), t0 + timedelta(seconds=30)))
    _transcript(tmp_path / "proj", sid, [
        {"type": "user", "uuid": "u1", "sessionId": sid, "timestamp": t0.isoformat(),
         "message": {"content": "reminder: atlas belongs to Zephyr"}},           # before the push → not delivered
        {"type": "user", "uuid": "u2", "sessionId": sid, "timestamp": (t0 + timedelta(minutes=1)).isoformat(),
         "message": {"content": "again: atlas belongs to Zephyr"}},             # after the push → delivered
        {"type": "user", "uuid": "u3", "sessionId": sid, "timestamp": (t0 + timedelta(minutes=2)).isoformat(),
         "message": {"content": "run the tests"}},                              # command → does not assert
        {"type": "assistant", "timestamp": (t0 + timedelta(minutes=3)).isoformat(),
         "message": {"id": "m1", "usage": {"input_tokens": 100, "cache_read_input_tokens": 900},
                     "content": [{"type": "tool_use", "name": "Grep", "input": {}}]}},
        {"type": "assistant", "timestamp": (t0 + timedelta(minutes=4)).isoformat(),
         "message": {"id": "m2", "usage": {"input_tokens": 50}, "content": [{"type": "tool_use", "name": "Edit", "input": {}}]}},
        {"type": "assistant", "timestamp": (t0 + timedelta(minutes=5)).isoformat(),
         "message": {"id": "m3", "usage": {"input_tokens": 70}, "content": [{"type": "text", "text": "pronto"}]}},
    ])
    seen = []

    def split(user, **_):
        return {"messages": [{"n": int(n), "asserts": "atlas" in txt}
                              for n, txt in re.findall(r"\[(\d+)\] (.*)", user)]}

    def judge(user, **_):
        seen.append(user)
        ids = re.findall(r"(cl_[0-9a-f]+)", user)
        return {"repeats": True, "claim_ids": ids[:1], "reason": "same information"}

    svc.llm.handlers["value_triage"] = split
    svc.llm.handlers["value_judge"] = judge
    out = reexplain.run(svc, since_days=1, root=tmp_path)
    assert out["sessions"] == 1 and out["messages"] == 3 and out["judged"] == 2
    rows = {r["message_id"]: r for r in svc.db.query("SELECT * FROM reexplain_messages")}
    assert rows["u1"]["bucket"] == "not_delivered" and rows["u2"]["bucket"] == "delivered"
    assert rows["u3"]["asserts"] is False and rows["u3"]["bucket"] is None
    assert all("push" not in s.lower() for s in seen), "the judge does not see what was pushed"
    m = svc.db.one("SELECT * FROM session_metrics WHERE session_id = %s", (sid,))
    assert (m["avoidable_delivered"], m["avoidable_not_delivered"], m["asserts"]) == (1, 1, 2)
    assert m["tokens_until_edit"] == 1050 and m["explore_until_edit"] == 1 and m["tokens_total"] == 1120
    assert reexplain.run(svc, since_days=1, root=tmp_path)["judged"] == 0, "messages already measured are not judged again"
    # Validation with the owner: agreement and kappa
    svc.db.execute("UPDATE reexplain_messages SET owner_label = true WHERE message_id IN ('u1', 'u2')")
    agr = reexplain.agreement(svc)
    assert agr["n"] == 2 and agr["agree"] == 1.0


def test_claims_said_in_the_same_conversation_are_not_counted_as_known(svc, tmp_path):
    """What the owner said earlier in the SAME conversation is already in the AI's context: it is not re-explaining to another LLM."""
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "Zephyr", "Atlas belongs to Zephyr.")]])
    add_unit(svc, ("user", "atlas belongs to Zephyr"))
    drain(svc, lanes=("cognition",))
    stream = svc.db.one("SELECT stream_id FROM evidence LIMIT 1")["stream_id"]
    sid = (stream or "claude_code:sess-b").split(":", 1)[1] if stream else "sess-b"
    svc.db.execute("UPDATE evidence SET stream_id = %s", (f"claude_code:{sid}",))
    t0 = datetime.now(timezone.utc) + timedelta(minutes=5)
    _transcript(tmp_path / "proj", sid, [{"type": "user", "uuid": "x1", "timestamp": t0.isoformat(),
                                          "message": {"content": "atlas belongs to Zephyr"}}])
    svc.llm.handlers["value_triage"] = lambda **_: {"messages": [{"n": 1, "asserts": True}]}
    svc.llm.handlers["value_judge"] = lambda **_: {"repeats": True, "claim_ids": [], "reason": "?"}
    out = reexplain.run(svc, since_days=1, root=tmp_path)
    r = svc.db.one("SELECT * FROM reexplain_messages WHERE message_id = 'x1'")
    assert r, (out, sid)
    assert r["bucket"] == "new" and r["candidate_ids"] == []
