"""Value: honest cost (maintain × serve × measure; cost per source ÷ what reached an LLM)."""

from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb

from pythia import value
from pythia.daemon import drain
from pythia.web.app import create_app

from .helpers import SOURCE, add_unit, claim, default_handlers, mention


def _call(svc, role, usd, source=None, batch=False):
    svc.db.execute("""INSERT INTO llm_calls (role, model, source_id, cost_usd, ok, batch) VALUES (%s, 'm', %s, %s, true, %s)""",
                   (role, source, usd, batch))


def test_buckets_and_cost_per_delivered_claim_by_source(svc):
    default_handlers(svc, [[claim("state", mention("atlas", "system"), "owner", "team X", "Atlas belongs to team X."),
                            claim("state", mention("nimbus", "system"), "owner", "team Y", "Nimbus belongs to team Y.")]])
    add_unit(svc, ("user", "atlas is X's, nimbus is Y's"))
    drain(svc, lanes=("cognition",))
    svc.db.execute("DELETE FROM llm_calls")
    _call(svc, "extraction", 1.0, SOURCE)
    _call(svc, "integration", 1.0, SOURCE, batch=True)
    _call(svc, "synthesis", 0.5)
    _call(svc, "briefing", 0.25, "push")
    _call(svc, "golden", 0.75)
    atlas = svc.db.one("SELECT id FROM claims WHERE statement LIKE 'Atlas belongs%%'")["id"]
    # A page citing the atlas claim was pushed (in version 1); the MCP returned the same claim.
    svc.db.execute("""INSERT INTO views (id, kind, title, l0, l1, cited_claim_ids, member_entity_ids, watermark, status, generated_at)
                      VALUES ('vw_t', 'entity', 'atlas', 'l0', 'l1', %s, '{}', 'w', 'active', now())""", ([atlas],))
    svc.db.execute("""INSERT INTO view_versions (view_id, version, title, l0, l1, cited_claim_ids, member_entity_ids, status, generated_at)
                      VALUES ('vw_t', 1, 'atlas', 'l0', 'l1', %s, '{}', 'active', now())""", ([atlas],))
    svc.db.execute("INSERT INTO pushes (session_id, event, refs) VALUES ('s', 'x', %s)", (Jsonb([{"id": "vw_t", "version": 1}]),))
    svc.db.execute("INSERT INTO pushes (session_id, event, refs, skipped) VALUES ('s', 'x', %s, 'already delivered')",
                   (Jsonb([{"id": "vw_t", "version": 1}]),))
    svc.db.execute("INSERT INTO mcp_calls (tool, refs) VALUES ('search', %s)", (Jsonb([{"id": atlas}]),))
    with svc.db.tx() as conn:
        r = value.report(conn)
    mo = r["months"][0]
    assert mo["buckets"] == {"maintain": 2.5, "serve": 0.25, "measure": 0.75} and mo["batch"] == 1.0
    assert mo["deliveries"]["pushes"] == 1, "the skipped push does not count as a delivery"
    src = {s["kind"]: s for s in mo["sources"]}
    cc = src["claude_code"]
    assert cc["usd"] == 2.0 and cc["living"] == 2 and cc["delivered"] == 1 and cc["by_push"] == 1 and cc["by_mcp"] == 1
    assert cc["usd_per_delivered"] == 2.0
    assert src[None]["shared"] and src[None]["usd_per_delivered"] is None, "online synthesis is not divided by claim"
    assert any("Claude Code" in c for c in mo["coverage"])
    assert r["daily"] and r["daily"][-1]["total"] == 3.5
    page = TestClient(create_app(svc, with_mcp=False)).get("/quality").text
    assert "Value: what it costs" in page and "What these numbers do not count" in page


def test_push_usage_floor_counts_only_explicit_use_after_the_push(svc, tmp_path):
    import json
    from datetime import datetime, timedelta, timezone
    sid = "sess-1"
    t0 = datetime.now(timezone.utc) - timedelta(minutes=10)
    svc.db.execute("""INSERT INTO view_versions (view_id, version, title, l0, l1, cited_claim_ids, member_entity_ids, status, generated_at)
                      VALUES ('vw_aaaaaaaaaaaa', 1, 't', 'l0', 'l1', '{cl_bbbbbbbbbbbb}', '{}', 'active', now())""")
    svc.db.execute("INSERT INTO pushes (session_id, event, refs, ts) VALUES (%s, 'x', %s, %s)",
                   (sid, Jsonb([{"id": "vw_aaaaaaaaaaaa", "version": 1}, {"id": "vw_cccccccccccc", "version": 1}]), t0))
    proj = tmp_path / "proj"
    proj.mkdir()
    lines = [
        # before the push: does not count
        {"type": "assistant", "timestamp": (t0 - timedelta(minutes=1)).isoformat(),
         "message": {"content": [{"type": "text", "text": "vw_cccccccccccc"}]}},
        # the injected context (hook attachment) cites the IDs, but is not the assistant's speech
        {"type": "attachment", "timestamp": (t0 + timedelta(seconds=1)).isoformat(), "content": "vw_cccccccccccc"},
        # after the push, the assistant cites a claim the page cited → the page counts as used
        {"type": "assistant", "timestamp": (t0 + timedelta(minutes=1)).isoformat(),
         "message": {"content": [{"type": "text", "text": "second cl_bbbbbbbbbbbb, ..."},
                                 {"type": "tool_use", "name": "mcp__pythia__open", "input": {"id": "vw_zzzzzzzzzzzz", "level": 2}}]}},
    ]
    (proj / f"{sid}.jsonl").write_text("\n".join(json.dumps(x) for x in lines))
    with svc.db.tx() as conn:
        a, b = value.month_bounds(conn, 0)
        u = value.push_usage(conn, tmp_path, a, b)
    assert u["delivered"] == 2 and u["used_explicitly"] == 1 and u["floor"] == 0.5
    assert u["sessions_with_mcp"] == 1 and u["tools"] == {"open": 1} and u["open_levels"] == {"2": 1}
