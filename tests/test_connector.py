import json
import os
import time
from pathlib import Path

from psycopg.types.json import Jsonb

from pythia.connectors.claude_code import ClaudeCodeConnector, read_delta, source_id_for


def line(**kw) -> str:
    base = {"sessionId": "s1", "cwd": "/repo/atlas", "gitBranch": "main", "isSidechain": False}
    base.update(kw)
    return json.dumps(base) + "\n"


def user(uuid, text, ts="2026-09-12T14:03:00Z", **kw):
    return line(type="user", uuid=uuid, timestamp=ts, message={"role": "user", "content": text}, **kw)


def assistant(uuid, mid, blocks, ts="2026-09-12T14:04:00Z", **kw):
    return line(type="assistant", uuid=uuid, timestamp=ts, message={"id": mid, "role": "assistant", "content": blocks}, **kw)


def write_session(path: Path, *lines: str) -> None:
    with open(path, "a") as f:
        f.writelines(lines)


def make_source(svc, project: Path) -> dict:
    sid = source_id_for(project)
    svc.db.execute("INSERT INTO sources (id, kind, name, config) VALUES (%s, 'claude_code', %s, %s)",
                   (sid, project.name, Jsonb({"path": str(project)})))
    return svc.db.one("SELECT * FROM sources WHERE id = %s", (sid,))


def test_read_delta_cleans_and_groups(tmp_path):
    f = tmp_path / "a.jsonl"
    write_session(
        f,
        json.dumps({"type": "custom-title", "customTitle": "PROJ-1"}) + "\n",
        user("u1", "shorten this <system-reminder>harness junk</system-reminder>"),
        assistant("a1", "m1", [{"type": "thinking", "thinking": "x"}]),
        assistant("a2", "m1", [{"type": "text", "text": "Done."}]),
        assistant("a3", "m1", [{"type": "tool_use", "name": "Bash", "input": {"description": "Run tests"}}]),
        user("u2", [{"type": "tool_result", "content": "output"}]),
        user("u3", "subagent", isSidechain=True),
        '{"type": "user", "incomplete',
    )
    d = read_delta(f, 0)
    assert d.title == "PROJ-1"
    assert [m.role for m in d.messages] == ["user", "assistant"]
    assert d.messages[0].text == "shorten this"
    assert d.messages[1].text == "Done.\n[tool Bash: Run tests]"
    assert d.end_offset < f.stat().st_size  # the incomplete line is left for the next read


def test_scan_settles_after_debounce_and_resumes_from_cursor(svc, tmp_path):
    project = tmp_path / "-repo-atlas"
    project.mkdir()
    f = project / "sess.jsonl"
    write_session(f, user("u1", "I prefer short sentences"), assistant("a1", "m1", [{"type": "text", "text": "ok"}]))
    source = make_source(svc, project)
    conn = ClaudeCodeConnector(svc.db, "Alex")

    # Still changing: does not settle.
    stats = conn.scan(source, debounce_s=300, now=time.time())
    assert stats["units"] == 0
    # After the debounce: settles.
    stats = conn.scan(source, debounce_s=300, now=time.time() + 301)
    assert stats["units"] == 1
    ev = svc.db.query("SELECT role, author, content FROM evidence ORDER BY occurred_at")
    assert [(e["role"], e["author"]) for e in ev] == [("user", "Alex"), ("assistant", "assistant")]
    assert svc.db.one("SELECT count(*) AS n FROM jobs WHERE kind = 'process_unit'")["n"] == 1

    # New reply: only the delta becomes a unit.
    write_session(f, user("u2", "and no emojis", ts="2026-09-12T15:00:00Z"))
    stats = conn.scan(source, debounce_s=300, now=time.time() + 301)
    assert stats["units"] == 1
    units = svc.db.query("SELECT id FROM units ORDER BY id")
    assert len(units) == 2
    last = svc.db.query("SELECT content FROM evidence WHERE unit_id = %s", (units[-1]["id"],))
    assert [e["content"] for e in last] == ["and no emojis"]


def test_deleted_file_tombstones_stream(svc, tmp_path):
    from pythia.daemon import drain

    project = tmp_path / "-repo-x"
    project.mkdir()
    f = project / "sess.jsonl"
    write_session(f, user("u1", "atlas is owned by the Zephyr team"))
    source = make_source(svc, project)
    conn = ClaudeCodeConnector(svc.db, "Alex")
    conn.scan(source, debounce_s=0, now=time.time() + 10)
    svc.db.execute("DELETE FROM jobs")  # without processing the unit
    os.remove(f)
    stats = conn.scan(source, debounce_s=0, now=time.time() + 10)
    assert stats["deleted"] == 1
    drain(svc, lanes=("cognition",))
    ev = svc.db.one("SELECT content, status FROM evidence")
    assert ev == {"content": None, "status": "tombstoned"}


def test_resumed_session_does_not_duplicate_and_deleting_original_keeps_shared_content(svc, tmp_path):
    from pythia.daemon import drain

    project = tmp_path / "-repo-y"
    project.mkdir()
    original = project / "a.jsonl"
    write_session(original, user("u1", "atlas belongs to Zephyr"))
    source = make_source(svc, project)
    conn = ClaudeCodeConnector(svc.db, "Alex")
    conn.scan(source, debounce_s=0, now=time.time() + 10)
    # Resumed session: copies the history with the same IDs and continues.
    resumed = project / "b.jsonl"
    write_session(resumed, user("u1", "atlas belongs to Zephyr"), user("u2", "and fraud belongs to Risk", ts="2026-09-13T10:00:00Z"))
    conn.scan(source, debounce_s=0, now=time.time() + 10)
    rows = svc.db.query("SELECT external_id FROM evidence ORDER BY seq")
    assert [r["external_id"] for r in rows] == ["u1", "u2"]
    svc.db.execute("DELETE FROM jobs")
    os.remove(original)
    conn.scan(source, debounce_s=0, now=time.time() + 10)
    drain(svc, lanes=("cognition",))
    ev = svc.db.one("SELECT status, content, stream_id FROM evidence WHERE external_id = 'u1'")
    assert ev["status"] == "active" and ev["content"] == "atlas belongs to Zephyr" and ev["stream_id"].endswith(":b")
