"""Claude Code sessions per chat (app identity, terminal chain, dedup)."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb

from pythia.connectors import claude_code as cc
from pythia.connectors.claude_code import ClaudeCodeConnector

from .test_connector import assistant, user, write_session


def app_chat(app_root: Path, chat_id: str, title: str, current: str, prior=(), previous_titles=()):
    d = app_root / "acct" / "org"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{chat_id}.json").write_text(json.dumps({
        "sessionId": chat_id, "cliSessionId": current, "priorCliSessionIds": list(prior), "title": title,
        "previousTitles": list(previous_titles), "cwd": "/repo/atlas", "lastActivityAt": time.time() * 1000,
        "isArchived": False}))


def setup_disk(tmp_path, svc, profile=False):
    projects, app_root = tmp_path / "projects", tmp_path / "app"
    (projects / "-repo-atlas").mkdir(parents=True)
    cfg = svc.settings.get("claude_code")
    cfg.update(projects_root=str(projects), app_sessions_root=str(app_root), debounce_minutes=0,
               profile_all_chats=profile)
    svc.settings.set("claude_code", cfg)
    return projects, app_root


def old(path: Path):
    t = time.time() - 3600
    os.utime(path, (t, t))


def test_app_chat_identity_survives_rename_and_resume(svc, tmp_path):
    projects, app_root = setup_disk(tmp_path, svc)
    f1 = projects / "-repo-atlas" / "s1.jsonl"
    write_session(f1, user("u1", "we decided to use Kafka"))
    app_chat(app_root, "local_A", "Kafka", "s1")
    chats = cc.discover_chats(projects, app_root)
    assert [(c["id"], c["title"], c["files"]) for c in chats] == [("local_A", "Kafka", ["s1"])]
    # Renamed and resumed: a new file copies the history with the same IDs.
    f2 = projects / "-repo-atlas" / "s2.jsonl"
    write_session(f2, user("u1", "we decided to use Kafka"), user("u2", "and the topic is order_updated"))
    app_chat(app_root, "local_A", "Kafka and topics", "s2", prior=["s1"], previous_titles=["Kafka"])
    chats = cc.discover_chats(projects, app_root)
    assert len(chats) == 1 and chats[0]["title"] == "Kafka and topics" and set(chats[0]["files"]) == {"s1", "s2"}


def test_terminal_chat_is_the_chain_of_files_sharing_messages(svc, tmp_path):
    projects, app_root = setup_disk(tmp_path, svc)
    d = projects / "-repo-atlas"
    write_session(d / "t1.jsonl", json.dumps({"type": "custom-title", "customTitle": "Bug X"}) + "\n", user("x1", "oi"))
    write_session(d / "t2.jsonl", user("x1", "oi"), user("x2", "continuando"))
    write_session(d / "t3.jsonl", user("y1", "another subject"))
    chats = {tuple(sorted(c["files"])): c for c in cc.discover_chats(projects, app_root)}
    assert set(chats) == {("t1", "t2"), ("t3",)}
    assert chats[("t1", "t2")]["origin"] == "terminal"


def test_new_chat_appears_off_and_enabling_reads_it_once_across_files(svc, tmp_path):
    projects, app_root = setup_disk(tmp_path, svc)
    d = projects / "-repo-atlas"
    write_session(d / "s1.jsonl", user("u1", "we decided to use Kafka"), assistant("a1", "m1", [{"type": "text", "text": "Ok."}]))
    write_session(d / "s2.jsonl", user("u1", "we decided to use Kafka"), assistant("a1", "m1", [{"type": "text", "text": "Ok."}]),
                  user("u2", "and the topic is order_updated"))
    old(d / "s1.jsonl"), old(d / "s2.jsonl")
    app_chat(app_root, "local_A", "Kafka", "s2", prior=["s1"])
    from pythia.daemon import scan_sources
    scan_sources(svc)
    assert svc.db.one("SELECT count(*) AS n FROM evidence")["n"] == 0, "a new chat comes in turned off"
    client = TestClient(__import__("pythia.web.app", fromlist=["create_app"]).create_app(svc, with_mcp=False))
    page = client.get("/sources").text
    assert "Kafka" in page and "/action/chat/enable" in page
    client.post("/action/chat/enable", data={"chat_id": "local_A"})
    scan_sources(svc)
    texts = sorted(r["content"] for r in svc.db.query("SELECT content FROM evidence"))
    assert texts == ["Ok.", "and the topic is order_updated", "we decided to use Kafka"], "each message only once"
    src = svc.db.one("SELECT * FROM sources WHERE id = %s", (cc.chat_source_id("local_A"),))
    assert src["enabled"] and src["backfill_until"] is not None


def test_rename_and_resume_update_the_same_source(svc, tmp_path):
    projects, app_root = setup_disk(tmp_path, svc)
    d = projects / "-repo-atlas"
    write_session(d / "s1.jsonl", user("u1", "first"))
    old(d / "s1.jsonl")
    app_chat(app_root, "local_A", "Before", "s1")
    svc.db.execute("""INSERT INTO sources (id, kind, name, config, enabled) VALUES (%s, 'claude_code', 'Before', %s, true)""",
                   (cc.chat_source_id("local_A"), Jsonb({"chat_id": "local_A", "files": ["s1"]})))
    from pythia.daemon import scan_sources
    scan_sources(svc)
    write_session(d / "s2.jsonl", user("u1", "first"), user("u2", "second"))
    old(d / "s2.jsonl")
    app_chat(app_root, "local_A", "After", "s2", prior=["s1"])
    scan_sources(svc)
    src = svc.db.one("SELECT * FROM sources WHERE id = %s", (cc.chat_source_id("local_A"),))
    assert src["name"] == "After" and set(src["config"]["files"]) == {"s1", "s2"}
    assert svc.db.one("SELECT count(*) AS n FROM sources WHERE kind = 'claude_code'")["n"] == 1
    assert sorted(r["content"] for r in svc.db.query("SELECT content FROM evidence")) == ["first", "second"]


def test_profile_source_reads_only_owner_messages_from_chats_not_enabled(svc, tmp_path):
    """Owner profile from all chats: of the chats not enabled, only the owner's messages come in (never the AI,
    never the compaction summary, never the excluded chat), and extraction only accepts profile. The reply he reacts to
    arrives as context read from the transcript, without being stored."""
    from pythia.daemon import drain, scan_sources
    from pythia.pipeline import prompts
    from .helpers import claim, default_handlers, mention
    projects, app_root = setup_disk(tmp_path, svc, profile=True)
    d = projects / "-repo-atlas"
    write_session(d / "s1.jsonl",
                  user("u1", "corrige o teste"),
                  assistant("a1", "m1", [{"type": "text", "text": "Done. I did it just as you asked: no verbs."}]),
                  user("u2", "do not write 'I did it just as you asked' at the end of documents"))
    with open(d / "s1.jsonl", "a") as f:  # compaction summary: recorded as the user, written by the AI
        f.write(json.dumps({"type": "user", "uuid": "cs", "isCompactSummary": True, "timestamp": "2026-10-01T10:00:00Z",
                            "message": {"content": "This session is being continued..."}}) + "\n")
    write_session(d / "s2.jsonl", user("p1", "secret of the Pythia chat"))
    old(d / "s1.jsonl"), old(d / "s2.jsonl")
    app_chat(app_root, "local_A", "PROJ-1", "s1")
    app_chat(app_root, "local_B", "Pythia", "s2")
    seen = []
    claims = [claim("profile", mention("Alex"), "rejects", "paraphrase of the request", "Alex does not want 'I did it just as you asked'.",
                    refs=("E2",)),
              claim("state", mention("atlas", "system"), "owner", "X", "Atlas belongs to X.", refs=("E1",))]
    default_handlers(svc, [claims])
    base = svc.llm.handlers["extraction"]
    svc.llm.handlers["extraction"] = lambda system, user, **kw: (seen.append((system, user)), base(system=system, user=user, **kw))[1]
    scan_sources(svc)
    src = svc.db.one("SELECT * FROM sources WHERE id = %s", (cc.PROFILE_SOURCE,))
    assert src and src["config"]["files"] == ["s1"], "the excluded chat (Pythia) stays out"
    ev = svc.db.query("SELECT role, content FROM evidence ORDER BY occurred_at")
    assert [e["role"] for e in ev] == ["user", "user"] and "secret" not in str(ev) and "continued" not in str(ev)
    svc.db.execute("UPDATE sources SET backfill_until = NULL WHERE id = %s", (cc.PROFILE_SOURCE,))  # online, no batch
    drain(svc, lanes=("cognition",))
    system, user_text = seen[0]
    assert system.startswith(prompts.EXTRACT_PROFILE.format(owner=svc.owner_name)[:50]) and "PROFILE MODE" in system
    assert "I did it just as you asked" in user_text.split("NEW")[0], "the AI reply arrives as context"
    kinds = [r["kind"] for r in svc.db.query("SELECT kind FROM claims")]
    assert kinds == ["profile"], "only preference comes in"
