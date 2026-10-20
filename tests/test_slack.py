"""Slack connector against a fake Web API (httpx.MockTransport)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pytest
from psycopg.types.json import Jsonb

from pythia.connectors import slack
from pythia.connectors.common import SourceUnavailable
from pythia.daemon import drain
from pythia.pipeline.process import render_unit

from .helpers import default_handlers

NOW = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
CH = "C111"
ME = "U0ALEX"


def ts(minutes_ago: float) -> str:
    return f"{(NOW - timedelta(minutes=minutes_ago)).timestamp():.6f}"


class FakeSlack:
    def __init__(self, scopes="users:read,channels:read,channels:history,im:read,im:history", bot=False):
        self.scopes = scopes
        self.bot = bot
        self.history: dict[str, list[dict]] = {CH: []}
        self.replies: dict[str, list[dict]] = {}
        self.calls: list[str] = []
        self.users = [{"id": ME, "name": "alexm", "profile": {"real_name": "Alex Moreira"}},
                      {"id": "U0ANA", "name": "ana", "profile": {"real_name": "Ana Lima"}},
                      {"id": "U0BOB", "name": "bob", "profile": {"real_name": "Bob Reis"}}]

    def msg(self, minutes_ago, user, text, **kw):
        m = {"ts": ts(minutes_ago), "user": user, "text": text, **kw}
        self.history[CH].append(m)
        return m

    def thread(self, parent, *replies):
        parent["thread_ts"] = parent["ts"]
        parent["reply_count"] = len(replies)
        parent["latest_reply"] = replies[-1]["ts"] if replies else parent["ts"]
        self.replies[parent["ts"]] = [parent, *replies]

    def reply(self, minutes_ago, user, text, **kw):
        return {"ts": ts(minutes_ago), "user": user, "text": text, **kw}

    def handler(self, request: httpx.Request) -> httpx.Response:
        method = request.url.path.rsplit("/", 1)[-1]
        self.calls.append(method)
        q = dict(request.url.params)
        headers = {"x-oauth-scopes": self.scopes}
        if method == "auth.test":
            body = {"ok": True, "user_id": ME, "team": "Acme", "url": "https://acme.slack.com/"}
            if self.bot:
                body["bot_id"] = "B1"
            return httpx.Response(200, json=body, headers=headers)
        if method == "users.list":
            return httpx.Response(200, json={"ok": True, "members": self.users}, headers=headers)
        if method == "users.conversations":
            return httpx.Response(200, json={"ok": True, "channels": [
                {"id": CH, "name": "zephyr", "is_private": False},
                {"id": "D1", "is_im": True, "user": "U0ANA"}]}, headers=headers)
        if method == "conversations.history":
            oldest = float(q.get("oldest") or 0)
            latest = float(q.get("latest") or 1e12)
            msgs = sorted((m for m in self.history[q["channel"]] if oldest <= float(m["ts"]) <= latest),
                          key=lambda m: -float(m["ts"]))
            # Pagination 2 by 2: the read must follow the cursor to the end.
            start = int(q.get("cursor") or 0)
            page = msgs[start:start + 2]
            nxt = str(start + 2) if start + 2 < len(msgs) else ""
            return httpx.Response(200, json={"ok": True, "messages": page,
                                             "response_metadata": {"next_cursor": nxt}}, headers=headers)
        if method == "conversations.replies":
            if q["ts"] not in self.replies:
                return httpx.Response(200, json={"ok": False, "error": "thread_not_found"}, headers=headers)
            oldest = float(q.get("oldest") or 0)
            msgs = [m for m in self.replies[q["ts"]] if m["ts"] == q["ts"] or float(m["ts"]) > oldest]
            start = int(q.get("cursor") or 0)
            nxt = str(start + 2) if start + 2 < len(msgs) else ""
            return httpx.Response(200, json={"ok": True, "messages": msgs[start:start + 2],
                                             "response_metadata": {"next_cursor": nxt}}, headers=headers)
        return httpx.Response(200, json={"ok": False, "error": "unknown_method"})

    def client(self):
        return slack.SlackClient("xoxp-test", transport=httpx.MockTransport(self.handler))


def add_source(svc, kind="channel", **cfg):
    sid = slack.source_id_for(CH)
    svc.db.execute("""INSERT INTO sources (id, kind, name, config, enabled) VALUES (%s, 'slack', '#zephyr', %s, true)
                      ON CONFLICT (id) DO NOTHING""", (sid, Jsonb({"channel": CH, "kind": kind, **cfg})))
    return sid


def scan(svc, fake, now=NOW, users=None):
    client = fake.client()
    me = slack.verify_token(client)
    users = users or slack.load_users(client)
    src = svc.db.one("SELECT * FROM sources WHERE id = %s", (slack.source_id_for(CH),))
    return slack.SlackConnector(svc.db, client, svc.settings.get("slack"), svc.owner_name, users, me).scan(src, now=now)


def units(svc):
    return svc.db.query("SELECT * FROM units WHERE source_id = %s ORDER BY id", (slack.source_id_for(CH),))


def evidence_of(svc, unit_id):
    return svc.db.query("SELECT * FROM evidence WHERE unit_id = %s ORDER BY occurred_at", (unit_id,))


def test_token_must_be_user_token_with_read_scopes():
    with pytest.raises(SourceUnavailable, match="bot"):
        slack.verify_token(FakeSlack(bot=True).client())
    with pytest.raises(SourceUnavailable, match="users:read"):
        slack.verify_token(FakeSlack(scopes="channels:history").client())
    c = FakeSlack(scopes="users:read,channels:read,channels:history").client()
    slack.verify_token(c)
    assert slack.missing_scopes(c, "dm") == {"im:history", "im:read"}
    assert not slack.missing_scopes(c, "channel")


def test_windows_threads_bots_and_formatting(svc):
    fake = FakeSlack()
    # Window 1 (5h ago): two close messages. 2h gap. Window 2 (2h50 ago).
    fake.msg(300, "U0ANA", "<@U0ALEX> asked to pin grpcio &amp; protodep")
    fake.msg(295, ME, "right, see <https://github.com/acme-corp/zephyr/pull/85|#85>")
    fake.msg(290, None, "deploy done", bot_id="B9", subtype="bot_message", username="argocd")
    fake.msg(289, "U0BOB", "", subtype="channel_join")
    root = fake.msg(170, "U0BOB", "why did the worker die?")
    fake.thread(root, fake.reply(160, "U0ANA", "the Kafka secret was missing"), fake.reply(150, ME, "let's unify in #113"))
    alert = fake.msg(120, None, "ALERT: 5xx error", bot_id="B7", subtype="bot_message", username="newrelic")
    fake.thread(alert, fake.reply(110, "U0ANA", "looking"))
    add_source(svc)
    stats = scan(svc, fake)
    assert stats["windows"] == 2 and stats["threads"] == 1, stats  # the bot window and thread do not exist
    us = units(svc)
    kinds = [u["kind"] for u in us]
    assert kinds.count("slack_window") == 2 and kinds.count("slack_thread") == 1
    w1 = evidence_of(svc, us[0]["id"])
    assert len(w1) == 2, "bot and channel join are left out"
    assert "@Alex Moreira [slack:U0ALEX]" in w1[0]["content"] and "grpcio & protodep" in w1[0]["content"]
    assert "(Alex, the owner)" in w1[1]["content"] and "#85 (https://github.com/acme-corp/zephyr/pull/85)" in w1[1]["content"]
    assert "looking" not in " ".join(e["content"] or "" for u in us for e in evidence_of(svc, u["id"])), \
        "a thread that starts with a bot leaves whole"
    thread = next(u for u in us if u["kind"] == "slack_thread")
    ev = evidence_of(svc, thread["id"])
    assert [e["role"] for e in ev] == ["reply", "reply"]
    with svc.db.tx() as conn:
        ctx = slack.unit_context(conn, thread, ev)
    assert len(ctx) == 1 and "why did the worker die?" in ctx[0]["content"]
    text, labels = render_unit(thread, ev, "Alex", ctx)
    assert "CONTEXT" in text and text.index("why did the worker") < text.index("[E1]") and len(labels) == 2
    assert "thread" in text and "#zephyr" in text


def test_unsettled_conversation_waits_and_thread_waits_for_root(svc):
    fake = FakeSlack()
    fake.msg(10, "U0ANA", "just sent it")  # idle for 10 min < 30
    add_source(svc)
    scan(svc, fake)
    assert units(svc) == []
    scan(svc, fake, now=NOW + timedelta(minutes=25))
    assert len(units(svc)) == 1


def test_thread_delta_brings_only_new_replies_with_context(svc):
    fake = FakeSlack()
    root = fake.msg(200, "U0BOB", "which python do we use?")
    r1 = fake.reply(190, "U0ANA", "3.14.5")
    fake.thread(root, r1)
    add_source(svc)
    scan(svc, fake)
    r2 = fake.reply(60, ME, "we upgraded to 3.14.6 today")
    fake.thread(root, r1, r2)
    scan(svc, fake)
    delta = [u for u in units(svc) if u["kind"] == "slack_thread"][-1]
    ev = evidence_of(svc, delta["id"])
    assert len(ev) == 1 and "3.14.6" in ev[0]["content"] and delta["meta"]["delta"]
    with svc.db.tx() as conn:
        ctx = [c["content"] for c in slack.unit_context(conn, delta, ev)]
    assert "which python do we use?" in ctx[0] and "3.14.5" in ctx[1]


def test_edit_retracts_old_version_and_delete_only_on_complete_listing(svc):
    fake = FakeSlack()
    m1 = fake.msg(300, "U0ANA", "atlas is owned by team X")
    fake.msg(298, "U0BOB", "ok")
    add_source(svc)
    scan(svc, fake)
    old = svc.db.one("SELECT id FROM evidence WHERE external_id = %s", (f"msg:{CH}:{m1['ts']}",))["id"]
    m1["text"], m1["edited"] = "atlas is owned by team Zephyr", {"ts": ts(100)}
    scan(svc, fake)
    assert svc.db.one("SELECT status, content FROM evidence WHERE id = %s", (old,)) == {"status": "tombstoned", "content": None}
    new = svc.db.one("SELECT content FROM evidence WHERE external_id = %s AND status = 'active'", (f"msg:{CH}:{m1['ts']}",))
    assert "Zephyr" in new["content"] and "(edited)" in new["content"]
    assert units(svc)[-1]["meta"]["delta"]
    # Bob's message was deleted: the window is entirely inside the re-read → tombstone.
    fake.history[CH] = [m for m in fake.history[CH] if m["text"] != "ok"]
    scan(svc, fake)
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE content LIKE '%%Bob Reis%%' AND status = 'active'")["n"] == 0


def test_window_partially_outside_revision_is_never_deleted(svc):
    cfg = svc.settings.get("slack")
    cfg["revision_days"] = 1
    svc.settings.set("slack", cfg)
    fake = FakeSlack()
    fake.msg(60 * 24 + 5, "U0ANA", "before the cut")   # 1 day and 5 min ago
    fake.msg(60 * 24 - 5, "U0BOB", "after the cut")  # same window (10 min gap)
    add_source(svc)
    scan(svc, fake)
    fake.history[CH] = []  # the revision read sees nothing (e.g. it only covers the last day)
    scan(svc, fake)
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE status = 'active'")["n"] == 2, \
        "a window that starts before the re-read cannot be deleted from an incomplete listing"


def test_excluded_author_is_retracted_and_full_rescan_requested(svc):
    fake = FakeSlack()
    fake.msg(300, "U0ANA", "Ana speaks")
    fake.msg(299, "U0BOB", "Bob speaks")
    add_source(svc)
    scan(svc, fake)
    cfg = svc.settings.get("slack")
    cfg["exclude_authors"] = ["bob"]
    svc.settings.set("slack", cfg)
    src = svc.db.one("SELECT * FROM sources WHERE id = %s", (slack.source_id_for(CH),))
    out = slack.reapply_filters(svc.db, src, cfg, slack.load_users(fake.client()))
    assert out["retracted"] == 1
    assert svc.db.one("SELECT config FROM sources WHERE id = %s", (src["id"],))["config"]["state"]["full_rescan"]
    scan(svc, fake)  # horizon re-read: Bob stays out, Ana stays as she was
    active = [r["content"] for r in svc.db.query("SELECT content FROM evidence WHERE status = 'active'")]
    assert len(active) == 1 and "Ana" in active[0]


def test_pipeline_processes_slack_unit_end_to_end(svc):
    from .helpers import claim, mention
    default_handlers(svc, [])
    decision = claim("decision", mention("worker", "system", "zephyr worker role"), "decision",
                     "unify", "The team decided to unify the worker in #113.", refs=("E2",))
    svc.llm.handlers["extraction"] = lambda user, **_: {"claims": [decision] if "CONTEXT" in user else []}
    seen = []
    triage = svc.llm.handlers["triage"]
    svc.llm.handlers["triage"] = lambda user, **kw: (seen.append(user), triage(user=user, **kw))[1]
    fake = FakeSlack()
    root = fake.msg(170, "U0BOB", "why did the worker die?")
    fake.thread(root, fake.reply(160, "U0ANA", "the secret was missing"), fake.reply(150, ME, "let's unify in #113"))
    add_source(svc)
    scan(svc, fake)
    drain(svc, lanes=("cognition",))
    thread_text = next(t for t in seen if "thread" in t and "CONTEXT" in t)
    assert "why did the worker die?" in thread_text
    c = svc.db.one("SELECT * FROM claims WHERE predicate = 'decision'")
    ev = svc.db.one("SELECT content FROM evidence WHERE id = %s", (c["evidence_ids"][0],))
    assert "unify" in ev["content"], "the citation points to the right reply, not to the context"


def test_estimate_counts_without_llm(svc):
    fake = FakeSlack()
    root = fake.msg(300, "U0ANA", "x" * 100)
    fake.thread(root, fake.reply(290, "U0BOB", "y"), fake.reply(280, ME, "z"))
    fake.msg(200, "U0BOB", "w" * 50)
    sid = add_source(svc)
    src = svc.db.one("SELECT * FROM sources WHERE id = %s", (sid,))
    est = slack.estimate_backfill(fake.client(), src, svc.settings.get("slack"), 0.0001)
    assert est["messages"] == 2 and est["replies"] == 2 and est["chars"] == 150 + 2 * 75
    assert est["usd"] == round(300 * 0.0001, 2)
    assert not svc.llm.calls
    known = {f"msg:{CH}:{root['ts']}"}
    after = slack.estimate_backfill(fake.client(), src, svc.settings.get("slack"), 0.0001, known=known)
    assert after["new"] == 1 and after["usd_new"] == round(50 * 0.0001, 2) and after["usd_new"] <= est["usd"]


def test_list_conversations_names_dms(svc):
    fake = FakeSlack()
    convs = slack.list_conversations(fake.client(), slack.load_users(fake.client()))
    assert {c["name"] for c in convs} == {"#zephyr", "DM with Ana Lima"}
    assert {c["kind"] for c in convs} == {"channel", "dm"}


def test_live_read_for_adjudicator(svc):
    fake = FakeSlack()
    root = fake.msg(170, "U0BOB", "question")
    r1 = fake.reply(160, "U0ANA", "reply")
    fake.thread(root, r1)
    users = slack.load_users(fake.client())
    out = slack.read_live(fake.client(), CH, r1["ts"], root["ts"], users)
    assert "← this one: reply" in out and "question" in out
    fake.replies[root["ts"]] = [root]
    assert "deleted" in slack.read_live(fake.client(), CH, r1["ts"], root["ts"], users)


def test_web_lists_conversations_adds_paused_and_saves_settings(svc, monkeypatch):
    from fastapi.testclient import TestClient
    from pythia import vault
    from pythia.web.app import create_app

    stored = {}
    monkeypatch.setattr(vault, "get_secret", lambda name: stored.get(name))
    monkeypatch.setattr(vault, "set_secret", lambda name, value: stored.__setitem__(name, value))
    svc.state.set("slack_conversations", {"items": [{"id": CH, "name": "#zephyr", "kind": "channel", "archived": False},
                                                    {"id": "D1", "name": "DM with Ana Lima", "kind": "dm", "archived": False}],
                                          "at": "2026-10-02T12:00:00+00:00"})
    client = TestClient(create_app(svc, with_mcp=False))
    page = client.get("/sources").text
    assert "no token" in page and "No conversation on the allowlist" in page
    r = client.post("/action/config", data={"slack_token": "xoxp-1", "sl_settle": "45", "sl_exclude": "bob"},
                    follow_redirects=False)
    assert r.status_code == 303 and stored[vault.SLACK_TOKEN] == "xoxp-1"
    assert svc.settings.get("slack")["settle_minutes"] == 45 and svc.settings.get("slack")["exclude_authors"] == ["bob"]
    assert svc.db.one("SELECT count(*) AS n FROM jobs WHERE kind = 'list_slack_conversations'")["n"] == 1
    client.post("/action/slack/add", data={"channel": "D1"})
    src = svc.db.one("SELECT * FROM sources WHERE id = 'slack:D1'")
    assert src["enabled"] is False and src["config"]["kind"] == "dm" and src["name"] == "DM with Ana Lima"
    assert svc.db.one("SELECT count(*) AS n FROM jobs WHERE kind = 'estimate_backfill'")["n"] == 1
    page = client.get("/sources").text
    assert "DM with Ana Lima" in page and "#zephyr" in page
    assert page.count("DM with Ana Lima") == 1, "the already added conversation leaves the list to add from"
    assert "User OAuth Token" in client.get("/config").text


def test_deleting_a_context_message_cancels_pending_batch_requests_that_carry_it(svc):
    from pythia import knowledge as K
    fake = FakeSlack()
    root = fake.msg(170, "U0BOB", "question secret")
    fake.thread(root, fake.reply(160, "U0ANA", "reply"))
    add_source(svc)
    scan(svc, fake)
    thread = next(u for u in units(svc) if u["kind"] == "slack_thread")
    svc.db.execute("""INSERT INTO batch_requests (custom_id, kind, role, unit_id, source_id, params, meta, status)
                      VALUES ('br_ctx', 'extraction', 'extraction', %s, %s, %s, '{}', 'submitted')""",
                   (thread["id"], thread["source_id"], Jsonb({"messages": [{"content": "question secret"}]})))
    root_ev = svc.db.one("SELECT id FROM evidence WHERE external_id = %s", (f"msg:{CH}:{root['ts']}",))["id"]
    with svc.db.tx() as conn:
        K.tombstone_evidence(conn, "connector", [root_ev], "deleted on Slack")
    br = svc.db.one("SELECT status, params FROM batch_requests WHERE custom_id = 'br_ctx'")
    assert br == {"status": "canceled", "params": None}


# ---------------------------------------------------------------- edge cases: windows, threads, filters

DAY = 24 * 60


def test_new_reply_in_old_watched_thread_is_read(svc):
    fake = FakeSlack()
    root = fake.msg(10 * DAY, "U0BOB", "how do you do the deploy?")
    r1 = fake.reply(10 * DAY - 10, "U0ANA", "through argocd")
    fake.thread(root, r1)
    add_source(svc)
    scan(svc, fake)
    r2 = fake.reply(60, ME, "now there is an approval gate")
    fake.thread(root, r1, r2)
    fake.history[CH].append({"ts": r2["ts"], "user": ME, "text": r2["text"], "thread_ts": root["ts"],
                             "subtype": "thread_broadcast"})
    scan(svc, fake, now=NOW + timedelta(hours=7))  # revision due (6 h)
    delta = [u for u in units(svc) if u["kind"] == "slack_thread"][-1]
    ev = evidence_of(svc, delta["id"])
    assert len(ev) == 1 and "approval gate" in ev[0]["content"] and delta["meta"]["delta"]


def test_pause_longer_than_revision_does_not_lose_loose_messages(svc):
    fake = FakeSlack()
    fake.msg(20 * DAY, "U0ANA", "first")
    add_source(svc)
    scan(svc, fake)
    fake.msg(12 * DAY, "U0BOB", "during the pause")  # between the last window and the 7-day cut
    scan(svc, fake)
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE content LIKE '%%during the pause%%'")["n"] == 1


def test_noisy_bot_does_not_keep_window_open_and_windows_have_max_span(svc):
    fake = FakeSlack()
    for i in range(0, 600, 10):  # bot every 10 min for 10 h
        fake.msg(700 - i, None, f"metric {i}", bot_id="B1", subtype="bot_message", username="grafana")
    fake.msg(650, "U0ANA", "did anyone see the alert?")
    fake.msg(400, "U0BOB", "yes, already fixed")  # 250 min later: another window, despite the bot in between
    for i in range(0, 480, 10):  # human every 10 min for 8 h: the window closes at 6 h
        fake.msg(1500 - i, "U0ANA", f"log {i}")
    add_source(svc)
    scan(svc, fake)
    spans = [(float(r["meta"]["last_ts"]) - float(r["meta"]["first_ts"])) / 3600 for r in svc.db.query(
        "SELECT meta FROM streams WHERE meta->>'slack' = 'window'")]
    assert len(spans) == 4 and max(spans) < 6
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE content LIKE '%%already fixed%%'")["n"] == 1


def test_thread_whose_replies_were_all_deleted_is_cleared_on_revision(svc):
    fake = FakeSlack()
    root = fake.msg(300, "U0BOB", "question")
    fake.thread(root, fake.reply(290, "U0ANA", "sensitive reply"))
    add_source(svc)
    scan(svc, fake)
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE content LIKE '%%sensitive%%' AND status = 'active'")["n"] == 1
    root.pop("reply_count")
    root.pop("latest_reply")
    fake.replies[root["ts"]] = [root]
    scan(svc, fake, now=NOW + timedelta(hours=7))
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE content LIKE '%%sensitive%%' AND status = 'active'")["n"] == 0
    # Root deleted for good: Slack says the thread does not exist → the known thread is emptied.
    root2 = fake.msg(280, "U0ANA", "another")
    fake.thread(root2, fake.reply(270, "U0BOB", "secret 2"))
    scan(svc, fake, now=NOW + timedelta(hours=8))
    fake.history[CH].remove(root2)
    del fake.replies[root2["ts"]]
    scan(svc, fake, now=NOW + timedelta(hours=15))
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE content LIKE '%%secret 2%%' AND status = 'active'")["n"] == 0


def test_renaming_a_person_is_not_an_edit(svc):
    fake = FakeSlack()
    fake.msg(300, "U0ANA", "oi <@U0BOB>")
    add_source(svc)
    scan(svc, fake)
    before = len(units(svc))
    users = slack.load_users(fake.client())
    users["U0BOB"]["name"] = "Roberto Reis"
    scan(svc, fake, users=users)
    assert len(units(svc)) == before
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE status = 'tombstoned'")["n"] == 0


def test_reapply_keeps_allowed_bot_even_with_user_field(svc):
    cfg = svc.settings.get("slack")
    cfg["read_bots"] = ["incident.io"]
    svc.settings.set("slack", cfg)
    fake = FakeSlack()
    fake.msg(300, "U0BOT", "INC-1 open", bot_id="B2", bot_profile={"name": "incident.io"})
    add_source(svc)
    scan(svc, fake)
    src = svc.db.one("SELECT * FROM sources WHERE id = %s", (slack.source_id_for(CH),))
    assert slack.reapply_filters(svc.db, src, cfg, {})["retracted"] == 0


def test_context_only_before_the_unit_and_within_budget(svc, monkeypatch):
    monkeypatch.setattr(slack, "CONTEXT_CHARS", 300)
    fake = FakeSlack()
    root = fake.msg(500, "U0BOB", "root")
    rs = [fake.reply(490 - i, "U0ANA", f"r{i} " + "x" * 100) for i in range(5)]
    fake.thread(root, *rs)
    add_source(svc)
    scan(svc, fake)
    thread = next(u for u in units(svc) if u["kind"] == "slack_thread")
    ev = evidence_of(svc, thread["id"])
    with svc.db.tx() as conn:
        ctx = slack.unit_context(conn, thread, ev[3:])  # pretends the unit is only the last two
    texts = [c["content"] for c in ctx]
    assert "root" in texts[0] and not any("r3 " in t or "r4 " in t for t in texts), "nothing from after the unit"
    assert sum(len(t) for t in texts) <= 300 + len(texts[0]) and any("r2 " in t for t in texts)


def test_live_read_finds_target_in_busy_channel_and_hides_filtered(svc):
    cfg = svc.settings.get("slack")
    cfg["exclude_authors"] = ["bob"]
    fake = FakeSlack()
    for i in range(30):
        fake.msg(100 - i, "U0BOB", f"noise {i}")
    target = fake.msg(50, "U0ANA", "the target")
    users = slack.load_users(fake.client())
    out = slack.read_live(fake.client(), CH, target["ts"], None, users, cfg)
    assert "← this one: the target" in out and "noise" not in out


def test_conversation_list_only_asks_for_readable_types():
    fake = FakeSlack(scopes="users:read,channels:read,channels:history")
    seen = []
    orig = fake.handler

    def spy(request):
        if request.url.path.endswith("users.conversations"):
            seen.append(dict(request.url.params)["types"])
        return orig(request)

    client = slack.SlackClient("xoxp", transport=httpx.MockTransport(spy))
    slack.verify_token(client)
    slack.list_conversations(client, {})
    assert seen == ["public_channel"]


def test_deleting_context_of_processed_unit_reprocesses_it(svc):
    from pythia import knowledge as K
    from .helpers import claim, mention
    default_handlers(svc, [])
    secret = claim("state", mention("vault", "system", "vault quoted in the question"), "default_password", "1234",
                   "The vault's default password is 1234.", refs=("E1",))
    svc.llm.handlers["extraction"] = lambda user, **_: {"claims": [secret] if "CONTEXT" in user and "1234" in user else []}
    fake = FakeSlack()
    root = fake.msg(170, "U0BOB", "the vault's default password is 1234, right?")
    fake.thread(root, fake.reply(160, "U0ANA", "yes"))
    add_source(svc)
    scan(svc, fake)
    drain(svc, lanes=("cognition",))
    c = svc.db.one("SELECT id, status FROM claims WHERE predicate = 'default_password'")
    assert c["status"] == "current"
    root_ev = svc.db.one("SELECT id FROM evidence WHERE external_id = %s", (f"msg:{CH}:{root['ts']}",))["id"]
    with svc.db.tx() as conn:
        K.tombstone_evidence(conn, "connector", [root_ev], "deleted on Slack")
    gone = svc.db.one("SELECT status, statement FROM claims WHERE id = %s", (c["id"],))
    assert gone["status"] == "retracted" and "1234" not in gone["statement"]
    drain(svc, lanes=("cognition",))
    assert svc.db.one("SELECT count(*) AS n FROM claims WHERE statement LIKE '%%1234%%'")["n"] == 0
    thread = next(u for u in units(svc) if u["kind"] == "slack_thread")
    assert svc.db.one("SELECT status FROM units WHERE id = %s", (thread["id"],))["status"] == "processed"


def test_removing_author_from_exclusion_brings_messages_back(svc):
    fake = FakeSlack()
    fake.msg(300, "U0ANA", "Ana speaks")
    fake.msg(299, "U0BOB", "Bob speaks")
    add_source(svc)
    cfg = svc.settings.get("slack")
    cfg["exclude_authors"] = ["bob"]
    svc.settings.set("slack", cfg)
    scan(svc, fake)
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE status = 'active'")["n"] == 1
    cfg["exclude_authors"] = []
    svc.settings.set("slack", cfg)
    src = svc.db.one("SELECT * FROM sources WHERE id = %s", (slack.source_id_for(CH),))
    slack.reapply_filters(svc.db, src, cfg, {})
    scan(svc, fake)
    assert svc.db.one("SELECT count(*) AS n FROM evidence WHERE status = 'active' AND content LIKE '%%Bob%%'")["n"] == 1


def test_thread_waits_until_root_window_settles(svc):
    fake = FakeSlack()
    fake.msg(100, "U0ANA", "conversation")
    root = fake.msg(90, "U0BOB", "opens thread")
    fake.thread(root, fake.reply(80, "U0ANA", "reply"))  # thread idle for 80 min: settled
    for m in (70, 50, 30, 10):  # the channel stays active (20-minute gaps): the root's window does not close
        fake.msg(m, "U0ANA", f"more conversation {m}")
    add_source(svc)
    scan(svc, fake)
    assert not [u for u in units(svc) if u["kind"] == "slack_thread"], "without the root stored, the thread waits"
    scan(svc, fake, now=NOW + timedelta(minutes=25))
    kinds = [u["kind"] for u in units(svc)]
    assert kinds == ["slack_window", "slack_thread"]


def test_horizon_increase_requests_full_rescan(svc):
    from fastapi.testclient import TestClient
    from pythia.web.app import create_app
    sid = add_source(svc, state={"initialized": True})
    TestClient(create_app(svc, with_mcp=False)).post("/action/slack/config", data={"source_id": sid, "horizon_days": 120})
    assert svc.db.one("SELECT config FROM sources WHERE id = %s", (sid,))["config"]["state"]["full_rescan"]


def test_source_lock_serializes_jobs_of_the_same_source(svc):
    from pythia.daemon import source_lock
    with source_lock(svc, "slack:C111"):
        with svc.db.pool.connection() as other:
            assert other.execute("SELECT pg_try_advisory_lock(hashtext('slack:C111')) AS ok").fetchone()["ok"] is False
            assert other.execute("SELECT pg_try_advisory_lock(hashtext('slack:C222')) AS ok").fetchone()["ok"] is True
            other.execute("SELECT pg_advisory_unlock(hashtext('slack:C222'))")
    with svc.db.pool.connection() as other:
        assert other.execute("SELECT pg_try_advisory_lock(hashtext('slack:C111')) AS ok").fetchone()["ok"] is True
        other.execute("SELECT pg_advisory_unlock(hashtext('slack:C111'))")


def test_interrupted_backfill_does_not_refetch_threads_already_done(svc):
    fake = FakeSlack()
    a = fake.msg(300, "U0BOB", "thread A")
    fake.thread(a, fake.reply(290, "U0ANA", "ra"))
    b = fake.msg(200, "U0BOB", "thread B")
    fake.thread(b, fake.reply(190, "U0ANA", "rb"))
    add_source(svc)
    orig = fake.handler
    state = {"fail": True}

    def flaky(request):
        q = dict(request.url.params)
        if request.url.path.endswith("conversations.replies") and q["ts"] == b["ts"] and state["fail"]:
            return httpx.Response(429, headers={"retry-after": "999"})
        return orig(request)

    fake.handler = flaky
    with pytest.raises(SourceUnavailable):
        scan(svc, fake)
    state["fail"] = False
    fake.calls.clear()
    scan(svc, fake)
    replies_for = [c for c in fake.calls if c == "conversations.replies"]
    assert len(replies_for) == 1, "only thread B is read on the retry"
    assert {u["kind"] for u in units(svc)} == {"slack_window", "slack_thread"} and len(
        [u for u in units(svc) if u["kind"] == "slack_thread"]) == 2


def test_group_conversations_are_named_by_people_without_the_owner():
    users = {"U0ALEX": {"id": "U0ALEX", "name": "Alex Moreira", "login": "alex.moreira"},
             "U1": {"id": "U1", "name": "Ana Lima", "login": "ana.lima"},
             "U2": {"id": "U2", "name": "Bruno Reis", "login": "bruno.reis"}}
    assert slack.group_name("mpdm-alex.moreira--ana.lima--bruno.reis-1", users, "U0ALEX") == "Ana Lima, Bruno Reis"
    assert slack.group_name("mpdm-alex.moreira--who.left-1", users, "U0ALEX") == "who.left"
