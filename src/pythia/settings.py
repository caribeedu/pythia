"""Settings editable from the web, stored in the `settings` table."""

from __future__ import annotations

import copy
from typing import Any

from psycopg.types.json import Jsonb

from .db import Database

HAIKU = "claude-haiku-4-5"
SONNET = "claude-sonnet-5-5"

# LLM roles. Each role has its own model and effort.
ROLES: dict[str, dict[str, str]] = {
    "triage": {"label": "Unit triage", "model": HAIKU},
    "resolution": {"label": "Entity resolution (typed decider)", "model": HAIKU},
    # Merging identities is practically irreversible for whoever reads, and a small model tends to merge parts into the whole (PRs into the repository).
    "reconciliation": {"label": "Identity reconciliation (discovered × catalog)", "model": SONNET},
    "briefing": {"label": "Push briefing", "model": HAIKU},
    "extraction": {"label": "Claim extraction", "model": SONNET},
    "integration": {"label": "Integration with existing knowledge", "model": SONNET},
    "synthesis": {"label": "Page and narrative synthesis", "model": SONNET},
    "correction": {"label": "Correction interpretation", "model": SONNET},
    "adjudication": {"label": "Tension adjudication", "model": SONNET},
    "sweep": {"label": "Periodic drift sweep", "model": SONNET},
    "golden": {"label": "Golden questions (answer and judge)", "model": SONNET},
    "value_triage": {"label": "Value: separate messages that assert something", "model": HAIKU},
    "value_judge": {"label": "Value: re-explanation judge", "model": SONNET},
}

# Prices per million tokens (input, output, cache read, cache write).
DEFAULT_PRICES = {
    "claude-haiku-4-5": {"input": 1.0, "output": 5.0, "cache_read": 0.10, "cache_write": 1.25},
    "claude-sonnet-5-5": {"input": 2.0, "output": 10.0, "cache_read": 0.20, "cache_write": 2.50},
    "claude-opus-5-5": {"input": 4.0, "output": 20.0, "cache_read": 0.20, "cache_write": 5.00},
}

DEFAULT_AUTHORITY_POLICY = """\
Source authority varies by claim kind. Use it as guidance, not as a blind rule:
- Corrections and definitions made by the owner (human origin) always prevail.
- Owner profile/preference: what the owner himself said or asked for in sessions outweighs any inference.
- System state (owner, dependencies, status): code, CODEOWNERS and catalogs outweigh documentation; documentation outweighs conversation.
- Decisions: an explicit decision record outweighs the code, because the code may lag behind the decision; more recent decisions replace earlier ones on the same subject.
- Episodes do not go stale; they are only complemented.
- Claims inferred by an LLM weigh less than observed claims.
- Within the same authority, the most recent tends to prevail, unless the evidence shows otherwise.
"""

# Rules: WHEN the consuming AI should use Pythia. Injected by the hook at the start of each conversation (and
# after /clear or compaction). Short on purpose: too many rules make agents worse (AGENTS.md paper).
DEFAULT_RULES = """\
Pythia rules ({owner}'s knowledge base, through the `pythia` MCP tools):
- Before asking {owner} about a person, team, system, decision, incident, bug or procedure, use `search`.
- When he mentions something without explaining it (ticket, PR, system, acronym, nickname), search before asking for an explanation.
- Before writing on his behalf (PR, document, comment, message, code), search for his preferences for that kind of text.
- Go deeper only when needed: `open(id, 1)` page, `2` claims with source, `3` original evidence.
- If he says something in Pythia is wrong or has changed, use `correct`; if he says something new and durable, use `define`.
- Pythia may lag behind the source: if he contradicts the code or the source you are looking at right now, trust the source and record it with `correct`."""

DEFAULTS: dict[str, Any] = {
    "models": {role: spec["model"] for role, spec in ROLES.items()},
    "effort": {role: "medium" for role in ROLES},
    "prices": DEFAULT_PRICES,
    "monthly_cap_usd": 50.0,
    "owner": {"name": "Alex", "aliases": ["Alex", "Alexandre", "alexm"]},
    "authority_policy": DEFAULT_AUTHORITY_POLICY,
    "claude_code": {
        "projects_root": "~/.claude/projects",
        "debounce_minutes": 5,
        "include_sidechains": False,
        # Owner profile from ALL chats (only his messages, only preferences).
        "profile_all_chats": True,
        "profile_excluded_chats": ["Pythia"],
    },
    "push": {"enabled": True, "max_chars": 6000, "rules_enabled": True, "rules": DEFAULT_RULES},
    "web": {"port": 7117},
    "github": {
        "token_type": "classic",     # classic (repo scope; no re-approval when the allowlist changes) | fine_grained
        "owner_login": "",           # owner's GitHub login (empty = discovered through the API with the token)
        "poll_minutes": 10,
        "horizon_days": 180,         # 6-month backfill
        "stale_days": 7,             # an open PR idle for longer than this is considered settled
        "exclude_authors": [],          # authors (humans) you decided not to read
        "exclude_paths": [],            # paths you decided not to read (glob, e.g. poetry.lock, *_pb2.py)
        "read_bots": [],                # bots you want to read (by default every bot is ignored)
    },
    "slack": {
        "poll_minutes": 10,
        "settle_minutes": 30,     # conversation idle for this long = settled (thread or window of loose messages)
        "horizon_days": 90,       # backfill
        "revision_days": 7,       # fully re-read on every scan: edits and deletions
        "revision_hours": 6,      # how often the threads in the revision period are re-read
        "window_max_hours": 6,    # window of loose messages closes after this, even with the channel active
        "thread_watch_days": 30,  # threads older than the revision period but with a reply in this period are watched
        "exclude_authors": [],    # people you do not want to read (name, login or Slack ID)
        "read_bots": [],          # exceptions: bots you want to read (every bot flagged by Slack is ignored)
        "ignore_subtypes": ["channel_join", "channel_leave"],  # system messages you do not want to read
    },
    "confluence": {
        "site": "",               # e.g. company.atlassian.net
        "email": "",              # the Atlassian account email (both token types use email + token)
        "token_type": "scoped",   # scoped (read scopes) | classic
        "cloud_id": "",           # discovered from the site when empty
        "poll_minutes": 30,
        "settle_minutes": 30,     # version idle for this long = settled
        "comment_revision_hours": 6,  # how often comments are re-read (edit/deletion)
        "exclude_authors": [],
    },
    "sweep": {"enabled": True, "daily_budget_usd": 0.5},
    # Narrative passes per hour (the most expensive synthesis). 1 = at most one per hour; 12 = every 5 min.
    "narratives": {"passes_per_hour": 1.0},
    # Chats left out of the measurement (conversations about Pythia itself), by title in the app.
    "value": {"excluded_chats": ["Pythia"]},
    "team": {"members": []},  # who your team is: [{name, email, slack, github, jira, zoom}]
    "incident_io": {
        "base_url": "https://api.incident.io",
        "poll_minutes": 360,
        # catalog type → entity type (empty = not synced)
        "types": {
            "User": "person",
            'Custom["Team"]': "team",
            "GitHubRepository": "repository",
            'Custom["Components"]': "system",
            'Custom["ServiceApis"]': "system",
            'Custom["CarrierApis"]': "system",
            'Custom["VendorApi"]': "system",
            'Custom["Partners"]': "organization",
            # IdP groups: the only place in the catalog with each team's members (off by default).
            "SCIMGroup": "",
        },
        # Attributes that are not read (neither stored in evidence nor turned into claims): "Attribute" applies to
        # any type, "Type: Attribute" only to that type. Owner's list; the default is what is of no use
        # to the oracle (identifiers from other systems, trivial repository metadata).
        "exclude_attributes": [
            "Avatar", "Timezone", "Phone number",
            "GitHubRepository: Default branch", "GitHubRepository: Private", "GitHubRepository: URL",
            "GitHubRepository: Topics", 'Custom["Team"]: PagerDuty Service',
        ],
    },
}


class Settings:
    def __init__(self, db: Database):
        self.db = db

    def get(self, key: str) -> Any:
        row = self.db.one("SELECT value FROM settings WHERE key = %s", (key,))
        default = copy.deepcopy(DEFAULTS.get(key))
        if row is None:
            return default
        value = row["value"]
        if isinstance(default, dict) and isinstance(value, dict):
            merged = default
            for k, v in value.items():
                # A nested dict level also merges: a new key in the default (a role, a
                # catalog type) shows up even when the settings were saved before.
                merged[k] = {**merged[k], **v} if isinstance(merged.get(k), dict) and isinstance(v, dict) else v
            return merged
        return value

    def set(self, key: str, value: Any) -> None:
        self.db.execute(
            """INSERT INTO settings (key, value, updated_at) VALUES (%s, %s, now())
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()""",
            (key, Jsonb(value)),
        )

    def model_for(self, role: str) -> str:
        return self.get("models").get(role) or ROLES[role]["model"]

    def effort_for(self, role: str) -> str:
        return self.get("effort").get(role, "medium")


class SystemState:
    def __init__(self, db: Database):
        self.db = db

    def get(self, key: str) -> Any:
        row = self.db.one("SELECT value FROM system_state WHERE key = %s", (key,))
        return None if row is None else row["value"]

    def set(self, key: str, value: Any) -> None:
        self.db.execute(
            """INSERT INTO system_state (key, value, updated_at) VALUES (%s, %s, now())
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()""",
            (key, Jsonb(value)),
        )

    def clear(self, key: str) -> None:
        self.db.execute("DELETE FROM system_state WHERE key = %s", (key,))
