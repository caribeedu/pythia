# Architecture

> A personal knowledge oracle. It continuously ingests everyday sources, keeps a base of truths with provenance, detects and resolves conflicts with an LLM, and delivers that knowledge to consuming LLMs through push (hook) and pull (MCP) — so that the user does not need to re-explain themselves and so that they spend fewer tokens getting their bearings.

`P#` references point to the numbered decisions in [decisions.md](decisions.md).

---

## 1. Scope

- **Personal** (D1): the knowledge is what the user sees, with their credentials. Provenance and per-source ACLs exist in the schema from the start so as not to block a future organizational version.
- **Local** (D10): runs on the user's machine. A sleeping laptop only delays things — cursors resume.
- **LLM**: Claude API (D10/D11).

## 2. Principles

1. **Judgment is LLM. Plumbing is deterministic.** (D7)
   - Deterministic allowed: connector I/O, cursors/checkpoints, hash dedup, ACLs, watermarks, invalidation cascade, reversal, queues, cache.
   - Forbidden outside an LLM: deciding whether something is an entity, which entity, whether there is a conflict, whether it is outdated, whether it is relevant, what it means, how to summarize. **No lexical/regex heuristic creates meaning.**
2. **No LLM, no function.** No fallback. (D19)
3. **A truth that is incorrect and correctable > a pending hypothesis.** Nothing waits for human approval; correcting is cheap. (D5)
4. **Rich internal agents, lean external interface.** Internal agents have broad tools and live access to the sources; the consumer receives few tools and data with IDs. (D6, D12)
5. **Derivatives are disposable.** Only evidence, entities, claims, verdicts and human decisions need backup. (D2)

## 3. Knowledge model

### 3.1 Building blocks

```
Evidence ──► Mention ──► Entity
    │           │
    └────► Claim ◄┘ ──► Tension ──► Verdict
              │
              ├──► Entity page (L0/L1)
              ├──► Narrative (L0/L1, links pages)
              └──► Map (index of L0s)
```

| Block | Nature | Essential content |
|---|---|---|
| **Evidence** | source, immutable (except edit/deletion at the origin) | original excerpt, source, author, timestamps, ACL, cursor, hash, version |
| **Mention** | source | surface form (`john`), context window, metadata (channel, code block, path), candidates, resolved entity, confidence |
| **Entity** | source | type, canonical name, aliases, external IDs (Slack, GitHub, email, repo…), status (`anchored` / `discovered`) |
| **Claim** | **source of truth** | subject (via mention), attribute/relation, value, **validity time** (when it is true in the world), **record time** (when the oracle came to believe it), evidence, origin (`observed` / `inferred` / `human`), status (`current` / `superseded` / `disputed` / `retracted`), extractor version |
| **Tension / Verdict** | derived, first-class | claims involved, kind (temporal supersession, real conflict, different scope, outdated source), applied result, justification, cited evidence, confidence, model + prompt version, dossier hash |
| **Page / Narrative / Map** | derived | L0 (1–3 lines), L1 (~1 screen), cited claim IDs, watermark |

Rules:
- Every derivative **cites claim IDs**, never the text of the layer below. Narratives link pages for navigation, but their basis is claims.
- Every derivative stores the **watermark** (set of claims + versions). It is stale **iff** any of those claims changed.
- **A narrative is a zoom level**, not a separate entity: it is the L0 of a view (person, system, meeting, decision, bug, topic). (D3)
- Narrative interpretations become `inferred` claims (source `llm:<role>-<version>`), subject to the same conflict machinery.

### 3.2 Zoom levels

| Level | Example |
|---|---|
| L0 — synthesis | "The owner writes short sentences, no flourish, hates marketing jargon." |
| L1 — page | patterns, examples and exceptions of the writing style |
| L2 — claims | `owner.pref.writing = short_sentences` (source, date) |
| L3 — evidence | Claude Code session of 12 Sep, 14:03: "shorten this, it's wordy" |

### 3.3 Knowledge kinds (D3, D4)

A minimal fixed schema per kind; open attributes within the kind, normalized by an LLM.

| Kind | How it changes | When it becomes outdated |
|---|---|---|
| Profile / preference | slowly, by accumulation | recent behavior diverges |
| Episode | immutable once it happens | never; only complemented |
| Decision | replaced by another decision | a new decision or code contradicts it |
| Entity state | continuous | a more authoritative source diverges |
| Causal explanation | inferred, revisable | new evidence about the incident |
| Glossary / jargon | slow | usage diverges; also serves as an alias table |
| Procedure / runbook | slow | cited systems/steps change |

People and teams are entities. **Profiles of colleagues** (D21): only observable professional preferences (review style, technical conventions, expertise). Forbidden: performance evaluation, personality, personal life — enforced as a policy in extraction.

Staleness judgment is **per kind**, always done by an LLM.

## 4. Entity resolution (D4, D5)

Open world, a first-class subsystem.

1. **Deterministic seed**: the incident.io catalog (users linked to their Slack, GitHub, Jira, Zoom and Google identities; teams linked to GitHub teams, Slack channels and groups, Jira projects and managers; repositories; components; services and APIs; partners and customers), the GitHub org, Slack users, Confluence spaces, glossary.
2. **Candidates** (deterministic, cheap): registries, aliases, glossary, embedding similarity with entity L0s.
3. **Typed decision** (Haiku, closed schema): `{candidate_i | new entity of type T | not an entity | ambiguous}` + confidence, from the mention's context.
4. A new entity is born `active` + `discovered`. Ambiguity links to the best candidate with low confidence — a quality signal, not a queue.
5. **Merge / split / relink** are recorded, reversible events.
6. Relink invalidates by watermark exactly the dependent claims → pages → narratives; a job rebuilds only that. Extraction is not redone.

## 5. Conflict, drift and staleness (D5, D6, D7)

1. **Integration** (LLM): each batch of new claims is confronted with its existing neighborhood. The LLM decides whether anything looks odd.
2. **Initial dossier** (deterministic hybrid search) + **free exploration** by the adjudicator agent with rich tools: the full store, graph, raw evidence, verdict history and **live queries to the sources** (current CODEOWNERS, Slack, Confluence). What the agent looked up beyond the dossier is a signal for improving dossier assembly.
3. **Authority policy per claim kind** enters as guidance in the prompt, not as a rule. `human:owner` (via `correct`) always has maximum weight.
4. **Verdict** applied immediately. Cached by dossier hash; re-judges only if the dossier changes.
5. `revert(verdict)` = compensating event + cascade. `override` = a `human:owner` claim. Corrected verdicts become examples for future judgments.
6. **Periodic sweep** (agent) covers drift without a trigger: e.g. a Confluence page describes flow X, X's code has changed since. Entities in rotation by priority (plumbing), daily budget, only with an empty queue. It **detects and collects** (what it read live becomes evidence) and opens a tension with a dossier; the **adjudicator judges**.

**Visibility** (D6): digest on the web; notice in the push when a verdict touches the subject being consulted.

## 6. Ingestion (D8, D9, D16, D18)

### 6.1 Units and settling

| Source | Unit | Settled when |
|---|---|---|
| Slack | thread (replies) / channel message window (gap < X min) | X min without activity (default 30); a new reply = delta, with the rest of the conversation as context; last 7 days re-read for edits/deletions |
| Zoom | meeting | transcript available |
| Confluence | changed sections of a version (the page is split by headings) + comments | version idle for X min; an edit is **not** a retraction: the old version stays as history and the adjudicator judges the affected claims |
| GitHub | PR: description, reviews, comments (inline ones with the `diff_hunk`) and the whole diff per file | merged, closed, or open and inactive > 7 days; later activity = delta |
| LLM sessions | chat (fixed identity in the Claude app; several session files per chat, dedup by message ID) | debounce after each LLM reply; cursor per turn; return = delta |
| incident.io catalog | records | periodic sync; no LLM (seed) |

Pipeline per settled unit: **triage** (Haiku) → **extraction** (Sonnet) → **integration** → **adjudication** (if needed) → **regeneration** of derivatives. Discarding at triage does not erase the evidence.

### 6.2 Sources

- **v1**: Slack (channels + DMs, app with an approved user token, no bot user), Zoom (user-level OAuth app), Confluence (personal token), GitHub (personal token), LLM sessions (Claude Code first; local transcripts), incident.io catalog.
- **Future**: Jira, incident.io incidents/postmortems, ArgoCD, New Relic dashboards and alerts.
- "Microservices" is not a source of its own: code/config arrives through GitHub; runtime is a future source.
- **Explicit allowlist** of channels, DMs, spaces, repositories and **Claude Code chats** (one by one; a new chat enters disabled), configured on the web. Repositories are added by hand.
- **GitHub:**
  - integration through the API only, with a personal token in the Keychain;
  - content is never cut; no commit messages;
  - bots (marked by GitHub) are ignored by default, along with the entire thread that starts with them; there is an exceptions list;
  - lists of ignored authors and paths, empty by default;
  - per-repository mode: all / owner only / owner or team (default); the team is a list on the web, with optional import from the catalog;
  - ignoring an author, ignoring a path or changing the mode **retracts** what came only from there (cascade, reversible);
  - repository: **Pause** keeps what was read; **Remove** retracts.

### 6.3 Backfill

| Source | Horizon |
|---|---|
| Slack | 90 days |
| Zoom | 90 days |
| Confluence | current version only |
| GitHub | 6 months |
| LLM sessions | everything that exists locally |
| incident.io | complete |

Order: most recent → oldest. Counts against the same cap. History beyond the horizon is fetched on demand by the agents.

In backfill: horizon configurable per repository; **Batch API** for triage and extraction; **deferred synthesis** of the entities touched by the backfill, in a single pass at the end; when enabling a repository, the web shows the cost estimate. Day-to-day does not change: immediate processing and synthesis.

### 6.4 Edit, deletion, archiving

- **Edit** = retraction of the previous version: old content discarded (hash + time remain), claims supported only by it retracted, the new version processed from scratch.
- **Deletion** = tombstone with the content really erased; claims supported only by it retracted; cascade.
- **Archiving** = `archived` marking; nothing is removed.
- Never delete based on an incomplete sweep.
- Text **derived** from erased evidence also goes away: mention context, the statement of retracted claims (redacted in the event log itself — the only exception to append-only), unit titles/triage and the text of pages that cited those claims, including old versions.
- Resumed sessions repeat messages with the same IDs: deduplication is per source, and erasing one file does not erase messages that still exist in another session.

## 7. Consumption (D12)

**Push** — Claude Code hooks (`SessionStart`, `UserPromptSubmit`). Haiku receives the prompt + directory/repo + the L0 map (in prompt cache) and returns a ~300–1500-token briefing: short profile, relevant L0s with IDs, related recent verdicts.

**Pull** — MCP with 5 tools:

| Tool | Function |
|---|---|
| `search(query)` | list of L0s + IDs |
| `open(id, level)` | L1 / claims / evidence |
| `timeline(id)` | changes and verdicts over time |
| `correct(id, …)` | writes a `human:owner` claim, replaces and triggers a cascade |
| `define(…)` | declares a new entity/fact |

Other clients: pull only until they have a mechanism equivalent to hooks.

## 8. Operation without an LLM (D19)

| Part | Behavior |
|---|---|
| Raw ingestion | continues |
| Triage → regeneration | stops; queue resumed later |
| Push | stops |
| Pull over existing derivatives | continues with a "cognition paused since T" notice |
| `correct` / `define` | writes; cascade queued |

## 9. Stack (D13, D14, D15)

- Own repository, local, from scratch.
- **Python**.
- **Postgres 17 + pgvector + full-text**, local, modeled with **event sourcing**: append-only event tables (`claim_asserted`, `verdict_applied`, `mention_relinked`, `verdict_reverted`, …) and current-state projections. Reversal = compensating event. This gives "what the oracle believed at T" (record time) and "when X was true" (validity time).
- **Single job queue in Postgres** for all processing (a single execution path).
- **Secrets in the macOS Keychain**.
- **Minimal CLI**: starts the daemon and the web server. Everything else is web.

### Models and cost (defaults configurable on the web)

| Role | Default |
|---|---|
| Triage, typed decision, entity resolution, push briefing | Claude Haiku 4.5 |
| Extraction, L0/L1/narrative synthesis, integration | Claude Sonnet 5.5 |
| Adjudication, periodic sweep | Claude Sonnet 5.5 |

- The typed decider's output is forced by a closed schema (tool use).
- **Prompt caching** on everything that repeats (instructions, L0 map, authority policy).
- **Default monthly cap US$50**, configurable. When reached: LLM calls stop, raw ingestion continues, the web warns. No priority-based cutoff mechanism.

## 10. Web interface (D20)

1. **Explore** — **ego graph** (central node + neighbors 1–2 hops away, ≤ ~50 nodes) with a **time slider** and a side panel (L1, claims, evidence, verdicts; `correct`/`relink`). Evolves into **semantic zoom**: zooming out collapses into narratives/domains, zooming in expands into claims.
2. **Sources** — "processing now" (workers, queue, batches, forecasts), allowlist, connector state, backfill cost (estimate before, actual cost after).
3. **Costs** — spend for the month vs. cap, by role/source/model.
4. **Quality** — correction rate, golden questions, "turn into a golden question".
5. **Config** — models, cap.
6. **Usage** — Push per chat and Pull per call, reassembled by versioned reference.
8. **Evolution** — what changed and why, in date order (most recent first), one line per cause (source item, correction, sweep, verdict), with before/after of claims, the L1 diff between versions, the trajectory of an attribute per entity and "updating since T". What produced nothing appears with the reason, hidden by default ("hide no effect"). Revert on the verdict's own line. Deterministic, with no LLM summary.

## 11. Quality (D17)

1. **Corrections as the main metric**: each `correct`, `revert`, relink is a typed error; rate per role and source.
2. **Golden questions**: 30–50 questions with expected answer and evidence, evaluated by an LLM judge. Mandatory to run when changing a prompt, model or schema.
3. *(Later)* Sampling audit of claims and verdicts against the original evidence.

## 12. Scope and extensions

| Area | Status |
|---|---|
| Sources | Claude Code sessions, GitHub, Slack, Confluence, incident.io catalog |
| Consumers | Claude Code: hook push and MCP pull |
| Web | Evolution, Explore, Sources, Usage, Costs, Quality, Config |
| Planned | Zoom; semantic zoom in Explore; sampling audit of claims and verdicts; more sources (Jira, incident.io incidents, ArgoCD, New Relic) |
| Known limits | [known-issues.md](known-issues.md); how value is measured in [measuring-value.md](measuring-value.md) |

## 13. Design influences

**[Twin](https://github.com/caribeedu/twin)** (the predecessor; [why it was abandoned](https://github.com/caribeedu/twin/blob/main/docs/WHY-I-STOPPED.md)) — what to avoid: human approval as a bottleneck and pending hypotheses instead of correctable truth; two cognition engines over the same data (duplicate cost); batches by ingestion time, which almost never cross sources; stages that do not read the previous state, so nothing accumulates; a stale latch that is too broad; open JSON schemas that let models invent formats; heuristics as a fallback for meaning; no prompt caching, per-role model routing or quality eval; two execution paths.

**gbrain** — what to take (as an idea): compiled truth + evidence timeline; facts with validity and a supersession chain; a contradiction taxonomy that separates temporal evolution from real conflict; judgment cache by hash; delivery with a token budget. What to avoid: a very wide MCP interface; truth only as a report; a markdown/git system of record for high volume.

**OpenViking** — what to take: L0/L1 composed bottom-up; propagation only when the child's L0 changes; never delete on an incomplete sweep; index separate from content. Useful evidence: tree-descent search was abandoned there — hierarchy serves navigation and token budgeting, not ranking.
