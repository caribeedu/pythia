# Decisions

The design decisions behind Pythia, in one place. The core decisions (D1–D22) are referenced by number from the other docs; the sections after them record how each area applies them. The reasoning is in [design-rationale.md](design-rationale.md); the architecture is in [architecture.md](architecture.md).

## Core decisions

| # | Decision |
|---|---|
| D1 | **Personal** oracle; schema prepared for organizational use (provenance + per-source ACL). |
| D2 | The atomic claim is the source of truth; pages and narratives are derivatives that cite claims. Blocks form a DAG, not a stack. Derivatives carry a watermark and are disposable. |
| D3 | Narrative = zoom level (the L0 of a view). Interpretations become `inferred` claims. Kinds: profile, episode, decision, entity state, causal explanation. |
| D4 | Also glossary/jargon and procedures/runbooks. Open-world entity resolution, by context, via LLM; a first-class subsystem. |
| D5 | No human approval; an incorrect truth is correctable. Mention as the central object; merge/split/relink are reversible; the cascade regenerates without re-extracting. Typed decider + generator. |
| D6 | Conflict resolved by an adjudicator agent with rich tools and live access to the sources; the verdict is persisted, applied immediately and reversible. No lexical heuristics. Visibility: web digest + notice in the push when relevant. |
| D7 | Deterministic only for plumbing; all judgment is LLM. The conflict trigger is LLM integration. Search assembles an initial dossier, the agent explores freely. |
| D8 | Process natural units when settled + periodic sweep. |
| D9 | GitHub: PR merged, closed, or open and idle > 7 days. LLM sessions: debounce per reply + cursor. Microservices are not a source of their own. Sources: Slack, Confluence, GitHub, LLM sessions, incident.io catalog. Planned: Zoom, Jira, incident.io incidents, ArgoCD, New Relic. |
| D10 | Runs locally. Claude API. DMs included. |
| D11 | Typed decider = Claude Haiku 4.5 with a closed schema. Slack via an app with a user token, no bot. |
| D12 | Push by hook (~300–1500-token briefing chosen by Haiku) + lean MCP (5 tools). |
| D13 | Own repository, local, from scratch. |
| D14 | Python + Postgres 17/pgvector, event sourcing. |
| D15 | Models per role with defaults configurable on the web (Haiku; Sonnet for extraction, synthesis, adjudication and sweep). Prompt caching. Configurable monthly cap; when hit, stop. No priority cutoff; spend panel. |
| D16 | Backfill: Slack/Zoom 90d, Confluence current version, GitHub 6m, sessions everything. Explicit allowlist. |
| D17 | Quality: corrections as a metric + golden questions. Sampling audit later. |
| D18 | Edit = retraction of the old version and reprocessing. Deletion = really erase + retract dependent claims. Archiving = mark. |
| D19 | Without an LLM: raw ingestion and pull of derivatives continue (with a notice); everything else stops. |
| D20 | Web: Evolution, Explore, Sources, Costs, Quality, Usage, Config (models, cap and sources on the web). The CLI only starts daemon + web. |
| D20b | Explore: ego graph + time slider, evolving into semantic zoom. |
| D21 | Colleague profiles: yes, restricted to observable professional traits. |
| D22 | Scope: the sources of D9 and the consumers of D12; Zoom and semantic zoom are the planned extensions. |

Technical defaults assumed without discussion: single job queue in Postgres; secrets in the macOS Keychain.

## Identity and entities

- Entities are `anchored` (from a registry, such as the incident.io catalog, or the owner) or `discovered` (found by an LLM in the text).
- Reconciliation (discovered × anchored) is a typed LLM decision with its own role (`reconciliation`, Sonnet by default, swappable on the web). Small models treat "belongs to" as "is the same" and merge PRs into repositories and systems into teams, so the schema forces the model to say what the discovered entity is and how it relates to the candidate; only `same` merges, and when in doubt it does not.
- The owner takes part in reconciliation: the catalog entry merges into them and they receive its external identifiers; the catalog sync finds the owner by the merged entry and never renames them.
- A merge records the aliases and external IDs it added, and undoing it returns them. An invalid or missing decision skips only that item.
- A resolution may choose any candidate ID shown in the request, including one that came from another mention of the same request.
- The team is a list on the web (per person: email, Slack, GitHub and other identifiers). An "import from catalog" button copies a team from the catalog; it never syncs on its own.

## Conflicts, drift and verdicts

- **Integration** confronts each batch of new claims with its neighborhood; when it decides "tension" without a target, the tension opens over the new claim only, and the target may be another new claim of the same batch.
- **The adjudicator** judges from a dossier and explores freely, with live-read tools: current file, CODEOWNERS and code search on GitHub, the current page on Confluence, the thread on Slack. The reach is everything the owner's credentials can read, not only the allowlist. Only what became proof in a verdict is stored as evidence, with provenance "read live on such date".
- **Verdict cache:** the key includes the dossier hash, the model and the prompt version. A verdict reverted by the owner stays in force until the dossier changes. A verdict is not applied if the dossier changed during the judgment (evidence erased meanwhile); the adjudicator judges again.
- **The sweep** covers drift with no trigger. It examines one entity per round, in rotation, with priority computed by plumbing (old documentation with newer activity in another source, what Push and MCP consult most, what has gone longest without review). It runs only with the cognition queue empty, under a daily budget configurable on the web. It **detects and collects** (stores as evidence what it read live) and opens a tension with a **structured dossier** (suspect claims, evidence by ID, hypothesis, reason); the **adjudicator judges**, treating the hypothesis as an accusation, not a conclusion. Whoever accuses is not whoever judges: this avoids confirmation bias and makes each detector's precision measurable. A failed read or judgment is never reported as "nothing found"; precision per detector comes with a Wilson interval and a warning for n < 30.
- **Confluence edits are not retractions** (exception to D18 for versioned sources): a page edit is often a correction but also often an evolution. The old version of the section stays as history, and the claims of changed or removed sections go to the adjudicator with both versions: still holds, evolved (`superseded`, validity until the edit) or was wrong (`retracted`). Chat messages and comments keep edit = retraction.
- **Declared status** on pages (FINAL, obsolete, DRAFT) is a signal for the LLM, never truth.
- **Superseded-by link:** a "superseded" claim records, as structured data, the claim that took its place (written by integration, correction and the adjudicator). Trajectories use the link, never summary text.
- Retained claims and verdicts keep their dossiers; undoing a verdict only undoes what is still as the verdict left it.

## Ingestion and sources

### Claude Code sessions
- The allowlist is **per chat**, not per project folder. Chats are listed by the title shown in the app, with project and last activity. A new chat appears disabled and is turned on by hand: many chats are one-off questions or opened in the wrong folder.
- Chat identity is the app's fixed ID (`sessionId`, title, `cliSessionId`, `priorCliSessionIds`); renaming only changes the title, and resuming (a new file) continues the same chat. A chat without that record (terminal) is identified by the chain of files that share messages. Deduplication by message ID applies across all chats and sources.
- The owner profile is extracted from **all** chats, only the owner's messages and only preferences, with the assistant's previous reply as non-citable context. Turned-off chats feed only this profile. Chats of Pythia itself are not a source: the assistant's text would repeat Pythia's own data.
- Enabling a chat reads it whole as backfill (batch and deferred synthesis).

### GitHub
- API only, with a personal token in the Keychain; no `gh` CLI or anything from the user's local environment. Token type chosen on the web: **classic** (default; `repo` scope plus SSO authorization once; changing the allowlist needs no new approval) or fine-grained (read-only; re-approval at every repository change). A classic token is checked before reading.
- Repositories are added by hand; they come in paused with a backfill estimate.
- PR content: no commit messages; the **whole diff per file**, never cut (each file is an evidence item; a patch missing on GitHub is recorded as missing); each inline comment carries its `diff_hunk`; a PR larger than the context becomes several per-file units. A configurable list of ignored paths, empty by default.
- Bots: every author GitHub marks as a bot is ignored, and so is the entire thread that starts with one, including human replies. An exceptions list lets the owner read specific bots. The criterion is structural, never the name.
- Per-repository mode: all / only PRs involving the owner / the owner or the team (default). Horizon: 6 months, configurable per repository.
- Ignoring an author, ignoring a path or changing the mode **retracts what came only from there** (tombstone + cascade, reversible). A repository has two actions: **Pause** (stops reading, keeps) and **Remove** (stops and retracts what came only from it).
- Content is never cut; whatever one does not want to read leaves through the ignored-authors list, which is the owner's decision, not the system's.

### Slack
- API only, with the owner's **user token** (`xoxp-`) in the Keychain, no bot. `auth.test` rejects a bot token and checks scopes; each conversation checks the scopes of its type (`users:read` always; `channels:*`, `groups:*`, `im:*` or `mpim:*` read+history).
- The owner's conversation list appears in Sources; each is added by hand and enters paused, with an estimate (the 90-day history counted without an LLM × the measured cost per character).
- **Units:** a *thread* (the replies to a message) settles 30 min after the last reply, and a new reply becomes a delta; a *window* groups channel-level messages while the gap between one and the next is under 30 min, settles when the channel is idle that long and lasts at most 6 h. The message that opens a thread stays in the window; the thread keeps only the replies. Times are configurable.
- **Context:** a thread delta or window edit reaches extraction with the rest of the conversation in a "CONTEXT" section without `[E#]` labels, which cannot be cited. It is read at processing time, only for what is still active, up to 60 thousand characters; erasing a message used as context reprocesses the units that used it.
- **Edits and deletions:** the last 7 days are re-read in full at each read, and the thread replies of that period every 6 h. An edit retracts the previous version. A vanished message becomes a tombstone only if the re-read covered its whole window. Threads older than the review but with a reply in the last 30 days are watched with a cheap `oldest` query every 6 h and re-read when there is news. Limitation: an edit to a message older than the review is not seen, nor a new reply to a thread idle for more than 30 days.
- The hash is over the raw text (renaming a person is not an edit). There is a per-source lock for read, reapply and remove, and state is saved per key as a checkpoint.
- Bots: structural criterion (`bot_id` or `subtype=bot_message`), same rule as GitHub (`read_bots` are the exceptions). Ignored authors and subtypes are owner lists (default `channel_join`, `channel_leave`). A caveat: the rule also discards human discussion in threads opened by alert bots; the way out is to put that bot in `read_bots`.
- Formatting (plumbing): `<@U…>` becomes `@Name [slack:U…]` (the ID helps resolution, since catalog people carry `slack:U…` aliases); channels, links, `@here` and HTML entities are translated; files enter as `[file: name (type)]` without downloading.

### Confluence
- The allowlist is **spaces and page trees**: a whole space or trees of pages within it; a new page in an enabled tree enters on its own. Blog posts enter when the space is enabled whole. Added sources come in paused with an estimate.
- Content: pages and **comments** (footer and inline; the inline one carries the commented excerpt). Pages about the owner enter (D21 restricts only colleague profiles). Backfill reads only the current version.
- A page is split by headings and each section is an evidence item; a changed section is extracted with the rest of the page as context.
- Deleting a page erases everything, including derivatives; archived pages are only marked. A page that leaves the enabled tree leaves the base after two reads (another enabled source may take over without re-reading). Never deletes on an incomplete listing; a 404 for lack of access to the space deletes nothing.
- Credential: an Atlassian token **with read scopes** (page, comment, space, hierarchical-content) in the Keychain; the web also accepts the classic token (email + token). Ignored authors, Pause and Remove work as in the other sources.

### incident.io catalog
- Deterministic reading, no LLM, with a key that needs only `catalog_types.view` and `catalog_entries.view`. `anchored` entities with external identifiers; reference attributes become claims that go through LLM integration. A configurable list leaves useless attributes out; SCIM groups pointed to by a team are folded into it.

### Cross-source rules
- Edit = retraction; deletion really erases, including derived text (mention context, retracted statements — redacted in the log itself, the only exception to append-only —, unit titles and triage, page text including old versions, tension reasons and dossiers, live-read proofs).
- Deduplication is per source; erasing one file does not erase messages that still exist elsewhere.
- Never delete from an incomplete listing.

## Backfill and cost

- Horizons per D16, configurable per source. Order: most recent first. It counts against the same cap.
- **Batch API** (50% discount, stackable with the cache) for triage and extraction **only in backfill**; day-to-day processing stays immediate. Resolution, integration and adjudication stay online and serial. An invalid batch result is redone up to 3 times; deletion cancels in-flight requests, and a request (which carries source content) is erased from the database when its result arrives.
- **Deferred synthesis:** pages and narratives of entities touched by a backfill are regenerated in a single pass at the end (via Batch API). Neither the periodic sweep nor online derivation may regenerate an entity that is deferred or has a pending or submitted batch synthesis.
- **Estimates:** a source comes in paused and the web shows the backfill estimate (items that pass the filter × measured average cost). The estimate shows only before the backfill; during and after, the column shows the **actual cost**, plus "+N to read ≈ US$X" when new items come in.
- **Regeneration of derivatives is grouped** (120 s per entity, 300 s for narratives) so it does not run at every unit.
- **Narratives:** at most one pass per hour (configurable from every 4 hours to every 5 minutes), with incremental input: only the affected narratives go in full, the others as summary only (a change to a summary-only narrative is not applied blindly: it comes back in full in a following pass), and only the pages that changed. A full review pass is available. A narrative with no cited claim still current or disputed is retired by the cascade; the pass shows the current claims under each narrative, and the prompt says "one story, one narrative": update instead of create, merge overlaps, keep citations.
- **Models:** the current ones. Older models are no longer cheaper (Haiku 4.5 is the floor). Extraction moves to a smaller model only if the golden questions show quality holds.
- The monthly cap is configurable on the web.

## Consumption (push and pull)

- **Push** only chooses IDs. The text injected is assembled by the code: the owner's L0, the L0 of each chosen ID exactly as in the map, and the summary of the chosen verdicts. Nonexistent IDs are discarded. This prevents the briefing from narrating the current request or inventing context; text that is not an owner request (subagent report, notification) is handled by the LLM, which chooses no ID.
- **Push without repetition:** it delivers only what the chat has not received. The chat includes all its session files, and the same ID with the same version does not come back. "Already delivered" restarts when the LLM loses context: `startup`, `/clear` or a compaction; `resume` does not restart. The owner profile goes once per conversation; pages on the subject of the request come back when they change version.
- **Rules** (when to use Pythia) are a fixed text from the settings, injected by the hook at the start of each conversation and after `/clear` or compaction. **Guides** (how to use each tool) live in the MCP tool descriptions and server instruction. The rules are short on purpose: too many rules make an agent explore more and perform worse.
- **Push map:** the L0 map is limited to 400 items (narratives first, then the most recently generated pages), which protects prompt size and keeps the prompt cache. See known limitations.
- **Usage screen:** per chat, what was injected (and what was not, and why), and each MCP call (tool, arguments, client and session, returned references). It stores **only versioned references** and rebuilds exactly what was delivered, without duplicating text; what was erased at the source shows as erased. Pull is not tied to a chat because the MCP does not report the Claude Code session, and guessing by time would be a heuristic.

## Evolution

- One line per cause (source item, judgment, correction, sweep, source read), with the before/after of the claims it touched, most recent first. A source's backfill is a single line. No "since the last visit" cursor: descending date order is enough.
- Each event records its cause (`events.cause`: `unit:`, `tension:`, `correction:`, `sweep:`, `source:`, `synthesis:`, `startup:`), defined by the job that produced it. The `events_caused` view computes a cause for events without one (the evidence's unit, the verdict's tension, or the same transaction). Page regenerations are grouped by hour into one line; with the entity filter they appear one by one.
- The L1 diff between versions of a page; a redacted version shows as redacted. The trajectory of an attribute per entity groups by predicate (two names for the same attribute give two chains).
- What produced nothing appears with the recorded reason, hidden by default ("hide no effect").
- "Updating since T" for a page with a pending regeneration (`views.stale_since`, kept by a database trigger).
- Deterministic and simple: a list assembled from the event log, no LLM summary.
- Verdicts are Evolution lines, with Revert on the line; metrics live in Quality and tensions under judgment in "Processing now".

## Sources screen

- **Processing now**, at the top and refreshing itself: each worker (what, stage, time in the stage, forecast from the measured mean duration), the queue per lane with the forecast of when it empties, the batches (how long ago sent, forecast from the average of previous ones) and what is waiting on the LLM, with the reason. Per-source progress is left out (chained forecasts are unreliable).
- Tables wrap long text and scroll inside their card.

## Quality and measuring value

- **Corrections** (`correct`, revert, relink) are typed errors; rate per role and source.
- **Golden questions** are written from the sources, never from what Pythia extracted: kinds (simple fact, multi-source, temporal, update, abstention, profile), with evidence, half sealed by hash of the question (the sealed half only decides), a judge checked against plausible wrong answers, three runs and a dumb baseline (an LLM with text search over a dump of the sources). A prompt or model change only goes in if no sealed question that passed starts failing, or with a written justification. Each run stores its cost. They run at every `prompts.VERSION` bump.
- The value of the whole system is measured by the method in [measuring-value.md](measuring-value.md): honest cost (maintain × serve × measure), usage of what is delivered, avoidable re-explanation and cost of getting oriented, a randomized per-session experiment and ROI as an interval. The ROI assumes a loaded hourly cost in a stated range (US$50–100); break-even is about 3 to 9 minutes per workday.
- The focus of the proof of value is an aggregation task (what only the big picture allows: a report over months of work, how a topic evolved), with a realistic baseline: Claude Code with the connectors the user already has and no extra instruction.
- Preference recorded through `define` is not echoed in the delivered text ("…as you asked").

## Operation

- Default monthly cap, sweep budget, intervals and thresholds are settings on the web.
- Only one daemon may run: `stop` takes down every `pythia run` process (SIGKILL after 10 s) and `start` refuses to start if there is one. After changing code, confirm that only one is running.
- Saving the configuration resumes the queue immediately.
- Robustness rules across connectors: a lock per source, state saved by key, resumable processing, explicit errors instead of silent defaults, and batch aging measured by the database clock.
