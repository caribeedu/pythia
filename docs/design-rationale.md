# Design Rationale

How and why Pythia is the way it is. The architecture is in [architecture.md](architecture.md) and the decisions in [decisions.md](decisions.md); this document gives the reasoning behind them. Before changing anything in the concept, read the principles (§3) and the checklist (§6).

---

## 1. Goal

Pythia is a **knowledge oracle** for someone who works with Slack, Zoom transcripts, Confluence, GitHub and many services. It should:

- keep a base of information that detects **drift, outdated information and contradiction**;
- **identify and reconcile** entities, people and systems;
- obtain data from the sources **continuously** and analyze it **regularly**.

The final goal has two parts:
1. **save on the LLM**: a base available over MCP lets the LLM query instead of reading and understanding everything;
2. a **faithful source of truths** that the user can consult.

In one sentence: *not keep re-explaining yourself to every LLM*. How you write, what happened with your team in yesterday's meeting, which decision was made for system X, what caused bug Y: an integration of the information of your team and your day-to-day, without re-explaining everything every day.

## 2. What the references taught

Pythia was designed after studying earlier approaches. Each lesson became a rule.

### Twin, the predecessor
Pythia is the spiritual successor of [Twin](https://github.com/caribeedu/twin), a local cognitive system for a single owner. Twin was abandoned after four months of use; the goal was right and the design was wrong. The architecture and product problems, and the fixes Pythia applies, are in [Why I stopped working on Twin](https://github.com/caribeedu/twin/blob/main/docs/WHY-I-STOPPED.md).

Lessons that became rules here: *regex, lexical heuristics or anything deterministic is not acceptable for judging meaning*; the internal decision mechanism needs more freedom and richer data structures; trying to create one interface that serves everything is a mistake.

### gbrain, OpenViking and Graphiti
Three projects solve parts of the same problem: [gbrain](https://github.com/garrytan/gbrain), [OpenViking](https://github.com/volcengine/OpenViking) and [Graphiti](https://github.com/getzep/graphiti). Pythia re-implements their common core (claims with validity, hybrid search, summaries per entity, session capture, hook push, an MCP server) and diverges where its rules demand it. None was adopted as a base, because each fails exactly where Pythia is demanding:

| Area | What they do | What Pythia does |
|---|---|---|
| Contradiction | Decided by a date rule (Graphiti), a cosine threshold (gbrain), or by rewriting memory in place (OpenViking); at best a report for the human | An LLM adjudicator decides, with a recorded, reversible verdict |
| Entity merging | By exact or similar name; no way to undo | Typed LLM decision, with merge, split and relink as reversible events |
| Deletion | Removes the item but leaves derived text, summaries and old versions | Erases derived text too, including page history |
| Authority | No human correction with maximum weight | Owner corrections always win and become metrics and examples |
| Interface | Up to 139 MCP tools (trimming the list measurably lowered task success) or a whole file system | Five tools, plus a small push |
| Cost | Ingestion cost not measured; synthesis storms in the issue trackers | Cost ledger, monthly cap, batch backfill with deferred synthesis |

**Ideas taken from them**, each with a test or a screen in Pythia:
- *Evolution tab* (gbrain's ingest log and version diffs, OpenViking's per-commit memory diff): one line per cause with the before/after of the claims it touched; the L1 diff between page versions; the trajectory of an attribute per entity; what produced nothing, with the recorded reason; "updating since T" for pages awaiting regeneration.
- *Judgment hygiene* (gbrain): the verdict cache key includes model and prompt version; a verdict is not applied if its dossier changed during the judgment; a failed read or judgment is never reported as "nothing found", and detector precision comes with a Wilson interval.
- *Profile extraction* (Graphiti, gbrain): extraction receives the previous assistant reply as non-citable context and records the user's preference literally.
- *Regression tests from their bugs*: unrelated facts must not supersede each other; a bare first name must not go to a namesake; an owner correction survives reprocessing; a deletion survives in no page version; deleting something must not regenerate the whole base; restarting must not requeue a regeneration per item.

**Considered and declined:** a "since the last visit" cursor (descending date order is enough); a mechanical quote check in synthesis (quotation marks in pages turned out to be search hints, not citations); batching several entities per synthesis call (the narrative pass was the real cost, so it runs hourly and incrementally); an identical-fact shortcut without an LLM (no identical current pairs exist); a stepwise token budget for the push and an explicit consolidation policy for narratives. A center-node search is open (see [known-issues.md](known-issues.md)).

## 3. Principles

| Principle | Why |
|---|---|
| Judgment, analysis and context are **always LLM**. Determinism only for plumbing. No lexical heuristic creates meaning. | Rule-based judgment fails in unexpected ways and cannot explain itself; the LLM can be asked, corrected and measured. (D6, D7) |
| **No LLM, no function.** No fallback. Only raw ingestion and reading what was already concluded continue, with a notice. | A lousy fallback is worse than no fallback. (D7, D19) |
| An **incorrect, correctable truth** is better than a pending hypothesis. No human approval gate. | Approval queues become bottlenecks and nothing becomes durable. (D3) |
| Correcting is cheap and has **maximum authority**. A correction becomes a metric and an example. | The owner's word is the strongest evidence, and corrections measure quality. (D5, D6, D17) |
| **Rich internal agents** (tools and live access to the sources); **lean external interface** (5 MCP tools + a small push). | Agents need freedom to investigate; consumers need a small, stable surface. (D7, D12) |
| The atomic claim is the source of truth. Derivatives cite claims, have a watermark and are disposable. | Drift and contradiction are detectable only when things are comparable; a summary of a summary hides errors. (D2) |
| A mention sits between claim and entity. Relink, merge and split are reversible, and the cascade does not redo extraction. | Entity resolution is contextual and will be wrong sometimes; fixing it must be cheap. (D5) |
| A conflict becomes a visible **Verdict**, applied immediately and reversible. | The user must be able to see, revert or override what was decided. (D6) |
| An edit retracts the previous version. Deletion really erases, including derived text. | If someone says the sky is green and then corrects it to blue, only blue should count. (D18) |
| Local, Claude, secrets in the Keychain, Postgres with event sourcing, Python. | Data stays on the machine; event sourcing gives "what was believed at T" and "when X was true". (D10, D14) |
| Configuration and sources on the web; the CLI only starts the process. | A single place to operate it. (D20) |

## 4. The design questions

The main questions, the options considered and the answer.

**Whose truth is it?** (A) a personal oracle, with what the user sees and their credentials, or (B) a shared organizational oracle with permissions per source and person. Answer: A, with the schema ready for B (provenance and per-source ACL), because B brings a political and security barrier that would block the project before the hard part is validated. (D1)

**What is the atomic unit of knowledge?** A page/document, a narrative, or an atomic claim with entity, attribute, value, validity, evidence excerpt, source, weight and extractor version. Answer: the claim as the source of truth, with *building blocks*, not one answer that synthesizes everything: Evidence, Mention, Entity, Claim, Tension/Verdict, Page, Narrative and Map. Every derivative cites claim IDs (never the text of the layer below), carries a watermark and is outdated *if and only if* a cited claim changed, and is disposable. (D2)

**Does a narrative condense or interpret?** It may interpret, but every interpretation becomes an `inferred` claim that goes through the same conflict machinery. A narrative is a **zoom level** (the L0 of a view), not a kind of object: L0 synthesis, L1 page, L2 claims, L3 evidence. Requests revealed **kinds of knowledge** that change in different ways (profile, episode, decision, entity state, causal explanation, then glossary and procedures), so staleness is a rule per kind, judged by an LLM. (D3, D4)

**Closed or open world?** A closed world (only what is anchored in a registry exists) was recommended; the answer was open: *entities are contextual* ("john" may be a coworker, a GitHub user or part of a JSON fixture). So the **mention** is the central object, resolution has three steps (candidates, a typed LLM decision, a new `discovered` entity), and swapping the entity of a claim and regenerating everything related must be easy. This separates a cheap, high-volume **typed decider** from an expensive **generator**. (D5)

**How is the truth chosen in a conflict?** Not by source-authority precedence (that starts from regex). The LLM notices "something odd", pulls everything related, decides, and the system saves that so the user can see, revert or change it: detection, then **adjudication by an agent** with a dossier and free exploration, then a persisted **Verdict**. The authority policy is guidance in the prompt, not a rule. Corrected verdicts become examples. (D6)

**Where is the boundary of the deterministic?** Plumbing only: I/O, cursors, hashes, ACLs, watermarks, cascade, reversal, queues, cache, budget. The "something odd" trigger is also an LLM (integration), because "same slot, different value" is itself a heuristic. Search assembles an initial dossier; the agent explores freely beyond it. (D7)

**When to process?** Natural units (thread, meeting, version diff, PR, session) when they **settle**, plus a periodic sweep. For LLM sessions, "when it ends" is too vague: debounce after each reply, with a cursor. (D8, D9)

**Where does it run, and with which models?** Locally, with Claude. Haiku is the typed decider with a closed schema; Sonnet handles extraction, synthesis, adjudication and sweep; prompt caching on everything that repeats; a configurable monthly cap with no priority-cutoff machinery, just a spend panel. (D10, D11, D15)

**Push and pull?** Both. Push by hook, small and chosen by an LLM; pull through a lean MCP of five tools: `search`, `open(id, level)`, `timeline`, `correct`, `define`. A wide tool list has a fixed token cost, worse choice among similar tools, and patches (like on-demand tool loading) that bring their own cost. (D12)

**Storage?** Postgres 17 with pgvector and event sourcing, Python, a repository of its own. (D13, D14)

**Backfill and allowlists?** Explicit allowlists of channels, DMs, spaces, repositories and chats; bounded horizons; backfill happens once per source item, not recurrently. (D16)

**Edits and deletions?** Editing regenerates: the old version is retracted. Deleting really erases. Archiving only marks. (D18)

**Colleague profiles?** Yes, but only observable professional traits: review style, technical conventions, expertise. No performance evaluation, personality or personal life. (D21)

The name **Pythia** is the priestess of the Oracle of Delphi.

## 5. What real use taught

- **Resumed sessions copy the history with the same message IDs** into another file; deduplication must be per source, and erasing one file must not erase a message that still exists in another session.
- **Silent defaults of judgment are errors**, the glossary belongs to resolution as an alias table, corrected verdicts must become examples, and the adjudicator's exploration must be recorded.
- **Deletion must erase derived text too**: statements in the log (redacted in place — the only exception to append-only), unit titles, page histories, dossiers and live-read proofs.
- **A typed decider must make invalid combinations impossible**: a schema of variants (`anyOf` + `const`) fixed "new entity" answers without a description.
- **Small models merge parts into wholes** (a PR into its repository); a dedicated reconciliation role with a stricter schema and a stronger model fixed it.
- **The push must not narrate**: letting the LLM write free text made the briefing answer the current request and invent context. It chooses IDs; the code injects stored text.
- **Cutting content does not fit the application**: a size cut for noisy bot comments was replaced by an owner-controlled list of ignored authors.
- **Batch and deferred synthesis cut the cost of a backfill** by about 40%, as long as nothing else schedules the deferred entities.
- **Narrative passes can dominate cost** when they send every narrative and claim each time; an hourly, incremental pass cut them by more than half.
- **Prefer structural criteria to names**: a human with a bot-like name is not a bot; a bot is identified by the source's marker.

## 6. Before changing anything

1. Does the change create a deterministic judgment? Then it does not go in; the judgment goes to the LLM.
2. Does it create a fallback for when the LLM is missing? It does not go in either.
3. Does it create a human approval gate? It does not go in either; the path is to correct afterwards.
4. Does it enlarge the external interface (more tools)? Prefer data with an ID and a zoom level.
5. Does it change a prompt or schema? Bump `prompts.VERSION`; the golden questions run on their own.
6. Does it touch deletion or editing? Make sure the derived text also goes away, and test `rebuild`.
7. Does it change the concept? Record the decision in [decisions.md](decisions.md).

## 7. Glossary

| Term | Meaning |
|---|---|
| Evidence | Original excerpt from the source (message, comment, catalog entry snapshot). Kept outside the log, so that it can be erased. |
| Unit | Group of evidence processed together (session delta, PR, definition). |
| Settle | Become ready to process (debounce, merge, idle for N days). |
| Mention | Occurrence of a name in an evidence item, linked to an entity; the relink point. |
| Claim | Atomic assertion with provenance, validity and status (current, superseded, disputed, retracted). |
| Anchored / discovered | Entity coming from a registry or from the owner / found by an LLM in the text. |
| Tension | "Something odd" detected by integration or by the sweep. |
| Verdict | Decision applied over claims (temporal supersession, real conflict, different scope, outdated source, duplicate, human correction). Reversible. |
| Page / Narrative / Map | Derivatives: L0/L1 per entity / per topic / index of L0s. |
| Watermark | Hash of the claims that generated a derivative; defines when it becomes outdated. |
| Dossier | What search assembles for the adjudicator before it explores. |
| Push / Pull | Briefing injected by hook / query through the MCP. |
| Golden questions | Regression suite with answers taken from the sources. Runs when changing the model or prompt version. |
