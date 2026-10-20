# Known Limitations

Problems and limits of Pythia as it is. Each item says what you see, why it happens and what the fix would look like within the rules of [design-rationale.md](design-rationale.md).

## A Confluence edit can generate two judgments on the same claims

| | |
|---|---|
| Component | `connectors/confluence.py` (`_read_page`), `pipeline/process.py` (integration) |
| Cost | low; nothing ends up wrong |

**Symptom.** When a section changes, the unit with the new text (priority 40) is processed before the edit tension (priority 50). Integration of the new claim may open its own tension against the old claim, and the adjudicator then judges twice.

**Cause.** The edit tension and integration detect the same thing through different paths.

**Fix.** Either integration does not open a tension over claims already in an open `confluence_edit` tension, or the new claim is included in the edit tension's dossier.

## The push map shows only the 400 most recent pages

| | |
|---|---|
| Component | `pipeline/derive.py` (`knowledge_map`), `consumer.py` (`briefing`) |

**Symptom.** The push LLM chooses IDs from an L0 map limited to 400 items: narratives first, then the most recently generated pages. With ~1,900 active derivatives, about 80% of the base never reaches the push.

**Cause.** The limit protects the prompt size (the map goes in the prompt cache), and ordering by recency is a simple default.

**Fix.** Rethink the map selection, measuring with the golden questions and the Usage screen. A search centered on the `cwd` repository was evaluated and should not be used as is:
- bubble: cross-cutting knowledge (profile, people, team decisions, other systems) loses to what is near the repository;
- ambiguous `cwd` (a folder that is not a repository, a chat opened in the wrong folder);
- hubs (the owner, the team) leave almost everything two steps away, and distance stops separating;
- wrong links become wrong proximity;
- a per-directory map breaks the prompt cache and makes every push more expensive.

## A narrative leaves "outdated" without having been updated

| | |
|---|---|
| Component | `pipeline/derive.py` (`derive_narratives`) |

**Symptom.** When claims cited by a narrative stop holding, the narrative becomes outdated and goes in full to the narrative pass. If the LLM decides not to rewrite it, the pass still clears the outdated mark, and the text keeps asserting what the retracted claims said.

**Fix.** The mark only leaves a narrative if it was rewritten or if no cited claim changed status; or the prompt requires an explicit decision (keep, update, retire) for each narrative with changed claims. Watch the cost: it must not become an hourly pass that never ends.

## Narrative consolidation is not spontaneous

**Symptom.** Merging overlapping narratives in the normal pass is not reliable: the model tends to create new narratives rather than fold old ones.

**Fix.** If fragmentation returns, add a dedicated consolidation pass that receives only the narratives, instead of mixing them with pages and recent claims.

## Short remarks in the owner profile

**Symptom.** Short reactions ("shorten this, it's wordy") are the hardest preferences to capture. Extraction receives the assistant's previous reply as context, but the profile questions of the golden suite remain the weakest kind (67%).

**Fix.** Keep measuring with the profile golden questions; improve the extraction prompt and the context given to it.

## Retrieval misses

**Symptom.** Where Pythia loses to the text-search baseline (multi-source and time questions), the information was not extracted or not found.

**Fix.** Improve extraction and triage recall, and search; compare with the baseline on each golden run.

## Slack limits

- An edit to a message older than the 7-day review window is not seen, nor a new reply to a thread idle for more than 30 days.
- Threads opened by alert bots lose their human discussion unless the bot is added to `read_bots`.
- The backfill estimate uses the per-character cost of other sources until Slack's own cost is measured.

## Equivalent attributes

The trajectory of an attribute groups by predicate, so two names for the same attribute give two chains.
