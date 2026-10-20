# Measuring Value

How to show that Pythia's cost makes sense and that it brings value: what others do, the metrics, the experiment and the results of the evaluation. The decisions are in [decisions.md](decisions.md#quality-and-measuring-value).

## Summary

1. **Nobody proves the value of LLM memory with real use.** Graphiti, Mem0, Letta, gbrain and OpenViking rely on synthetic benchmarks (LoCoMo, LongMemEval, DMR, BEAM), full of problems and of disputes between vendors. Their numbers do not transfer; their **method** does.
2. **The effect may be null or negative.** In the METR study, experienced developers were 19% slower with AI while believing they were 20% faster; in a 2026 paper, context files (AGENTS.md) lowered agent success and raised cost by more than 20%. So the owner's impression proves nothing, and the push has a plausible null hypothesis.
3. **The cost bar is low.** A budget of US$100–150 per month against an assumed loaded engineering cost of US$50–100 per hour is **1 to 3 hours per month, or about 3 to 9 minutes per workday**. Proving that requires measuring minutes (or a good proxy), not benchmark hits.
4. **The metric closest to the goal** ("not keep re-explaining myself") is **avoidable re-explanation**: how many times the user explains to an LLM something Pythia already knew. No project studied measures it.
5. **The design that proves it:** randomize, session by session, whether the push is on, and measure in the transcripts with a judge that does not know the arm. With ~10 sessions per week, a large effect takes **6 to 8 weeks**; a small effect will be "inconclusive", and that must be said as such.
6. **Almost all possible value arrives through the push.** The MCP is called rarely because the AI does not know *when* to ask. **Rules** (when to use Pythia) attack that; **cartridges** (task-oriented knowledge packages the AI asks for) would change the push from "guessing the content" to "offering the menu".

## 1. What "value" means

| Axis | Question | Unit |
|---|---|---|
| Re-explanation | Did the user need to say again what Pythia already knew? | times per session |
| Getting oriented | How much did the LLM spend before doing something useful? | tokens, tool calls and turns until the first useful action |
| Cost | How much does Pythia cost to maintain and to serve? | US$/month, US$ per use |

Value is what the first two save, converted into time, against the third.

## 2. What others do

| Project | How it proves | Problem |
|---|---|---|
| **Zep/Graphiti** | DMR and LongMemEval; accuracy, latency and context tokens against "full context" (LongMemEval with GPT-4o: 60.2% → 71.2%, latency 28.9 s → 2.6 s, context 115k → 1.6k tokens) | DMR measures little; **does not measure the cost of building the graph**; worse at "remembering what the assistant said" (94.6% → 80.4%) |
| **Mem0** | LoCoMo with an LLM judge, p50/p95 latency, tokens (66.9% against 72.9% for full context, with 91% lower p95 latency and ~90% fewer tokens) | Loses to full context on accuracy; public dispute with Zep over how the competitor was configured |
| **Letta/MemGPT** | DMR; later Context-Bench with accuracy **and cost in US$** | A grep agent over files (74%) beat Mem0 (68.5%) on LoCoMo; conclusion: what matters is using retrieval in the real flow |
| **LongMemEval / LoCoMo** | The benchmarks themselves | Synthetic personas; in LoCoMo everything fits the window; an audit found 6.4% wrong answer keys and a judge that accepts 63% of wrong answers "on the right topic" |
| **gbrain** | **Pre-registered** evals, a **sealed** set, a gate that **kills the feature**; BrainBench with *know-to-ask* and *false fire* rates | No human labels; generated corpus; the "delivered context was used" signal is approximate |
| **OpenViking** | LoCoMo/LongMemEval against each tool's native memory; tau2 with and without memory; a **retrieved → actually read** funnel; **"asked again within 10 min"** | Generous judge; weak, different baselines; unclear whether token counts include ingestion |

Details of the three memory systems Pythia compares itself with are in [design-rationale.md](design-rationale.md#gbrain-openviking-and-graphiti).

**Worth copying:**
1. With and without, on both axes at once. The honest "without" is not "115k of history" but **what the user does today without Pythia**.
2. Pre-registration and a gate (hypothesis, metric, bar and window written before looking at the result).
3. A score per kind, never a single score.
4. Abstention questions, whose right answer is "I don't know".
5. Check the judge with plausible wrong answers.
6. Cost of writing separate from the cost of reading.
7. Precision and recall of the delivered context; delivered → used; "asked again".
8. A mandatory dumb baseline: if a grep over a dump of the sources ties with Pythia, the cost is not justified.

**Not worth copying:** running public benchmarks (no Slack, Confluence, GitHub, provenance or cross-source contradiction); vendor numbers; F1/BLEU; a "generous" judge; eval budgets of hundreds of dollars.

## 3. Metrics, in layers

From the cheapest to the one that proves the most.

### 3.1 Honest cost (SQL only)
- **Maintain × serve × measure**, per role, per source, per month. Maintain = ingestion, synthesis, sweep; serve = the push; measure = golden questions. Backfills are one-off spend and distort averages, so the report shows spend per day over the last 14 days instead of a projection.
- **Declared coverage:** the report says what it does **not** count (local embeddings, the tokens on Claude Code's side, the owner's time maintaining Pythia).
- **Cost per claim that reached an LLM**, per source: it shows which sources pay the bill and which only cost. Online synthesis serves every source and is not divided per claim.

### 3.2 Usage (SQL + transcripts)
- **Delivered → used:** a pushed reference counts as used if, after the push and in the same session, the assistant's reply cites its ID or content, or it is opened through the MCP. It is approximate (paraphrase is missed; a cited ID may not have helped), and the report says so. The transcript has the `mcp__pythia__*` calls with arguments, so the MCP session need not be linked to the chat; the hook's `session_id` links push to transcript.
- **Pull funnel:** `search` → `open`, and at what level. Much level 3 (raw evidence) means the summary is not enough.
- **MCP usage rate per session**, the natural metric for the rules.

### 3.3 Offline quality (golden questions)
- 40–60 questions written by a subagent **reading the sources directly**, never from what Pythia extracted.
- **Kinds, with a score per kind:** simple fact, multi-source, temporal, **update** (the fact changed; the current one must come back), **abstention**, owner profile. Update and abstention are Pythia's differentiator and what public benchmarks do not measure.
- **Answer key with evidence:** each question carries the answer, the evidence and the IDs retrieval should bring, so `recall@k` is measured beyond the answer.
- **Sealed half** by hash of the question: the calibration half can be used for tuning, the sealed half only decides.
- **Checked judge:** plausible wrong answers it must fail; a strong model as judge.
- **Three runs,** reporting mean and range; **the cost of the run stored with the run**, reported as cost per hit.
- **Dumb baseline:** the same questions answered by an LLM with text search over a text dump of the same sources.

### 3.4 Real value (transcripts + blind judge)
- **Avoidable re-explanation (north star).** For each owner message that asserts something, a judge decides whether it repeats what the claims current *before the session* already said (time travel through the event log; only messages after the first claim count). Three classes: it was in the push and was explained anyway; it was in the base and was not delivered (*know-to-ask* failure); it was not in the base. A repetition inside the same conversation, with no compaction in between, is a separate signal (the AI had the information and did not follow it) tied to the rules.
- **Cost of getting oriented** (by code, no LLM): input tokens, exploration calls (Read, Grep, Glob, read-only Bash) and turns **until the first useful action**, from each turn's `usage`.
- **Corrections and clarifications:** how many times the user says "no, it's X" and how many clarifying questions the assistant asks. **"Asked again":** the user returns to the same subject right after.
- **The judge does not see the arm:** the block injected by the push is removed from the transcript before it judges. Cost ~US$0.02–0.05 per session.
- **The owner's score (1 to 5, "how much I had to re-explain")** is shown **next to** the objective metric, to expose the gap between perception and measurement.

### 3.5 ROI
- Value = minutes saved per session × sessions per month × loaded hourly cost (assumed range US$50–100), minus Pythia's API cost, minus the owner's time maintaining it (building time is investment, not operation).
- Convert re-explanation into minutes by the average time to write one, measured in the transcripts (approximate; say so).
- Present as an **interval**: the effect's confidence interval × the hourly-cost range. "Pays for itself" only if the entire interval passes the break-even (about 3 to 9 minutes per workday).
- Tokens saved in Claude Code become money only if usage is paid by API; on a subscription they become time and usage limit.

## 4. Evaluation results

A single-user, self-hosted evaluation over the user's own sources. The numbers show the method at work, not a general claim.

### Golden questions (50 questions, 3 runs)

| Kind | n | Pythia (mean, range) | No Pythia (text search) | Evidence recall |
|---|---|---|---|---|
| Simple fact | 12 | 72% (67–75) | 58% | 83% |
| Multiple sources | 10 | 57% (40–80) | 70% | 72% |
| Time | 8 | 83% (75–88) | 100% | 81% |
| Update | 8 | 88% (88–88) | 75% | 70% |
| Abstention | 6 | 100% | 100% | — |
| Owner profile | 6 | 67% | 83% | 61% |
| **Total** | 50 | **76% (70–82)** | **78%** | 75% |

- Pythia **ties with the dumb baseline** overall; the variation between its own runs (70–82%) is larger than the difference.
- It **wins where it is different**: update (88% × 75%) and simple fact (72% × 58%).
- It **loses where retrieval failed** (recall 0): the information was not extracted or not found (a colleague's availability, a stated preference for how to explain technical topics, who to ask about priorities). These are extraction/triage or search failures, not reasoning ones.
- **Bias in favor of the baseline:** the questions were written from raw text, so they use the words of the text, and the corpus is small enough that all sources fit in one search.
- The judge failed all 100 plausible wrong answers. A Pythia run costs about the same as a baseline run (US$0.098 per hit); the difference is the cost of **maintaining** Pythia, which the baseline does not have.

### Re-explanation (35 sessions, excluding Pythia's own chat)
- 540 owner messages; 240 assert something; most pre-date the first claim and cannot repeat anything. Of the **41 assertions after the first claim, in 7 sessions, 1** repeated what Pythia already knew (not delivered); 3 more repeated something said in the same conversation.
- Median until the first file edit: ~2.2 million tokens processed (counting the re-reading of context at each call) and 24.5 exploration calls.
- **Caveat:** the judge was validated by another LLM with the full context (weaker than a human), and only one chat was enabled as a source, so the metric has little power until more chats feed the base.

### Usage
- Explicit use of pushed references is close to zero (the only uses were IDs cited while debugging Pythia itself), and the MCP was used only in that conversation. This does not prove the push does not help (the AI may use content without citing it); it shows the answer depends on the blind judge. Analyses must exclude conversations about Pythia itself.

### Aggregation task (blind test)
A report summarizing several months of work, drafted with and without Pythia, in randomized order. Blind, the user preferred the draft **with Pythia** (small difference), and only the draft without Pythia had a known factual error. Effort: 1.5 min and 1 message with Pythia against 231 min and 12 messages without. **Positive by the pre-registered criterion, but confounded:** a Confluence page already held the final content, published after the no-Pythia draft. What can safely be attributed to Pythia: the rules made the AI consult it on its own. The next test should use a task whose answer is not yet written anywhere.

## 5. Rules and tool guides

| | What | Where | In the experiment |
|---|---|---|---|
| **Tool guides** | **How** to use each tool: what `search` returns, when to descend with `open` (level 1, 2, 3), what `timeline` shows, when `correct` and `define` | In each MCP tool's description and in the server instruction | **The same in both arms** (part of the tool) |
| **Rules** | **When** to use Pythia: before asking the user about a person, system, decision or bug; when they mention something without explaining; when they say something is wrong (`correct`); cite the IDs used | Delivered **by the hook** at `SessionStart` | **Only in the on arm** (part of full Pythia) |

The rules go through the hook, not `CLAUDE.md`, because `CLAUDE.md` applies to every session and cannot be randomized. They are short (up to ~10 lines), because too many rules make agents obey and explore more and perform worse. They are a hypothesis, measured by useful MCP usage per session, avoidable re-explanation and tokens until the first useful action; if they raise tokens without reducing re-explanation, they go.

## 6. The experiment

**Question:** does Pythia, as it is, reduce re-explanation and the cost of getting oriented?

- **Arms:** *on* = push + rules via the hook + MCP with guides; *off* = the hook runs but returns empty. The MCP stays available with the guides; residual use is recorded and reported.
- **Unit and randomization:** the Claude Code session. At `SessionStart` the hook draws in balanced blocks (every 4 sessions, 2 of each arm, in random order) and records the arm; the user neither chooses nor looks. Out of the analysis: sessions about Pythia itself and resumptions of earlier sessions.
- **Duration and power:** with ~10 sessions per week, 6 to 8 weeks give ~30–40 per arm and detect a large effect (d ≈ 0.6–0.8); a small effect comes out "inconclusive" (power estimate by the research agent, not taken from a source).
- **Frozen:** the Claude Code model, `prompts.VERSION`, guides and rules, `CLAUDE.md` and **Claude Code's auto-memory** (it competes with Pythia and would leak what an on session taught an off one). Ingestion continues in both arms.
- **Metrics:** primary = avoidable re-explanation per session; secondary = cost of getting oriented, corrections and clarifications, total session tokens, the owner's 1–5 score.
- **Pre-registration:** hypothesis, metrics, bars and window in a commit before starting. Proposed bar: Pythia is worth it if it reduces avoidable re-explanation by **≥ 30%** without increasing tokens until the first useful action, and if the estimated saving passes the break-even.
- **Analysis:** difference of medians per arm, stratified by task type and repository, bootstrap interval and permutation p-value.
- **Report:** effect with interval → minutes per session × sessions per month × hourly cost against API cost plus maintenance time, with the owner's score alongside.
- **Risks:** few data (only large effects show; "inconclusive" is an honest result); contamination (what the user learns in an on session goes with them to the off one, so the measured effect tends to be underestimated); a wrong judge (hence validation against the user's own markings); change of behavior from being measured (randomization helps).
- **If it shows no effect** the next step gains importance, because the problem may be the push guessing badly, not the idea of giving context.

## 7. Next: knowledge cartridges

Instead of Pythia guessing what to push, the AI asks for a ready-made package when it needs it: a **task-oriented** bundle ("work on system X", "on-call for team Y", "how the owner reviews PRs") with procedures, current decisions, people, pitfalls and style, within a token budget, versioned and citing claims. Different from a **page** (about one thing) and a **narrative** (one story). Reachable through the existing tools (`search` finds, `open(id)` loads), so the interface stays at five tools; the push becomes the menu (the owner's short profile and a one-line list of cartridges); generated on demand and regenerated only when requested again after a change. It depends on the AI knowing how to ask, so it builds on the rules, and it is one more derivative to keep fresh. It would be tested with the same method: rules on in both arms, content push × cartridge menu, with "cartridge loaded → used" as an extra metric. Open design questions: who defines the cartridges, when they are born, their relation to narratives, the token budget and the menu size.

## Sources

**LLM memory:**
- [Graphiti](https://github.com/getzep/graphiti)
- [Zep paper](https://arxiv.org/html/2501.13956)
- [Mem0 paper](https://arxiv.org/html/2504.19413) and [Mem0 on LoCoMo](https://mem0.ai/blog/locomo-benchmark)
- [Zep × Mem0](https://www.getzep.com/blog/lies-damn-lies-statistics-is-mem0-really-sota-in-agent-memory/)
- [MemGPT paper](https://arxiv.org/html/2310.08560)
- [Letta: Benchmarking agent memory](https://www.letta.com/blog/benchmarking-ai-agent-memory) abd [Letta: Context-Bench](https://letta.com/blog/context-bench)
- [LongMemEval](https://arxiv.org/html/2410.10813)
- [LoCoMo](https://arxiv.org/abs/2402.17753) and [LoCoMo audit](https://dev.to/penfieldlabs/we-audited-locomo-64-of-the-answer-key-is-wrong-and-the-judge-accepts-up-to-63-of-intentionally-33lg)
- [Mechanism-level evaluation](https://zylos.ai/research/2026-08-13-mechanism-level-agent-memory-evaluation)
- [A-Mem paper](https://arxiv.org/pdf/2502.12110)
- [MemoryAgentBench](https://arxiv.org/abs/2507.05257)
- GBrain: [`docs/eval/BRAINBENCH.md`](https://github.com/garrytan/gbrain/blob/f7f52b5/docs/eval/BRAINBENCH.md), [`docs/eval/CORE_MEMORY_PREREGISTRATION.md`](https://github.com/garrytan/gbrain/blob/f7f52b5/docs/eval/CORE_MEMORY_PREREGISTRATION.md), [`docs/eval/system-one/PROTOCOL.md`](https://github.com/garrytan/gbrain/blob/f7f52b5/docs/eval/system-one/PROTOCOL.md), [`docs/eval/decisions/p6-fact-keys/README.md`](https://github.com/garrytan/gbrain/blob/f7f52b5/docs/eval/decisions/p6-fact-keys/README.md), [`src/core/context/volunteer.ts`](https://github.com/garrytan/gbrain/blob/f7f52b5/src/core/context/volunteer.ts), [`src/core/ops/usage.ts`](https://github.com/garrytan/gbrain/blob/f7f52b5/src/core/ops/usage.ts)
- OpenViking: [`benchmark/locomo/claudecode/`](https://github.com/volcengine/OpenViking/tree/25ad26a/benchmark/locomo/claudecode/), [`benchmark/tau2/`](https://github.com/volcengine/OpenViking/tree/25ad26a/benchmark/tau2/), [`openviking/usage_reporter/extractors.py`](https://github.com/volcengine/OpenViking/blob/25ad26a/openviking/usage_reporter/extractors.py), [`bot/vikingbot/observability/outcome.py`](https://github.com/volcengine/OpenViking/blob/25ad26a/bot/vikingbot/observability/outcome.py)

**AI productivity and context:**
- [Copilot controlled experiment](https://arxiv.org/abs/2302.06590)
- [Microsoft/Accenture study](https://economics.mit.edu/sites/default/files/inline-files/draft_copilot_experiments.pdf)
- [Google study](https://arxiv.org/abs/2410.12944v3)
- [METR: experienced open-source developers (2025)](https://metr.org/blog/2025-07-10-early-2025-ai-experienced-os-dev-study/) and [METR: uplift update (2026)](https://metr.org/blog/2026-02-24-uplift-update/)
- [SPACE framework](https://queue.acm.org/detail.cfm?id=3454124)
- [DX Core 4](https://getdx.com/research/measuring-developer-productivity-with-the-dx-core-4/)
- [Anthropic: Contextual Retrieval](https://www.anthropic.com/news/contextual-retrieval) and [Anthropic: Effective context engineering](https://anthropic.com/engineering/effective-context-engineering-for-ai-agents)
- [Sourcegraph Cody](https://arxiv.org/abs/2408.05344)
- [Evaluating AGENTS.md](https://arxiv.org/abs/2602.11988v1)
- [Single-case designs book](https://bookdown.org/dorothy_bishop/Evaluating_What_Works/Single.html) and [paper](https://arxiv.org/pdf/2412.15076)

**Not verified directly:** issue #5 of zep-papers and Zep's "84%"; Mem0's later numbers (LoCoMo 92.5) and the reproduction failures reported; the full Context-Bench methodology; the full tables of A-Mem and MemoryAgentBench; vendor ROI numbers (DX, "~2 h/week", "cut self-reporting in half"). The statistical power estimate is the research agent's, not from a source.
