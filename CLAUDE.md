# Pythia — instructions for Claude

Before proposing or changing anything, read in this order:

1. [docs/design-rationale.md](docs/design-rationale.md): why the project is the way it is, and the principles.
2. [docs/architecture.md](docs/architecture.md): the architecture.
3. [docs/decisions.md](docs/decisions.md): decisions D1–D22 and how each area applies them.
4. [docs/known-issues.md](docs/known-issues.md), [docs/measuring-value.md](docs/measuring-value.md).

## Non-negotiable rules

- Judgment, analysis and context are **always LLM**. Determinism only for plumbing (I/O, cursors, hashes, ACLs, watermarks, cascade, queues, cache, budget). No regex or heuristics for meaning.
- **No LLM, no function.** No fallback. Only raw ingestion and reading what was already concluded continue, with a notice.
- **No human approval gates.** Truth is correctable: the owner's corrections have maximum authority.
- **Rich internal agents, lean external interface**: 5 MCP tools plus the push.
- An edit retracts the previous version; a deletion really erases, including derived text.

## How to work here

- Changed a prompt or schema? Bump `pipeline/prompts.py:VERSION`; the golden questions run on their own.
- New decisions go to `docs/decisions.md`.

## Commands

```bash
.venv/bin/pythia start
```

```bash
.venv/bin/python -m pytest -q
```
