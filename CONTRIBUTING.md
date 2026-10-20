# Contributing

Thanks for your interest in Pythia. Before proposing a change, read [docs/design-rationale.md](docs/design-rationale.md): the project has a few non-negotiable principles, and changes that break them will not be merged.

## Principles in short

- Judgment, analysis and context are always done by an LLM. Determinism is for plumbing only (I/O, cursors, hashes, ACLs, watermarks, cascade, queues, cache, budget). No regex or heuristics for meaning.
- No LLM, no function. No fallback that pretends otherwise.
- No human approval gates. Truth is correctable, and the owner's corrections win.
- Rich agents inside, a lean external interface (five MCP tools plus the push).
- An edit retracts the previous version; a deletion erases everything derived from it.

## Development setup

You need Python 3.13+, [uv](https://docs.astral.sh/uv/) and Docker.

```bash
uv sync
```

```bash
docker compose up -d
```

```bash
docker exec pythia-db psql -U pythia -c "create database pythia_test"
```

```bash
uv run python -m pytest -q
```

The tests use a real Postgres, a fake LLM and a fake embedder, so they need no API key and cost nothing. Set `PYTHIA_TEST_DATABASE_URL` to use another database.

## Pull requests

- Keep a change focused, and add or update tests. Bug fixes should come with a regression test.
- If you change a prompt or a schema, bump `VERSION` in `src/pythia/pipeline/prompts.py`; the golden questions run on their own.
- If you touch deletion or editing, make sure derived text also goes away, and test `rebuild`.
- Record a new design decision in [docs/decisions.md](docs/decisions.md).
- Write code, comments and documentation in English.

## Reporting problems

Open an issue with what you did, what you expected and what happened. For security problems, follow [SECURITY.md](SECURITY.md) instead.
