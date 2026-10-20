"""Live reading: GitHub (current file, CODEOWNERS, code search) and Confluence (current page),
for the sweep and the adjudicator.

The reach is everything the owner's credentials can read, not just the allowlist. Each read gets an ID
(`read:N`) and lives only in the context of the call; whatever the agent uses as **proof** is stored as
evidence (source `live:github` or `live:confluence`, with "read live on such a date") and goes through the
normal pipeline, like any other evidence.
"""

from __future__ import annotations

import base64
from datetime import datetime, timezone

from ..connectors.common import Item, NotFound, SourceUnavailable, create_unit
from ..ids import sha
from ..llm.client import AgentTool, schema_object
from ..services import Services

STR = {"type": "string"}
PROMPT_CHARS = 40_000   # how much of each read goes into the agent's context
CODEOWNERS = [".github/CODEOWNERS", "CODEOWNERS", "docs/CODEOWNERS"]


class LiveReader:
    def __init__(self, svc: Services, billed_to: str | None = None):
        """`billed_to`: cost source for the stored proofs (the sweep pays for what it collects)."""
        self.svc = svc
        self.billed_to = billed_to
        self.reads: dict[str, dict] = {}
        # Reads that did not happen (network, credential, limit). "Not found" is an answer, not a failure.
        self.failures: list[dict] = []

    def _failed(self, tool: str, inp: dict, exc: SourceUnavailable) -> None:
        if not isinstance(exc, NotFound):
            self.failures.append({"tool": tool, "input": inp, "error": str(exc)[:300]})

    def _keep(self, source: str, locator: str, title: str, content: str, ref: dict | None = None) -> dict:
        rid = f"read:{len(self.reads) + 1}"
        # The proof is stored whole (never cut content); only the agent's context is limited.
        self.reads[rid] = {"source": source, "locator": locator, "title": title, "content": content,
                           "at": datetime.now(timezone.utc), "ref": ref or {}}
        cut = "" if len(content) <= PROMPT_CHARS else f"\n[… {len(content) - PROMPT_CHARS} characters omitted from the context]"
        return {"read": rid, "source": locator, "content": content[:PROMPT_CHARS] + cut}

    # ------------------------------------------------------------ GitHub

    def _gh(self):
        from ..connectors import github
        cfg = self.svc.settings.get("github")
        return github.GitHubClient(github.resolve_token(cfg))

    def read_github_file(self, inp: dict):
        repo, path = inp["repo"].strip().removeprefix("https://github.com/").strip("/"), inp["path"].strip("/")
        params = {"ref": inp["ref"]} if inp.get("ref") else None
        try:
            data = self._gh().get(f"/repos/{repo}/contents/{path}", params)
        except SourceUnavailable as exc:
            self._failed("read_github_file", inp, exc)
            return f"could not read {repo}/{path}: {exc}"
        if isinstance(data, list):  # directory
            return {"directory": f"{repo}/{path}", "items": [f"{d['type']}: {d['path']}" for d in data][:300]}
        if data.get("encoding") == "none" or (not data.get("content") and data.get("size")):
            return f"{repo}/{path} is {data.get('size')} bytes: too large for GitHub's contents API"
        text = base64.b64decode(data.get("content") or "").decode("utf-8", errors="replace")
        ref = inp.get("ref") or "default branch"
        return self._keep("github", f"https://github.com/{repo}/blob/{inp.get('ref') or 'HEAD'}/{path}",
                          f"{repo}/{path} ({ref})", text, {"repo": repo.lower()})

    def read_codeowners(self, inp: dict):
        repo = inp["repo"].strip().removeprefix("https://github.com/").strip("/")
        for path in CODEOWNERS:
            out = self.read_github_file({"repo": repo, "path": path})
            if isinstance(out, dict) and "read" in out:
                return out
        return f"{repo} has no CODEOWNERS (looked in {', '.join(CODEOWNERS)})"

    def search_code(self, inp: dict):
        # With no repository: the organizations of the allowlisted repositories (a global search would bring in other people's public code).
        orgs = sorted({r["config"]["repo"].split("/")[0] for r in self.svc.db.query(
            "SELECT config FROM sources WHERE kind = 'github'") if r["config"].get("repo")})
        scope = f" repo:{inp['repo']}" if inp.get("repo") else "".join(f" org:{o}" for o in orgs)
        q = inp["query"] + scope
        try:
            r = self._gh().get("/search/code", {"q": q, "per_page": 20})
        except SourceUnavailable as exc:
            self._failed("search_code", inp, exc)
            return f"search unavailable: {exc}"
        return {"total": r.get("total_count"), "files": [f"{i['repository']['full_name']}/{i['path']}" for i in r.get("items") or []]}

    # ------------------------------------------------------------ Confluence

    def read_confluence(self, inp: dict):
        from ..connectors import confluence
        try:
            client = confluence.client_from(self.svc.settings.get("confluence"))
            url, title, text = confluence.read_live(client, inp["page"])
        except SourceUnavailable as exc:
            self._failed("read_confluence", inp, exc)
            return f"Confluence unavailable right now: {exc}"
        from ..connectors.confluence import page_ref
        return self._keep("confluence", url, title, text, {"page_id": page_ref(url)[0] or page_ref(inp["page"])[0]})

    def tools(self) -> list[AgentTool]:
        return [
            AgentTool("read_github_file", "Reads a file LIVE (or lists a directory) from any repository the "
                      "owner can access, on the default branch or at a ref. Returns a read ID (use it in `proofs`).",
                      schema_object({"repo": STR, "path": STR, "ref": {"anyOf": [STR, {"type": "null"}]}}),
                      self.read_github_file),
            AgentTool("read_codeowners", "Reads a repository's current CODEOWNERS LIVE (who owns what).",
                      schema_object({"repo": STR}), self.read_codeowners),
            AgentTool("search_code", "Searches GitHub code LIVE (file names, symbols, text). "
                      "Returns paths; read the file to use it as proof.",
                      schema_object({"query": STR, "repo": {"anyOf": [STR, {"type": "null"}]}}), self.search_code),
            AgentTool("read_confluence", "Reads the current version of a Confluence page LIVE (ID or URL).",
                      schema_object({"page": STR}), self.read_confluence),
        ]

    # ------------------------------------------------------------ proofs → evidence

    def store_proofs(self, read_ids: list[str]) -> dict[str, str]:
        """Stores as evidence the reads used as proof and returns read → evidence. Content already
        stored is reused. The unit goes through the normal pipeline (becomes claims with provenance)."""
        out: dict[str, str] = {}
        wanted = [r for r in dict.fromkeys(read_ids) if r in self.reads]
        if not wanted:
            return out
        with self.svc.db.tx() as conn:
            for rid in wanted:
                rd = self.reads[rid]
                sid = f"live:{rd['source']}"
                conn.execute("""INSERT INTO sources (id, kind, name, config, enabled) VALUES (%s, 'live', %s, '{}', false)
                                ON CONFLICT (id) DO NOTHING""",
                             (sid, "Live read of " + ("GitHub" if rd["source"] == "github" else "Confluence")))
                ext = f"live:{sha(rd['locator'] + '|' + rd['content'])[:20]}"
                hit = conn.execute("SELECT id FROM evidence WHERE source_id = %s AND external_id = %s AND status = 'active'",
                                   (sid, ext)).fetchone()
                if hit:
                    out[rid] = hit["id"]
                    continue
                item = Item(external_id=ext, author="live read", role="live_read", occurred_at=rd["at"],
                            content=f"Read live on {rd['at']:%Y-%m-%d %H:%M} UTC: {rd['title']} ({rd['locator']})\n\n{rd['content']}",
                            meta={"kind": "live_read", "locator": rd["locator"], **rd["ref"]})
                uid = create_unit(conn, source_id=sid, stream_id=None, kind="live_read",
                                  title=f"Live read: {rd['title']}", items=[item],
                                  meta={"live": {"locator": rd["locator"], "billed_to": self.billed_to}}, priority=40)
                ev = conn.execute("SELECT id FROM evidence WHERE unit_id = %s", (uid,)).fetchone()
                out[rid] = ev["id"]
        return out
