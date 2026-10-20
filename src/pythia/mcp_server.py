"""Lean MCP server: 5 tools. Depth comes from the IDs and the level, not from more tools."""

from __future__ import annotations

import anyio
from mcp.server.mcpserver import Context, MCPServer
from mcp.types import ToolAnnotations

from . import consumer, usage
from .services import Services

INSTRUCTIONS = """\
Pythia is the user's knowledge oracle: preferences, people, teams, systems, \
decisions, episodes, bug causes, glossary and procedures, with provenance. Use it BEFORE asking \
the user something about their work world, when they mention something without explaining it, and before writing on \
their behalf (to follow their preferences). Start with `search`; each result carries an ID and a short synthesis \
(L0). Go deeper only when needed: `open(id, level)` with level 1 (page), 2 (atomic claims) or 3 \
(original evidence). If something is wrong, call `correct` — the user's corrections have maximum authority."""


def build_mcp(svc: Services) -> MCPServer:
    mcp = MCPServer("pythia", instructions=INSTRUCTIONS)
    read_only = ToolAnnotations(readOnlyHint=True, openWorldHint=False)

    async def run(fn, *args, tool: str, call_args: dict, ctx: Context | None = None):
        """Runs the tool and records the call: only the references of what was returned."""
        headers = {}
        try:
            headers = dict(ctx.headers or {}) if ctx else {}
        except Exception:  # outside an HTTP request (tests, stdio)
            headers = {}
        client, session = headers.get("user-agent"), headers.get("mcp-session-id")
        try:
            out = await anyio.to_thread.run_sync(fn, *args)
        except Exception as exc:
            await anyio.to_thread.run_sync(lambda: usage.log_call(svc, tool, call_args, client, session, None, exc))
            raise
        await anyio.to_thread.run_sync(lambda: usage.log_call(svc, tool, call_args, client, session, out, None))
        return out

    @mcp.tool(annotations=read_only)
    async def search(query: str, ctx: Context) -> str:
        """Searches the user's knowledge. Use it before asking them about people, systems,
        decisions, incidents or preferences, and when they mention something without explaining it. Returns IDs with a
        short synthesis (L0): narratives, pages, claims and entities. Short subject queries work best."""
        return await run(consumer.search, svc, query, tool="search", call_args={"query": query}, ctx=ctx)

    @mcp.tool(annotations=read_only)
    async def open(id: str, ctx: Context, level: int = 1) -> str:
        """Opens an ID at a zoom level: 0 = synthesis, 1 = page, 2 = claims with provenance, 3 = original
        evidence. Start at 1; go down to 2 when you need source and date, and to 3 only to read the original."""
        return await run(consumer.open_item, svc, id, level, tool="open", call_args={"id": id, "level": level}, ctx=ctx)

    @mcp.tool(annotations=read_only)
    async def timeline(id: str, ctx: Context) -> str:
        """How the knowledge about an ID changed over time, including verdicts (supersessions, conflicts).
        Use it when what was true before matters, or why something changed."""
        return await run(consumer.timeline, svc, id, tool="timeline", call_args={"id": id}, ctx=ctx)

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, openWorldHint=False))
    async def correct(id: str, correction: str, ctx: Context) -> str:
        """Corrects something that is wrong (claim, entity, mention, verdict, page). Use it when the user
        says information in Pythia is wrong or outdated. Maximum authority."""
        return await run(consumer.request_correction, svc, id, correction, tool="correct",
                         call_args={"id": id, "correction": correction}, ctx=ctx)

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, openWorldHint=False))
    async def define(text: str, ctx: Context) -> str:
        """Records a new, durable fact stated by the user (e.g. "X is team Y's system", "I prefer small
        PRs"). Use it when they assert something that should hold for future conversations."""
        return await run(consumer.request_definition, svc, text, tool="define", call_args={"text": text}, ctx=ctx)

    return mcp
