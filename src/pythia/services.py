"""Container for the services shared by the daemon, web and MCP."""

from __future__ import annotations

from dataclasses import dataclass

from .db import Database
from .embed import Embedder, LocalEmbedder
from .jobs import Queue
from .llm.client import LLM
from .retrieval import Retriever
from .settings import Settings, SystemState


@dataclass
class Services:
    db: Database
    settings: Settings
    state: SystemState
    llm: LLM
    embedder: Embedder
    retriever: Retriever
    queue: Queue

    @property
    def owner_name(self) -> str:
        return self.settings.get("owner")["name"]


def build(db: Database, llm: LLM | None = None, embedder: Embedder | None = None) -> Services:
    settings = Settings(db)
    embedder = embedder or LocalEmbedder()
    return Services(
        db=db,
        settings=settings,
        state=SystemState(db),
        llm=llm or LLM(db, settings),
        embedder=embedder,
        retriever=Retriever(db, embedder),
        queue=Queue(db),
    )
