"""Local embeddings for search (plumbing: decides what the LLM sees, never what it means)."""

from __future__ import annotations

import threading
from typing import Protocol

DIM = 384
MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


class Embedder(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]: ...


class LocalEmbedder:
    """Small multilingual model (ONNX), loaded on demand and reused."""

    def __init__(self, model: str = MODEL):
        self.model_name = model
        self._model = None
        self._lock = threading.Lock()

    def _load(self):
        with self._lock:
            if self._model is None:
                from fastembed import TextEmbedding

                self._model = TextEmbedding(self.model_name)
        return self._model

    def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        model = self._load()
        return [list(map(float, v)) for v in model.embed([t[:2000] for t in texts])]

    def embed_one(self, text: str) -> list[float]:
        return self.embed([text])[0]
