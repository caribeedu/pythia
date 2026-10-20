"""Postgres connection and migrations."""

from __future__ import annotations

import os
from contextlib import contextmanager
from importlib import resources
from typing import Any, Iterator

import psycopg
from pgvector.psycopg import register_vector
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DEFAULT_URL = "postgresql://pythia:pythia@127.0.0.1:54329/pythia"


def database_url() -> str:
    return os.environ.get("PYTHIA_DATABASE_URL", DEFAULT_URL)


def _configure(conn: psycopg.Connection) -> None:
    register_vector(conn)


class Database:
    def __init__(self, url: str | None = None, min_size: int = 1, max_size: int = 10):
        self.url = url or database_url()
        self.pool = ConnectionPool(
            self.url,
            min_size=min_size,
            max_size=max_size,
            kwargs={"row_factory": dict_row, "autocommit": False},
            configure=_configure,
            open=False,
        )

    def open(self) -> None:
        # The vector extension must exist before register_vector.
        with psycopg.connect(self.url, autocommit=True) as conn:
            conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        self.pool.open(wait=True)
        self.migrate()

    def close(self) -> None:
        self.pool.close()

    @contextmanager
    def tx(self) -> Iterator[psycopg.Connection]:
        """Connection in a transaction; commits on a clean exit."""
        with self.pool.connection() as conn:
            with conn.transaction():
                yield conn

    def query(self, sql: str, params: Any = None) -> list[dict]:
        with self.tx() as conn:
            return conn.execute(sql, params).fetchall()

    def one(self, sql: str, params: Any = None) -> dict | None:
        with self.tx() as conn:
            return conn.execute(sql, params).fetchone()

    def execute(self, sql: str, params: Any = None) -> None:
        with self.tx() as conn:
            conn.execute(sql, params)

    def migrate(self) -> None:
        files = sorted(
            f for f in resources.files("pythia.migrations").iterdir() if f.name.endswith(".sql")
        )
        with self.tx() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
            )
            done = {r["name"] for r in conn.execute("SELECT name FROM schema_migrations").fetchall()}
            for f in files:
                if f.name in done:
                    continue
                conn.execute(f.read_text())
                conn.execute("INSERT INTO schema_migrations (name) VALUES (%s)", (f.name,))
