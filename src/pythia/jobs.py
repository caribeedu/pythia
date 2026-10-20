"""Single job queue in Postgres (one execution path for everything).

Lanes:
- io: connectors and source scans (deterministic, parallel);
- cognition: everything that changes knowledge (triage → adjudication, corrections). A
  single worker, in series, so there are no races between integrations;
- derivation: regeneration of pages/narratives and golden questions.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from .db import Database

LANES = ("io", "cognition", "derivation")


def enqueue(conn: psycopg.Connection, kind: str, lane: str, payload: dict[str, Any] | None = None, *,
            priority: int = 100, dedup_key: str | None = None, delay_s: float = 0,
            max_attempts: int = 3) -> int | None:
    """Enqueues a job. With dedup_key, does nothing if an identical one is already queued."""
    assert lane in LANES
    row = conn.execute(
        """INSERT INTO jobs (kind, lane, payload, priority, dedup_key, run_after, max_attempts)
           VALUES (%s, %s, %s, %s, %s, now() + %s, %s)
           ON CONFLICT (dedup_key) WHERE status = 'queued' AND dedup_key IS NOT NULL DO NOTHING
           RETURNING id""",
        (kind, lane, Jsonb(payload or {}), priority, dedup_key, timedelta(seconds=delay_s), max_attempts),
    ).fetchone()
    return row["id"] if row else None


class Queue:
    def __init__(self, db: Database):
        self.db = db

    def enqueue(self, kind: str, lane: str, payload: dict[str, Any] | None = None, **kw) -> int | None:
        with self.db.tx() as conn:
            return enqueue(conn, kind, lane, payload, **kw)

    def claim(self, lane: str, worker: str) -> dict | None:
        with self.db.tx() as conn:
            return conn.execute(
                """UPDATE jobs SET status = 'running', worker = %s, started_at = now(), attempts = attempts + 1
                   WHERE id = (
                       SELECT id FROM jobs q
                       WHERE q.lane = %s AND q.status = 'queued' AND q.run_after <= now()
                         -- dedup also holds for what is running: never two identical ones at the same time
                         AND (q.dedup_key IS NULL OR NOT EXISTS (
                               SELECT 1 FROM jobs r WHERE r.status = 'running' AND r.dedup_key = q.dedup_key))
                       ORDER BY q.priority, q.id
                       FOR UPDATE SKIP LOCKED LIMIT 1)
                   RETURNING *""",
                (worker, lane),
            ).fetchone()

    def done(self, job_id: int) -> None:
        self.db.execute("UPDATE jobs SET status = 'done', finished_at = now(), last_error = NULL WHERE id = %s",
                        (job_id,))

    def retry_later(self, job_id: int, delay_s: float, error: str, *, count_attempt: bool) -> None:
        """Puts the job back in the queue. LLM unavailability does not count as an attempt.

        If an identical job (same dedup_key) entered the queue while this one was running, this one is
        closed as superseded — the queued one will do the same work."""
        with self.db.tx() as conn:
            row = conn.execute(
                """UPDATE jobs SET status = 'queued', run_after = now() + %s, last_error = %s,
                                  attempts = attempts - %s, worker = NULL
                   WHERE id = %s AND (dedup_key IS NULL OR NOT EXISTS (
                         SELECT 1 FROM jobs t WHERE t.status = 'queued' AND t.dedup_key = jobs.dedup_key))
                   RETURNING id""",
                (timedelta(seconds=delay_s), error[:2000], 0 if count_attempt else 1, job_id),
            ).fetchone()
            if row is None:
                conn.execute("UPDATE jobs SET status = 'done', finished_at = now(), last_error = %s WHERE id = %s",
                             (f"superseded by an identical job in the queue ({error[:200]})", job_id))

    def fail(self, job: dict, error: str) -> bool:
        """Records the failure. Returns True if it was the last attempt (final failure)."""
        if job["attempts"] < job["max_attempts"]:
            self.retry_later(job["id"], 30 * job["attempts"], error, count_attempt=True)
            return False
        self.db.execute(
            "UPDATE jobs SET status = 'failed', finished_at = now(), last_error = %s WHERE id = %s",
            (error[:2000], job["id"]),
        )
        return True

    def requeue_stuck(self) -> None:
        """Only at daemon startup: 'running' jobs from a previous process go back to the queue."""
        for job in self.db.query("SELECT id FROM jobs WHERE status = 'running'"):
            self.retry_later(job["id"], 0, "previous process interrupted", count_attempt=False)

    def counts(self) -> list[dict]:
        return self.db.query(
            "SELECT lane, status, count(*) AS n FROM jobs GROUP BY lane, status ORDER BY lane, status"
        )
