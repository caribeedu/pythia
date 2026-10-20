-- Pythia schema.
-- Append-only event log + projections. Evidence keeps content outside the log
-- so that deletion/edit at the source really erases the content.

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ---------------------------------------------------------------- events
CREATE TABLE events (
    id          bigserial PRIMARY KEY,
    ts          timestamptz NOT NULL DEFAULT now(),
    type        text NOT NULL,
    actor       text NOT NULL,
    payload     jsonb NOT NULL,
    cause text
);
CREATE INDEX events_type_idx ON events (type);
CREATE INDEX events_ts_idx ON events (ts);

-- ---------------------------------------------------------------- settings
CREATE TABLE settings (
    key         text PRIMARY KEY,
    value       jsonb NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE system_state (
    key         text PRIMARY KEY,
    value       jsonb NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------- sources (plumbing)
CREATE TABLE sources (
    id          text PRIMARY KEY,          -- e.g.: claude_code:/Users/x/.claude/projects/-Users-x-repo
    kind        text NOT NULL,             -- claude_code | human
    name        text NOT NULL,
    config      jsonb NOT NULL DEFAULT '{}',
    enabled     boolean NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now(),
    backfill_until timestamptz,  -- units older than this are backfill
    backfill_done_at timestamptz  -- backfill finished (everything after is day to day)
);

CREATE TABLE streams (
    id              text PRIMARY KEY,      -- e.g.: claude_code:<session file>
    source_id       text NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    locator         text NOT NULL,         -- file path
    title           text,
    cursor          jsonb NOT NULL DEFAULT '{}',   -- {"offset": n}
    observed_size   bigint NOT NULL DEFAULT 0,
    observed_mtime  double precision NOT NULL DEFAULT 0,
    last_change_at  timestamptz,
    last_settled_at timestamptz,
    status          text NOT NULL DEFAULT 'active',  -- active | deleted
    meta            jsonb NOT NULL DEFAULT '{}',
    updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX streams_source_idx ON streams (source_id);

CREATE TABLE units (
    id          bigserial PRIMARY KEY,
    source_id   text NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    stream_id   text REFERENCES streams(id) ON DELETE CASCADE,
    kind        text NOT NULL,             -- session_delta | human_statement
    title       text,
    occurred_at timestamptz NOT NULL,
    status      text NOT NULL DEFAULT 'pending',   -- pending | triaged_out | processed | failed
    triage      jsonb,
    meta        jsonb NOT NULL DEFAULT '{}',
    created_at  timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz
);
CREATE INDEX units_status_idx ON units (status);

CREATE TABLE evidence (
    id           text PRIMARY KEY,         -- ev_...
    seq          bigserial,                -- arrival order
    unit_id      bigint REFERENCES units(id) ON DELETE SET NULL,
    source_id    text NOT NULL,
    stream_id    text,
    external_id  text,                     -- message uuid at the source
    author       text,
    role         text,                     -- user | assistant | human
    occurred_at  timestamptz NOT NULL,
    content      text,                     -- NULL after tombstone
    content_hash text NOT NULL,
    status       text NOT NULL DEFAULT 'active',  -- active | tombstoned
    meta         jsonb NOT NULL DEFAULT '{}',
    created_at   timestamptz NOT NULL DEFAULT now(),
    tsv          tsvector GENERATED ALWAYS AS (to_tsvector('simple', coalesce(content, ''))) STORED
);
CREATE UNIQUE INDEX evidence_ext_idx ON evidence (stream_id, external_id) WHERE external_id IS NOT NULL;
CREATE INDEX evidence_unit_idx ON evidence (unit_id);
CREATE INDEX evidence_tsv_idx ON evidence USING gin (tsv);

-- ---------------------------------------------------------------- knowledge projections
CREATE TABLE entities (
    id           text PRIMARY KEY,         -- en_...
    type         text NOT NULL,            -- person | team | system | project | concept | ...
    name         text NOT NULL,
    aliases      text[] NOT NULL DEFAULT '{}',
    external_ids jsonb NOT NULL DEFAULT '{}',
    description  text,
    origin       text NOT NULL,            -- anchored | discovered
    status       text NOT NULL DEFAULT 'active',   -- active | merged
    merged_into  text,
    is_owner     boolean NOT NULL DEFAULT false,
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL,
    embedding    vector(384)
);
CREATE INDEX entities_name_trgm ON entities USING gin (lower(name) gin_trgm_ops);

CREATE TABLE mentions (
    id           text PRIMARY KEY,         -- mn_...
    evidence_id  text,
    unit_id      bigint,
    surface      text NOT NULL,
    context      text,
    type_hint    text,
    entity_id    text,
    confidence   real,
    resolved_by  text,
    candidates   jsonb NOT NULL DEFAULT '[]',
    created_at   timestamptz NOT NULL
);
CREATE INDEX mentions_entity_idx ON mentions (entity_id);

CREATE TABLE mention_link_history (
    mention_id   text NOT NULL,
    entity_id    text,
    from_ts      timestamptz NOT NULL,
    to_ts        timestamptz,
    event_id     bigint NOT NULL
);
CREATE INDEX mlh_mention_idx ON mention_link_history (mention_id);

CREATE TABLE claims (
    id                 text PRIMARY KEY,   -- cl_...
    kind               text NOT NULL,      -- profile | episode | decision | state | causal | glossary | procedure
    subject_mention_id text,
    subject_entity_id  text,
    predicate          text NOT NULL,
    object_value       text,
    object_mention_id  text,
    object_entity_id   text,
    statement          text NOT NULL,
    valid_from         timestamptz,
    valid_to           timestamptz,
    recorded_at        timestamptz NOT NULL,
    origin             text NOT NULL,      -- observed | inferred | human
    source_id          text,
    source_label       text,               -- e.g.: claude_code, llm:narrative-v1, human:owner
    status             text NOT NULL,      -- current | superseded | disputed | retracted
    confidence         real,
    extractor_version  text,
    evidence_ids       text[] NOT NULL DEFAULT '{}',
    supporting_claim_ids text[] NOT NULL DEFAULT '{}',
    embedding          vector(384),
    tsv                tsvector GENERATED ALWAYS AS (to_tsvector('simple', statement || ' ' || predicate || ' ' || coalesce(object_value, ''))) STORED
);
CREATE INDEX claims_subject_idx ON claims (subject_entity_id);
CREATE INDEX claims_object_idx ON claims (object_entity_id);
CREATE INDEX claims_status_idx ON claims (status);
CREATE INDEX claims_tsv_idx ON claims USING gin (tsv);

CREATE TABLE claim_status_history (
    claim_id     text NOT NULL,
    status       text NOT NULL,
    from_ts      timestamptz NOT NULL,
    to_ts        timestamptz,
    event_id     bigint NOT NULL,
    verdict_id   text
);
CREATE INDEX csh_claim_idx ON claim_status_history (claim_id);

CREATE TABLE tensions (
    id           text PRIMARY KEY,         -- tn_...
    claim_ids    text[] NOT NULL,
    reason       text NOT NULL,
    opened_by    text NOT NULL,
    status       text NOT NULL DEFAULT 'open',   -- open | resolved
    verdict_id   text,
    created_at   timestamptz NOT NULL,
    dossier jsonb
);

CREATE TABLE verdicts (
    id             text PRIMARY KEY,       -- vd_...
    tension_id     text,
    kind           text NOT NULL,          -- temporal_supersession | real_conflict | different_scope | outdated_source | duplicate | human_correction
    decided_by     text NOT NULL,          -- role or human:owner
    claim_ids      text[] NOT NULL,
    changes        jsonb NOT NULL,         -- [{claim_id, from, to}]
    summary        text NOT NULL,
    rationale      text NOT NULL,
    cited_evidence text[] NOT NULL DEFAULT '{}',
    confidence     real,
    model          text,
    prompt_version text,
    dossier_hash   text,
    impact         real NOT NULL DEFAULT 0,
    status         text NOT NULL DEFAULT 'applied',  -- applied | reverted | overridden
    created_at     timestamptz NOT NULL,
    reverted_at    timestamptz,
    exploration jsonb NOT NULL DEFAULT '[]'
);
CREATE INDEX verdicts_created_idx ON verdicts (created_at DESC);

-- ---------------------------------------------------------------- derived (disposable)
CREATE TABLE views (
    id               text PRIMARY KEY,     -- vw_...
    kind             text NOT NULL,        -- entity | narrative
    entity_id        text,
    title            text NOT NULL,
    l0               text NOT NULL,
    l1               text NOT NULL,
    cited_claim_ids  text[] NOT NULL DEFAULT '{}',
    member_entity_ids text[] NOT NULL DEFAULT '{}',
    watermark        text NOT NULL,
    stale            boolean NOT NULL DEFAULT false,
    status           text NOT NULL DEFAULT 'active',  -- active | retired
    version          int NOT NULL DEFAULT 1,
    model            text,
    generated_at     timestamptz NOT NULL,
    embedding        vector(384),
    tsv              tsvector GENERATED ALWAYS AS (to_tsvector('simple', title || ' ' || l0 || ' ' || l1)) STORED,
    stale_since timestamptz
);
CREATE UNIQUE INDEX views_entity_idx ON views (entity_id) WHERE kind = 'entity';
CREATE INDEX views_tsv_idx ON views USING gin (tsv);

CREATE TABLE view_versions (
    view_id          text NOT NULL,
    version          int NOT NULL,
    title            text NOT NULL,
    l0               text NOT NULL,
    l1               text NOT NULL,
    cited_claim_ids  text[] NOT NULL,
    member_entity_ids text[] NOT NULL,
    status           text NOT NULL,
    generated_at     timestamptz NOT NULL,
    PRIMARY KEY (view_id, version),
    cause text
);

-- ---------------------------------------------------------------- corrections and quality
CREATE TABLE corrections (
    id              text PRIMARY KEY,      -- co_...
    target_id       text,
    text            text NOT NULL,
    via             text NOT NULL,         -- mcp | web
    status          text NOT NULL DEFAULT 'pending',  -- pending | applied | failed
    ops             jsonb,
    error_type      text,                  -- entity | claim | verdict | synthesis | other
    blamed_role     text,
    blamed_source   text,
    result          text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    applied_at      timestamptz
);

CREATE TABLE golden_questions (
    id               text PRIMARY KEY,     -- gq_...
    question         text NOT NULL,
    expected         text NOT NULL,
    expected_ids     text[] NOT NULL DEFAULT '{}',
    from_correction  text,
    active           boolean NOT NULL DEFAULT true,
    created_at       timestamptz NOT NULL DEFAULT now(),
    kind text,
    sealed boolean NOT NULL DEFAULT false,
    evidence_ids text[] NOT NULL DEFAULT '{}',
    evidence_note text,
    wrong_answers text[] NOT NULL DEFAULT '{}'
);

CREATE TABLE golden_runs (
    id           text PRIMARY KEY,         -- gr_...
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    config       jsonb NOT NULL,
    passed       int NOT NULL DEFAULT 0,
    total        int NOT NULL DEFAULT 0,
    status       text NOT NULL DEFAULT 'running',
    arm text NOT NULL DEFAULT 'pythia',
    battery text,
    cost_usd numeric(12, 6)
);

CREATE TABLE golden_results (
    run_id       text NOT NULL,
    question_id  text NOT NULL,
    answer       text,
    passed       boolean,
    rationale    text,
    PRIMARY KEY (run_id, question_id),
    retrieved_evidence_ids text[] NOT NULL DEFAULT '{}',
    recall numeric
);

-- ---------------------------------------------------------------- operation
CREATE TABLE jobs (
    id           bigserial PRIMARY KEY,
    kind         text NOT NULL,
    lane         text NOT NULL,            -- io | cognition | derivation
    payload      jsonb NOT NULL DEFAULT '{}',
    status       text NOT NULL DEFAULT 'queued',  -- queued | running | done | failed
    priority     int NOT NULL DEFAULT 100,        -- lower = sooner
    run_after    timestamptz NOT NULL DEFAULT now(),
    attempts     int NOT NULL DEFAULT 0,
    max_attempts int NOT NULL DEFAULT 3,
    dedup_key    text,
    worker       text,
    last_error   text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    started_at   timestamptz,
    finished_at  timestamptz
);
CREATE UNIQUE INDEX jobs_dedup_idx ON jobs (dedup_key) WHERE status = 'queued' AND dedup_key IS NOT NULL;
CREATE INDEX jobs_pick_idx ON jobs (lane, status, priority, id);

CREATE TABLE llm_calls (
    id              bigserial PRIMARY KEY,
    ts              timestamptz NOT NULL DEFAULT now(),
    role            text NOT NULL,
    model           text NOT NULL,
    source_id       text,
    job_id          bigint,
    input_tokens    int NOT NULL DEFAULT 0,
    output_tokens   int NOT NULL DEFAULT 0,
    cache_read      int NOT NULL DEFAULT 0,
    cache_write     int NOT NULL DEFAULT 0,
    cost_usd        numeric(12, 6) NOT NULL DEFAULT 0,
    duration_ms     int NOT NULL DEFAULT 0,
    ok              boolean NOT NULL,
    error           text,
    batch boolean NOT NULL DEFAULT false
);
CREATE INDEX llm_calls_ts_idx ON llm_calls (ts);

CREATE TABLE pushes (
    id           bigserial PRIMARY KEY,
    ts           timestamptz NOT NULL DEFAULT now(),
    session_id   text,
    event        text,
    selected_ids text[] NOT NULL DEFAULT '{}',
    chars        int NOT NULL DEFAULT 0,
    skipped      text,
    refs jsonb NOT NULL DEFAULT '[]',  -- pages/narratives with version + verdicts
    cwd text,
    source text
);

-- Claude Code message IDs are global: resumed/forked sessions copy the history
-- into another file with the same IDs. Deduplicate per source, not per file.
DROP INDEX IF EXISTS evidence_ext_idx;
CREATE UNIQUE INDEX evidence_source_ext_idx ON evidence (source_id, external_id) WHERE external_id IS NOT NULL;

-- What the adjudicator went looking for beyond the initial dossier (signal to improve dossiers).

-- entity lookup by external identifier (GitHub login, catalog id, email...).
CREATE INDEX entities_external_ids_idx ON entities USING gin (external_ids jsonb_path_ops);

-- backfill with the Batch API and deferred synthesis.

UPDATE sources SET backfill_until = created_at, backfill_done_at = now() WHERE kind <> 'human';

CREATE TABLE llm_batches (
    id           text PRIMARY KEY,           -- batch id at Anthropic
    status       text NOT NULL,              -- in_progress | ended | failed
    request_count int NOT NULL,
    submitted_at timestamptz NOT NULL DEFAULT now(),
    ended_at     timestamptz,
    error        text,
    progress jsonb
);

CREATE TABLE batch_requests (
    custom_id    text PRIMARY KEY,           -- br_...
    kind         text NOT NULL,              -- triage | extraction | synthesis
    role         text NOT NULL,
    unit_id      bigint,
    entity_id    text,
    source_id    text,
    params       jsonb,                      -- full request; erased after the result arrives
    meta         jsonb NOT NULL DEFAULT '{}',
    status       text NOT NULL DEFAULT 'pending',  -- pending | submitted | done | failed
    attempts     int NOT NULL DEFAULT 1,
    batch_id     text,
    result       jsonb,
    error        text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    done_at      timestamptz
);
CREATE INDEX batch_requests_status_idx ON batch_requests (status);
CREATE INDEX batch_requests_unit_idx ON batch_requests (unit_id, kind);

CREATE TABLE deferred_derivations (
    source_id    text NOT NULL,
    entity_id    text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, entity_id)
);

-- Usage screen. References only (with version), never the delivered text: the screen rebuilds from
-- the history (view_versions, claims, evidence), and what was erased at the source shows up as erased.
CREATE TABLE mcp_calls (
    id          bigserial PRIMARY KEY,
    ts          timestamptz NOT NULL DEFAULT now(),
    tool        text NOT NULL,
    args        jsonb NOT NULL DEFAULT '{}',
    client      text,
    session     text,
    refs        jsonb NOT NULL DEFAULT '[]',   -- [{"id": "vw_…", "version": 3}, {"id": "cl_…"}, …] in delivery order
    chars       integer NOT NULL DEFAULT 0,
    ok          boolean NOT NULL DEFAULT true,
    error       text
);
CREATE INDEX mcp_calls_ts_idx ON mcp_calls (ts DESC);

-- Batch progress reported by Anthropic (completed requests), to predict the return from the real pace.

-- Why the conversation (re)started (SessionStart: startup | resume | clear | compact): only startup, clear
-- and compact take away from the LLM what it had already received; on a resume the history stays with it.

-- Tension with dossier: what the detector (sweep, Confluence edit) already investigated.

-- Sweep rotation: when each entity was last reviewed.
CREATE TABLE sweep_reviews (
    entity_id   text PRIMARY KEY,
    reviewed_at timestamptz NOT NULL DEFAULT now(),
    suspicions  integer NOT NULL DEFAULT 0,
    summary text,
    tools jsonb NOT NULL DEFAULT '[]',
    cost_usd numeric(12, 6) NOT NULL DEFAULT 0
);
-- Evidence: besides active | tombstoned, `superseded` = previous version of a Confluence section,
-- kept as history. Deleting the page also deletes this history.

-- Sweep audit: what the agent concluded and what it investigated in each round (without the content read).

-- Evolution: the cause of each change, to group the log into "one line per cause".

-- New events record the cause (the job that produced them: unit:<id>, tension:<id>, correction:<id>…).

CREATE INDEX events_cause_idx ON events (cause);

-- Page versions also record the cause (regeneration, backfill synthesis).

-- "Updating since T": when the page became stale. Kept by a trigger, so it holds on every path
-- that sets or clears `stale`.

CREATE FUNCTION views_stale_since() RETURNS trigger AS $$
BEGIN
    IF NEW.stale AND (TG_OP = 'INSERT' OR NOT OLD.stale) THEN
        NEW.stale_since := now();
    ELSIF NOT NEW.stale THEN
        NEW.stale_since := NULL;
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER views_stale_since BEFORE INSERT OR UPDATE OF stale ON views
    FOR EACH ROW EXECUTE FUNCTION views_stale_since();

-- Effective cause, computed at read time (the log is not rewritten: the only exception to append-only is redaction).
-- Events without a recorded cause: new or corroborated claim → the evidence's unit; verdict → tension; the rest
-- inherits the cause of the same transaction (same `ts`); with none of those, group by actor and day.
CREATE VIEW events_caused AS
WITH base AS (
    SELECT e.*,
           coalesce(
               e.cause,
               CASE WHEN e.type IN ('claim_asserted', 'claim_corroborated') THEN
                   (SELECT 'unit:' || ev.unit_id FROM evidence ev
                    WHERE ev.id = e.payload -> 'evidence_ids' ->> 0 AND ev.unit_id IS NOT NULL) END,
               CASE WHEN e.type = 'verdict_applied' AND e.payload ->> 'tension_id' IS NOT NULL THEN
                   'tension:' || (e.payload ->> 'tension_id') END) AS direct_cause
    FROM events e)
SELECT b.*,
       coalesce(b.direct_cause, max(b.direct_cause) OVER (PARTITION BY b.ts),
                'unattributed:' || b.actor || ':' || to_char(b.ts, 'YYYY-MM-DD')) AS eff_cause
FROM base b;

-- Golden questions: kind, evidence, sealed half, wrong answers to check the judge;
-- runs per arm (Pythia × baseline without Pythia), grouped into a battery, with cost.

-- Evidence the retrieval brought back (for recall@k) and the fraction of the expected ones that came.

-- Judge check: every plausible wrong answer MUST be failed.
CREATE TABLE golden_judge_checks (
    run_id      text NOT NULL,
    question_id text NOT NULL,
    wrong_answer text NOT NULL,
    passed      boolean NOT NULL,      -- true = the judge approved a wrong answer (judge failure)
    rationale   text
);

-- avoidable re-explanation and cost of getting oriented, per message and per Claude Code session.
CREATE TABLE reexplain_messages (
    message_id    text PRIMARY KEY,        -- message id in the transcript (dedup across files of the same chat)
    session_id    text NOT NULL,
    chat_title    text,
    ts            timestamptz NOT NULL,
    text          text NOT NULL,
    asserts       boolean NOT NULL,         -- the message asserts something about the work world (not a command/question)
    repeats       boolean,                  -- repeats something Pythia already knew before the session (null if it asserts nothing)
    claim_ids     text[] NOT NULL DEFAULT '{}',   -- claims it repeats
    candidate_ids text[] NOT NULL DEFAULT '{}',   -- claims the judge saw (current before the session)
    bucket        text,                     -- delivered | not_delivered | new
    reason        text,
    prompt_version text,
    owner_label   boolean,                  -- hand label from the owner (judge validation)
    labeled_at    timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX reexplain_messages_session ON reexplain_messages (session_id);

CREATE TABLE session_metrics (
    session_id     text PRIMARY KEY,
    chat_title     text,
    started_at     timestamptz,
    arm            text,                    -- experiment arm; null outside it
    user_messages  int NOT NULL DEFAULT 0,
    asserts        int NOT NULL DEFAULT 0,
    avoidable_delivered      int NOT NULL DEFAULT 0,  -- was in the push and was explained anyway
    avoidable_not_delivered  int NOT NULL DEFAULT 0,  -- was in the base and was not delivered
    new_context    int NOT NULL DEFAULT 0,  -- Pythia did not know it
    tokens_total   bigint,
    tokens_until_edit bigint,               -- input tokens (with cache) until the first file edit
    explore_until_edit int,                 -- exploration calls (Read/Grep/Glob/Bash…) until the first edit
    had_edit       boolean,
    computed_at    timestamptz NOT NULL DEFAULT now()
);
