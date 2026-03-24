-- ledger/projections/schema.sql

-- Projection Checkpoints
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name TEXT PRIMARY KEY,
    last_position   BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 1. ApplicationSummary
CREATE TABLE IF NOT EXISTS application_summary (
    application_id           TEXT PRIMARY KEY,
    state                    TEXT NOT NULL,
    applicant_id             TEXT,
    requested_amount_usd     NUMERIC,
    approved_amount_usd      NUMERIC,
    risk_tier                TEXT,
    fraud_score              NUMERIC,
    compliance_status        TEXT,
    decision                 TEXT,
    agent_sessions_completed TEXT[],
    last_event_type          TEXT,
    last_event_at            TIMESTAMPTZ,
    human_reviewer_id        TEXT,
    final_decision_at        TIMESTAMPTZ
);

-- 2. AgentPerformanceLedger
CREATE TABLE IF NOT EXISTS agent_performance_ledger (
    agent_id             TEXT,
    model_version        TEXT,
    analyses_completed   BIGINT NOT NULL DEFAULT 0,
    decisions_generated  BIGINT NOT NULL DEFAULT 0,
    avg_confidence_score DOUBLE PRECISION,
    avg_duration_ms      DOUBLE PRECISION,
    approve_rate         DOUBLE PRECISION,
    decline_rate         DOUBLE PRECISION,
    refer_rate           DOUBLE PRECISION,
    human_override_rate  DOUBLE PRECISION,
    first_seen_at        TIMESTAMPTZ,
    last_seen_at         TIMESTAMPTZ,
    PRIMARY KEY (agent_id, model_version)
);

-- 3. ComplianceAuditView
CREATE TABLE IF NOT EXISTS compliance_audit_view (
    application_id       TEXT PRIMARY KEY,
    state                JSONB NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS compliance_audit_snapshots (
    application_id       TEXT,
    snapshot_at          TIMESTAMPTZ NOT NULL,
    state                JSONB NOT NULL,
    PRIMARY KEY (application_id, snapshot_at)
);
