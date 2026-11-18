-- Additive storage for deterministic complexity explanations.
-- Replace demooc28 with the deployment's configured PG_SCHEMA if different.

ALTER TABLE demooc28.etl_object_etl
    ADD COLUMN IF NOT EXISTS complexity_breakdown JSONB NOT NULL
        DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS complexity_version VARCHAR(50);