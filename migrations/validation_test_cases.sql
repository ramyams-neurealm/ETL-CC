BEGIN;

CREATE TABLE IF NOT EXISTS demooc28.validation_test_case_etl (
    id BIGSERIAL PRIMARY KEY,
    workflow_run_id BIGINT NOT NULL REFERENCES demooc28.workflow_run_etl(id) ON DELETE CASCADE,
    etl_object_id BIGINT NULL REFERENCES demooc28.etl_object_etl(id) ON DELETE SET NULL,
    agent_response_id BIGINT NULL REFERENCES demooc28.agent_response_etl(id) ON DELETE SET NULL,
    test_id VARCHAR(100) NOT NULL,
    test_name VARCHAR(500) NOT NULL,
    category VARCHAR(100) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'PLANNED',
    severity VARCHAR(30) NOT NULL DEFAULT 'MEDIUM',
    expected_result TEXT NULL,
    actual_result TEXT NULL,
    details TEXT NULL,
    evidence_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    duration_milliseconds INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_validation_test_case_scope UNIQUE (workflow_run_id, etl_object_id, test_id)
);

CREATE INDEX IF NOT EXISTS ix_validation_test_case_workflow_status
ON demooc28.validation_test_case_etl(workflow_run_id, status, category);

CREATE INDEX IF NOT EXISTS ix_validation_test_case_mapping
ON demooc28.validation_test_case_etl(etl_object_id, created_at DESC);

COMMIT;
