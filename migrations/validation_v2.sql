-- Existing agent_response_etl and validation_test_case_etl tables store V2 evidence.
CREATE INDEX IF NOT EXISTS ix_agent_response_synthetic_lookup
ON demooc28.agent_response_etl(workflow_run_id, etl_object_id, agent_name, id DESC);
