/*
ETL Migration Command Center database migration.

Release 1 tables:
- repository_etl
- etl_object_etl
- workflow_run_etl
- agent_response_etl
- workflow_event_etl
- generated_artifact_etl
- knowledge_base_etl

This migration adds support for three Informatica ingestion methods:
- POWERCENTER
- GITHUB
- XML_UPLOAD
*/


-- ============================================================
-- Add the source-ingestion method
-- ============================================================

ALTER TABLE demooc28.repository_etl
ADD COLUMN IF NOT EXISTS connection_type VARCHAR(30);


-- ============================================================
-- Populate existing records
-- ============================================================

UPDATE demooc28.repository_etl
SET connection_type = 'POWERCENTER'
WHERE connection_type IS NULL;


-- ============================================================
-- Make connection_type mandatory
-- ============================================================

ALTER TABLE demooc28.repository_etl
ALTER COLUMN connection_type SET NOT NULL;


-- ============================================================
-- XML upload does not require a password or access token
-- ============================================================

ALTER TABLE demooc28.repository_etl
ALTER COLUMN credential_ciphertext DROP NOT NULL;


-- ============================================================
-- Restrict the accepted ingestion methods
-- ============================================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_repository_etl_connection_type'
          AND conrelid = 'demooc28.repository_etl'::regclass
    ) THEN
        ALTER TABLE demooc28.repository_etl
        ADD CONSTRAINT ck_repository_etl_connection_type
        CHECK (
            connection_type IN (
                'POWERCENTER',
                'GITHUB',
                'XML_UPLOAD'
            )
        );
    END IF;
END
$$;


-- ============================================================
-- Add an index for filtering sources by ingestion method
-- ============================================================

CREATE INDEX IF NOT EXISTS ix_repository_etl_connection_type
ON demooc28.repository_etl (
    connection_type
);