BEGIN;

ALTER TABLE demooc28.generated_artifact_etl
ADD COLUMN IF NOT EXISTS etl_object_id BIGINT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_generated_artifact_etl_object'
          AND conrelid = 'demooc28.generated_artifact_etl'::regclass
    ) THEN
        ALTER TABLE demooc28.generated_artifact_etl
        ADD CONSTRAINT fk_generated_artifact_etl_object
        FOREIGN KEY (etl_object_id)
        REFERENCES demooc28.etl_object_etl(id)
        ON DELETE SET NULL;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_generated_artifact_etl_object_id
ON demooc28.generated_artifact_etl(etl_object_id);

COMMIT;
