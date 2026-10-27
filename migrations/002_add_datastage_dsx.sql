/* Add IBM DataStage DSX upload support to existing installations. */

ALTER TABLE demooc28.repository_etl
DROP CONSTRAINT IF EXISTS ck_repository_etl_connection_type;

ALTER TABLE demooc28.repository_etl
ADD CONSTRAINT ck_repository_etl_connection_type
CHECK (
    connection_type IN (
        'POWERCENTER',
        'GITHUB',
        'XML_UPLOAD',
        'DSX_UPLOAD'
    )
);