/*
Reconcile the repository ingestion-method constraint.

002_add_ab_initio_graph_upload.sql and 002_add_datastage_dsx.sql each replace
ck_repository_etl_connection_type with a list that omits the other's method,
so whichever ran last breaks the other source type. This restores the full
list declared in RepositoryETL (src/etl_cc/models.py).
*/

ALTER TABLE demooc28.repository_etl
DROP CONSTRAINT IF EXISTS ck_repository_etl_connection_type;

ALTER TABLE demooc28.repository_etl
ADD CONSTRAINT ck_repository_etl_connection_type
CHECK (
    connection_type IN (
        'POWERCENTER',
        'GITHUB',
        'XML_UPLOAD',
        'AB_INITIO_GRAPH_UPLOAD',
        'DSX_UPLOAD'
    )
);
