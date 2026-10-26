# ETL-CC Meeting Demo Script

**Audience:** Engineering, architecture, and project stakeholders  
**Suggested length:** 8-10 minutes  
**Recommended live demo:** Ab Initio sample, end to end  
**Demo method:** Demonstrate the ETL-CC workflow using a prepared Ab Initio sample.

## Meeting objective

Explain how ETL-CC takes supported legacy ETL metadata, analyzes it, converts a
mapping to Databricks PySpark, validates the result, and tracks the work in
auditable workflows. Be explicit that Informatica and Ab Initio currently have
working ingestion paths, while DataStage does not yet have an ingestion/parser
path in this repository.

## Before the meeting

### 1. Check prerequisites

- Python dependencies are installed in the project virtual environment.
- PostgreSQL is running and reachable.
- The project `.env` is configured. Do not display it or copy secrets into
  meeting notes or terminal recordings.
- A supported JDK is installed and `JAVA_HOME` points to it. In each PowerShell
  terminal that runs ETL-CC, make Java discoverable:

```powershell
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
java -version
```

- GLiNER is available at the configured `GLINER_SERVICE_URL` and the SSH tunnel
  is open if the demo is intended to show enforced GLiNER decisions. GLiNER is
  enabled by default. For a demo that intentionally excludes GLiNER, set
  `$env:ENABLE_GLINER = 'false'` in **each** ETL-CC process terminal and explain
  that GLiNER is not part of that run.
- Git deployment is configured if you plan to show an actual deployment. The
  migration orchestrator runs deployment after validation passes; without a
  deployment target and credentials, that final stage cannot complete.

### 2. Start the API and workers

Open separate VS Code PowerShell terminals from the repository root:

```powershell
Set-Location 'C:\Users\viaan.sharma\Downloads\ETL-CC-viaan'
$env:PYTHONPATH = 'src'
$env:ENABLE_AB_INITIO = 'true'
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
.\.venv\Scripts\python.exe -m uvicorn etl_cc.main:app --app-dir src --reload
```

Open `http://127.0.0.1:8000/docs` and confirm `GET /api/v1/health` returns
`{"status":"ok"}`.

In a second terminal, start the discovery worker:

```powershell
Set-Location 'C:\Users\viaan.sharma\Downloads\ETL-CC-viaan'
$env:PYTHONPATH = 'src'
$env:ENABLE_AB_INITIO = 'true'
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
.\.venv\Scripts\python.exe -m etl_cc.worker
```

In a third terminal, start the migration orchestrator:

```powershell
Set-Location 'C:\Users\viaan.sharma\Downloads\ETL-CC-viaan'
$env:PYTHONPATH = 'src'
$env:ENABLE_AB_INITIO = 'true'
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
.\.venv\Scripts\python.exe -m etl_cc.langgraph_orchestrator
```

Use the same required environment configuration in all terminals. The
orchestrator owns migration conversion, validation, and deployment. **Do not
start** the legacy `conversion_worker`, `validation_worker`, or
`deployment_worker` alongside it.

Confirm the orchestrator startup log reports
`graph_version=2.1.0` and
`unit_test_specification_version=1.4.0`.

### 3. Prepare the ETL-CC demo

Confirm ETL-CC is running and prepare an approved Ab Initio graph JSON sample.
The API docs at `http://127.0.0.1:8000/docs` can be used to invoke the
ETL-CC API and inspect its available request schemas.

## Suggested talk track and live actions

### 0:00-1:00 — Introduce the problem

**Say:**

> ETL-CC is a migration command center for moving legacy ETL mappings toward
> Databricks PySpark. It turns source metadata into a common representation so
> the same discovery, conversion, validation, and workflow tracking can be
> applied without treating every source format as the same parser.

**Show:** `GET /api/v1/etl-products` in ETL-CC. Explain that the catalog shows
product availability and ingestion methods; a product being listed does not
necessarily mean its full ingestion path is implemented.

### 1:00-2:00 — Explain the architecture

**Say:**

> The API accepts source uploads and creates durable workflow records in
> PostgreSQL. The discovery worker parses and analyzes source mappings. The
> migration orchestrator then sequences conversion, validation, and deployment.
> Workflow events and agent responses provide the audit trail.

```text
Informatica XML / Ab Initio graph JSON
                  |
                  v
       FastAPI source-analysis API
                  |
                  v
       PostgreSQL workflow queue
                  |
                  v
       Discovery worker and parser
                  |
                  v
       Canonical mappings, rules,
       lineage, and dependency plan
                  |
                  v
       LangGraph migration orchestrator
         Conversion -> Validation -> Deployment
```

### 2:00-4:00 — Demonstrate source analysis and discovery

Submit the prepared graph sample through ETL-CC's Ab Initio source-analysis
endpoint, then start discovery and follow the resulting workflow.

**Say while it runs:**

> This sample is uploaded through the Ab Initio graph-upload API. Discovery
> validates the source format, creates a repository workflow, and sends it to
> the shared discovery worker. The Ab Initio-specific parser produces the
> canonical mapping; discovery then enriches it with business rules, readiness,
> lineage, complexity, and dependencies.

**Show in ETL-CC:**

- `GET /api/v1/workflows/{workflow_id}` for workflow status and failure details.
- `GET /api/v1/repositories/{repository_id}/mappings` for mapping readiness.
- `GET /api/v1/mappings/{etl_object_id}/discovery` for discovery details,
  including GLiNER decision/confidence when available.
- `GET /api/v1/mappings/{etl_object_id}/lineage` for lineage.

### 4:00-7:00 — Demonstrate conversion and validation

Start migration/conversion for the discovered mapping through ETL-CC and follow
its progress. This may take longer because conversion and validation call model
services and execute PySpark tests.

**Say:**

> Once a mapping is ready, the migration API queues conversion. The conversion
> stage generates PySpark code, a unit-test artifact, and configuration, then
> checks the package contract and critiques the result. If conversion succeeds,
> validation runs static checks, generated tests, synthetic-data checks, a
> deterministic reference plan, target execution, and output comparison.
> Validation evidence is persisted so a failure can be traced to a particular
> check rather than treated as a black box.

**Show:**

- The conversion workbench:
  `GET /api/v1/conversions/{workflow_id}/workbench/{etl_object_id}`.
- The migration dashboard:
  `GET /api/v1/migrations/{migration_id}/dashboard`.
- The dashboard’s validation stages and individual check details. If validation
  fails, show its actual `failure_reason` and check evidence instead of claiming
  a successful run.

### 7:00-8:00 — Explain GLiNER and workflow safety

**Say:**

> GLiNER is a separate decision service used for coarse routing and uncertainty
> signals in discovery, validation, and failure triage. It does not replace the
> deterministic validators or explain an error by itself. With enforcement
> enabled, low-confidence or unavailable advice can require human review.

Show the workflow failure details and validation report in ETL-CC, including
GLiNER uncertainty fields when available.

### 8:00-9:00 — State product coverage and next steps

| Product | Current repository support | Meeting-safe description |
|---|---|---|
| Informatica | PowerCenter live repository, GitHub repository, and XML upload ingestion; live connections require their configured tooling and access | “Supported ingestion paths normalize Informatica mappings for shared discovery.” |
| Ab Initio | Feature-flagged portable graph JSON upload; uses the shared discovery worker | “Graph JSON is parsed into the common mapping model, then follows the shared migration flow.” |
| DataStage | Listed in the product catalog, but no DataStage ingestion method or discovery parser is implemented here | “Catalog visibility is not end-to-end support yet; a DataStage adapter/parser remains future work.” |

**Close with:**

> The platform already demonstrates a shared workflow from supported source
> ingestion through analysis and migration. The next product-coverage milestone
> is implementing and testing a DataStage ingestion adapter and parser before
> presenting DataStage as end-to-end supported.

## ETL-CC demo by product

Use ETL-CC to submit a prepared sample for a supported product. Informatica and
Ab Initio have ingestion paths described above; DataStage is listed in the
catalog but does not currently have an ingestion method or discovery parser
implemented in this repository. Do not present DataStage as an end-to-end
conversion demo.

## If the live run does not complete

- **Discovery remains queued:** confirm `python -m etl_cc.worker` is running.
- **Migration remains queued:** confirm `python -m etl_cc.langgraph_orchestrator`
  is running and check its logs.
- **GLiNER requires review or is unavailable:** verify the model service/tunnel
  and configured URL, or explicitly disable GLiNER for the demo as described
  above.
- **PySpark reports `JAVA_GATEWAY_EXITED`:** verify `JAVA_HOME`, run
  `java -version`, and ensure `JAVA_HOME\bin` is on `PATH`. Validation also
  prepends an existing `JAVA_HOME\bin` to Spark child processes.
- **Conversion or validation fails:** use the migration dashboard and workflow
  event log to show the actual failure stage and check evidence. Do not rerun
  repeatedly without addressing the reported cause.
- **Deployment fails after validation:** check that the Git deployment target
  is configured. Deployment is a separate gated stage after validation.

If meeting time is limited, demonstrate discovery in ETL-CC and present the
conversion, validation, and deployment sequence from the migration dashboard
using a prepared run.
