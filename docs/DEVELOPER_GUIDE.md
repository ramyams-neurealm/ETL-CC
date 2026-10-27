# ETL-CC Developer Guide

**What this is:** the single starting point for anyone who needs to run, understand, debug, or change ETL-CC (ETL Migration Command Center).
**Last updated:** 2026-10-26, after the discovery → deployment flow fixes (orchestrator `graph_version=2.2.0`).

Read sections 1–4 once to understand the system. Use sections 5–9 as a reference when you make a change.

> Older documents in `docs/` (`KNOWLEDGE_TRANSFER_informatica.md`, `PROJECT_KNOWLEDGE_TRANSFER_AB_INITIO.md`) contain useful background, but parts are out of date. For example, they describe LangGraph as a stub and say DataStage is not implemented. Where they disagree with this guide, this guide and the code win.

---

## Contents

1. [What ETL-CC does](#1-what-etl-cc-does)
2. [Architecture at a glance](#2-architecture-at-a-glance)
3. [Repository map](#3-repository-map)
4. [The flow, step by step](#4-the-flow-step-by-step)
5. [Data model and status values](#5-data-model-and-status-values)
6. [Configuration reference](#6-configuration-reference)
7. [Running it locally](#7-running-it-locally)
8. [Testing](#8-testing)
9. [How to make common changes](#9-how-to-make-common-changes)
10. [Troubleshooting](#10-troubleshooting)
11. [Known limitations and open decisions](#11-known-limitations-and-open-decisions)
12. [Change log of the 2026-10-26 fixes](#12-change-log-of-the-2026-10-26-fixes)

---

## 1. What ETL-CC does

ETL-CC helps migrate legacy ETL mappings to **Databricks PySpark**. For each mapping it:

1. **Ingests** the source definition from one of these sources:
   - Informatica PowerCenter (live via `pmrep`, from GitHub, or an XML upload)
   - an Ab Initio portable graph (`.mp`, JSON inside)
   - an IBM DataStage export (`.dsx`)
2. **Normalises** it into one vendor-neutral model, `CanonicalMapping`.
3. **Discovers** business rules, risks, complexity, field lineage and dependency "waves", meaning which mapping must migrate before which.
4. **Converts** it with GPT-4o into three files: a pure PySpark `transform()`, a pytest unit test, and a JSON config. A critique agent reviews the result and requests revisions if needed.
5. **Validates** the generated code with static checks, the unit test, LLM-generated synthetic data, an independent in-memory "reference" implementation, a real PySpark run, and a row-by-row comparison.
6. **Deploys** validated files to a GitHub branch, optionally opening a pull request.

It is a migration **tool**. It does not run production ETL itself, and PostgreSQL is its own metadata store, not a data warehouse.

---

## 2. Architecture at a glance

```text
            ┌──────────────────────────── FastAPI (src/etl_cc/main.py, api.py) ────────────────────────────┐
 client ──► │ /sources/*  /discoveries  /migrations  /conversions  /validations  /deployments  dashboards │
            └──────────────┬─────────────────────────────────────────────────────────────────────────────┘
                           │ writes rows with job_status = QUEUED
                           ▼
                 PostgreSQL (schema PG_SCHEMA, default demooc28)
                 workflow_run_etl = the job queue  ·  events  ·  audits  ·  artifacts
                           ▲                                     ▲
     claims DISCOVERY jobs │                                     │ claims CONVERSION / VALIDATION(API) / DEPLOYMENT jobs
            ┌──────────────┴──────────────┐        ┌─────────────┴──────────────────────────────┐
            │ Discovery worker            │        │ LangGraph orchestrator                      │
            │ python -m etl_cc.worker     │        │ python -m etl_cc.langgraph_orchestrator     │
            │ parse → DiscoveryAgent →    │        │ conversion → validation → deployment        │
            │ LineageAgent → planner      │        │ (conversion_service / validation_service /  │
            └─────────────────────────────┘        │  deployment_service)                        │
                                                   └─────────────────────────────────────────────┘
 External: Azure Key Vault (holds the OpenAI key) · OpenAI GPT-4o + embeddings · pgvector · Java/PySpark · git + GitHub API · optional GLiNER service
```

**Key design points**

- **The database is the queue.** API calls only insert rows. Workers claim them with `SELECT … FOR UPDATE SKIP LOCKED`, so multiple workers can never take the same job.
- **Exactly three processes run:** the API, the discovery worker, and the orchestrator. `deploy_dev.sh` starts only these.
- **`conversion_worker.py`, `validation_worker.py` and `deployment_worker.py` are legacy copies.** Do not run them, and do not edit them expecting an effect. Make changes in the matching `*_service.py`.
- **`CanonicalMapping` is the boundary.** Everything vendor-specific ends at the parser. Downstream agents only see the canonical model, plus `source_vendor` for vendor-specific prompt wording.
- **Every agent call is audited.** Each call writes a row to `agent_response_etl` (request, response, tokens, errors). Each stage writes timeline rows to `workflow_event_etl`, which the SSE endpoints stream to clients.

---

## 3. Repository map

```text
ETL-CC-dev-viaan/
├── README.md, requirements.txt, Dockerfile, deploy_dev.sh
├── docs/                     ← this guide + older KT docs + API runbook + demo script
├── migrations/               ← hand-written SQL (see §5.3)
├── prompts/*.md              ← placeholders only (TODO). Real prompts live in agent .py files.
├── scripts/                  ← create_tables.py, check_tables.py, test_database.py, generate_encryption_key.py
├── runtime_sources/          ← sample inputs (Ab Initio .mp, Informatica XML copies)
├── runtime_validation_data/  ← old validation output (stale; current code doesn't write here)
├── tests/                    ← pytest suite, runs offline (see §8)
└── src/etl_cc/
    ├── main.py               FastAPI app, CORS, request logging
    ├── api.py                all HTTP routes
    ├── services.py           business logic behind the routes (start_* / get_* / list_*)
    ├── config.py             all settings (pydantic-settings, reads .env)
    ├── database.py           async engine + SessionFactory
    ├── models.py             ORM tables + Pydantic request/response + CanonicalMapping
    ├── security.py           Fernet credential encryption + HMAC connection-test tokens
    ├── logging_config.py     log file path + sanitised event logging
    ├── key_vault_service.py  fetches the OpenAI key from Key Vault; DynamicChatOpenAI wrapper
    ├── embedding_service.py  OpenAI embeddings for RAG
    ├── gliner_service.py     optional GLiNER routing/triage advisor (off by default)
    │
    ├── informatica_parser.py ┐
    ├── ab_initio_parser.py   ├─ source → CanonicalMapping
    ├── datastage_parser.py   ┘
    ├── parser_factory.py     (product, method) → parser
    ├── connectors.py         PowerCenter (pmrep), GitHub source, GitHub target probe, Postgres table reader
    ├── xml_package.py        safe XML/ZIP upload handling
    ├── source_store.py       temporary uploaded source bytes in Postgres
    ├── validation_store.py   uploaded validation datasets in Postgres
    │
    ├── worker.py             DISCOVERY worker (process #2)
    ├── dependency_planner.py migration waves + cycle detection (NetworkX)
    ├── economics.py          effort/cost estimates for dashboards
    │
    ├── langgraph_orchestrator.py  orchestrator (process #3)
    ├── conversion_service.py      conversion + critique stage
    ├── validation_service.py      validation stage
    ├── deployment_service.py      Git deployment stage
    ├── validation_test_case_service.py
    ├── *_worker.py (conversion/validation/deployment)  ← LEGACY, not run
    ├── orchestrator.py, engines.py, artifact_store.py, rag_backfill.py  ← stub / helpers
    └── agents/
        ├── discovery_agent.py     GPT-4o + deterministic rules/risks/complexity
        ├── lineage_agent.py       NetworkX field/component lineage (no LLM)
        ├── rag_retrieval_agent.py pgvector similarity + metadata rerank
        ├── conversion_agent.py    GPT-4o code generation + output contract checks
        ├── critique_agent.py      deterministic checks + GPT-4o review
        ├── unit_test_specification.py / unit_test_renderer.py / unit_test_execution_agent.py
        ├── validation_agent.py    the whole validation pipeline (largest file, ~3.6k lines)
        ├── test_plan_agent.py, matchflow_comparator.py, reproducible_validation.py, spark_runtime.py
        ├── deployment_agent.py    git clone/commit/push/PR
        └── gliner_decision_client.py
```

---

## 4. The flow, step by step

Every stage below names the endpoint, the code that runs, what it writes, and the status values to look for. All routes are prefixed with `/api/v1`.

### Step 0 — Check the API and catalog
- `GET /health` returns `{"status":"ok"}`.
- `GET /etl-products` lists products and ingestion methods. They are controlled by the `ENABLE_*` flags (§6).

### Step 1 — Source analysis
| Source | Endpoint | Service function | Parser |
|---|---|---|---|
| PowerCenter live | `POST /sources/powercenter/test` (JSON) | `analyze_powercenter` | `PowerCenterSource` → `InformaticaXMLParser` |
| GitHub XML folder | `POST /sources/github/test` (JSON) | `analyze_github` | `GitHubSource` → `InformaticaXMLParser` |
| Informatica XML upload | `POST /sources/xml/analyze` (form: `product_code=INFORMATICA`, `method_code=XML_UPLOAD`) | `analyze_xml_upload` | `InformaticaXMLParser` |
| DataStage DSX upload | `POST /sources/dsx/analyze` (form: `product_code=DATASTAGE`, `method_code=DSX_UPLOAD`) | `analyze_xml_upload` | `DataStageParser` |
| Ab Initio graph | `POST /sources/ab-initio/analyze` (form; `.mp` file) | `analyze_ab_initio_graph` | `AbInitioGraphParser` |

**What happens:**
1. The file or connection is parsed to list its mappings.
2. The bytes and a manifest are stored in `source_snapshot_etl`. Credentials are Fernet-encrypted.
3. The response returns `source_id` and a short-lived signed `test_token`, valid for `CONNECTION_TEST_TTL_SECONDS` (default 300).

### Step 2 — Discovery
**Call:** `POST /discoveries` with this body:

```json
{
  "source_id": "...",
  "test_token": "...",
  "scope_type": "ENTIRE_REPOSITORY",
  "selected_mapping_keys": []
}
```

For a subset, use `SELECTED_MAPPINGS` with real keys such as `accounts/MAPPING/m_x`, `project/graph_name` or `Folder/DSJOB/Job`.

1. `services.start_discovery` verifies the token. It then creates a `repository_etl` row and a `DISC-…` workflow (`job_type=DISCOVERY`, `QUEUED`). A repeated `(name, environment)` returns **409**.
2. `worker._claim` picks the job up. `worker._load_mappings` chooses the parser:
   - **Ab Initio:** when the product is `AB_INITIO` or the method is `AB_INITIO_GRAPH_UPLOAD`.
   - **DataStage:** when the product is `DATASTAGE` and the method is `DSX_UPLOAD`.
   - **Informatica XML:** when the method is `XML_UPLOAD`.
   - **PowerCenter or GitHub connector:** otherwise.
3. For each mapping, the worker runs:
   - `_upsert`, which saves the `CanonicalMapping` into `etl_object_etl.source_definition`.
   - `DiscoveryAgent`, which makes one GPT-4o call plus deterministic rules. It fills `business_rules`, `complexity` and related fields.
   - `LineageAgent`, which fills `lineage` and sets `discovery_status=ANALYZED`.
4. `DependencyPlanner` matches target names to source names across the selected mappings. It then assigns `migration_wave` and `has_dependency_cycle`.
5. The uploaded source bytes are deleted. The workflow then ends `COMPLETED` (stage `DEPENDENCY_PLANNING_COMPLETED`). If anything fails, `worker._fail` instead records `FAILED` plus a `WORKFLOW_FAILED` event with triage fields.

**Inspect the results with:**
- `GET /workflows/{id}` and `/workflows/{id}/events/stream`
- `GET /repositories/{id}/mappings` and `/discovery-dashboard`
- `GET /mappings/{etl_object_id}/discovery` and `/lineage`
- `GET /workflows/{id}/dependency-plan`

### Step 3 — Start a migration (conversion)
**Call (recommended):** `POST /migrations` with `{repository_id, etl_object_ids, validation_options}`.

1. `services.start_migration` creates a parent `MIG-…` workflow (`job_type=MIGRATION`). It then calls `start_conversion`, which creates a `CONV-…` child with `batch_id = MIG id`.
2. **A mapping can convert only if all of these hold** (checked in `start_conversion`; otherwise you get a 400):
   - `discovery_status == ANALYZED`
   - no dependency cycle
   - `migration_wave` is set
   - when GLiNER is enforced, a GLiNER `AUTO_CONVERT` decision
3. Mapping status becomes `QUEUED`.

`POST /conversions` creates a conversion on its own. Pass `migration_id` to attach it to a migration. Without one it is a standalone conversion: it converts only, with no automatic validation.

### Step 4 — The orchestrator runs conversion
`langgraph_orchestrator.run_orchestrator` loops through these steps:
1. `claim_migration()` takes the oldest QUEUED `CONVERSION`:
   - with a parent migration, it runs the LangGraph graph;
   - with no parent, it runs the conversion alone.
2. If there is no conversion, `claim_standalone_job()` takes either a `VALIDATION` with `scope_payload.trigger == "API"` (from `POST /validations`) or a `DEPLOYMENT` (from `POST /deployments`).

Graph: `conversion → validation → (deployment | complete) → END`, with any failure → `failed`.

**`conversion_node` calls `conversion_service.execute_conversion_workflow`.** For each mapping, in wave order:
1. Status becomes `CONVERTING`.
2. `RAGRetrievalAgent` fetches approved examples from `knowledge_base_etl`.
3. `ConversionAgent(mapping.source_vendor)` generates the three files. If they break the output contract (§9.4), the agent retries up to `CONVERSION_MAX_ATTEMPTS`.
4. `CritiqueAgent(mapping.source_vendor)` reviews them. If it answers REVISE, the critique feedback goes back to step 3.
5. Approved files are stored in `generated_artifact_etl` and `artifact_content_etl` with `validation_status=CRITIQUE_APPROVED`. Status becomes `CONVERTED`, or `REVIEW_REQUIRED` if the critique never passed.
6. When the conversion succeeds and has a parent, `_queue_automatic_validation` creates a `VAL-…` child in SIMULATE mode.

If an exception occurs, `_fail` marks the conversion and the parent migration `FAILED`, and leftover `QUEUED`/`CONVERTING` mappings become `CONVERSION_FAILED`.

**Inspect the results with:**
- `GET /conversions/{id}`
- `GET /conversions/{id}/workbench/{etl_object_id}` (shows everything for one mapping)
- `GET /mappings/{id}/artifacts`
- `GET /artifacts/{artifact_id}`

### Step 5 — Validation
**`validation_node` calls `validation_service.execute_validation_workflow`.** For each mapping:
1. Status becomes `VALIDATING`.
2. The artifacts are written to a temporary folder.
3. Input data is prepared according to `input_mode`:
   - `SIMULATE`: LLM synthetic rows expanded to `target_row_count`.
   - `DATASET_FILE`: uploaded via `POST /validations/datasets`.
   - `DATABASE_TABLES`: read through a connection saved with `POST /validation-database-connections`.
4. `ValidationAgent.run` executes this pipeline:
   1. test plan
   2. synthetic data and its validation
   3. LLM **reference plan**, which is checked and then executed in memory by `RelationalIRExecutor`
   4. static artifact checks
   5. generated pytest
   6. **target PySpark run** in a subprocess
   7. **MatchFlow** comparison of reference vs target rows
   8. confidence score
   9. optional GLiNER override
5. **A mapping passes only if all of these hold:**
   - static, unit, synthetic, reference and target checks all PASSED
   - the reference oracle is `STRONG` with full coverage and no human-review flag
   - match % ≥ `minimum_functional_parity × 100`
   - confidence ≥ `VALIDATION_CONFIDENCE_THRESHOLD`
6. On pass:
   - mapping status becomes `VALIDATED`, and the artifacts become `VALIDATED` + `is_deployable=True`;
   - if Git env settings exist and `AUTO_DEPLOY_AFTER_VALIDATION=true`, the parent is set to `DEPLOYMENT_QUEUED`;
   - otherwise the parent ends `COMPLETED` / `READY_FOR_DEPLOYMENT`.

   On fail, mapping status becomes `VALIDATION_FAILED` and the parent becomes `FAILED`.

**Inspect the results with:**
- `GET /validations/{id}`, `/report` and `/test-cases`
- `GET /migrations/{id}/dashboard`, which shows all stages together

**Manual re-validation:** `POST /validations` with a `conversion_workflow_id` that has COMPLETED. The new workflow inherits the conversion's migration (`batch_id`) and is tagged `trigger=API`. The orchestrator runs it, and if that queues deployment, it deploys too.

### Step 6 — Deployment
There are two ways to deploy.

**A. Automatic, after validation, using env settings.** `deployment_node` calls `deployment_service.execute_deployment_workflow(migration_id)` and, for each mapping:
1. Checks the validation evidence. `GitDeploymentAgent` also enforces the STRONG oracle and the other gate conditions.
2. Picks the **newest validated artifact of each type from the conversion that was validated**.
3. Clones `GIT_DEPLOYMENT_REPOSITORY_URL` and writes `<GIT_DEPLOYMENT_REPOSITORY_PATH>/<mapping>/{src,tests,config}` plus a manifest.
4. Commits, pushes branch `<prefix>/<mapping>/<id8>`, and optionally opens a PR.

The migration ends `DEPLOYMENT_COMPLETED` or `DEPLOYMENT_FAILED`.

**B. Manual, using a saved GitHub target:**
1. `POST /deployment-targets/github/test` returns a `target_id` and `test_token`.
2. `POST /deployment-targets/github` saves the target as a `repository_etl` row with `repository_role=DEPLOYMENT_TARGET`.
3. `POST /deployments` with `{repository_id, etl_object_ids, validation_workflow_id, target_repository_id, …}` creates one `DEP-…` workflow per mapping. The validation must belong to a migration.
4. The orchestrator claims each one. `execute_manual_deployment_workflow` deploys with the saved target's URL and decrypted token. When the last `DEP` job finishes, it marks the parent migration completed or failed.

**Inspect the results with:** `GET /deployments/{id}` and `GET /deployments/{id}/mappings` (branch, commit SHA, PR URL).

---

## 5. Data model and status values

### 5.1 Main tables (all defined in `models.py`)
| Table | Holds |
|---|---|
| `repository_etl` | Sources, and also saved GitHub deployment targets (`connection_config.repository_role=DEPLOYMENT_TARGET`). Encrypted credentials. |
| `etl_object_etl` | One row per mapping: canonical definition, discovery results, lineage, dependencies, `migration_wave`, `discovery_status`, `migration_status`. |
| `workflow_run_etl` | **The job queue.** `job_type` (DISCOVERY / MIGRATION / CONVERSION / VALIDATION / DEPLOYMENT), `job_status`, `current_stage`, `scope_payload`, `batch_id` (= parent MIG id), failure fields. |
| `workflow_event_etl` | Append-only timeline; streamed by the SSE endpoints. |
| `agent_response_etl` | One row per agent call: request/response JSON, tokens, errors. |
| `generated_artifact_etl` + `artifact_content_etl` | Generated files (metadata and content), with `validation_status` and `is_deployable`. |
| `knowledge_base_etl` + `rag_retrieval_event_etl` | RAG corpus (pgvector `embedding`) and retrieval telemetry. |
| `validation_test_case_etl` | Per-test validation evidence. |
| `source_snapshot_etl` (+ `_file_etl`) | Temporary uploaded source bytes (cleared after discovery). |
| `validation_dataset_etl`, `validation_database_connection_etl` | Validation inputs. |

### 5.2 Status lifecycles
**Mapping `migration_status`:**

```text
NOT_STARTED → QUEUED → CONVERTING → CONVERTED → VALIDATING → VALIDATED → DEPLOYMENT_QUEUED → DEPLOYMENT_IN_PROGRESS → DEPLOYED
                           │             │                         │                                         └→ DEPLOYMENT_FAILED
                           │             └→ REVIEW_REQUIRED         └→ VALIDATION_FAILED
                           └→ CONVERSION_FAILED
```

**Mapping `discovery_status`:** `DISCOVERED → ANALYZED`.

**Workflow `job_status` / `overall_status`:** `QUEUED → RUNNING → COMPLETED | FAILED`.

**Migration `current_stage`:**

```text
CONVERSION → VALIDATION → DEPLOYMENT_QUEUED → DEPLOYMENT_IN_PROGRESS → DEPLOYMENT_COMPLETED
```

The other ending states are `READY_FOR_DEPLOYMENT` (passed, no Git config), `CONVERSION_FAILED`, `VALIDATION_FAILED` and `DEPLOYMENT_FAILED`.

> If you add or rename a status, search the whole codebase for the string. Dashboards in `services.py` hard-code these sets, for example in `get_migration_dashboard` and `get_validation_report`.

### 5.3 Schema and migrations
- **Fresh database:** run `python scripts/create_tables.py`. It runs `Base.metadata.create_all()` from `models.py` and creates every table.
- **Existing database:** apply the files in `migrations/` by hand, in order. Two `002_*` files conflict; **always apply `003_allow_all_connection_types.sql` after them.**
- The SQL files hard-code schema `demooc28`. If your `PG_SCHEMA` differs, edit them before running.
- **Adding a column:** add it to the ORM class in `models.py` *and* write a new numbered `migrations/00N_*.sql` for existing databases. No Alembic is configured.

---

## 6. Configuration reference

All settings live in `src/etl_cc/config.py` (class `Settings`). Values are read from environment variables or from `.env` in the project root; names are case-insensitive. **Never commit `.env`.**

| Group | Variables (default) | Notes |
|---|---|---|
| **Required** | `PG_HOST`, `PG_DB`, `PG_USER`, `PG_PASSWORD`, `ETL_CREDENTIAL_ENCRYPTION_KEY`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_KEYVAULT_URL` | The app will not start without them. Generate the key with `python scripts/generate_encryption_key.py`. The same key signs connection tokens. |
| Database | `PG_PORT` (5432), `PG_SCHEMA` (demooc28) | |
| LLM | `AZURE_KEYVAULT_SECRET_NAME` (openai-key), `OPENAI_MODEL` (gpt-4o), `OPENAI_TEMPERATURE` (0.1), `OPENAI_TIMEOUT_SECONDS` (120), `OPENAI_MAX_OUTPUT_TOKENS` | The OpenAI key is fetched from Key Vault on **every** call. |
| Products | `ENABLE_INFORMATICA*` (true), `ENABLE_DATASTAGE` (true), `ENABLE_AB_INITIO` (true) | Drive `/etl-products`. Ab Initio upload is rejected when disabled. |
| GLiNER | `ENABLE_GLINER` (**false**), `GLINER_SERVICE_URL` (http://127.0.0.1:8100), `GLINER_SHADOW_MODE` (false), `GLINER_AUTO_CONVERT_THRESHOLD` (0.90) | When enabled and not in shadow mode, it gates conversion and can force validation to human review. See `docs/GLINER_INTEGRATION.md`. |
| Conversion / RAG | `CONVERSION_MAX_ATTEMPTS` (2), `RAG_*` (embedding model text-embedding-3-small, 1536 dims, similarity thresholds 0.50 / 0.65) | |
| Validation | `MINIMUM_FUNCTIONAL_PARITY` (**0.50**), `VALIDATION_CONFIDENCE_THRESHOLD` (0.60), `VALIDATION_TEST_TIMEOUT_SECONDS` (300), simulation and upload limits | Each request can set its own parity (API default 0.95). See §11. |
| Deployment | `GIT_DEPLOYMENT_REPOSITORY_URL`, `GIT_DEPLOYMENT_ACCESS_TOKEN`, `GIT_DEPLOYMENT_BASE_BRANCH` (main), `GIT_DEPLOYMENT_REPOSITORY_PATH` (migrations), `GIT_DEPLOYMENT_BRANCH_PREFIX` (neuflow), `GIT_DEPLOYMENT_CREATE_PULL_REQUEST` (false), `AUTO_DEPLOY_AFTER_VALIDATION` (true) | Used by **automatic** deployment only. Manual deployments use the saved target. |
| Runtime | `WORKER_POLL_SECONDS` (2), `MEMORY_WORKSPACE_ROOT` (/dev/shm/etl_cc), `ETL_CC_LOG_PATH` | Log default: `/home/authuser/dev/chatbots/ETL_CC/logs/etl_cc.log` on Linux, `<project>/logs/etl_cc.log` elsewhere. |

---

## 7. Running it locally

**Prerequisites:**
- Python 3.11+ (3.12 in Docker)
- PostgreSQL with the `pgvector` extension
- A JDK for PySpark, with `JAVA_HOME` set and `JAVA_HOME\bin` on `PATH`
- `git`
- Network access to Azure Key Vault and OpenAI

```powershell
# 1. Create a virtual environment in the project (don't copy one from another machine — its interpreter path won't exist)
py -3.12 -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt   # torch/gliner2 are only needed for a local GLiNER model

# 2. Configure: create .env in the project root with the required variables (§6)

# 3. Database
.\.venv\Scripts\python scripts\test_database.py
.\.venv\Scripts\python scripts\create_tables.py

# 4. Start the three processes, each in its own terminal, each with:
$env:PYTHONPATH = 'src'
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
.\.venv\Scripts\python -m uvicorn etl_cc.main:app --app-dir src --reload   # terminal 1: API → http://127.0.0.1:8000/docs
.\.venv\Scripts\python -m etl_cc.worker                                    # terminal 2: discovery
.\.venv\Scripts\python -m etl_cc.langgraph_orchestrator                    # terminal 3: conversion/validation/deployment
```

Check the startup logs. The orchestrator should print `graph_version=2.2.0`, and the discovery worker `parser_routing_version=3`. **Restart workers after every code change**, because they do not hot-reload.

**On the Linux dev server:** run `sudo ./deploy_dev.sh start|stop|restart|status|logs|health`. The script assumes `/home/authuser/dev/chatbots/ETL_CC`, Python 3.11 and API port 4046. Only the orchestrator's systemd unit is defined by the script; the API and discovery units must already exist.

**Quick demo with the bundled samples:**
- Ab Initio: `runtime_sources/ab_initio_graph/sample_ab_initio_graph.mp`
- Informatica: `runtime_sources/xml_upload/<any>/sample_informatica_mapping.xml` (all copies are identical)
- DataStage: `tests/fixtures/sample_customers.dsx`

---

## 8. Testing

```powershell
$env:PYTHONPATH = 'src'
.\.venv\Scripts\python -m pytest tests -q
```

- **Dummy env vars are enough.** The tests need no database, LLM, Java or network, but `Settings` still demands the required variables, so any values work. The encryption key must be a valid Fernet key.
- **54 tests** cover:
  - all three parsers
  - discovery routing and failure handling
  - lineage
  - dependency waves
  - conversion contract checks
  - unit-test spec, renderer and retries
  - validation reference plans and comparison keys
  - simulated-row expansion
  - orchestrator routing
  - deployment artifact selection
  - GLiNER
  - security helpers
- **Pattern for new tests:** use `monkeypatch` to replace `SessionFactory`, `load_manifest`/`load_content` or service functions with fakes. `tests/test_pipeline_flow_fixes.py` has a reusable `_FakeSession`.
- **What tests cannot prove:** real LLM output quality, SQL against a real schema, the real PySpark run, and Git pushes. After changing those areas, run one real migration end to end (§4) on a dev database.

---

## 9. How to make common changes

### 9.1 Add a new source product (e.g. Talend)
1. **Parser:** create `src/etl_cc/<product>_parser.py` with `list_mappings_bytes(content, source_reference)` and `parse_selected_bytes(content, selected_keys, source_vendor, source_reference)`. These return `MappingSummary` and `CanonicalMapping`. Copy the shape of `ab_initio_parser.py`.
2. **Register the parser:** add it in `parser_factory.get_parser`.
3. **Models:** in `models.py`, add the product and method to `ProductCode`/`MethodCode`, to `ETLIngestionMethodResponse`/`ETLProductResponse`, and to the `RepositoryETL` check constraint.
4. **Database:** write `migrations/00N_*.sql` that extends `ck_repository_etl_connection_type`.
5. **Services:** add the pair to `SUPPORTED_SOURCE_SELECTIONS` and an `analyze_*` function in `services.py`.
6. **API:** add an endpoint in `api.py` and an entry in `/etl-products`, plus an `ENABLE_*` flag in `config.py`.
7. **Discovery worker:** add a routing branch in `worker._load_mappings`. Also add the method to the upload set in `worker._process`, so the uploaded bytes are cleared after discovery.
8. **Vendor-specific prompts (optional):** add wording to `ConversionAgent`/`CritiqueAgent`. See how `DATASTAGE` is handled at the top of each class.
9. **Tests:** add a fixture under `tests/fixtures/` and parser tests.

### 9.2 Change an LLM prompt
- The prompts live **in code**; the files in `prompts/` are not loaded.
  - Discovery: `agents/discovery_agent.py`
  - Conversion: `agents/conversion_agent.py` (the `system_prompt` strings in `run` and in the unit-test-spec call, plus the evidence contract)
  - Critique: `agents/critique_agent.py`
  - Unit-test spec: `ConversionAgent`
  - Reference plan and synthetic data: `agents/validation_agent.py` (`ReferencePlanGenerator`, `SyntheticTestDataGenerator`)
- **Bump the agent's `PROMPT_VERSION`/`AGENT_VERSION`.** It is stored in every `agent_response_etl` row, which lets you compare runs before and after.
- Keep the prompt consistent with the deterministic checks (§9.4). A prompt that asks for something the validator rejects burns every retry.

### 9.3 Change validation strictness
- **Thresholds:** `MINIMUM_FUNCTIONAL_PARITY` and `VALIDATION_CONFIDENCE_THRESHOLD` in `.env`, or per request with `minimum_functional_parity`.
- **The pass gate:** `ValidationAgent.run` in `agents/validation_agent.py` (the `passed = (` block near the end of `run`; the GLiNER override follows it). The confidence formula is `calculate_confidence` in the same file.
- **Deployment eligibility:** `GitDeploymentAgent` in `agents/deployment_agent.py`, and also `start_deployment` in `services.py` for manual deploys. Keep the two aligned.

### 9.4 Change what the generated code must look like
- **PySpark contract:** `ConversionAgent._validate_pyspark_contract`.
- **Test contract:** `_validate_unit_test_contract`.
- **Config contract:** the config validation in the same file.
- **Critique checks:** `CritiqueAgent` deterministic checks.
- **Static checks before execution:** `StaticArtifactValidator` in `validation_agent.py`.
- **How the code is executed:** `TargetPySparkExecutor` (the validation harness) and `UnitTestRenderer` (the generated tests).

All of these must agree. For example, if `transform` gains a parameter, update the prompt, the validator, the renderer *and* the harness.

### 9.5 Add a new pipeline stage or change stage order
1. Implement `execute_*_workflow(run_id)` and `fail_*_workflow(run_id, exc)` in a `*_service.py`.
2. Add a node and conditional edges in `langgraph_orchestrator.build_graph`. Route on values that the previous stage wrote to the database.
3. Bump `graph_version` in `run_orchestrator`, and update the version in `docs/API_FLOW_RUNBOOK.md` and `docs/MEETING_DEMO_SCRIPT.md`.
4. If the API can queue the stage on its own, extend `claim_standalone_job` and `run_standalone_job`.

### 9.6 Add an API endpoint
1. Add Pydantic request/response models to `models.py`.
2. Put the logic in `services.py`.
3. Add a thin route in `api.py` that maps `ValueError` → 400, connector errors → 502 and `IntegrityError` → 409, like the existing routes.
4. Don't define a route twice. FastAPI silently keeps the first definition.

### 9.7 Add RAG knowledge
1. Insert rows into `knowledge_base_etl` with `approval_status='APPROVED'` and `metadata_payload` keys `source_vendor`, `target_framework` and `transformation_types`.
2. Run `python -m etl_cc.rag_backfill` to embed them.

Only APPROVED rows with `embedding_status='COMPLETED'` are retrieved.

### 9.8 Before you commit, check that…
- [ ] `pytest tests -q` passes.
- [ ] A new status string is handled in the dashboards (§5.2).
- [ ] A schema change has both the ORM change and a numbered SQL migration.
- [ ] A new setting is in `config.py` and in §6 of this guide.
- [ ] Changes went into `*_service.py`, not the legacy `*_worker.py`.
- [ ] Agent and graph versions are bumped if behaviour changed.
- [ ] This guide is updated.

---

## 10. Troubleshooting

| Symptom | Likely cause / where to look |
|---|---|
| API won't start: validation error on Settings | A required variable from §6 is missing in `.env` or the environment. |
| Discovery stays `QUEUED` | `python -m etl_cc.worker` isn't running. Check its log. |
| Conversion, validation or deployment stays `QUEUED` | The orchestrator isn't running, or it is running old code. Restart it and confirm `graph_version=2.2.0`. |
| `POST /discoveries` returns 400 for an invalid or expired token | The token lasts 300 s, and source snapshots last `SOURCE_SNAPSHOT_TTL_SECONDS` (1800 s). Re-run source analysis. |
| `POST /discoveries` returns 409 | `(connection_name, environment)` already exists. Use a new name. |
| `POST /migrations` returns 400 "not ready" | The mapping is not ANALYZED, is in a dependency cycle, has no wave, or GLiNER didn't return AUTO_CONVERT. |
| "AI service is temporarily unavailable" | Key Vault access or OpenAI key retrieval failed. Check the `AZURE_*` settings and the secret name. |
| RAG `NO_MATCH` | No APPROVED, embedded knowledge rows, or the scores are below threshold. Not fatal. |
| Conversion `REVIEW_REQUIRED` / contract failures | Read `agent_response_etl` for `CONVERSION_AGENT`/`CRITIQUE_AGENT`, or open the workbench endpoint. |
| `Critique did not approve` | Inspect the `issues` and failed `deterministic_checks` in the latest `CRITIQUE_AGENT` response. The workflow failure reason also reports these findings and separate conversion/critique attempt counts; a conversion contract retry can consume an attempt before critique runs. |
| `JAVA_GATEWAY_EXITED` in validation | The JDK is not installed or not on `PATH`. |
| Validation FAILED while the unit tests passed | Expected when the reference oracle is not STRONG or the match % is too low. Check `/validations/{id}/test-cases`. |
| Migration ends `READY_FOR_DEPLOYMENT` | Validation passed but Git env settings are missing. Deploy manually (§4 Step 6B) or set the variables. |
| Deployment fails | Check the target URL, token push rights, base branch, and the `MAPPING_DEPLOYMENT_FAILED` event payload. |

**Useful SQL:**

```sql
SELECT workflow_id, job_type, job_status, current_stage, failure_reason
FROM demooc28.workflow_run_etl ORDER BY id DESC LIMIT 20;

SELECT event_type, status, message, event_payload
FROM demooc28.workflow_event_etl WHERE workflow_run_id = <id> ORDER BY id;

SELECT agent_name, status, error_message
FROM demooc28.agent_response_etl WHERE etl_object_id = <id> ORDER BY id DESC;
```

Never edit workflow rows to `COMPLETED` by hand. Fix the cause and start a new workflow.

---

## 11. Known limitations and open decisions

These are known gaps, left unchanged on purpose. Fix them deliberately and update this list.

- **No authentication or authorisation** on any endpoint, and CORS allows `*`. Put the API behind a gateway before anyone outside the team can reach it.
- **Generated code runs unsandboxed** as the worker user during validation. The forbidden-pattern checks are simple substring matches.
- **`MINIMUM_FUNCTIONAL_PARITY` defaults to 0.50,** while the docs and the API default say 0.95. Decide which is right.
- **Test-ordering and contract mismatches** can make correct code fail:
  - `UnitTestRenderer` sorts expected rows by their string form but actual rows by Spark order.
  - `ConversionAgent` rejects module docstrings.
  - The validation harness never passes `parameters=` to `transform`.
- **Parameter matching misses Informatica and DataStage syntax:** `$$PARAM` names don't match the `$PARAM` regex in `unit_test_specification.py`, and DataStage `#PARAM#` is not matched at all.
- **`SafeExpression` gaps:** it does not support `IS NULL`, and it rewrites keywords inside string literals.
- **Discovery scoring is Informatica-centric,** so Ab Initio and DataStage complexity scores come out low.
- **Cleanup and housekeeping are missing:**
  - Expired source snapshots and validation uploads are never purged.
  - `runtime_validation_data/` is stale.
  - `prompts/*.md` are placeholders.
  - Legacy `*_worker.py` files should eventually be deleted.
  - There is no lock file, CI or Alembic.

---

## 12. Change log of the 2026-10-26 fixes

| Area | File(s) | Fix |
|---|---|---|
| Discovery | `worker.py` | `_fail` crashed on the undefined `failed_at`/`duration_ms`; it now records the failure with triage fields. DataStage DSX is routed to `DataStageParser`. |
| Schema | `migrations/003_allow_all_connection_types.sql` | Resolves the two conflicting `002` constraints. |
| Conversion | `conversion_service.py` | Agents receive `mapping.source_vendor`. On failure, mappings are no longer left `CONVERTING`/`QUEUED`. |
| Orchestration | `langgraph_orchestrator.py` | Standalone conversions run. API validations and deployments are now picked up. Deployment runs only when validation queued it (previously it overwrote `READY_FOR_DEPLOYMENT`). |
| Validation | `agents/validation_agent.py`, `services.py` | Fixed an infinite loop for short string keys. Manual validations inherit the migration and are tagged `trigger=API`. The report counts mappings that moved on to deployment as passed. |
| Deployment | `deployment_service.py`, `services.py` | Deploys only the newest validated artifacts of the validated conversion. Manual `DEPLOYMENT` workflows use the saved GitHub target and record results under the deployment workflow. Fixed a crash on a missing mapping. |
| API | `api.py`, `services.py` | Removed duplicate route and function definitions. Informatica no longer lists `DSX_UPLOAD`. |
| Ops | `logging_config.py`, `.gitignore` | Log path is configurable (`ETL_CC_LOG_PATH`), with a project-local default off Linux. |
| Tests and fixtures | `tests/`, `sample_ab_initio_graph.mp` | Fixed the sample's `PROC_DATE` parameter and stale tests, and added `test_pipeline_flow_fixes.py`. 54/54 pass. |
