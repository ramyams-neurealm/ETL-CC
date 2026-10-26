# ETL PROJECT KNOWLEDGE TRANSFER DOCUMENT

**Repository reviewed:** `ETL-CC-main`  
**Review basis:** Static review of every file present in the repository inventory, including Python modules, SQL migrations, prompt stubs, scripts, Docker configuration, dependency manifest, README, tests, and ignore rules. No live database, Azure tenant, Informatica repository, GitHub repository, Databricks workspace, environment file, or production logs were available.  
**Evidence rule:** Statements below distinguish implementation found in the repository from operational facts that cannot be established from source. “Not Found in Repository” means there is no supporting configuration or implementation in this checkout; it does not prove that a service is absent from the deployed environment.

---

## One-page Executive KT Summary

**What it is:** ETL Migration Command Center (ETL CC), described in the README as the “NeuFlow ETL Migration Command Center.” It is an async Python/FastAPI application that assists with moving Informatica PowerCenter mappings toward Databricks PySpark. It is a migration and governance platform, not a repository of conventional nightly ETL pipelines.

**Main route through the system:**

```text
Informatica PowerCenter / GitHub XML / XML or ZIP upload
        -> API source analysis and short-lived source reference
        -> PostgreSQL repository + discovery job
        -> canonical mapping + discovery, lineage, dependency waves
        -> RAG examples + GPT-4o PySpark, pytest, and config artifacts
        -> deterministic critique and bounded regeneration
        -> static, unit, synthetic/reference/data validation
        -> optionally push validated artifacts to a GitHub repository
```

**System of record:** PostgreSQL in configured schema `demooc28` by default; it stores repositories, mappings, workflow state/events, agent audit payloads, generated artifact content, knowledge/embeddings, uploaded validation data, and validation evidence. A pgvector-backed knowledge table supports retrieval. **It is not the target analytical warehouse.**

**Important runtime components:** `src\etl_cc\main.py` serves the API; `src\etl_cc\worker.py` polls discovery work; `src\etl_cc\langgraph_orchestrator.py` owns parent migration sequencing; conversion, validation, and deployment stage services live in `conversion_service.py`, `validation_service.py`, and `deployment_service.py`. `deploy_dev.sh` refers to systemd services, but the unit files for the API and discovery worker are not included.

**What is documented vs unknown:** source formats, API endpoints, metadata schema, AI agent stages, configuration setting names, and the Linux development service script are visible. Business owners, production schedules/SLAs, deployed service topology, actual data volumes, live schema state, access policies, Databricks workspace/runtime, and rollback/release approvals are **Not Found in Repository**.

**First production-support checks:** inspect workflow status and event records; check PostgreSQL connectivity/schema; check `logs\etl_cc.log` on the configured Linux host and systemd/journal status; verify Azure Key Vault service-principal access and OpenAI secret; for PowerCenter verify `pmrep`, network, repository credentials; for GitHub verify branch/repository permissions; for local test/deployment stages check available disk/RAM and Spark/pytest dependencies.

**Immediate ownership risks:** no auth/RBAC implementation is evident in the API; no automated test/build/release pipeline is present; dependency/service unit definitions are incomplete; there are parallel legacy workers and newer LangGraph stage services; deployment entry points/configuration are not fully aligned; source data and full agent payloads are persisted in PostgreSQL; no demonstrated retention cleanup for expired source snapshots or uploaded validation datasets.

**Project understanding score: 72/100.** The code gives a good understanding of intended product and workflow, but operational confidence is limited by absent environment-specific configuration, infrastructure definitions, business documentation, deployment records, and broad tests. See Section 30 for rationale.

---

## 1. Executive Summary

**Confidence: High for code-level purpose; Low for business ownership and production topology.**

| Item | Repository-supported understanding |
|---|---|
| Project name | ETL Migration Command Center / NeuFlow ETL Migration Command Center. README identifies the project; API title defaults to “ETL Migration Command Center” in `src\etl_cc\config.py`. |
| Business purpose | Analyze Informatica PowerCenter metadata, explain mappings, derive lineage/dependencies, generate Databricks PySpark conversion artifacts, validate them, and optionally deploy to GitHub. |
| Business stakeholders | **Not Found in Repository.** No named business sponsor, data owner, migration program, support group, or consumer team. |
| Source systems | Informatica PowerCenter live repository (via `pmrep`); GitHub repository containing Informatica XML; uploaded Informatica XML/ZIP package. Validation can additionally use uploaded CSV/JSON/Parquet data or PostgreSQL tables. See `src\etl_cc\connectors.py`, `src\etl_cc\services.py`, `src\etl_cc\validation_store.py`. |
| Target systems | PostgreSQL metadata store; generated Databricks/PySpark code, pytest unit tests, configuration artifacts; optional GitHub deployment repository. No Databricks workspace/table target is configured in this repository. |
| Main deliverables | Mapping inventory and canonical metadata; discovery/business-rule and lineage evidence; migration-wave plan; generated `PYSPARK_CODE`, `UNIT_TEST`, `CONFIGURATION` files; validation evidence/report; optional Git branch/commit/pull request. |
| High-level ETL overview | Source metadata ingestion → canonicalization → deterministic/AI discovery and lineage → dependency graph → RAG-assisted code generation and critique → validation → optional Git deployment. It does not extract/transform/load production business datasets into a curated warehouse as a recurring job. |

## 2. Project Overview

**Confidence: High for product behavior; Medium for the intended business problem.**

The README describes a simple migration command center and its early focus: test Informatica repository connectivity, enumerate mappings, select all or selected mappings, encrypt/save credentials, and save mapping metadata. The implementation has grown beyond that initial README: it includes discovery, lineage, RAG, conversion, critique, validation, economics, and deployment modules.

The problem addressed is the labor and risk of understanding and migrating legacy Informatica mappings. The system models mappings as structured `CanonicalMapping` objects (defined in `src\etl_cc\models.py`), runs deterministic analysis and LLM-assisted explanation, and produces pure transformation code with tests rather than embedding source/target connection details into generated logic.

Expected output is migration evidence and code, not a live data mart. The generated transformation contract in `src\etl_cc\agents\conversion_agent.py` is `transform(inputs: dict[str, DataFrame]) -> DataFrame`; generated code is explicitly constrained from owning Spark session creation or source/target I/O. Downstream deployment writes the artifacts into a Git repository structure (`src`, `tests`, `config`) through `src\etl_cc\agents\deployment_agent.py`.

Critical use cases evidenced:

1. Inventory and analyze Informatica mappings before migration (`src\etl_cc\api.py`, `src\etl_cc\worker.py`).
2. Understand field lineage, business rules, prerequisites, risks, and dependency waves (`src\etl_cc\agents\discovery_agent.py`, `src\etl_cc\agents\lineage_agent.py`, `src\etl_cc\dependency_planner.py`).
3. Generate and critique PySpark, tests, and non-secret configuration (`src\etl_cc\agents\conversion_agent.py`, `src\etl_cc\agents\critique_agent.py`).
4. Validate the generated conversion against static contracts, tests, synthetic cases, and/or reference data (`src\etl_cc\agents\validation_agent.py`).
5. Deploy only evidence-qualified results to GitHub (`src\etl_cc\agents\deployment_agent.py`, `src\etl_cc\deployment_service.py`).

The end business domain, subject areas, consumers, and value measures are **Not Found in Repository**.

## 3. Architecture Overview

**Confidence: High for application architecture; Low for deployed topology.**

### Components and movement

- **FastAPI API:** REST endpoints under `/api/v1`; entry point `src\etl_cc\main.py`, router `src\etl_cc\api.py`.
- **PostgreSQL:** persistence for metadata, queues, events, artifacts, reference knowledge, and validation data; async SQLAlchemy/asyncpg; default schema setting `demooc28`.
- **Discovery worker:** polls the PostgreSQL workflow table, reads sources, parses Informatica XML, executes Discovery and Lineage agents, and plans dependency waves; `src\etl_cc\worker.py`.
- **Migration orchestrator:** claims a conversion associated with a parent Migration and sequences conversion, validation, and deployment through LangGraph; `src\etl_cc\langgraph_orchestrator.py`.
- **AI and retrieval services:** Key Vault credential retrieval, OpenAI chat/embedding calls, PostgreSQL pgvector retrieval; `src\etl_cc\key_vault_service.py`, `src\etl_cc\embedding_service.py`, `src\etl_cc\agents\rag_retrieval_agent.py`.
- **Validation/runtime tools:** PySpark, pytest, pandas/Arrow, optional PostgreSQL reference reads; `src\etl_cc\agents\validation_agent.py`, `src\etl_cc\agents\unit_test_execution_agent.py`, `src\etl_cc\connectors.py`.
- **Deployment:** materialize validated artifact content into a temporary Git worktree, commit/push and optionally create a pull request; `src\etl_cc\agents\deployment_agent.py`.
- **External integrations:** Informatica `pmrep`, GitHub REST API and Git CLI, Azure Key Vault/identity, standard OpenAI API. No ADF, Airflow, Dagster, Kafka, Databricks Jobs, or cloud storage integration is defined in the checkout.

```text
 Informatica PowerCenter ---- pmrep ----\
 GitHub XML exports -------- GitHub API ---+--> FastAPI source analysis
 XML/ZIP upload --------------------------/          |
                                                   PostgreSQL
                                      transient source + repository/
                                      canonical mappings + queued jobs
                                                   |
                          +------------------------+--------------------+
                          |                                             |
                   Discovery worker                           LangGraph orchestrator
              XML parse -> Discovery/Lineage                 Conversion -> Validation
               -> dependency graph/waves                     -> optional Deployment
                          |                                             |
                          +---------------- PostgreSQL -----------------+
                                                   |
          approved knowledge -> pgvector RAG -> generated PySpark + tests + config
                                                   |
                        validation inputs: SIMULATE / files / PostgreSQL
                                                   |
                      optional GitHub branch, commit, pull request
                                                   |
               Databricks execution / warehouse consumers: Not Found
```

## 4. Repository Structure

**Confidence: High.** This tree reflects files in the reviewed checkout; it is not a claim about omitted deployment repositories.

```text
ETL-CC-main/
|-- .gitignore
|-- README.md
|-- requirements.txt
|-- Dockerfile
|-- deploy_dev.sh
|-- migrations/
|   |-- 001_create_etl_tables.sql
|   |-- conversion_migration.sql
|   |-- validation_test_cases.sql
|   `-- validation_v2.sql
|-- prompts/
|   |-- conversion.md
|   |-- critique.md
|   |-- deployment.md
|   |-- discovery.md
|   |-- lineage.md
|   |-- rag_retrieval.md
|   `-- validation.md
|-- scripts/
|   |-- check_tables.py
|   |-- create_tables.py
|   |-- generate_encryption_key.py
|   `-- test_database.py
|-- src/etl_cc/
|   |-- API, config, persistence, connectors, services and worker modules
|   `-- agents/
|       `-- discovery, lineage, retrieval, conversion, critique,
|           deployment, validation and test-support modules
`-- tests/
    |-- test_dependency.py
    `-- test_security.py
```

| Folder | Purpose, key files, dependencies |
|---|---|
| `src\etl_cc\` | Application implementation. `api.py`, `services.py`, `models.py`, workers, connectors, security, logging, orchestration. Depends on packages in `requirements.txt` and configured external services. |
| `src\etl_cc\agents\` | Agent logic and validation/test tooling. Includes Informatica discovery/lineage, RAG, code generation, critique, Git deployment, synthetic/static/reference validation, generated unit-test renderer/executor. |
| `scripts\` | PostgreSQL connectivity, schema/table creation/checking, Fernet-key generation. |
| `migrations\` | Incremental PostgreSQL DDL/DML. The file named `001_create_etl_tables.sql` is an ALTER migration, not the baseline table-creation script; the SQLAlchemy `create_tables.py` script creates model tables. |
| `prompts\` | Seven version-heading placeholders only; each currently says TODO. Actual agent instructions are embedded in Python modules. |
| `tests\` | Two very small tests: wave ordering and Fernet key length. No broad end-to-end or API test suite found. |
| Root files | README launch instructions, pinned-by-range requirements, API Docker image, and Linux systemd development control script. |

**Folders/files not present:** notebooks, `configs`, `.github` workflows, Airflow/Dagster DAGs, ADF JSON, Terraform/Bicep, Kubernetes/Helm, environment-specific YAML/JSON, production runbooks, dashboards, and data model documentation.

## 5. Technology Stack

**Confidence: High for declared dependencies; Medium for runtime usage.**

| Technology | Purpose and evidence |
|---|---|
| Python | Application language. Docker uses `python:3.12-slim` in `Dockerfile`; deployment script hardcodes Python 3.11. README does not state a version. |
| FastAPI / Uvicorn | REST API and ASGI server; `src\etl_cc\main.py`, `src\etl_cc\api.py`, `requirements.txt`. |
| Pydantic v2 / pydantic-settings | Request/response validation and environment-backed settings; `src\etl_cc\models.py`, `src\etl_cc\config.py`. |
| SQLAlchemy async / asyncpg | PostgreSQL model persistence, async sessions, queue claims; `src\etl_cc\database.py`, `src\etl_cc\models.py`, `requirements.txt`. |
| PostgreSQL + pgvector | Metadata persistence and vector similarity retrieval; `src\etl_cc\models.py`, `src\etl_cc\agents\rag_retrieval_agent.py`, `requirements.txt`. PostgreSQL version and managed service are **Not Found in Repository**. |
| NetworkX | Dependency DAG, cycle detection, migration-wave calculation; `src\etl_cc\dependency_planner.py`, `src\etl_cc\engines.py`. |
| Informatica PowerCenter `pmrep` | Live source repository command-line access; `src\etl_cc\connectors.py`. Binary/license/install path is environment-provided. |
| XML / defusedxml | Safe parsing of Informatica exports and XML packages; `src\etl_cc\informatica_parser.py`, `src\etl_cc\xml_package.py`. |
| GitHub REST API + Git CLI | XML source acquisition and target repository operations; `src\etl_cc\connectors.py`, `src\etl_cc\agents\deployment_agent.py`. |
| LangGraph | Parent migration stage graph; `src\etl_cc\langgraph_orchestrator.py`. |
| LangChain OpenAI / OpenAI | Structured GPT-4o outputs and embeddings; `src\etl_cc\key_vault_service.py`, agent files, `src\etl_cc\embedding_service.py`. |
| Azure Identity / Key Vault | Service-principal authentication and dynamic OpenAI key lookup; `src\etl_cc\key_vault_service.py`. Azure OpenAI endpoint/resource configuration is **Not Found**; code uses standard OpenAI client with a retrieved key. |
| PySpark | Generated artifact target framework and local validation/runtime; declared `pyspark>=4.2,<4.3`. Databricks Runtime/Spark cluster version is **Not Found**. |
| pandas / PyArrow | Parse uploaded CSV/JSON/Parquet validation datasets; `src\etl_cc\validation_store.py`. |
| cryptography / Fernet | Encrypt saved source and validation database credentials; `src\etl_cc\security.py`. |
| Docker | API image only; `Dockerfile`. No compose, Kubernetes, or image publishing pipeline found. |
| systemd / journalctl | Development deployment controls and logs; `deploy_dev.sh`. Unit definitions for all active services are not included. |
| pytest | Tests and generated unit-test execution; `requirements.txt`, `tests\`, `src\etl_cc\agents\unit_test_execution_agent.py`. |

## 6. ETL Pipeline Inventory

**Confidence: High for implemented workflow types; Low for deployed schedules.**

There are not ten independent production ETL pipelines in this repository. The product implements migration workflows and subflows. No business-data schedule is defined.

| Workflow / pipeline | Purpose | Trigger / schedule | Source → destination | Dependencies |
|---|---|---|---|---|
| Source analysis: PowerCenter | Test `pmrep`, enumerate mappings | API `POST /sources/powercenter/test`; on demand, `src\etl_cc\api.py` | Live Informatica metadata → source snapshot + preview | `pmrep`, network, PostgreSQL, credentials |
| Source analysis: GitHub XML | Download XML exports and list mappings | API `POST /sources/github/test`; on demand | GitHub repo XML → source snapshot/manifest and preview | GitHub API, token if private, PostgreSQL |
| Source analysis: XML/ZIP | Validate files/package and merge mapping exports | API `POST /sources/xml/analyze`; on demand | Uploaded XML/ZIP → transient PostgreSQL source package | upload limits, defusedxml, PostgreSQL |
| Discovery and lineage | Canonicalize mapping metadata; business explanation, risks, field lineage | `POST /discoveries` queues work; `worker.py` polls every `worker_poll_seconds` (default 2 seconds) | Source metadata → repository/mapping/audit records | Discovery and Lineage agents, OpenAI, DB |
| Dependency planning | Infer producer-consumer edges and waves; detect cycles | End of Discovery worker execution | Selected canonical mappings → dependencies/waves in PostgreSQL | NetworkX |
| Standalone Conversion | Generate PySpark/test/configuration with RAG and Critique | `POST /conversions`; LangGraph or legacy worker paths exist | Canonical mappings + approved knowledge → artifacts stored in PostgreSQL | OpenAI, Key Vault, PostgreSQL/pgvector |
| Parent Migration | Orchestrate Conversion → Validation → Deployment branch | `POST /migrations`; `langgraph_orchestrator.py` claims conversion jobs | Mapping/artifacts/evidence → parent/child workflow records | LangGraph, Conversion and Validation services |
| Automatic validation | Static, unit, synthetic/reference/data comparison | After successful parent Migration conversion; validation child uses SIMULATE mode by default | Generated files + mapping evidence + optional uploaded/DB reference data → evidence | PySpark, pytest, pandas, optional PostgreSQL |
| Standalone validation | Validate a completed conversion using SIMULATE, uploaded datasets, or database tables | `POST /validations` | Artifacts + test/reference inputs → validation results | completed conversion and required artifacts |
| Git deployment | Deploy qualified files to branch, optionally PR | Automatic parent flow or `POST /deployments` | PostgreSQL artifacts → GitHub repository | strong validation evidence, Git, GitHub credentials |
| RAG embedding backfill | Generate embeddings for approved knowledge lacking current embeddings | Manual `python -m etl_cc.rag_backfill`; no schedule found | Approved knowledge → pgvector embedding columns | Key Vault, OpenAI embeddings, PostgreSQL |
| MatchFlow data comparison | Compare legacy/target rows using keys and numeric tolerance | API `POST /validations/{workflow_id}/data-comparison` | Legacy and generated output rows → comparison result | in-memory rows, comparison key |

Triggers, route names and worker paths are defined in `src\etl_cc\api.py`, `src\etl_cc\services.py`, `src\etl_cc\worker.py`, `src\etl_cc\langgraph_orchestrator.py`, and `src\etl_cc\deployment_worker.py`. **Cron schedules, business SLA, and deployed job concurrency are Not Found in Repository.**

## 7. Source Systems Analysis

**Confidence: High for supported source modes; Medium for production authentication setup.**

| Source | Type / connection | Authentication | Format / frequency |
|---|---|---|---|
| Informatica PowerCenter | Live repository via `pmrep` subprocess; configured executable path and timeout in `src\etl_cc\config.py`, implementation in `src\etl_cc\connectors.py` | Repository username/password, encrypted at rest after source analysis; password handed to subprocess through environment variable name `ETL_CC_PMREP_PASSWORD` rather than CLI argument | Folder/mapping inventory then individual XML exports; on-demand, no refresh schedule |
| GitHub source repository | GitHub REST contents API; `GitHubSource` in `src\etl_cc\connectors.py` | Optional access token stored encrypted if supplied; exact deployed token scope is **Not Found** | XML files in configured folder/branch; on-demand |
| XML/ZIP upload | Multipart file upload; safe ZIP expansion and XML validation in `src\etl_cc\xml_package.py` | API-level user authentication is **Not Found in Repository**; no identity/role enforcement visible | Informatica XML only; package limits in settings; on-demand |
| Validation file dataset | Upload to `POST /validations/datasets`; `src\etl_cc\validation_store.py` | API-level authentication is **Not Found**; bytes stored in PostgreSQL with SHA-256 | CSV, JSON/JSONL, Parquet; on-demand |
| Validation PostgreSQL tables | `asyncpg` table reader in `src\etl_cc\connectors.py` | Separate encrypted DB password; connection row stored in `validation_database_connection_etl` | PostgreSQL only; row-limited table reads; on-demand validation |

Oracle, SQL Server, SAP, Salesforce, REST business APIs, SFTP, XML business feeds, and other source adapters are **Not Found**. `src\etl_cc\config.py` has disabled feature flags for DataStage and Ab Initio; their parsers/connectors are not present.

## 8. Target Systems Analysis

**Confidence: High for configured targets; Low for actual consumer adoption.**

| Target | Contents / schema | Consumer and usage |
|---|---|---|
| PostgreSQL metadata database | Dynamic `pg_schema` (default `demooc28`); SQLAlchemy tables in `src\etl_cc\models.py` | API, workers, event/status UI, audit, artifacts, RAG, validation |
| GitHub deployment repository | Per mapping `src/`, `tests/`, `config/` plus deployment manifest; `src\etl_cc\agents\deployment_agent.py` | Code review and later execution; repository URL/base branch are configuration or user target values |
| Databricks/PySpark artifact contract | Generated transformation and test/config files; platform/framework literals in `ConversionAgent` | Intended downstream runtime; Workspace, catalog, schema, table names, job, and consumers are **Not Found** |
| PostgreSQL source/reference tables | Used only as optional validation inputs through `load_postgresql_table` | Validation reference input; not an application-managed warehouse destination |

No target table inventory or curated business schema is included. Generated artifacts are initially stored in `generated_artifact_etl` and `artifact_content_etl`, not directly in Databricks.

## 9. Detailed Data Flow

**Confidence: High.**

1. **Analyze source:** API source endpoint dispatches to `analyze_powercenter`, `analyze_github`, or `analyze_xml_upload` in `src\etl_cc\services.py`. It verifies method selection, lists mapping summaries, creates a source snapshot/manifest, and returns a short-lived signed connection-test token. XML and GitHub source flows use `src\etl_cc\informatica_parser.py`; uploads are expanded/merged by `src\etl_cc\xml_package.py`.
2. **Save and queue Discovery:** `POST /discoveries` calls `start_discovery` in `services.py`. The signed token is bound to a hash of the source connection details. The service creates `repository_etl` and a queued `workflow_run_etl`, stores the source ID in scope, and writes a workflow event.
3. **Extract mapping definitions:** `src\etl_cc\worker.py` claims a queued DISCOVERY row using a PostgreSQL row lock with `SKIP LOCKED`. It selects a connector based on `connection_type`; XML packages load from the transient source store, GitHub uses XML exports, and PowerCenter uses `pmrep`.
4. **Canonicalize and persist:** `InformaticaXMLParser` returns canonical mappings (datasets, fields, transformations, ports, connectors, parameters, sessions/workflows). `_upsert` in `worker.py` stores mapping definition and hash in `etl_object_etl`.
5. **Enrich and validate metadata:** `DiscoveryAgent.run` combines deterministic facts/risk/complexity with structured GPT-4o semantic analysis. `LineageAgent.run` builds graph and target-field paths. Both are audited in `agent_response_etl`; progress is emitted in `workflow_event_etl`.
6. **Plan dependencies:** `DependencyPlanner.run` matches normalized target dataset names against source dataset names, includes explicit dependency evidence, identifies cycles, then assigns waves. Results are written to `etl_object_etl.dependencies`, `migration_wave`, and `has_dependency_cycle`. Cyclic mappings are blocked from conversion by `start_conversion`.
7. **Retrieve examples and convert:** Conversion retrieves approved, embedded knowledge with vector similarity plus metadata scores (`src\etl_cc\agents\rag_retrieval_agent.py`); GPT-4o generates PySpark, a pytest test, and configuration. `ConversionAgent` enforces output structure and code/config contracts. `CritiqueAgent` applies deterministic checks plus LLM critique; up to `conversion_max_attempts` attempts are allowed.
8. **Persist artifacts:** `_save_artifacts` in `src\etl_cc\conversion_service.py` writes metadata/content to `generated_artifact_etl` and `artifact_content_etl`; SHA-256 hashes are recorded. Conversion-only file artifacts are not a runnable production job by themselves.
9. **Validate:** `ValidationAgent` runs static checks, unit tests, synthetic data, reference/functional parity, and optional data checks. `validation_service.py` materializes artifacts temporarily and reads SIMULATE, uploaded, or PostgreSQL table input. It records agent responses/test cases and promotes artifacts to validated/deployable only on pass.
10. **Deploy/consume:** Git deployment checks validation evidence and artifact hashes, builds a temporary worktree, pushes a branch and optionally creates a PR. Actual Databricks import/run and business reporting consumption are **Not Found**.

## 10. Data Transformation Logic

**Confidence: High for the migration analysis and generated-code contract; Low for business rules before a real mapping is supplied.**

This repository generally transforms **mapping metadata**, not business rows. It represents a mapping’s source, target, transformation graph, expressions, and field lineage. Generated PySpark contains the migrated business transformation for an individual mapping.

| Transformation stage | Input | Logic | Output |
|---|---|---|---|
| Informatica parser | PowerCenter XML | Identifies mapping, folder, sources, targets, transformation instances/ports, expressions, parameters, connectors, session/workflow metadata; validates XML root and accepted port types | `CanonicalMapping` in `src\etl_cc\informatica_parser.py` and schemas in `models.py` |
| Discovery | Canonical mapping | Counts transformation types; derives deterministic rule/prerequisite/risk and complexity evidence; obtains structured semantic interpretation from GPT-4o | Purpose, rules, risks, unsupported constructs, confidence, human-review flag |
| Lineage | Canonical transformations, ports and connectors | Builds component graph and traces target fields upstream to source fields; flags warnings/cycles | Nodes, edges, field paths and coverage in `lineage_agent.py` |
| Dependency planner | Selected mappings | Normalize dataset names to uppercase; producer target / consumer source matches form directed edges; explicit mapping dependencies are resolved by key/name; cycles found with NetworkX | Upstream/downstream lists and topological migration waves |
| RAG | Mapping details + approved knowledge | Embedding cosine similarity; metadata score combines transformation overlap (60%), PySpark-framework match (20%), Informatica-vendor match (20%); weighted final score default 70% vector + 30% metadata; thresholds default vector 0.50 and final 0.65; return at most 8 | Grounded examples/evidence passed to conversion prompt |
| Conversion | Canonical mapping, discovery, lineage, dependency evidence, RAG and critique feedback | GPT-4o generates a transform-only PySpark module, in-memory pytest suite, and non-secret configuration; AST/contract checks prohibit session creation, I/O and credential/connection placeholders | Exactly one artifact of each required type: PySpark, unit test, configuration |
| Validation | Canonical mapping + generated artifact files + chosen validation inputs | Static artifact validation, safe generated unit test execution, synthetic-case generation, legacy/reference planning/execution, functional parity and optional dataset comparison | Pass/fail, deployability, parity/confidence, test outcomes, evidence and recommendation |
| MatchFlow comparison | Two row datasets + keys + Decimal tolerance | Reject missing/duplicate keys; compare keyed rows and per-column values; report missing/additional/different rows and at most 100 sample differences | `MatchFlowResult` from `matchflow_comparator.py` |

**Illustrative dependency example (conceptual, not a repository business mapping):**

```text
Mapping A produces CUSTOMER_STAGE
Mapping B reads CUSTOMER_STAGE and produces CUSTOMER_CURATED
=> A -> B; A can be migration wave 1, B wave 2.
```

**Illustrative generated transformation contract (contract only, not generated code from a real mapping):**

```python
def transform(inputs: dict[str, DataFrame]) -> DataFrame:
    source_df = inputs["EXACT_CANONICAL_SOURCE_NAME"]
    # Apply only transformations evidenced in the mapping.
    return source_df.select("TARGET_FIELD")
```

## 11. SQL Analysis

**Confidence: High.**

There is no library of ETL business SQL or SQL transformation query files. The checked-in SQL is database DDL/DML:

| SQL file | Purpose and tables | Logic / relationship |
|---|---|---|
| `migrations\001_create_etl_tables.sql` | Despite its name, adds `connection_type` to `demooc28.repository_etl`; backfills null to `POWERCENTER`, makes it required, drops credential `NOT NULL`, adds accepted-method check for POWERCENTER/GITHUB/XML_UPLOAD and an index | Assumes the base table already exists. Uses a guarded constraint creation block. |
| `migrations\conversion_migration.sql` | Adds nullable `etl_object_id` FK to `generated_artifact_etl`, references `etl_object_etl(id)` with `ON DELETE SET NULL`; adds index | Transactional migration |
| `migrations\validation_test_cases.sql` | Creates `validation_test_case_etl`; FKs workflow run with cascade, mapping/agent response with set-null; unique scope `(workflow_run_id, etl_object_id, test_id)` and two indexes | Stores category/status/severity, expected/actual/details, JSONB evidence, duration and timestamps |
| `migrations\validation_v2.sql` | Adds `ix_agent_response_synthetic_lookup` over workflow, object, agent, descending id | Query-performance support |

`src\etl_cc\connectors.py` also builds a parameterized PostgreSQL `SELECT * FROM schema.table LIMIT $1` for validation table sampling after validating schema/table identifiers. The other database queries are expressed in SQLAlchemy in `services.py`, workers, and stores. There are no SQL views, stored procedures, warehouse DDL, or transformation queries found.

## 12. Databricks Analysis

**Confidence: High that Databricks is a generated-code target; Low for runtime deployment.**

- Databricks is selected as `target_platform` and PySpark as `target_framework` in conversion schemas/agent prompts (`src\etl_cc\agents\conversion_agent.py`, `src\etl_cc\models.py`).
- Required output files are PySpark code, unit tests, and configuration. Generated code must implement `transform(inputs)` and avoid SparkSession creation, external reads/writes, JDBC, and embedded connection details.
- Unit tests are rendered/executed locally with pytest/Spark fixture logic (`unit_test_renderer.py`, `unit_test_execution_agent.py`).
- **Not Found in Repository:** workspace URL, catalog/schema, Unity Catalog objects, cluster policy, Databricks Runtime, SQL warehouse, DLT, notebook source/export, Jobs/Workflows configuration, secret scopes, service principal, Delta table definitions, or deployment API.
- The dependency `pyspark>=4.2,<4.3` does not establish the version of Spark in a deployed Databricks environment.

No Databricks notebook files exist in the inventory. There is consequently no notebook-by-notebook input/processing/output documentation to provide.

## 13. Spark Processing Analysis

**Confidence: High for implemented generation/validation constraints; Medium for local Spark execution details.**

The application itself is predominantly async Python and does not submit a distributed Spark job. Spark is the generated-code framework and part of local validation. `ConversionAgent` asks for a pure DataFrame transform; `UnitTestRenderer` creates in-memory test data; `UnitTestExecutionAgent` executes tests in a controlled environment; `ValidationAgent` has a `TargetPySparkExecutor` for generated code.

No Spark caching, repartitioning, broadcast join strategy, cluster sizing, Delta optimization, Z-order, AQE configuration, or Databricks job scheduling is implemented as a project runtime feature. Those practices may be selected in future generated code, but the prompt expressly says not to invent optimization behavior.

Potential scale costs are primarily metadata/API and validation costs: per-mapping sequential LLM calls and database commits; `pmrep` calls per folder/mapping; GitHub file downloads; in-memory pandas parsing; local Spark test startup; and bounded but potentially large validation datasets. Settings cap validation uploads at 100 MB/100,000 rows, simulated data at 10,000 rows, and database reads at 10,000 rows by default (`src\etl_cc\config.py`).

## 14. Configuration Management

**Confidence: High for declared setting names/defaults; actual values are absent.**

`src\etl_cc\config.py` uses Pydantic Settings, reads root `.env` if present, case-insensitively, and ignores extra variables. `.env` is ignored by `.gitignore` and was not present in the inventory. Secret values must not be inferred from defaults.

| Variable / setting | Description | File | Value source |
|---|---|---|---|
| `APP_NAME` | Display/API name; default `ETL Migration Command Center` | `config.py` | Environment/`.env`, else default |
| `APP_ENV` | Environment label; default `local` | `config.py` | Environment/`.env`, else default |
| `API_PREFIX` | API prefix; default `/api/v1` | `config.py` | Environment/`.env`, else default |
| `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD` | PostgreSQL connection; host/db/user/password required, port default 5432 | `config.py`, `database.py` | Environment/`.env`; credentials not committed |
| `PG_SCHEMA` | Metadata schema; default `demooc28` | `config.py`, `models.py` | Environment/`.env`, else default |
| `ETL_CREDENTIAL_ENCRYPTION_KEY` | Required Fernet key used for persisted credentials and token HMAC signing | `config.py`, `security.py` | Secret manager/environment; required |
| `ETL_CREDENTIAL_KEY_VERSION` | Credential key version, default `v1` | `config.py` | Environment/`.env`, else default |
| `CONNECTION_TEST_TTL_SECONDS` | Signed source test-token TTL; default 300 seconds | `config.py`, `security.py` | Environment/default |
| `INFORMATICA_PMREP_PATH` | `pmrep` command; default `pmrep` | `config.py`, `connectors.py` | Environment/default |
| `INFORMATICA_COMMAND_TIMEOUT_SECONDS` | Subprocess timeout; default 120 | `config.py`, `connectors.py` | Environment/default |
| `INFORMATICA_EXPORT_DIRECTORY` | PowerCenter export scratch location; default `runtime_exports` | `config.py`, `connectors.py` | Environment/default |
| `WORKER_POLL_SECONDS` | Queue polling interval; default 2 seconds | `config.py`, worker modules | Environment/default |
| `SOURCE_SNAPSHOT_TTL_SECONDS` | Temporary source expiry; default 1800 seconds | `config.py`, `source_store.py` | Environment/default |
| `MAX_XML_UPLOAD_BYTES` | Single source XML cap; default 25 MiB | `config.py`, `source_store.py` | Environment/default |
| `XML_PACKAGE_MAX_FILES`, `XML_PACKAGE_MAX_FILE_BYTES`, `XML_PACKAGE_MAX_TOTAL_BYTES`, `XML_PACKAGE_ALLOW_ZIP` | XML package limits; defaults 100 files, 25 MiB/file, 100 MiB total, ZIP allowed | `config.py`, `xml_package.py` | Environment/default |
| `MEMORY_WORKSPACE_ROOT` | GitHub source scratch path; default `/dev/shm/etl_cc` | `config.py`, `worker.py`, `services.py` | Environment/default |
| `ENABLE_INFORMATICA`, `ENABLE_INFORMATICA_POWERCENTER`, `ENABLE_INFORMATICA_GITHUB`, `ENABLE_INFORMATICA_XML_UPLOAD` | Capability flags; default true | `config.py`, API | Environment/default |
| `ENABLE_DATASTAGE`, `ENABLE_AB_INITIO` | Disabled product capability flags; default false | `config.py`, API | Environment/default |
| `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_KEYVAULT_URL` | Service-principal credentials and vault URL; required fields | `config.py`, `key_vault_service.py` | Secret environment/`.env`; actual values absent |
| `AZURE_KEYVAULT_SECRET_NAME` | OpenAI secret name; default `openai-key` | `config.py`, `key_vault_service.py` | Environment/default |
| `OPENAI_MODEL`, `OPENAI_TEMPERATURE`, `OPENAI_TIMEOUT_SECONDS`, `OPENAI_MAX_OUTPUT_TOKENS` | LLM defaults; default model `gpt-4o`, temperature 0.1, timeout 120 sec, optional token cap | `config.py`, `key_vault_service.py` | Environment/default |
| `ARTIFACT_DIRECTORY_NAME` | Local artifact store default `runtime_artifacts` | `config.py`, `artifact_store.py` | Environment/default; conversion artifacts are also persisted in PostgreSQL |
| `RAG_MAX_ITEMS`, `RAG_EMBEDDING_DEPLOYMENT`, `RAG_EMBEDDING_DIMENSIONS`, `RAG_CANDIDATE_COUNT` | RAG limits/model/dimensions; 8, `text-embedding-3-small`, 1536, 30 defaults | `config.py`, RAG/embedding modules | Environment/default |
| `RAG_MIN_VECTOR_SIMILARITY`, `RAG_MIN_SIMILARITY`, `RAG_VECTOR_WEIGHT`, `RAG_METADATA_WEIGHT`, `RAG_EMBEDDING_BATCH_SIZE` | RAG thresholds/weights/batch; .50, .65, .70, .30, 50 defaults | `config.py`, RAG modules | Environment/default |
| `CONVERSION_MAX_ATTEMPTS` | Conversion/critique attempts; default 2 | `config.py`, conversion service | Environment/default |
| `VALIDATION_TEST_TIMEOUT_SECONDS`, `MINIMUM_FUNCTIONAL_PARITY`, `VALIDATION_CONFIDENCE_THRESHOLD` | Validation limits/thresholds; 120 sec, .95, .60 defaults | `config.py`, validation modules | Environment/default |
| `VALIDATION_UPLOAD_MAX_BYTES`, `VALIDATION_UPLOAD_MAX_ROWS` | Upload limit 100 MiB and 100,000 rows | `config.py`, `validation_store.py` | Environment/default |
| `SYNTHETIC_VALIDATION_MAX_SCENARIOS`, `SYNTHETIC_VALIDATION_MAX_ROWS` | Synthetic caps 30 scenarios / 10,000 rows | `config.py`, validation agent | Environment/default |
| `VALIDATION_SIMULATION_DEFAULT_ROWS`, `VALIDATION_SIMULATION_MAX_ROWS` | Simulation defaults 1,000 / max 10,000 | `config.py`, validation agent | Environment/default |
| `VALIDATION_DATABASE_ROW_LIMIT`, `VALIDATION_DATABASE_TIMEOUT_SECONDS` | DB sample cap 10,000 rows / 60 sec | `config.py`, `connectors.py` | Environment/default |
| `DISCOVERY_LOW_MANUAL_HOURS`, `DISCOVERY_MEDIUM_MANUAL_HOURS`, `DISCOVERY_HIGH_MANUAL_HOURS`, `DISCOVERY_VERY_HIGH_MANUAL_HOURS`, `DISCOVERY_HOURLY_LABOR_COST`, `DISCOVERY_AUTOMATION_SAVINGS_PERCENTAGE` | Optional economics baselines; unset means economics are not configured per comments | `config.py`, `economics.py` | Environment; no actual values found |
| `GIT_DEPLOYMENT_REPOSITORY_PATH`, `GIT_DEPLOYMENT_BRANCH_PREFIX`, `GIT_COMMAND_TIMEOUT_SECONDS`, `GITHUB_API_TIMEOUT_SECONDS` | Git defaults `migrations`, `neuflow`, 180 sec, 30 sec | `config.py`, deployment/connectors | Environment/default |
| `DEPLOYMENT_MAX_ATTEMPTS`, `AUTO_DEPLOY_AFTER_VALIDATION` | Defaults 1 and true | `config.py`, deployment/validation | Environment/default |
| `GIT_DEPLOYMENT_REPOSITORY_URL`, `GIT_DEPLOYMENT_BASE_BRANCH`, `GIT_DEPLOYMENT_CREATE_PULL_REQUEST`, `GIT_DEPLOYMENT_ACCESS_TOKEN` | Deployment target; optional URL/token, base `main`, PR false | `config.py`, deployment | Environment or API target request; actual values absent |

No Key Vault URI, PostgreSQL host, API key, or service principal value is checked in. `scripts\generate_encryption_key.py` emits a Fernet key; generated keys must be stored securely and never placed in source control.

## 15. Scheduling and Orchestration

**Confidence: High for code polling; Low for production schedule.**

- No cron, Airflow, Dagster, ADF pipeline, or event-trigger configuration exists.
- Work is persisted as workflow rows in PostgreSQL. Workers poll every `WORKER_POLL_SECONDS` (default 2), claim jobs in a transaction with `FOR UPDATE SKIP LOCKED`, and mark them RUNNING.
- Discovery queue consumer: `src\etl_cc\worker.py` (`run_worker`).
- Parent migration consumer: `src\etl_cc\langgraph_orchestrator.py` (`run_orchestrator`), which claims queued conversion children and calls the graph.
- Graph: conversion → validation → deployment → complete, with failed branches; see `build_graph`, `after_conversion`, `after_validation`, and `after_deployment`.
- The conversion service queues a SIMULATE validation child for a parent Migration after conversion; `src\etl_cc\conversion_service.py`.
- Validation either marks migration ready for deployment or, if config and evidence allow, queues automatic deployment; `src\etl_cc\validation_worker.py`.
- `deploy_dev.sh` names active services `etl-cc-api`, `etl-cc-discovery`, `etl-cc-orchestrator`; it writes only the orchestrator systemd unit. API/discovery unit definitions and scheduler installation are **Not Found**.
- `conversion_worker.py`, `validation_worker.py`, and `deployment_worker.py` contain legacy polling loops, but `deploy_dev.sh` disables services named `etl-cc-conversion`, `etl-cc-validation`, and `etl-cc-deployment`. Exact production selection must be confirmed before operating.

## 16. Error Handling

**Confidence: High for code-level failures; Medium for operational recovery.**

- API routes translate validation errors to HTTP 400, connector errors to 502, integrity conflicts to 409, and unknowns may propagate as server errors (`src\etl_cc\api.py`).
- PowerCenter connector maps missing `pmrep`, timeout, auth failure, and command failures to connector exceptions (`connectors.py`).
- XML upload validates file count, byte sizes, ZIP paths, file suffixes, duplicate names, XML structure, and empty payloads (`xml_package.py`, `informatica_parser.py`).
- Source and validation content have SHA-256 integrity checks (`source_store.py`, `validation_store.py`).
- Conversion has `conversion_max_attempts` default 2 and critique feedback. Workflow-level max attempts are usually set to 1; no general automatic retry policy is evidenced.
- Discovery, conversion, validation, and deployment persist terminal status, failure stage/reason, audit rows/events, and sanitized logs in their worker/service modules.
- Deployment returns a structured failure result and records it. Git push uses a separate temporary worktree; there is no rollback of an already-pushed Git branch documented.
- Recovery generally requires inspecting the failed workflow/event and correcting connection, key, schema, artifact, or mapping input, then submitting a new workflow. A generic “retry failed workflow” API is **Not Found**.

## 17. Logging Strategy

**Confidence: High for application logging; Medium for deployed log handling.**

- `src\etl_cc\logging_config.py` configures root INFO logging to `/home/authuser/dev/chatbots/ETL_CC/logs/etl_cc.log` using `WatchedFileHandler`.
- `src\etl_cc\main.py` logs startup and each HTTP request start/completion/failure; service/worker/agent modules emit structured event names, metrics and stage events.
- Credentials are redacted by key-name patterns. Large payloads and row contents are dropped or summarized in operational logs; full agent request/response evidence is instead stored in PostgreSQL.
- The logger classifies failed/error events at ERROR and warning events at WARNING; exact levels are configured in `logging_config.py`.
- Deployment script `deploy_dev.sh logs` tails `journalctl` for API, discovery, orchestrator. The script's service logs may differ from the absolute Python file path.
- No dashboard, log aggregation, retention/rotation policy, alert integration, or production observability platform is defined.

## 18. Monitoring and Alerting

**Confidence: Medium; monitoring primitives are present, external alerting is absent.**

Available evidence: workflow state, events, agent audit status, durations/token counts, validation reports, RAG retrieval events, and structured logs. API offers health and workflow status/event endpoints plus Server-Sent Events for workflow updates (`src\etl_cc\api.py`).

**Not Found in Repository:** alert rules, paging/email/Teams/PagerDuty integration, SLA thresholds, uptime dashboards, pipeline success-rate calculation job, latency SLO, data freshness monitor, or automatic data quality alert delivery. Production support must use the API/database/logs or an external platform not represented here.

## 19. Data Quality Framework

**Confidence: High for implemented validation categories; Medium for fitness against production data.**

Quality checks include:

- XML validity, root and port type checks; package size/member safety (`informatica_parser.py`, `xml_package.py`).
- Canonical source/target metadata, parser field extraction, explicit field lineage and unresolved dependency reporting.
- Conversion contract checks: required artifact types, Python AST syntax, pure `transform` contract, output schema/projection, no external I/O or connection credentials, unit-test/config contract (`conversion_agent.py`).
- Critique deterministic and semantic rules coverage, lineage coverage, unsupported constructs, placeholders, and unsafe/incomplete generated output (`critique_agent.py`).
- Validation static artifact checks, synthetic null/boundary/negative cases, unit tests, reference/oracle execution, functional parity threshold, schema/business-rule/lineage coverage and target-field coverage (`validation_agent.py`).
- Dataset file format, byte/row cap and integrity; table row cap; required comparison keys and no duplicate keys (`validation_store.py`, `matchflow_comparator.py`).

Default parity threshold is 0.95; deployment eligibility additionally requires PASSED, deployable, STRONG oracle, full reference coverage, no human review requirement, and no unsupported construct in the user-configured deployment route (`services.py`, `deployment_agent.py`). Automated validation default can be SIMULATE; therefore “passed” does not always mean comparison to a live legacy production output. The validation mode and oracle strength must be read per workflow.

## 20. Database Documentation

**Confidence: High for declared ORM schema; actual live tables/indexes are unverified.**

Database is PostgreSQL, schema default `demooc28`, dynamically set through `ETL_SCHEMA` in `src\etl_cc\models.py`. `scripts\create_tables.py` invokes SQLAlchemy `Base.metadata.create_all`; `scripts\check_tables.py` checks only seven Release 1 tables, not every model/migration table.

| Table | Role / relationships |
|---|---|
| `repository_etl` | Source and deployment repositories; one-to-many with mapping objects and workflows; encrypted credential columns |
| `etl_object_etl` | Canonical mapping inventory and discovery/dependency/migration state; FK to repository |
| `workflow_run_etl` | Discovery/conversion/validation/deployment/migration job and parent-child batch state; FKs to repository and optional mapping |
| `agent_response_etl` | Agent request/response, attempts, token/model usage and errors; FK to workflow and optional mapping |
| `workflow_event_etl` | Status/progress/audit timeline; FK to workflow and optional agent response |
| `generated_artifact_etl` | Artifact metadata, version/hash/type/deployability; FK to workflow and optional mapping |
| `artifact_content_etl` | Text/JSON/binary artifact content; linked to generated artifact |
| `knowledge_base_etl` | Approved knowledge, metadata and pgvector embedding; optional repository/mapping/workflow links |
| `rag_retrieval_event_etl` | Candidate/accepted counts, similarity, timing and query hash; optional workflow/repository/mapping links |
| `validation_test_case_etl` | Planned/executed test case evidence; workflow FK cascades, optional mapping/response |
| `source_snapshot_etl` | Short-lived source reference, manifest, source content/hash/expiry |
| `source_snapshot_file_etl` | Package-file bytes/hash/object summary; child of source snapshot |
| `validation_dataset_etl` | Uploaded CSV/JSON/Parquet data bytes/hash/row count/columns |
| `validation_database_connection_etl` | Saved reference database connection configuration and encrypted credential |

ORM classes establish several FKs, but the repository has no standalone baseline ERD or complete SQL DDL. Indexes/constraints visible in migrations: connection type index and check constraint; generated-artifact mapping index/FK; validation test workflow-status and mapping-created indexes/unique scope; agent synthetic lookup index. Additional ORM indexes may exist in `models.py`.

```text
repository_etl 1----* etl_object_etl
      |                   |  \
      |                   |   \----* generated_artifact_etl 1----1 artifact_content_etl
      |                   |                     |
      |                   +----* agent_response_etl <----* workflow_event_etl
      |                                  ^                 /
      +----* workflow_run_etl -----------+----------------/
                    | \
                    |  +----* validation_test_case_etl
                    +-------* generated_artifact_etl
knowledge_base_etl / rag_retrieval_event_etl -> optional repository/mapping/workflow
source_snapshot_etl 1----* source_snapshot_file_etl
validation_dataset_etl and validation_database_connection_etl are validation inputs
```

## 21. Performance Optimization

**Confidence: Medium; repository-based risks, not production profiling.**

Existing controls include SQL indexes declared in migrations/models, row locks with `SKIP LOCKED`, database `pool_pre_ping`, bounded validation uploads/simulation/database reads, RAG candidate and result limits, source-package size caps, async I/O for API/database, temporary workspaces, and per-call model timeouts.

Likely cost centers to measure:

1. Discovery processes mappings sequentially and writes/commits per mapping and agent; large repositories can incur many round trips and LLM calls (`worker.py`).
2. PowerCenter enumerates folders then mappings and exports selected mappings individually (`connectors.py`).
3. GitHub source retrieves XML files sequentially (`connectors.py`).
4. Per-call OpenAI embeddings plus RAG nearest-neighbor search and Key Vault secret lookup (`embedding_service.py`, `rag_retrieval_agent.py`).
5. Local Spark startup/tests and in-memory DataFrame/CSV/Parquet handling (`unit_test_execution_agent.py`, `validation_store.py`).
6. Several dashboard/service queries may load many event/audit/artifact rows; inspect query plans and indexes before large deployments (`services.py`, `models.py`).
7. Logs/audits may contain large canonical metadata in the database; retention/partitioning policy is not present.

No real workload benchmark, `EXPLAIN ANALYZE`, skew sample, throughput target, or production profile exists. Recommendations: profile representative repository sizes, add metrics on per-stage duration and DB rows, define retention/partitioning, batch where semantics allow, and avoid speculative Spark tuning until generated jobs run on target Databricks.

## 22. Security Review (Repository Controls, Not a Penetration Test)

**Confidence: Medium for source-level controls; Low for deployed security posture.**

- Credentials are encrypted using Fernet before persistence; version and algorithm fields accompany repository credentials. `src\etl_cc\security.py`.
- Connection test token is short-lived, signed with HMAC, and bound to connection configuration hash; default lifetime is 300 seconds. The same configured encryption-key setting is used for both Fernet and HMAC signing (`security.py`).
- Azure service-principal credentials are loaded from settings; Key Vault is queried for an OpenAI key on each model call (`key_vault_service.py`).
- Database credential encryption uses `validation_credential_cipher()` (`security.py`); source credential cipher used in services/worker.
- XML parser uses `defusedxml`; ZIP checks reject path traversal and non-XML members; size/member limits exist (`informatica_parser.py`, `xml_package.py`).
- Validation uploads and source content are persisted in PostgreSQL. Source content is cleared after successful canonicalization; expiry is checked when accessed. No cleanup/retention worker for expired snapshots or validation datasets is present in the inventory (`source_store.py`, `validation_store.py`).
- Logging includes sensitive-key redaction and payload compaction (`logging_config.py`). Full prompt/evidence payloads are stored in `agent_response_etl`; database access therefore needs protection.
- Git deployment validates eligibility, branch/path, artifact hashes and uses a temporary directory (`deployment_agent.py`).
- CORS allows all origins but disables credentials (`main.py`). API authentication, authorization/RBAC, tenant isolation, request rate limits, audit identity, and TLS termination configuration are **Not Found in Repository**.
- PII classification, masking policy, data residency, encryption-at-rest configuration, key rotation procedure, database backup policy, and GitHub token scopes are **Not Found**.

The repo contains no secret values in the tracked file inventory; `.env` is ignored. This document is not a vulnerability assessment and does not establish production controls.

## 23. Deployment Process

**Confidence: Medium for the checked-in development path; Low for production release.**

### Visible paths

1. Local API: `pip install -r requirements.txt`, then `uvicorn etl_cc.main:app --app-dir src --reload` as in `README.md`.
2. Container: `Dockerfile` installs requirements, copies the repository, sets `PYTHONPATH=/app/src`, starts Uvicorn on port 8000.
3. Linux development service script: `deploy_dev.sh {start|stop|restart|status|logs|health}`; writes `/etc/systemd/system/etl-cc-orchestrator.service`, disables legacy conversion/validation/deployment service names, starts API/discovery/orchestrator units that must already exist, and checks `http://127.0.0.1:4046/api/v1/health`.

### Environment progression, release, rollback

Development → Testing → UAT → Production gates, approvals, build artifact promotion, database migration order, secret injection, blue/green/canary policy, and rollback procedure are **Not Found in Repository**. No CI/CD workflow or versioned deployment manifest is checked in. SQL scripts are not shown as an automated migration chain. `deploy_dev.sh` is development-specific and hardcodes `/home/authuser/dev/chatbots/ETL_CC`, Python 3.11, root user, service names, log route, and API port expectation; Docker uses Python 3.12 and port 8000. Reconcile before production use.

Rollback options evidenced are limited to stopping/restarting services and handling Git through branches/PRs. Database downgrade scripts and workflow replay semantics are not defined.

## 24. Production Support Guide

**Confidence: Medium; operational checklist inferred from implementation, not a live runbook.**

### Daily checks

1. Confirm API `GET /api/v1/health` and systemd service state.
2. Verify PostgreSQL connectivity and that configured schema/tables exist (`scripts\test_database.py`, `scripts\check_tables.py`).
3. Inspect workflow status/events for stuck QUEUED/RUNNING jobs; check worker/orchestrator service and process logs.
4. Inspect `logs\etl_cc.log` at configured Linux path and journal logs using `deploy_dev.sh logs`.
5. Confirm Key Vault service-principal access and OpenAI secret retrieval before high-volume LLM work.
6. For deployment, verify target repo/branch, token permissions, Git availability, and validation evidence.
7. Watch disk/RAM and temp directories for validation/Spark/Git jobs; no cleanup schedule is provided.

### Troubleshooting matrix

| Issue | Likely root cause | Resolution / evidence |
|---|---|---|
| API fails on startup | Missing required PG, Fernet, or Azure settings; DB connection initialization/config | Check `.env`/service environment names in `config.py`; verify DB, schema, secret references; do not log secret values |
| Health endpoint unreachable | API service down, incorrect port/path, script-vs-Docker port mismatch | Check `deploy_dev.sh` status/journal; container listens 8000 while health script expects 4046 |
| Discovery remains QUEUED | Discovery worker not running or cannot claim DB job | Check `etl-cc-discovery` unit/process, worker poll logs, DB lock/connectivity |
| PowerCenter test fails | `pmrep` absent/wrong path, network/repository/credential/security-domain issue, command timeout | Check `INFORMATICA_PMREP_PATH`, timeout and connector error in `connectors.py`; validate repository from host |
| GitHub source listing fails | Bad URL/branch/folder, missing token/access, API rate/network failure | Check source request config and GitHub API errors; `GitHubSource` in `connectors.py` |
| XML upload rejected | Invalid root/XML/port, ZIP path or member rule violation, size/count cap | Correct package; inspect `xml_package.py`/`informatica_parser.py` error |
| LLM stage unavailable | Key Vault identity/vault permission, missing OpenAI secret, model timeout/schema response | Check Key Vault logs and settings; `key_vault_service.py`; do not persist secret in workflow |
| RAG returns NO_MATCH | No approved/current embedded knowledge or scores below thresholds | Check `knowledge_base_etl` approval/embedding status and `rag_retrieval_event_etl`; backfill only approved records |
| Mapping blocked from Conversion | Discovery not ANALYZED, dependency cycle, or no migration wave | Inspect discovery status, dependency plan and cycle evidence; remediate mapping/dependency, rerun analysis |
| Conversion fails/repeats | LLM contract violation, unsupported construct, critique revision failure | Inspect agent response + `failure_stage`; review conversion artifacts/critique and mapping evidence; attempts default 2 |
| Validation fails | Missing required artifact, static/test/parity failure, missing input table/data, weak oracle | Read validation report/test cases, test inputs, confidence and oracle strength; correct mapping/evidence or supply suitable reference data |
| Deployment is blocked | Validation not deployable/STRONG/full coverage; missing target or push rights | Confirm validation evidence, saved target capability and repository/branch configuration |
| Deployment fails after queue | Git/network/token or file/hash/branch/path issue; worker/orchestrator mismatch | Inspect `GIT_DEPLOYMENT` stage/evidence and logs; verify which deployment runtime is active |

Do not manually change workflow rows to COMPLETED. Use a new workflow after correcting the cause unless an approved operator procedure says otherwise.

## 25. Dependency Analysis

**Confidence: High for declared dependencies and call roles; Medium for production criticality.**

| Dependency | Purpose | Impact if unavailable |
|---|---|---|
| PostgreSQL, asyncpg, SQLAlchemy | Main state, queue, audit, artifacts, source/upload persistence | API/workers cannot provide normal product operations |
| `pmrep` and PowerCenter network | Live source ingestion | PowerCenter analysis/discovery blocked; XML/GitHub sources remain options |
| GitHub REST API + `git` | GitHub XML source and optional target deployment | GitHub source unavailable; deployment unavailable |
| Azure Identity + Key Vault | Fetch OpenAI key | AI discovery/conversion/critique/embedding fail |
| OpenAI API / GPT-4o | Semantic discovery, conversion, critique, some validation planning | AI-assisted workflows fail or cannot meet current agent requirements |
| OpenAI embedding endpoint | RAG vectorization and query embedding | Retrieval/backfill fails; conversion may fail if RAG is mandatory in path |
| PostgreSQL pgvector extension/type | Similarity storage/query | RAG retrieval schema/query fails |
| NetworkX | Dependency graph and wave/cycle logic | Discovery dependency planning fails |
| PySpark + pytest | Generated code runtime and validation | Unit/target execution cannot validate generated artifact |
| pandas + PyArrow | Parse validation upload datasets | CSV/JSON/Parquet upload validation cannot load |
| Linux/systemd/curl/journalctl | Development service operation/health/log path | `deploy_dev.sh` operations fail; not required by Windows local API |
| `defusedxml`, `cryptography` | Secure XML parse, credential encryption | XML ingestion or credential handling unavailable |

Declared ranges are in `requirements.txt`; no lock file is present. Docker installs at build time from those ranges, so dependency resolution may vary over time.

## 26. Critical Files — TOP 20

**Confidence: High.**

| # | File | Why important / what to read |
|---:|---|---|
| 1 | `README.md` | Product scope and local startup; it is brief and lags behind current implementation. |
| 2 | `requirements.txt` | All declared package ranges and target runtime clues. |
| 3 | `Dockerfile` | Container build and API entrypoint; does not start workers. |
| 4 | `deploy_dev.sh` | Development service lifecycle, hardcoded paths, health route and logs. |
| 5 | `src\etl_cc\main.py` | ASGI app, CORS, middleware, router registration and health root. |
| 6 | `src\etl_cc\api.py` | All external REST endpoints, status codes, streaming routes and request wiring. |
| 7 | `src\etl_cc\config.py` | Environment variable names/defaults, external integrations, limits and database URL. |
| 8 | `src\etl_cc\models.py` | Canonical metadata/request/response models and database schema. |
| 9 | `src\etl_cc\database.py` | Async engine/session lifecycle. |
| 10 | `src\etl_cc\services.py` | Source analysis, workflow creation, status/report/service operations. |
| 11 | `src\etl_cc\connectors.py` | PowerCenter/GitHub adapters and PostgreSQL reference table reads. |
| 12 | `src\etl_cc\informatica_parser.py` | XML-to-canonical mapping extraction. |
| 13 | `src\etl_cc\worker.py` | Discovery queue claim, agent execution, persistence, dependency plan. |
| 14 | `src\etl_cc\dependency_planner.py` | Dataset dependency edges, cycle handling and migration waves. |
| 15 | `src\etl_cc\langgraph_orchestrator.py` | Current parent Migration sequence and stage routing. |
| 16 | `src\etl_cc\conversion_service.py` | RAG → conversion → critique → artifact save → validation queue. |
| 17 | `src\etl_cc\validation_service.py` | Validation workflow creation and status/report APIs. |
| 18 | `src\etl_cc\validation_worker.py` | Actual validation inputs, agent invocation, evidence and parent-state update. |
| 19 | `src\etl_cc\agents\validation_agent.py` | Core static/synthetic/reference/PySpark validation logic (largest agent module). |
| 20 | `src\etl_cc\agents\deployment_agent.py` | Deployment eligibility, integrity checks, Git commit/push and optional PR. |

Other key modules: `security.py`, `key_vault_service.py`, `logging_config.py`, `embedding_service.py`, `agents\conversion_agent.py`, `agents\critique_agent.py`, `agents\discovery_agent.py`, `agents\lineage_agent.py`, `agents\rag_retrieval_agent.py`, `deployment_service.py`, and `migrations\`.

## 27. End-to-End Execution Flow

**Confidence: High for source flow; Medium for deployed worker wiring.**

```text
User / UI
 -> FastAPI app: etl_cc.main:app
 -> POST /sources/{powercenter|github}/test OR /sources/xml/analyze
 -> services.analyze_* -> connector/parser -> source_snapshot_etl
 -> POST /discoveries -> services.start_discovery
 -> worker.run_worker -> _claim -> _load_mappings -> InformaticaXMLParser
 -> _upsert -> DiscoveryAgent.run -> LineageAgent.run
 -> DependencyPlanner.run -> mapping dependency waves -> discovery complete
 -> POST /migrations -> services.start_migration -> parent MIG + child CONVERSION
 -> langgraph_orchestrator.run_orchestrator -> claim_migration -> build_graph
 -> conversion_service.execute_conversion_workflow
 -> RAGRetrievalAgent -> ConversionAgent.run -> CritiqueAgent.run
 -> _save_artifacts -> generated_artifact_etl/artifact_content_etl
 -> _queue_automatic_validation -> validation child
 -> validation_service.execute_validation_workflow
 -> ValidationAgent.run -> static/unit/synthetic/reference checks
 -> validation status + report + test_cases + artifact deployable state
 -> deployment_service.execute_deployment_workflow (configured automatic path)
 -> GitDeploymentAgent.run -> validate/hash -> clone/copy/manifest/commit/push/PR
 -> parent Migration completed/failed
```

API also exposes standalone paths (`POST /conversions`, `/validations`, `/deployments`) and reports for each stage in `src\etl_cc\api.py`. `src\etl_cc\conversion_worker.py`, `validation_worker.py`, and `deployment_worker.py` implement legacy polling code alongside the service/orchestrator pattern. Confirm which processes are actually active before changing queue state or starting a second consumer.

## 28. New Joiner KT Section

**Confidence: High for recommended reading order; Medium for access/process specifics.**

### Day 1 — Learn system shape

- Read this document, `README.md`, `requirements.txt`, `Dockerfile`, and `config.py`.
- Trace `main.py` → `api.py` → `services.py`.
- Understand `models.py` and PostgreSQL setup via `database.py`, `scripts\test_database.py`, `scripts\create_tables.py`.
- Confirm development environment, `.env` delivery process, database access, and service entrypoints with the team; those operational details are not committed.

### Day 2 — Learn source and discovery

- Read `connectors.py`, `informatica_parser.py`, `xml_package.py`, and `source_store.py`.
- Follow `/sources/*` then `/discoveries` in `api.py` and `services.py`.
- Read `worker.py`, `discovery_agent.py`, `lineage_agent.py`, `dependency_planner.py`.
- Inspect one representative approved Informatica XML and its resulting canonical mapping in an authorized non-production environment.

### Day 3 — Learn migration, validation, support

- Read `langgraph_orchestrator.py`, `conversion_service.py`, `conversion_agent.py`, `critique_agent.py`.
- Follow `validation_service.py`, `validation_worker.py`, `validation_agent.py`, `unit_test_execution_agent.py`, `matchflow_comparator.py`.
- Read `deployment_service.py`, `deployment_worker.py`, `deployment_agent.py`, and `deploy_dev.sh`.
- Walk through a known failed workflow by status/events/agent responses and practice using the support matrix.

### Week 1 — Ownership roadmap

1. Obtain architecture and production runbooks, actual env inventory, service manifests, database backup/retention approach, and escalation contacts.
2. Verify active worker model and remove ambiguity between legacy worker loops and LangGraph service orchestration.
3. Run local/unit checks and a non-production source analysis with safe sample XML.
4. Trace each stage's persisted status, audit row, emitted event and output artifact.
5. Verify Key Vault/OpenAI, PostgreSQL, `pmrep`, GitHub, and PySpark permissions on the actual host.
6. Review API authentication/authorization and network boundaries with security owners.
7. Confirm Git deployment branch policy, PR approval, rollback and actual Databricks execution handoff.
8. Propose tests for API workflows, parser edge cases, queue idempotency, and failed-stage recovery before changing production behavior.

## 29. Risks and Technical Debt

**Confidence: Medium. These are repository observations and must be prioritized with the owners.**

1. **API auth/RBAC not found:** endpoint protection is not visible in `main.py`/`api.py`; confirm upstream gateway controls before exposure.
2. **Incomplete deployment topology:** script expects existing API/discovery service units but defines only orchestrator; worker lifecycle cannot be recreated from this checkout (`deploy_dev.sh`).
3. **Multiple orchestration implementations:** legacy worker loops coexist with newer LangGraph stage services (`conversion_worker.py`, `validation_worker.py`, `deployment_worker.py` vs corresponding `*_service.py`); risk of duplicate consumers or behavioral drift.
4. **Deployment path mismatch:** saved target API workflow configuration and environment-driven automatic deployment coexist; verify which target settings each production route consumes (`services.py`, `deployment_service.py`, `deployment_worker.py`).
5. **No CI/CD pipeline or lockfile:** reproducibility and release gates are not recorded (`requirements.txt`, root inventory).
6. **README and prompt docs stale:** README describes only initial connection/mapping focus; seven prompt markdown files contain TODO placeholders while real prompts are embedded in Python (`prompts\*.md`).
7. **Insufficient test breadth:** only two tiny test files found; no API, DB integration, parser fixture, workflow, validation, or deployment tests.
8. **Retention unclear:** source expiry is enforced on read but no cleanup job is evident; validation uploads and full agent payloads can remain in PostgreSQL (`source_store.py`, `validation_store.py`, `models.py`).
9. **Environment/runtime inconsistencies:** Docker Python 3.12/port 8000; development systemd Python 3.11, hardcoded paths, health port 4046 (`Dockerfile`, `deploy_dev.sh`).
10. **RAG schema dependency:** pgvector extension/schema/index deployment and embedding dimension compatibility must be verified; no DB provisioning guide.
11. **No Databricks execution infrastructure:** workspace, job, runtime, Unity Catalog and post-deployment verification are absent.
12. **Business requirements and ownership absent:** no domain definitions, data owners, consumers, SLA, support rotations or escalation paths.

Prioritize validating production access boundaries, process topology, and deployment wiring before expanding the feature set.

## 30. Interview-Ready Project Explanation

**Confidence: High for describing source implementation; Medium for real business impact.**

### 2-Minute Project Overview

“ETL Migration Command Center is a Python/FastAPI application for accelerating Informatica PowerCenter to Databricks PySpark migration. It can inspect a live PowerCenter repository, Informatica XML from GitHub, or uploaded XML packages. It parses the mapping metadata into a canonical model, then produces business-rule, lineage, risk and dependency-wave analysis. For conversion, it retrieves approved examples from a PostgreSQL/pgvector knowledge base, uses GPT-4o to generate a pure PySpark transform plus tests and configuration, and runs deterministic critique and validation. Workflow state, generated artifacts and audit evidence are stored in PostgreSQL. A GitHub deployment path can push validated artifacts. This repository does not include recurring production Spark pipelines, Databricks job definitions, or the final warehouse consumer setup.”

### 5-Minute Technical Explanation

“The API lives in `src\etl_cc\main.py` and `api.py`, with settings in `config.py`, SQLAlchemy models in `models.py`, and async database wiring in `database.py`. Source adapters in `connectors.py` handle `pmrep` and GitHub; XML is parsed by `informatica_parser.py` and safe package handling is in `xml_package.py`. The discovery worker claims PostgreSQL queue rows with `SKIP LOCKED`, persists canonical mappings, runs Discovery and Lineage agents, and invokes the NetworkX `DependencyPlanner`. Conversion stores three generated artifact types. The conversion service uses RAG embeddings and a critique loop, while validation can execute static checks, generated pytest cases, synthetic cases, and reference data comparisons. LangGraph sequences parent migration stages; workflow events and agent responses provide auditability. Deployment verifies evidence and hashes before writing a branch/commit and optionally a pull request.”

### 10-Minute Deep Dive

Discuss the canonical schema and source-object keys; how folder/mapping XML becomes datasets, transformations, ports and connections; how discovery merges deterministic and LLM semantic evidence; how field-level lineage follows connectors; how dataset producer-consumer matches form a directed graph and topological waves; why cycles/human review block automatic conversion; how approved knowledge is filtered by embedding/model/hash and weighted metadata similarity; how the generated transform contract prevents source/target I/O and secrets; how critique feedback controls bounded regeneration; why synthetic validation is not equivalent to a strong legacy oracle; how workflow and agent evidence are persisted; and how Git deployment requires STRONG oracle evidence/full reference coverage. Conclude with current ownership gaps: no included production service manifests, CI/CD, Databricks workspace configuration, full API tests, business ownership, or confirmed production rollback instructions.

### Project Understanding Score: 72/100

| Dimension | Score | Reason |
|---|---:|---|
| Application purpose and core flow | 18/20 | README plus service/agent structure provide a coherent purpose |
| Code-level architecture | 18/20 | API, DB, workers, graph, integrations and artifact handling are visible |
| Data model and transformations | 14/20 | ORM and mapping model are detailed; business mapping examples absent |
| Operations/deployment | 9/15 | Dev script exists but omits service unit definitions and release lifecycle |
| Security/monitoring | 7/15 | Some encryption, token, logging and validation controls; auth/alerts/retention unverified |
| Tests and runtime proof | 6/10 | Two small tests; no external service or end-to-end validation available |
| **Total** | **72/100** | Static repository understanding, not production readiness |

---

# Production Support Cheat Sheet

**Confidence: Medium; verify names, paths and access in the deployed environment.**

| Check | What to inspect |
|---|---|
| API | `GET /api/v1/health`; local README command is Uvicorn; Docker listens on 8000; `deploy_dev.sh` health uses 4046 |
| Service state | `deploy_dev.sh status`; active names: `etl-cc-api`, `etl-cc-discovery`, `etl-cc-orchestrator` |
| Logs | `/home/authuser/dev/chatbots/ETL_CC/logs/etl_cc.log`; `deploy_dev.sh logs` tails systemd journal |
| Database | Run `scripts\test_database.py`; then `scripts\check_tables.py`; creation is `scripts\create_tables.py` |
| Queue | Look at `workflow_run_etl.job_type`, `job_status`, `current_stage`, `failure_stage`, `failure_reason`; review `workflow_event_etl` |
| Agent evidence | `agent_response_etl` by workflow/mapping/agent/stage; inspect model, status, attempts, tokens, error |
| Artifacts | `generated_artifact_etl` and `artifact_content_etl`; verify hash, critique status, validation status, deployable flag |
| Key Vault / LLM | Confirm service principal can read `AZURE_KEYVAULT_SECRET_NAME` (default `openai-key`); never print the secret |
| PowerCenter | Check `pmrep` installed and executable, repository network reachability, credentials, configured timeout |
| GitHub | Check target URL/branch, token permission, API/git network access, branch/path and PR settings |
| Local validation | Check installed PySpark/pytest, temporary disk/RAM, selected input mode, row limits, and oracle strength |
| Failed job | Fix root cause and start a new workflow; no generic retry endpoint is documented |

---

# Appendix A. Complete ETL Architecture Summary

The product is a control-plane and migration-assistance application. It uses FastAPI for user/API interaction, PostgreSQL for all durable metadata/workflow/audit/artifact state, separate Python workers for source discovery and migration orchestration, Informatica XML/`pmrep`/GitHub connectors for metadata ingestion, OpenAI and Azure Key Vault for AI functions, pgvector for approved knowledge retrieval, PySpark/pytest/pandas for conversion validation, and GitHub/Git for optional artifact deployment. Databricks execution and data warehouse consumption are downstream handoffs not implemented in this repository.

# Appendix B. Top 10 ETL Pipelines / Workflow Paths

These are paths/subflows, not ten independent scheduled data pipelines:

1. PowerCenter repository analysis.
2. GitHub Informatica XML analysis.
3. XML/ZIP package analysis.
4. Discovery plus business semantic analysis.
5. Mapping field lineage derivation.
6. Dependency/cycle/migration-wave planning.
7. RAG-assisted conversion and deterministic/LLM critique.
8. SIMULATE-mode static/unit/synthetic validation.
9. Uploaded-dataset or PostgreSQL-reference validation/data comparison.
10. Validation-gated Git deployment and optional pull request.

Primary implementation references are `src\etl_cc\api.py`, `services.py`, `worker.py`, `dependency_planner.py`, `conversion_service.py`, `validation_worker.py`, and `deployment_service.py`.

# Appendix C. Top 10 Critical Tables

1. `repository_etl` — source/deployment repository metadata.
2. `etl_object_etl` — canonical mapping inventory and state.
3. `workflow_run_etl` — queue, parent-child workflow and status.
4. `agent_response_etl` — detailed agent execution evidence.
5. `workflow_event_etl` — progress and event timeline.
6. `generated_artifact_etl` — generated artifact metadata and deployability.
7. `artifact_content_etl` — artifact bytes/text/JSON.
8. `knowledge_base_etl` — approved RAG corpus and vectors.
9. `validation_test_case_etl` — validation case evidence.
10. `rag_retrieval_event_etl` — retrieval quality and duration telemetry.

Also important: `source_snapshot_etl`, `source_snapshot_file_etl`, `validation_dataset_etl`, and `validation_database_connection_etl`. Schema definitions are in `src\etl_cc\models.py`.

# Appendix D. Top 10 Important Configuration Files

Only four actual configuration/deployment/migration files are present among the top ten; the remaining six slots are intentionally marked absent rather than fabricated.

1. `src\etl_cc\config.py` — application settings and defaults.
2. `requirements.txt` — dependency ranges.
3. `Dockerfile` — API container configuration.
4. `deploy_dev.sh` — Linux systemd development configuration.
5. `.gitignore` — ignored `.env` and runtime/cache paths.
6. `.env` — **Not Found in Repository**; expected to supply environment-specific settings.
7. CI workflow YAML — **Not Found in Repository**.
8. Kubernetes/Helm configuration — **Not Found in Repository**.
9. Databricks job/cluster configuration — **Not Found in Repository**.
10. Production service/environment manifests — **Not Found in Repository**.

SQL migration files are enumerated in Section 11; they are schema changes, not environment configuration.

# Appendix E. Top 10 Risks

1. API authentication/RBAC not found in code; external gateway controls unverified.
2. Worker/systemd service definitions are incomplete.
3. Legacy polling workers coexist with LangGraph/service implementations.
4. Deployment API target model and environment-driven deployment path may not align.
5. No CI/CD, dependency lock, or release/rollback procedure.
6. README and prompt markdown are stale/placeholders.
7. Only two very small tests; no integration/end-to-end suite.
8. Expired source and uploaded validation-data cleanup is not implemented visibly.
9. Runtime versions, filesystem paths, and health ports conflict across Docker/dev deployment.
10. No Databricks runtime/workspace execution path or downstream acceptance test.

# Appendix F. Top 10 Things Every New Engineer Must Know

1. ETL CC migrates ETL metadata and generates code; it is not the recurring ETL workload.
2. PostgreSQL is the workflow/metadata/artifact store, not the target analytical system.
3. Supported ingestion is Informatica PowerCenter, Informatica XML in GitHub, or XML/ZIP upload.
4. Discovery runs in a separate worker; starting the API alone does not process a Discovery queue.
5. Parent migration sequencing is owned by `langgraph_orchestrator.py`; verify active consumers before starting legacy workers.
6. Dependency cycles and unanalyzed mappings block Conversion.
7. Conversion output is PySpark transform + pytest + configuration; connection/I/O behavior is intentionally external.
8. SIMULATE validation is not automatically equivalent to strong comparison against legacy production output.
9. Git deployment is validation-gated; check oracle strength, full coverage, artifact integrity, and repository permissions.
10. Production-specific secrets, service units, schedules, access control, Databricks configuration, retention, alerts, and rollback instructions must be obtained from the service owner; they are not in this repository.

---

## Repository File-by-File Coverage Register

This register accounts for all files returned by the repository inventory. Individual behavior is described in the sections above; placeholder and small-test files are called out explicitly.

| File(s) | KT coverage |
|---|---|
| `.gitignore`, `README.md`, `requirements.txt`, `Dockerfile`, `deploy_dev.sh` | Sections 4, 5, 14, 15, 17, 23 |
| `migrations\001_create_etl_tables.sql`, `migrations\conversion_migration.sql`, `migrations\validation_test_cases.sql`, `migrations\validation_v2.sql` | Sections 11, 20 |
| `prompts\conversion.md`, `prompts\critique.md`, `prompts\deployment.md`, `prompts\discovery.md`, `prompts\lineage.md`, `prompts\rag_retrieval.md`, `prompts\validation.md` | Section 29; all contain version heading and TODO placeholder, with implementation prompts embedded in agent code |
| `scripts\check_tables.py`, `scripts\create_tables.py`, `scripts\generate_encryption_key.py`, `scripts\test_database.py` | Sections 14, 20, 24 |
| `src\etl_cc\__init__.py`, `src\etl_cc\agents\__init__.py` | Package markers; no additional code |
| `src\etl_cc\api.py`, `main.py`, `config.py`, `database.py`, `models.py`, `services.py` | Sections 3, 4, 6–10, 14, 20, 27 |
| `src\etl_cc\connectors.py`, `informatica_parser.py`, `xml_package.py`, `source_store.py`, `validation_store.py`, `security.py` | Sections 7, 9, 14, 16, 19, 22, 24 |
| `src\etl_cc\worker.py`, `dependency_planner.py`, `engines.py`, `orchestrator.py`, `langgraph_orchestrator.py` | Sections 6, 9, 15, 27, 29 |
| `src\etl_cc\conversion_service.py`, `conversion_worker.py`, `validation_service.py`, `validation_worker.py`, `deployment_service.py`, `deployment_worker.py` | Sections 6, 9, 15, 16, 24, 27, 29 |
| `src\etl_cc\agents\conversion_agent.py`, `critique_agent.py`, `discovery_agent.py`, `lineage_agent.py`, `rag_retrieval_agent.py`, `deployment_agent.py`, `validation_agent.py` | Sections 9, 10, 12, 13, 19, 22, 27 |
| `src\etl_cc\agents\matchflow_comparator.py`, `test_plan_agent.py`, `unit_test_execution_agent.py`, `unit_test_renderer.py`, `unit_test_specification.py`, `reproducible_validation.py` | Sections 10, 19, 24 |
| `src\etl_cc\artifact_store.py`, `src\etl_cc\embedding_service.py`, `src\etl_cc\key_vault_service.py`, `src\etl_cc\logging_config.py`, `src\etl_cc\economics.py`, `src\etl_cc\validation_test_case_service.py`, `src\etl_cc\validation_store.py`, `src\etl_cc\rag_backfill.py`, `src\etl_cc\source_store.py` | Sections 6, 9, 14, 17–19, 22, 25 |
| `src\etl_cc\agents\reproducible_validation.py`, `src\etl_cc\agents\test_plan_agent.py`, `src\etl_cc\agents\unit_test_renderer.py`, `src\etl_cc\agents\unit_test_specification.py` | Sections 10, 19, 24 |
| `tests\test_dependency.py`, `tests\test_security.py` | Sections 4, 23, 29; current tests cover one wave example and Fernet key length only |

Files such as `src\etl_cc\orchestrator.py` are only a stub that returns `None`; the implemented graph is `src\etl_cc\langgraph_orchestrator.py`. `src\etl_cc\engines.py` provides a small DAG wave helper distinct from the richer `DependencyPlanner`.

---

**End of KT document.** Validate all environment-specific assumptions with the application owner before applying this document as a production runbook.
