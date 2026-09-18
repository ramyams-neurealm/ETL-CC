# ETL CC

Simple NeuFlow ETL Migration Command Center project structure.

Current implementation focus:
1. Test direct Informatica repository connection
2. Retrieve mapping list
3. Select entire repository or selected mappings
4. Encrypt and save repository credentials
5. Retrieve and save full mapping metadata
6. Analyze portable Ab Initio graph JSON exports

For the complete architecture, API, worker, database, validation, deployment,
and knowledge-transfer guide, see
`docs/PROJECT_KNOWLEDGE_TRANSFER.md`.

Run:
```bash
pip install -r requirements.txt
uvicorn etl_cc.main:app --app-dir src --reload
```

Ab Initio graph upload is feature-flagged. Enable it before starting the API
and discovery worker:

```powershell
$env:ENABLE_AB_INITIO = "true"
$env:PYTHONPATH = "src"
uvicorn etl_cc.main:app --app-dir src --reload
```

The synthetic graph fixture is at
`runtime_sources/ab_initio_graph/sample_ab_initio_graph.json`. Upload it with
`POST /api/v1/sources/ab-initio/analyze`, then submit its returned `source_id`
and `test_token` to `POST /api/v1/discoveries`. The existing discovery worker
will process the graph and run discovery, lineage, and dependency planning.
