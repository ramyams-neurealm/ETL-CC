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
`docs/DEVELOPER_GUIDE.md`.

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

Synthetic graph fixtures are in `runtime_sources/ab_initio_graph/`:

- `sample_ab_initio_graph.mp`: high-value transaction alerts with a threshold
  filter and a fixed-date parameter.
- `sample_active_customers.mp`: active-customer filter and field reformat.
- `sample_inventory_reorder.mp`: below-reorder-level filter and calculated
  reorder quantity.

Upload one `.mp` file with `POST /api/v1/sources/ab-initio/analyze`, then submit
its returned `source_id` and `test_token` to `POST /api/v1/discoveries`. The
existing discovery worker will process the graph and run discovery, lineage,
and dependency planning. These fixtures are synthetic test inputs; generated
conversion output still goes through the normal critique and validation checks.

## Discovery evaluation

Create one directory per evaluation case. Each case directory contains an
Informatica PowerCenter `input.xml` and an `expected_output.json` canonical
mapping reference. The evaluator parses the XML and compares extracted mapping
fields with the JSON snapshot. Expected JSON may contain a full canonical
mapping or only the fields you want to assert. Specified object fields must
match; omitted fields are ignored; arrays must match in order and length.
No OpenAI API credentials are required.

```text
discovery_eval_data/
  employee_load/
    input.xml
    expected_output.json
```

Run from the repository root in PowerShell:

```powershell
$env:PYTHONPATH = "src"
python -m etl_cc.discovery_eval discovery_eval_data --threshold 100 --output discovery_eval_report.json
```

The report includes the extracted canonical mappings and a field-level list of
mismatches. A non-zero process exit code means at least one case scored below
the threshold.

python -m etl_cc.worker
python -m etl_cc.conversion_worker
python -m etl_cc.validation_worker
python -m etl_cc.deployment_worker