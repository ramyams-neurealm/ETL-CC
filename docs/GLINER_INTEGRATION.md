# GLiNER integration

ETL-CC calls the separately hosted GLiNER `/decide` endpoint. The API response
must be JSON with `label` set to `AUTO_CONVERT`, `HUMAN_REVIEW`, or `BLOCK`, and
`confidence` set to a number between 0 and 1. ETL-CC validates this contract;
unavailable or invalid responses are logged and handled conservatively.
Start the model service and SSH tunnel before launching the discovery worker or
migration orchestrator when GLiNER enforcement is enabled.

Configure the ETL-CC process with:

- `ENABLE_GLINER=true` to enable GLiNER (default).
- `GLINER_SERVICE_URL` set to the local tunnel URL (default
  `http://127.0.0.1:8100`).
- `GLINER_TIMEOUT_SECONDS` for the request timeout (default 30 seconds).
- `GLINER_AUTO_CONVERT_THRESHOLD` for the minimum automatic-routing confidence
  (default 0.90).
- `GLINER_SHADOW_MODE=true` to record advice without gating work. The default
  is `false`: discovery requires `AUTO_CONVERT`, and validation requires a
  confident `AUTO_CONVERT` result. If GLiNER is unavailable while enforcement
  is enabled, discovery requires review and validation marks otherwise-passing
  output as requiring human review. Set `ENABLE_GLINER=false` to disable both
  gates intentionally.

## Where results appear

- Discovery decision and confidence are included in the Discovery Agent result,
  `GET /api/v1/mappings/{etl_object_id}/discovery`, and
  `GET /api/v1/mappings/{etl_object_id}/agent-responses`.
- Discovery routing is advisory in shadow mode. Outside shadow mode, a
  non-`AUTO_CONVERT` decision prevents starting Conversion for that mapping.
- Validation uncertainty and confidence are included in
  `GET /api/v1/validations/{workflow_id}/report`. Outside shadow mode, uncertain
  GLiNER results can only make a passing validation require human review; they
  cannot make a failed validation pass.
- Discovery worker failures store `failure_category`,
  `failure_suggested_step`, `gliner_decision`, and `gliner_confidence` in the
  `WORKFLOW_FAILED` event payload. `GET /api/v1/workflows/{workflow_id}`
  returns these fields. Failure triage uses the same three labels, so it is a
  coarse routing recommendation, not a detailed root-cause diagnosis.

The discovery worker and migration orchestrator call the shared GLiNER adapter.
Do not run the legacy conversion, validation, or deployment polling workers
alongside the orchestrator.
