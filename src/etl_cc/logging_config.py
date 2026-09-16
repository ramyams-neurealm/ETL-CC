"""Readable, sanitized, combined operational logging for ETL CC.

All ETL services append to one live file:
    /home/authuser/dev/chatbots/ETL_CC/logs/etl_cc.log

Detailed agent payloads remain in PostgreSQL metadata tables. This module writes
only compact operational summaries to the application log.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sys
import threading
import traceback
from logging.handlers import WatchedFileHandler
from pathlib import Path
from typing import Any

LOG_PATH = Path("/home/authuser/dev/chatbots/ETL_CC/logs/etl_cc.log")
_CONFIGURED = False
_CONFIG_LOCK = threading.Lock()

CREDENTIAL_FIELDS = re.compile(
    r"password|passwd|authorization|api[_-]?key|client[_-]?secret|"
    r"access[_-]?token|refresh[_-]?token|credential|encryption[_-]?key|"
    r"secret_value|credential_ciphertext",
    re.I,
)
SAFE_TOKEN_FIELDS = {
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "token_count",
    "token_utilization",
    "actual_input_tokens",
    "actual_output_tokens",
    "actual_total_tokens",
}
DROP_FIELDS = {
    "canonical_mapping",
    "request_payload",
    "response_payload",
    "validation_result",
    "generated_files",
    "content",
    "rows",
    "input_rows",
    "stdout",
    "stderr",
    "traceback",
    "native_graph",
    "connectors",
    "field_lineage",
    "component_edges",
    "component_nodes",
    "test_cases",
    "operations",
}
COUNT_FIELDS = {
    "generated_files": "artifact_count",
    "rows": "row_count",
    "input_rows": "input_row_count",
    "test_cases": "test_case_count",
    "operations": "operation_count",
    "connectors": "connector_count",
    "field_lineage": "lineage_field_count",
    "component_edges": "edge_count",
    "component_nodes": "node_count",
    "scenarios": "scenario_count",
    "deployed_paths": "deployed_path_count",
}
SUMMARY_KEYS = {
    "status", "result", "decision", "revision_required", "confidence",
    "confidence_index", "deployable", "match_percentage", "rows_compared",
    "row_count", "valid_row_count", "passed", "failed", "skipped",
    "passed_count", "failed_count", "schema_coverage", "scenario_coverage",
    "business_rule_coverage", "lineage_coverage", "target_field_coverage",
    "precision_scale_coverage", "overall_functional_parity", "complexity",
    "complexity_score", "human_review_required", "safe_to_execute",
    "full_reference_coverage", "oracle_strength", "reference_execution_mode",
    "failure_reason", "recommendation", "error_message", "branch_name",
    "commit_sha", "pull_request_url", "artifact_type", "artifact_count",
    "mapping_name", "comparison_keys", "unsupported_constructs",
}
NOISY_LOGGERS = {
    "azure": logging.WARNING,
    "azure.core": logging.WARNING,
    "azure.identity": logging.WARNING,
    "azure.keyvault": logging.WARNING,
    "azure.core.pipeline.policies.http_logging_policy": logging.WARNING,
    "httpx": logging.WARNING,
    "httpx2": logging.WARNING,
    "httpcore": logging.WARNING,
    "openai": logging.WARNING,
    "urllib3": logging.WARNING,
}


def _is_sensitive_key(key: str) -> bool:
    normalized = key.strip().lower()
    if normalized in SAFE_TOKEN_FIELDS:
        return False
    return bool(CREDENTIAL_FIELDS.search(normalized))


def sanitize(value: Any, depth: int = 0) -> Any:
    if depth > 8:
        return "[MAX_DEPTH]"
    if isinstance(value, dict):
        return {
            str(key): (
                "[REDACTED]"
                if _is_sensitive_key(str(key))
                else sanitize(item, depth + 1)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [sanitize(item, depth + 1) for item in value]
    if isinstance(value, bytes):
        return {"type": "bytes", "size": len(value)}
    if hasattr(value, "model_dump"):
        try:
            return sanitize(value.model_dump(mode="json"), depth + 1)
        except Exception:
            return str(value)
    if isinstance(value, str):
        return value if len(value) <= 800 else value[:800] + "... [TRUNCATED]"
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return str(value)


def _summary_from_payload(payload: Any) -> dict[str, Any]:
    if hasattr(payload, "model_dump"):
        payload = payload.model_dump(mode="json")
    if not isinstance(payload, dict):
        return {}
    summary: dict[str, Any] = {}
    for key, value in payload.items():
        normalized = str(key).lower()
        if _is_sensitive_key(normalized):
            continue
        if normalized in COUNT_FIELDS and isinstance(value, (list, tuple, dict)):
            summary[COUNT_FIELDS[normalized]] = len(value)
            if normalized == "generated_files" and isinstance(value, list):
                summary["artifact_types"] = [
                    item.get("artifact_type")
                    for item in value
                    if isinstance(item, dict) and item.get("artifact_type")
                ]
            continue
        if normalized in SUMMARY_KEYS:
            if isinstance(value, list):
                summary[normalized + "_count"] = len(value)
            elif isinstance(value, dict):
                summary.update({
                    f"{normalized}_{child_key}": child_value
                    for child_key, child_value in _summary_from_payload(value).items()
                })
            else:
                summary[normalized] = sanitize(value)
    return summary


def _compact_event_fields(event: str, fields: dict[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key, value in fields.items():
        normalized = str(key).lower()
        if _is_sensitive_key(normalized):
            compact[key] = "[REDACTED]"
            continue
        if normalized in {"request_payload", "canonical_mapping", "discovery", "lineage"}:
            continue
        if normalized in {"response_payload", "validation_result"}:
            compact.update(_summary_from_payload(value))
            continue
        if normalized in DROP_FIELDS:
            if normalized in COUNT_FIELDS and isinstance(value, (list, tuple, dict)):
                compact[COUNT_FIELDS[normalized]] = len(value)
            continue
        if isinstance(value, dict):
            nested = _summary_from_payload(value)
            if nested:
                compact.update({f"{key}_{name}": item for name, item in nested.items()})
            continue
        if isinstance(value, (list, tuple, set)):
            compact[key + "_count"] = len(value)
            if normalized in {"artifact_types", "comparison_keys"}:
                compact[key] = sanitize(list(value))
            continue
        compact[key] = sanitize(value)
    return compact


def configure_logging(component: str) -> logging.Logger:
    global _CONFIGURED
    with _CONFIG_LOCK:
        if not _CONFIGURED:
            LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            root = logging.getLogger()
            root.setLevel(logging.INFO)
            root.handlers.clear()

            handler = WatchedFileHandler(
                LOG_PATH,
                mode="a",
                encoding="utf-8",
                delay=False,
            )
            handler.setLevel(logging.INFO)
            handler.setFormatter(logging.Formatter(
                "%(asctime)s %(levelname)s [%(name)s] %(message)s",
                "%Y-%m-%d %H:%M:%S",
            ))
            root.addHandler(handler)

            for logger_name, level in NOISY_LOGGERS.items():
                noisy = logging.getLogger(logger_name)
                noisy.setLevel(level)

            _CONFIGURED = True

    logger = logging.getLogger(component)
    logger.setLevel(logging.INFO)
    logger.propagate = True
    return logger


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    compact = _compact_event_fields(event, fields)
    upper = event.upper()
    level = (
        logging.ERROR
        if "FAILED" in upper or "ERROR" in upper
        else logging.WARNING
        if any(word in upper for word in ("WARNING", "RETRY", "REVISE"))
        else logging.INFO
    )
    logger.log(level, "%s | %s", event, _format_fields(compact))
    _flush()


def log_exception(logger: logging.Logger, event: str, exc: Exception, **fields: Any) -> None:
    compact = _compact_event_fields(event, fields)
    compact["error_type"] = type(exc).__name__
    compact["error"] = sanitize(str(exc))
    logger.error("%s | %s", event, _format_fields(compact))
    logger.debug("%s traceback=%s", event, traceback.format_exc())
    _flush()


def banner(logger: logging.Logger, title: str, **fields: Any) -> None:
    line = "=" * 96
    logger.info(line)
    logger.info(title.upper())
    for key, value in _compact_event_fields(title, fields).items():
        logger.info("%-22s : %s", str(key).replace("_", " ").title(), value)
    logger.info(line)
    _flush()


def stage_started(logger: logging.Logger, stage: str, **fields: Any) -> None:
    logger.info("[START] %-32s | %s", stage.upper(), _format_fields(_compact_event_fields(stage, fields)))
    _flush()


def stage_completed(logger: logging.Logger, stage: str, **fields: Any) -> None:
    logger.info("[DONE]  %-32s | %s", stage.upper(), _format_fields(_compact_event_fields(stage, fields)))
    _flush()


def stage_warning(logger: logging.Logger, stage: str, message: str, **fields: Any) -> None:
    values = _compact_event_fields(stage, fields)
    values["message"] = message
    logger.warning("[WARN]  %-32s | %s", stage.upper(), _format_fields(values))
    _flush()


def stage_failed(logger: logging.Logger, stage: str, error: Any, **fields: Any) -> None:
    values = _compact_event_fields(stage, fields)
    values["error"] = sanitize(error)
    logger.error("[FAIL]  %-32s | %s", stage.upper(), _format_fields(values))
    _flush()


def metric(logger: logging.Logger, stage: str, **fields: Any) -> None:
    logger.info("[METRICS] %-29s | %s", stage.upper(), _format_fields(_compact_event_fields(stage, fields)))
    _flush()


def _format_fields(fields: dict[str, Any]) -> str:
    if not fields:
        return "-"
    return " | ".join(f"{key}={value}" for key, value in fields.items())


def _flush() -> None:
    for handler in logging.getLogger().handlers:
        try:
            handler.flush()
        except Exception:
            pass
