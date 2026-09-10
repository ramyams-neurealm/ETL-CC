"""Consolidated NeuFlow Validation Agent.

GPT-4o generates only mapping-specific test scenarios and synthetic records.
Deterministic code owns schema checks, static safety, execution, comparison,
confidence, and the final pass/fail decision.
"""

from __future__ import annotations

import ast
import json
import math
import operator
import os
import re
import subprocess
import sys
import tempfile
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from etl_cc.agents.matchflow_comparator import MatchFlowComparator, MatchFlowResult
from etl_cc.agents.test_plan_agent import TestPlanAgent
from etl_cc.agents.unit_test_execution_agent import UnitTestExecutionAgent
from etl_cc.config import settings
from etl_cc.key_vault_service import DynamicChatOpenAI
from etl_cc.models import CanonicalMapping


class ReferenceExecutionMode(StrEnum):
    RELATIONAL_IR = "RELATIONAL_IR"
    METAMORPHIC = "METAMORPHIC"
    HUMAN_REVIEW_REQUIRED = "HUMAN_REVIEW_REQUIRED"


class OracleStrength(StrEnum):
    STRONG = "STRONG"
    PROVISIONAL = "PROVISIONAL"
    NONE = "NONE"


class ReferenceExpression(BaseModel):
    model_config = ConfigDict(extra="forbid")

    output_field: str
    expression: str


class ReferenceAggregate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    output_field: str
    function: Literal["COUNT", "COUNT_DISTINCT", "SUM", "AVG", "MIN", "MAX"]
    input_field: str | None = None


class ReferenceSortField(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field_name: str
    direction: Literal["ASC", "DESC"] = "ASC"


class ReferenceInvariant(BaseModel):
    model_config = ConfigDict(extra="forbid")

    invariant_id: str
    invariant_type: Literal[
        "OUTPUT_SCHEMA",
        "FILTER_PREDICATE",
        "UNIQUE_KEYS",
        "ROW_COUNT_NON_INCREASING",
        "ROW_COUNT_PRESERVED",
        "SORT_ORDER",
        "NON_NULL",
        "EXPRESSION_PREDICATE",
    ]
    description: str
    expression: str | None = None
    fields: list[str] = Field(default_factory=list)


class ReferenceOperation(BaseModel):
    """One constrained relational operation.

    Fields not required by an operation remain empty. The deterministic plan
    validator enforces operation-specific requirements.
    """

    model_config = ConfigDict(extra="forbid")

    operation_id: str
    operation: Literal[
        "PASSTHROUGH",
        "FILTER",
        "DERIVE",
        "PROJECT",
        "RENAME",
        "JOIN",
        "AGGREGATE",
        "UNION",
        "SORT",
        "DEDUPLICATE",
        "ROUTE",
    ]
    input_name: str
    output_name: str
    right_input_name: str | None = None
    condition: str | None = None
    expressions: list[ReferenceExpression] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    rename_from: str | None = None
    rename_to: str | None = None
    join_type: Literal["INNER", "LEFT", "RIGHT", "FULL"] | None = None
    left_keys: list[str] = Field(default_factory=list)
    right_keys: list[str] = Field(default_factory=list)
    group_by: list[str] = Field(default_factory=list)
    aggregates: list[ReferenceAggregate] = Field(default_factory=list)
    union_inputs: list[str] = Field(default_factory=list)
    sort_fields: list[ReferenceSortField] = Field(default_factory=list)
    deduplicate_keys: list[str] = Field(default_factory=list)
    route_name: str | None = None


class ReferenceAssumption(BaseModel):
    """A concrete material evidence gap, never general commentary."""

    model_config = ConfigDict(extra="forbid")

    description: str
    assumption_type: Literal[
        "MISSING_SEMANTICS",
        "MISSING_METADATA",
        "RUNTIME_DEPENDENCY",
        "UNSUPPORTED_CONSTRUCT",
    ]
    evidence_gap: str
    transformation_name: str | None
    source_expression: str | None
    affected_fields: list[str]


class ReferencePlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mapping_name: str
    plan_version: str = "1.0"
    primary_input: str
    output_name: str
    comparison_keys: list[str] = Field(min_length=1)
    operations: list[ReferenceOperation] = Field(default_factory=list, max_length=100)
    invariants: list[ReferenceInvariant] = Field(default_factory=list, max_length=100)
    source_evidence: list[str] = Field(default_factory=list)
    assumptions: list[ReferenceAssumption] = Field(default_factory=list)


class ReferencePlanIssue(BaseModel):
    code: str
    message: str
    operation_id: str | None = None


class ReferencePlanValidationResult(BaseModel):
    status: Literal["PASSED", "FAILED"]
    safe_to_execute: bool
    full_reference_coverage: bool
    issues: list[ReferencePlanIssue] = Field(default_factory=list)
    referenced_inputs: list[str] = Field(default_factory=list)
    unsupported_constructs: list[str] = Field(default_factory=list)


class ReferenceExecutionResult(BaseModel):
    status: Literal["PASSED", "FAILED", "SKIPPED"]
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    reference_execution_mode: ReferenceExecutionMode
    oracle_strength: OracleStrength
    full_reference_coverage: bool = False
    human_review_required: bool = False
    unsupported_constructs: list[str] = Field(default_factory=list)
    invariant_results: list[dict[str, Any]] = Field(default_factory=list)
    error_message: str | None = None


class ReferencePlanGenerator:
    AGENT_NAME = "REFERENCE_PLAN_GENERATOR"
    AGENT_VERSION = "1.2.0"
    STAGE_NAME = "REFERENCE_PLAN_GENERATION"
    MODEL_NAME = "gpt-4o"
    PROMPT_NAME = "ETL_REFERENCE_RELATIONAL_IR"
    PROMPT_VERSION = "1.0.0"

    def __init__(self) -> None:
        self.model_name = self.MODEL_NAME
        self.prompt_name = self.PROMPT_NAME
        self.prompt_version = self.PROMPT_VERSION
        self.input_tokens = 0
        self.output_tokens = 0

    async def run(
        self,
        mapping: CanonicalMapping,
        discovery: dict[str, Any],
        lineage: dict[str, Any],
    ) -> ReferencePlan:
        """Generate a plan from canonical evidence only.

        Generated PySpark code is intentionally not accepted as an input.
        """
        prompt = """You are a reference-plan compiler for ETL validation.
Translate only the supplied canonical mapping, discovery evidence, and lineage
into the requested constrained relational IR. Do not generate Python, PySpark,
SQL, shell commands, file paths, network calls, or pass/fail decisions.

Use only these operations: PASSTHROUGH, FILTER, DERIVE, PROJECT, RENAME,
JOIN, AGGREGATE, UNION, SORT, DEDUPLICATE, ROUTE. Preserve operation order. Every input,
column, expression, join key, group key, output field, and comparison key must
be grounded in supplied evidence. Expressions use a constrained Python-like
syntax with column names, literals, arithmetic, comparisons, boolean and/or/not,
and approved functions: UPPER, LOWER, TRIM, LTRIM, RTRIM, SUBSTR, LENGTH,
COALESCE, ISNULL, ABS, ROUND, CEIL, FLOOR, CONCAT, REPLACE, TO_INT,
TO_DECIMAL, TO_STRING, DATE_ADD, DATE_DIFF, YEAR, MONTH, DAY, CASE.

A behavior explicitly present in canonical transformations, port expressions,
properties, business rules, discovery evidence, or lineage is confirmed
behavior and MUST NOT be recorded as an assumption. For example, a grounded
UPPER(column) expression is an executable rule, not an assumption.

Create an assumption only when a specific missing fact can materially change
output values, output rows, schema, cardinality, ordering, state, side effects,
or runtime behavior. Every assumption must identify the exact evidence gap,
affected transformation, source expression when available, and affected
fields. Never place commentary, rationale, business interpretation, harmless
observations, or confirmed behavior in assumptions. If there is no material
evidence gap, return an empty assumptions list.

Select comparison keys only from stable record-identity fields that exist in
both source and target, are non-nullable in both schemas, and pass through
unchanged. Never use derived fields, transformed fields, measures, balances,
descriptions, timestamps, nullable fields, or all output columns merely to
create uniqueness. Prefer explicit primary keys, business keys, connector keys,
or fields named as identifiers in canonical evidence. If no grounded stable
identity key exists, create a MISSING_METADATA assumption instead of inventing
one.

Use PASSTHROUGH only for a grounded stage, instance, or connector boundary
that preserves every row and field unchanged while assigning a new output_name.
RENAME is only for renaming one field and must always provide both rename_from
and rename_to. Never use RENAME to represent a transformation instance,
dataset alias, stage name, connector boundary, or unchanged flow.

The primary input and final output names must exactly match canonical source and
target dataset names. Intermediate output names may match grounded canonical
transformation or instance names. Do not invent operations or semantics to
avoid an assumption."""
        evidence = {
            "canonical_mapping": mapping.model_dump(mode="json"),
            "discovery": {
                "business_rules": discovery.get("business_rules", []),
                "assumptions": discovery.get("assumptions", []),
                "risks": discovery.get("risks", []),
            },
            "lineage": lineage,
            "limits": {
                "max_operations": int(
                    getattr(settings, "reference_plan_max_operations", 100)
                ),
                "max_invariants": int(
                    getattr(settings, "reference_plan_max_invariants", 100)
                ),
            },
        }
        llm = DynamicChatOpenAI(
            operation_name=f"reference-plan:{mapping.source_object_key}",
            temperature=0.0,
        )
        response = await llm.with_structured_output(
            ReferencePlan,
            include_raw=True,
        ).ainvoke(
            [
                ("system", prompt),
                ("human", json.dumps(evidence, sort_keys=True, default=str)),
            ]
        )
        parsed = response.get("parsed") if isinstance(response, dict) else response
        raw = response.get("raw") if isinstance(response, dict) else None
        if parsed is None:
            raise RuntimeError("GPT-4o returned no valid reference plan.")
        if parsed.mapping_name != mapping.mapping_name:
            raise RuntimeError("Reference plan mapping_name does not match mapping.")

        for index, operation_item in enumerate(parsed.operations, start=1):
            operation_item.operation_id = f"REF-{index:03d}"

            # Normalize an empty field RENAME into an explicit no-op stage
            # boundary. A real field rename still requires both field names.
            if (
                operation_item.operation == "RENAME"
                and operation_item.rename_from is None
                and operation_item.rename_to is None
            ):
                operation_item.operation = "PASSTHROUGH"

        for index, invariant in enumerate(parsed.invariants, start=1):
            invariant.invariant_id = f"INV-{index:03d}"

        usage = getattr(raw, "usage_metadata", None) or {}
        metadata = getattr(raw, "response_metadata", None) or {}
        token_usage = metadata.get("token_usage", {})
        self.input_tokens = int(
            usage.get("input_tokens") or token_usage.get("prompt_tokens") or 0
        )
        self.output_tokens = int(
            usage.get("output_tokens")
            or token_usage.get("completion_tokens")
            or 0
        )
        self.model_name = (
            metadata.get("model_name") or metadata.get("model") or llm.model
        )
        return parsed


class SafeExpression:
    """Parse and evaluate a small expression language without eval()."""

    _FUNCTIONS = {
        "UPPER",
        "LOWER",
        "TRIM",
        "LTRIM",
        "RTRIM",
        "SUBSTR",
        "LENGTH",
        "COALESCE",
        "ISNULL",
        "ABS",
        "ROUND",
        "CEIL",
        "FLOOR",
        "CONCAT",
        "REPLACE",
        "TO_INT",
        "TO_DECIMAL",
        "TO_STRING",
        "DATE_ADD",
        "DATE_DIFF",
        "YEAR",
        "MONTH",
        "DAY",
        "CASE",
    }
    _BIN_OPS = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.Mod: operator.mod,
    }
    _CMP_OPS = {
        ast.Eq: operator.eq,
        ast.NotEq: operator.ne,
        ast.Gt: operator.gt,
        ast.GtE: operator.ge,
        ast.Lt: operator.lt,
        ast.LtE: operator.le,
        ast.In: lambda left, right: left in right,
        ast.NotIn: lambda left, right: left not in right,
        ast.Is: operator.is_,
        ast.IsNot: operator.is_not,
    }

    @classmethod
    def normalize(cls, expression: str) -> str:
        text = expression.strip()
        text = re.sub(r"\bAND\b", " and ", text, flags=re.I)
        text = re.sub(r"\bOR\b", " or ", text, flags=re.I)
        text = re.sub(r"\bNOT\b", " not ", text, flags=re.I)
        text = re.sub(r"\bNULL\b", "None", text, flags=re.I)
        text = re.sub(r"(?<![<>=!])=(?!=)", "==", text)
        text = text.replace("<>", "!=")
        return text

    @classmethod
    def validate(cls, expression: str, allowed_fields: set[str]) -> list[str]:
        issues: list[str] = []
        try:
            tree = ast.parse(cls.normalize(expression), mode="eval")
        except SyntaxError as exc:
            return [f"Invalid expression syntax: {exc.msg}"]
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                if node.id not in allowed_fields and node.id.upper() not in cls._FUNCTIONS:
                    if node.id not in {"True", "False", "None"}:
                        issues.append(f"Unknown field or function: {node.id}")
            elif isinstance(node, ast.Call):
                if not isinstance(node.func, ast.Name):
                    issues.append("Only direct approved function calls are allowed.")
                elif node.func.id.upper() not in cls._FUNCTIONS:
                    issues.append(f"Unsupported function: {node.func.id}")
            elif isinstance(
                node,
                (
                    ast.Attribute,
                    ast.Subscript,
                    ast.Lambda,
                    ast.DictComp,
                    ast.ListComp,
                    ast.SetComp,
                    ast.GeneratorExp,
                    ast.Await,
                    ast.Yield,
                    ast.NamedExpr,
                ),
            ):
                issues.append(f"Unsafe expression node: {type(node).__name__}")
        return sorted(set(issues))

    @classmethod
    def evaluate(cls, expression: str, row: dict[str, Any]) -> Any:
        tree = ast.parse(cls.normalize(expression), mode="eval")
        return cls._node(tree.body, row)

    @classmethod
    def _node(cls, node: ast.AST, row: dict[str, Any]) -> Any:
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            return row.get(node.id)
        if isinstance(node, ast.List):
            return [cls._node(item, row) for item in node.elts]
        if isinstance(node, ast.Tuple):
            return tuple(cls._node(item, row) for item in node.elts)
        if isinstance(node, ast.BoolOp):
            values = [bool(cls._node(item, row)) for item in node.values]
            return all(values) if isinstance(node.op, ast.And) else any(values)
        if isinstance(node, ast.UnaryOp):
            value = cls._node(node.operand, row)
            if isinstance(node.op, ast.Not):
                return not bool(value)
            if isinstance(node.op, ast.USub):
                return -value
            if isinstance(node.op, ast.UAdd):
                return +value
        if isinstance(node, ast.BinOp):
            left = cls._node(node.left, row)
            right = cls._node(node.right, row)
            if left is None or right is None:
                return None
            function = cls._BIN_OPS.get(type(node.op))
            if function is None:
                raise ValueError(f"Unsupported operator: {type(node.op).__name__}")
            return function(left, right)
        if isinstance(node, ast.Compare):
            left = cls._node(node.left, row)
            for operation, comparator in zip(node.ops, node.comparators):
                right = cls._node(comparator, row)
                function = cls._CMP_OPS.get(type(operation))
                if function is None or not function(left, right):
                    return False
                left = right
            return True
        if isinstance(node, ast.IfExp):
            return cls._node(node.body if cls._node(node.test, row) else node.orelse, row)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            name = node.func.id.upper()
            arguments = [cls._node(item, row) for item in node.args]
            return cls._call(name, arguments)
        raise ValueError(f"Unsupported expression node: {type(node).__name__}")

    @staticmethod
    def _call(name: str, args: list[Any]) -> Any:
        if name == "UPPER":
            return None if args[0] is None else str(args[0]).upper()
        if name == "LOWER":
            return None if args[0] is None else str(args[0]).lower()
        if name == "TRIM":
            return None if args[0] is None else str(args[0]).strip()
        if name == "LTRIM":
            return None if args[0] is None else str(args[0]).lstrip()
        if name == "RTRIM":
            return None if args[0] is None else str(args[0]).rstrip()
        if name == "SUBSTR":
            if args[0] is None:
                return None
            start = int(args[1]) - 1
            return str(args[0])[start:] if len(args) < 3 else str(args[0])[start : start + int(args[2])]
        if name == "LENGTH":
            return None if args[0] is None else len(str(args[0]))
        if name == "COALESCE":
            return next((item for item in args if item is not None), None)
        if name == "ISNULL":
            return args[0] is None
        if name == "ABS":
            return None if args[0] is None else abs(args[0])
        if name == "ROUND":
            if args[0] is None:
                return None
            places = int(args[1]) if len(args) > 1 else 0
            quantum = Decimal(1).scaleb(-places)
            return Decimal(str(args[0])).quantize(quantum, rounding=ROUND_HALF_UP)
        if name == "CEIL":
            return None if args[0] is None else math.ceil(args[0])
        if name == "FLOOR":
            return None if args[0] is None else math.floor(args[0])
        if name == "CONCAT":
            return "".join("" if item is None else str(item) for item in args)
        if name == "REPLACE":
            return None if args[0] is None else str(args[0]).replace(str(args[1]), str(args[2]))
        if name == "TO_INT":
            return None if args[0] is None else int(Decimal(str(args[0])))
        if name == "TO_DECIMAL":
            return None if args[0] is None else Decimal(str(args[0]))
        if name == "TO_STRING":
            return None if args[0] is None else str(args[0])
        if name in {"YEAR", "MONTH", "DAY"}:
            if args[0] is None:
                return None
            value = args[0]
            if isinstance(value, str):
                value = datetime.fromisoformat(value).date()
            return getattr(value, name.lower())
        if name == "CASE":
            return args[1] if bool(args[0]) else args[2]
        if name in {"DATE_ADD", "DATE_DIFF"}:
            raise ValueError(f"{name} requires explicit platform-neutral date semantics.")
        raise ValueError(f"Unsupported function: {name}")


class ReferencePlanValidator:
    AGENT_NAME = "REFERENCE_PLAN_VALIDATOR"
    AGENT_VERSION = "1.2.0"
    STAGE_NAME = "REFERENCE_PLAN_VALIDATION"
    MODEL_NAME = "DETERMINISTIC_IR_VALIDATOR"

    def run(
        self,
        mapping: CanonicalMapping,
        plan: ReferencePlan,
    ) -> ReferencePlanValidationResult:
        issues: list[ReferencePlanIssue] = []
        unsupported: list[str] = []
        datasets = {
            dataset.name: {field.name for field in dataset.fields}
            for dataset in mapping.sources
        }
        source_field_models = {
            field.name: field
            for dataset in mapping.sources
            for field in dataset.fields
        }
        target_field_models = {
            field.name: field
            for dataset in mapping.targets
            for field in dataset.fields
        }
        target_fields = set(target_field_models)
        derived_fields: set[str] = set()

        if plan.primary_input not in datasets:
            issues.append(ReferencePlanIssue(code="UNKNOWN_PRIMARY_INPUT", message=f"Unknown primary input {plan.primary_input}."))
        if not plan.operations:
            issues.append(ReferencePlanIssue(code="EMPTY_PLAN", message="Reference plan has no operations."))

        available = dict(datasets)
        operation_ids: set[str] = set()
        for operation_item in plan.operations:
            operation_id = operation_item.operation_id
            if operation_id in operation_ids:
                issues.append(ReferencePlanIssue(code="DUPLICATE_OPERATION_ID", message=f"Duplicate operation ID {operation_id}.", operation_id=operation_id))
            operation_ids.add(operation_id)
            if operation_item.input_name not in available:
                issues.append(ReferencePlanIssue(code="UNKNOWN_INPUT", message=f"Unknown input {operation_item.input_name}.", operation_id=operation_id))
                continue
            fields = set(available[operation_item.input_name])
            output_fields = set(fields)
            kind = operation_item.operation

            def expression_issues(expression: str) -> None:
                for message in SafeExpression.validate(expression, fields):
                    issues.append(ReferencePlanIssue(code="INVALID_EXPRESSION", message=message, operation_id=operation_id))

            if kind == "PASSTHROUGH":
                pass
            elif kind in {"FILTER", "ROUTE"}:
                if not operation_item.condition:
                    issues.append(ReferencePlanIssue(code="MISSING_CONDITION", message=f"{kind} requires condition.", operation_id=operation_id))
                else:
                    expression_issues(operation_item.condition)
            elif kind == "DERIVE":
                if not operation_item.expressions:
                    issues.append(ReferencePlanIssue(code="MISSING_EXPRESSIONS", message="DERIVE requires expressions.", operation_id=operation_id))
                for expression in operation_item.expressions:
                    expression_issues(expression.expression)
                    output_fields.add(expression.output_field)
                    if expression.expression.strip().upper() != expression.output_field.upper():
                        derived_fields.add(expression.output_field)
            elif kind == "PROJECT":
                unknown = set(operation_item.columns) - fields
                if unknown:
                    issues.append(ReferencePlanIssue(code="UNKNOWN_COLUMNS", message=f"Unknown projected columns: {sorted(unknown)}", operation_id=operation_id))
                output_fields = set(operation_item.columns)
            elif kind == "RENAME":
                if operation_item.rename_from not in fields or not operation_item.rename_to:
                    issues.append(ReferencePlanIssue(code="INVALID_RENAME", message="RENAME requires an existing source and target field.", operation_id=operation_id))
                else:
                    output_fields.discard(operation_item.rename_from)
                    output_fields.add(operation_item.rename_to)
            elif kind == "JOIN":
                if not operation_item.right_input_name or operation_item.right_input_name not in available:
                    issues.append(ReferencePlanIssue(code="UNKNOWN_RIGHT_INPUT", message="JOIN requires a known right input.", operation_id=operation_id))
                else:
                    right_fields = available[operation_item.right_input_name]
                    if len(operation_item.left_keys) != len(operation_item.right_keys) or not operation_item.left_keys:
                        issues.append(ReferencePlanIssue(code="INVALID_JOIN_KEYS", message="JOIN requires equally sized non-empty key lists.", operation_id=operation_id))
                    if set(operation_item.left_keys) - fields or set(operation_item.right_keys) - right_fields:
                        issues.append(ReferencePlanIssue(code="UNKNOWN_JOIN_KEYS", message="JOIN references unknown keys.", operation_id=operation_id))
                    output_fields |= set(right_fields)
            elif kind == "AGGREGATE":
                if set(operation_item.group_by) - fields:
                    issues.append(ReferencePlanIssue(code="UNKNOWN_GROUP_FIELDS", message="AGGREGATE references unknown group fields.", operation_id=operation_id))
                output_fields = set(operation_item.group_by)
                for aggregate in operation_item.aggregates:
                    if aggregate.function != "COUNT" and aggregate.input_field not in fields:
                        issues.append(ReferencePlanIssue(code="UNKNOWN_AGGREGATE_FIELD", message=f"Unknown aggregate field {aggregate.input_field}.", operation_id=operation_id))
                    output_fields.add(aggregate.output_field)
            elif kind == "UNION":
                names = operation_item.union_inputs or [operation_item.input_name]
                if any(name not in available for name in names):
                    issues.append(ReferencePlanIssue(code="UNKNOWN_UNION_INPUT", message="UNION contains unknown inputs.", operation_id=operation_id))
                elif any(available[name] != available[names[0]] for name in names[1:]):
                    issues.append(ReferencePlanIssue(code="UNION_SCHEMA_MISMATCH", message="UNION inputs must have equal schemas.", operation_id=operation_id))
                output_fields = set(available[names[0]]) if names else set()
            elif kind == "SORT":
                unknown = {item.field_name for item in operation_item.sort_fields} - fields
                if unknown:
                    issues.append(ReferencePlanIssue(code="UNKNOWN_SORT_FIELDS", message=f"Unknown sort fields: {sorted(unknown)}", operation_id=operation_id))
            elif kind == "DEDUPLICATE":
                unknown = set(operation_item.deduplicate_keys) - fields
                if not operation_item.deduplicate_keys or unknown:
                    issues.append(ReferencePlanIssue(code="INVALID_DEDUP_KEYS", message=f"Invalid deduplication keys: {sorted(unknown)}", operation_id=operation_id))
            else:
                unsupported.append(kind)
            available[operation_item.output_name] = output_fields

        if plan.output_name not in available:
            issues.append(ReferencePlanIssue(code="UNKNOWN_OUTPUT", message=f"Unknown output {plan.output_name}."))
        else:
            actual = available[plan.output_name]
            missing = target_fields - actual
            extra = actual - target_fields
            if missing:
                issues.append(ReferencePlanIssue(code="MISSING_TARGET_FIELDS", message=f"Plan output misses target fields: {sorted(missing)}"))
            if extra:
                issues.append(ReferencePlanIssue(code="EXTRA_OUTPUT_FIELDS", message=f"Plan output has extra fields: {sorted(extra)}"))

        source_fields = set().union(*datasets.values()) if datasets else set()
        invalid_keys = set(plan.comparison_keys) - (source_fields & target_fields)
        if invalid_keys:
            issues.append(
                ReferencePlanIssue(
                    code="INVALID_COMPARISON_KEYS",
                    message=f"Comparison keys do not exist in source and target: {sorted(invalid_keys)}",
                )
            )

        derived_keys = set(plan.comparison_keys) & derived_fields
        if derived_keys:
            issues.append(
                ReferencePlanIssue(
                    code="DERIVED_COMPARISON_KEYS",
                    message=f"Comparison keys must pass through unchanged: {sorted(derived_keys)}",
                )
            )

        nullable_keys = {
            key
            for key in plan.comparison_keys
            if (
                key in source_field_models
                and key in target_field_models
                and (
                    source_field_models[key].nullable
                    or target_field_models[key].nullable
                )
            )
        }
        if nullable_keys:
            issues.append(
                ReferencePlanIssue(
                    code="NULLABLE_COMPARISON_KEYS",
                    message=f"Comparison keys must be non-nullable: {sorted(nullable_keys)}",
                )
            )

        blocking_assumptions = list(plan.assumptions)
        assumption_descriptions = [item.description for item in blocking_assumptions]
        full_coverage = not issues and not unsupported and not blocking_assumptions
        safe_to_execute = not issues and not unsupported
        return ReferencePlanValidationResult(
            status="PASSED" if safe_to_execute else "FAILED",
            safe_to_execute=safe_to_execute,
            full_reference_coverage=full_coverage,
            issues=issues,
            referenced_inputs=sorted(available),
            unsupported_constructs=sorted(set(unsupported + assumption_descriptions)),
        )


class RelationalIRExecutor:
    AGENT_NAME = "REFERENCE_EXECUTION_COORDINATOR"
    AGENT_VERSION = "1.1.0"
    STAGE_NAME = "REFERENCE_EXECUTION"
    MODEL_NAME = "DETERMINISTIC_RELATIONAL_IR"

    def run(
        self,
        mapping: CanonicalMapping,
        plan: ReferencePlan,
        validation: ReferencePlanValidationResult,
        input_datasets: dict[str, list[dict[str, Any]]],
    ) -> ReferenceExecutionResult:
        if not validation.safe_to_execute:
            return ReferenceExecutionResult(
                status="SKIPPED",
                reference_execution_mode=ReferenceExecutionMode.HUMAN_REVIEW_REQUIRED,
                oracle_strength=OracleStrength.NONE,
                human_review_required=True,
                unsupported_constructs=validation.unsupported_constructs,
                error_message="Reference plan failed deterministic validation.",
            )
        missing_inputs = {
            name
            for name in validation.referenced_inputs
            if name in {item.name for item in mapping.sources} and name not in input_datasets
        }
        if missing_inputs:
            return ReferenceExecutionResult(
                status="SKIPPED",
                reference_execution_mode=ReferenceExecutionMode.HUMAN_REVIEW_REQUIRED,
                oracle_strength=OracleStrength.NONE,
                human_review_required=True,
                unsupported_constructs=[f"Missing synthetic dataset: {item}" for item in sorted(missing_inputs)],
                error_message="Synthetic data was not supplied for every referenced source dataset.",
            )
        try:
            datasets = {name: [dict(row) for row in rows] for name, rows in input_datasets.items()}
            for operation_item in plan.operations:
                datasets[operation_item.output_name] = self._execute_operation(operation_item, datasets)
            rows = datasets[plan.output_name]
            target_fields = [field.name for dataset in mapping.targets for field in dataset.fields]
            if target_fields:
                rows = [{field: row.get(field) for field in target_fields} for row in rows]
            invariants = self._invariants(plan, rows)
            failed_invariants = [item for item in invariants if not item["passed"]]
            return ReferenceExecutionResult(
                status="PASSED" if not failed_invariants else "FAILED",
                rows=rows,
                row_count=len(rows),
                reference_execution_mode=ReferenceExecutionMode.RELATIONAL_IR,
                oracle_strength=(OracleStrength.STRONG if validation.full_reference_coverage else OracleStrength.PROVISIONAL),
                full_reference_coverage=validation.full_reference_coverage,
                human_review_required=not validation.full_reference_coverage,
                unsupported_constructs=validation.unsupported_constructs,
                invariant_results=invariants,
                error_message=("One or more reference invariants failed." if failed_invariants else None),
            )
        except Exception as exc:
            return ReferenceExecutionResult(
                status="FAILED",
                reference_execution_mode=ReferenceExecutionMode.RELATIONAL_IR,
                oracle_strength=OracleStrength.NONE,
                human_review_required=True,
                unsupported_constructs=validation.unsupported_constructs,
                error_message=f"{type(exc).__name__}: {str(exc)[:3500]}",
            )

    def _execute_operation(
        self,
        item: ReferenceOperation,
        datasets: dict[str, list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        rows = [dict(row) for row in datasets[item.input_name]]
        if item.operation == "PASSTHROUGH":
            return rows
        if item.operation in {"FILTER", "ROUTE"}:
            return [row for row in rows if bool(SafeExpression.evaluate(item.condition or "False", row))]
        if item.operation == "DERIVE":
            for row in rows:
                for expression in item.expressions:
                    row[expression.output_field] = SafeExpression.evaluate(expression.expression, row)
            return rows
        if item.operation == "PROJECT":
            return [{field: row.get(field) for field in item.columns} for row in rows]
        if item.operation == "RENAME":
            for row in rows:
                row[item.rename_to or ""] = row.pop(item.rename_from or "", None)
            return rows
        if item.operation == "SORT":
            result = rows
            for sort_field in reversed(item.sort_fields):
                result = sorted(
                    result,
                    key=lambda row, name=sort_field.field_name: (row.get(name) is None, row.get(name)),
                    reverse=sort_field.direction == "DESC",
                )
            return result
        if item.operation == "DEDUPLICATE":
            seen: set[tuple[Any, ...]] = set()
            output: list[dict[str, Any]] = []
            for row in rows:
                key = tuple(row.get(name) for name in item.deduplicate_keys)
                if key not in seen:
                    seen.add(key)
                    output.append(row)
            return output
        if item.operation == "UNION":
            return [dict(row) for name in item.union_inputs for row in datasets[name]]
        if item.operation == "JOIN":
            return self._join(item, rows, datasets[item.right_input_name or ""])
        if item.operation == "AGGREGATE":
            return self._aggregate(item, rows)
        raise ValueError(f"Unsupported operation: {item.operation}")

    @staticmethod
    def _join(item: ReferenceOperation, left: list[dict[str, Any]], right: list[dict[str, Any]]) -> list[dict[str, Any]]:
        right_index: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
        for row in right:
            right_index[tuple(row.get(key) for key in item.right_keys)].append(row)
        output: list[dict[str, Any]] = []
        matched_right: set[int] = set()
        right_fields = set().union(*(row.keys() for row in right)) if right else set()
        left_fields = set().union(*(row.keys() for row in left)) if left else set()
        for left_row in left:
            matches = right_index.get(tuple(left_row.get(key) for key in item.left_keys), [])
            if matches:
                for right_row in matches:
                    output.append({**left_row, **right_row})
                    matched_right.add(id(right_row))
            elif item.join_type in {"LEFT", "FULL"}:
                output.append({**{field: None for field in right_fields}, **left_row})
        if item.join_type in {"RIGHT", "FULL"}:
            for right_row in right:
                if id(right_row) not in matched_right:
                    output.append({**{field: None for field in left_fields}, **right_row})
        return output

    @staticmethod
    def _aggregate(item: ReferenceOperation, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[tuple(row.get(key) for key in item.group_by)].append(row)
        output: list[dict[str, Any]] = []
        for key, group_rows in groups.items():
            result = dict(zip(item.group_by, key))
            for aggregate in item.aggregates:
                values = [row.get(aggregate.input_field or "") for row in group_rows]
                non_null = [value for value in values if value is not None]
                if aggregate.function == "COUNT":
                    value = len(group_rows) if aggregate.input_field is None else len(non_null)
                elif aggregate.function == "COUNT_DISTINCT":
                    value = len(set(non_null))
                elif aggregate.function == "SUM":
                    value = sum(non_null) if non_null else None
                elif aggregate.function == "AVG":
                    value = sum(non_null) / len(non_null) if non_null else None
                elif aggregate.function == "MIN":
                    value = min(non_null) if non_null else None
                elif aggregate.function == "MAX":
                    value = max(non_null) if non_null else None
                else:
                    raise ValueError(f"Unsupported aggregate: {aggregate.function}")
                result[aggregate.output_field] = value
            output.append(result)
        return output

    @staticmethod
    def _invariants(plan: ReferencePlan, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for invariant in plan.invariants:
            passed = True
            details = invariant.description
            if invariant.invariant_type == "OUTPUT_SCHEMA":
                passed = all(set(invariant.fields).issubset(row) for row in rows)
            elif invariant.invariant_type == "UNIQUE_KEYS":
                keys = [tuple(row.get(field) for field in invariant.fields) for row in rows]
                passed = len(keys) == len(set(keys))
            elif invariant.invariant_type == "NON_NULL":
                passed = all(all(row.get(field) is not None for field in invariant.fields) for row in rows)
            elif invariant.invariant_type in {"FILTER_PREDICATE", "EXPRESSION_PREDICATE"}:
                passed = bool(invariant.expression) and all(bool(SafeExpression.evaluate(invariant.expression or "False", row)) for row in rows)
            elif invariant.invariant_type == "SORT_ORDER":
                keys = [tuple(row.get(field) for field in invariant.fields) for row in rows]
                passed = keys == sorted(keys)
            results.append({
                "invariant_id": invariant.invariant_id,
                "invariant_type": invariant.invariant_type,
                "passed": passed,
                "details": details,
            })
        return results



class ValidationContext(BaseModel):
    workflow_id: str
    mapping: CanonicalMapping
    discovery: dict[str, Any] = Field(default_factory=dict)
    lineage: dict[str, Any] = Field(default_factory=dict)
    artifact_paths: dict[str, Path]
    minimum_functional_parity: float = Field(default=0.95, ge=0, le=1)
    input_mode: Literal["SIMULATE", "DATASET_FILE", "DATABASE_TABLES"] = "SIMULATE"
    input_datasets: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    expected_output_rows: list[dict[str, Any]] | None = None
    requested_row_count: int = 1000


class ValidationTestResult(BaseModel):
    test_id: str
    test_name: str
    category: str
    status: Literal["PLANNED", "PASSED", "FAILED", "SKIPPED", "WARNING"]
    severity: Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"] = "HIGH"
    expected_result: str | None = None
    actual_result: str | None = None
    details: str | None = None
    evidence_payload: dict[str, Any] = Field(default_factory=dict)
    duration_seconds: float = 0.0


class ValidationStageResult(BaseModel):
    agent_name: str
    agent_version: str
    stage_name: str
    model_name: str
    prompt_name: str | None = None
    prompt_version: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    status: Literal["COMPLETED", "FAILED", "SKIPPED"]
    response_payload: dict[str, Any] = Field(default_factory=dict)
    error_message: str | None = None


class ValidationResult(BaseModel):
    mapping_name: str
    status: Literal["PASSED", "FAILED"]
    deployable: bool
    minimum_functional_parity: float
    match_percentage: float | None = None
    rows_compared: int = 0
    confidence_index: float = 0.0
    reference_execution_mode: str | None = None
    oracle_strength: str | None = None
    full_reference_coverage: bool = False
    human_review_required: bool = False
    unsupported_constructs: list[str] = Field(default_factory=list)
    failure_reason: str | None = None
    recommendation: str | None = None
    test_cases: list[ValidationTestResult] = Field(default_factory=list)
    stages: list[ValidationStageResult] = Field(default_factory=list)
    data_files: dict[str, str] = Field(default_factory=dict)


class SyntheticCell(BaseModel):
    """One strict-schema field/value pair in a synthetic input row."""

    field_name: str
    value: str | int | float | bool | None = None


class SyntheticInputRow(BaseModel):
    """Strict-schema row representation compatible with structured output."""

    cells: list[SyntheticCell] = Field(min_length=1)

    def as_dict(self) -> dict[str, Any]:
        return {cell.field_name: cell.value for cell in self.cells}


class SyntheticScenario(BaseModel):
    test_id: str
    category: Literal[
        "HAPPY_PATH", "NEGATIVE", "NULL_HANDLING", "BOUNDARY",
        "DUPLICATE_KEY", "BUSINESS_RULE",
    ]
    title: str
    description: str
    input_rows: list[SyntheticInputRow] = Field(min_length=1, max_length=25)
    expected_behavior: str
    source_expression: str | None = None
    severity: Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"] = "HIGH"


class SyntheticTestDataResult(BaseModel):
    mapping_name: str
    comparison_keys: list[str] = Field(min_length=1)
    scenarios: list[SyntheticScenario] = Field(min_length=2, max_length=30)
    generation_notes: list[str] = Field(default_factory=list)


class SyntheticTestDataGenerator:
    AGENT_NAME = "SYNTHETIC_TEST_DATA_AGENT"
    AGENT_VERSION = "1.1.0"
    STAGE_NAME = "SYNTHETIC_DATA_GENERATION"
    MODEL_NAME = "gpt-4o"
    PROMPT_NAME = "ETL_SYNTHETIC_VALIDATION_DATA"
    PROMPT_VERSION = "1.0.0"

    def __init__(self) -> None:
        self.model_name = self.MODEL_NAME
        self.prompt_name = self.PROMPT_NAME
        self.prompt_version = self.PROMPT_VERSION
        self.input_tokens = 0
        self.output_tokens = 0

    async def run(self, mapping: CanonicalMapping, discovery: dict, lineage: dict) -> SyntheticTestDataResult:
        prompt = """Generate fictional, non-sensitive ETL validation scenarios and
synthetic input rows using only the supplied canonical evidence. Never invent
columns. Use comparison keys present in both source and target. Keep key values
populated and unique across rows. Cover happy path, every grounded business
rule, filter rejection when applicable, nullable fields when applicable, and
relevant boundaries. Preserve source_expression exactly. Keep decimal values
inside declared precision and scale. Do not decide pass or fail. Return only the
requested structured output. Represent each input row as a cells array. Each
cell must have field_name and value. Do not return dynamic JSON object keys for
input rows. Use provisional test IDs SYN-001, SYN-002, and so on. The backend
will deterministically overwrite every test ID before validation. For integer
values, obey canonical precision and Spark-compatible bounds. Integer fields
with precision 9 or less must remain between -2147483648 and 2147483647.
Integer fields with precision above 9 may use signed 64-bit values but must
remain between -9223372036854775808 and 9223372036854775807. Keep comparison
keys unique across every generated row unless canonical evidence explicitly
contains duplicate-handling behavior. Generate a DUPLICATE_KEY scenario only
when a grounded deduplication, rank, distinct, or aggregation rule exists."""
        evidence = {
            "canonical_mapping": mapping.model_dump(mode="json"),
            "business_rules": discovery.get("business_rules", []),
            "risks": discovery.get("risks", []),
            "assumptions": discovery.get("assumptions", []),
            "lineage": lineage,
            "limits": {
                "max_scenarios": int(getattr(settings, "synthetic_validation_max_scenarios", 30)),
                "max_rows": int(getattr(settings, "synthetic_validation_max_rows", 250)),
            },
        }
        llm = DynamicChatOpenAI(operation_name=f"synthetic-validation:{mapping.source_object_key}")
        response = await llm.with_structured_output(SyntheticTestDataResult, include_raw=True).ainvoke([
            ("system", prompt),
            ("human", json.dumps(evidence, sort_keys=True, default=str)),
        ])
        parsed = response.get("parsed") if isinstance(response, dict) else response
        raw = response.get("raw") if isinstance(response, dict) else None
        if parsed is None:
            raise RuntimeError("GPT-4o returned no valid synthetic test-data result.")
        if parsed.mapping_name != mapping.mapping_name:
            raise RuntimeError("Synthetic result mapping_name does not match the mapping.")

        # GPT-4o generates semantic scenario content. System identifiers are
        # deterministic and are never trusted to the model.
        for index, scenario in enumerate(parsed.scenarios, start=1):
            scenario.test_id = f"SYN-{index:03d}"

        usage = getattr(raw, "usage_metadata", None) or {}
        metadata = getattr(raw, "response_metadata", None) or {}
        token_usage = metadata.get("token_usage", {})
        self.input_tokens = int(usage.get("input_tokens") or token_usage.get("prompt_tokens") or 0)
        self.output_tokens = int(usage.get("output_tokens") or token_usage.get("completion_tokens") or 0)
        self.model_name = metadata.get("model_name") or metadata.get("model") or llm.model
        return parsed


class SyntheticDataCheck(BaseModel):
    check_name: str
    passed: bool
    details: str


class SyntheticDataValidationResult(BaseModel):
    status: Literal["PASSED", "FAILED"]
    checks: list[SyntheticDataCheck]
    valid_row_count: int
    scenario_coverage: float
    schema_coverage: float


class SyntheticDataValidator:
    AGENT_NAME = "SYNTHETIC_DATA_VALIDATOR"
    AGENT_VERSION = "1.2.0"
    STAGE_NAME = "SYNTHETIC_DATA_VALIDATION"
    MODEL_NAME = "DETERMINISTIC_SCHEMA"

    def run(
        self,
        mapping: CanonicalMapping,
        generated: SyntheticTestDataResult,
        discovery: dict,
        physical_rows: list[dict[str, Any]] | None = None,
    ) -> SyntheticDataValidationResult:
        fields = {field.name: field for dataset in mapping.sources for field in dataset.fields}
        target_fields = {field.name for dataset in mapping.targets for field in dataset.fields}
        checks: list[SyntheticDataCheck] = []
        test_id_errors: list[str] = []
        schema_errors: list[str] = []
        ids: set[str] = set()
        keys_seen: set[tuple[str, ...]] = set()
        row_count = 0
        duplicate_handling_tokens = {
            "deduplicate",
            "dedup",
            "distinct",
            "rank",
            "aggregator",
            "aggregate",
        }
        mapping_handles_duplicates = any(
            any(
                token in transformation.transformation_type.strip().lower()
                for token in duplicate_handling_tokens
            )
            for transformation in mapping.transformations
        )

        keys_ok = bool(generated.comparison_keys) and all(key in fields and key in target_fields for key in generated.comparison_keys)
        checks.append(SyntheticDataCheck(
            check_name="COMPARISON_KEYS",
            passed=keys_ok,
            details="Comparison keys exist in source and target." if keys_ok else "Invalid comparison keys.",
        ))

        for scenario in generated.scenarios:
            if not re.fullmatch(r"SYN-\d{3,}", scenario.test_id):
                test_id_errors.append(f"Invalid test ID: {scenario.test_id}")
            if scenario.test_id in ids:
                test_id_errors.append(f"Duplicate test ID: {scenario.test_id}")
            ids.add(scenario.test_id)

        rows_to_validate: list[tuple[str, dict[str, Any], str | None]] = []
        if physical_rows is not None:
            rows_to_validate = [
                (f"INPUT-{index:06d}", dict(row), None)
                for index, row in enumerate(physical_rows, start=1)
            ]
        else:
            rows_to_validate = [
                (scenario.test_id, row_model.as_dict(), scenario.category)
                for scenario in generated.scenarios
                for row_model in scenario.input_rows
            ]

        for row_id, row, scenario_category in rows_to_validate:
            row_count += 1
            unknown = set(row) - set(fields)
            missing = set(fields) - set(row)
            if unknown:
                schema_errors.append(f"{row_id}: unknown fields {sorted(unknown)}")
            if missing:
                schema_errors.append(f"{row_id}: missing fields {sorted(missing)}")
            for name, field in fields.items():
                value = row.get(name)
                if value is None:
                    if not field.nullable:
                        schema_errors.append(f"{row_id}: {name} is non-nullable")
                    continue
                schema_errors.extend(self._validate_value(row_id, name, value, field))
            if keys_ok:
                key = tuple(
                    "" if row.get(name) is None else str(row[name])
                    for name in generated.comparison_keys
                )
                if "" in key:
                    schema_errors.append(f"{row_id}: null comparison key")
                elif key in keys_seen:
                    duplicate_allowed = (
                        physical_rows is None
                        and scenario_category == "DUPLICATE_KEY"
                        and mapping_handles_duplicates
                    )
                    if not duplicate_allowed:
                        schema_errors.append(f"{row_id}: duplicate comparison key {key}")
                keys_seen.add(key)

        test_ids_ok = not test_id_errors
        checks.append(SyntheticDataCheck(
            check_name="TEST_ID_FORMAT",
            passed=test_ids_ok,
            details=(
                "All synthetic test IDs use the SYN-NNN format and are unique."
                if test_ids_ok
                else "; ".join(test_id_errors)
            ),
        ))

        max_scenarios = int(getattr(settings, "synthetic_validation_max_scenarios", 30))
        max_rows = int(getattr(settings, "synthetic_validation_max_rows", 250))
        bounded = 0 < len(generated.scenarios) <= max_scenarios and 0 < row_count <= max_rows
        checks.append(SyntheticDataCheck(
            check_name="BOUNDED_DATA", passed=bounded,
            details=f"Generated {len(generated.scenarios)} scenarios and {row_count} rows." if bounded else "Configured limits exceeded.",
        ))
        schema_ok = not schema_errors
        checks.append(SyntheticDataCheck(
            check_name="SCHEMA_CONFORMANCE", passed=schema_ok,
            details="All rows conform to canonical metadata." if schema_ok else "; ".join(schema_errors[:30]),
        ))

        categories = {scenario.category for scenario in generated.scenarios}
        required = {"HAPPY_PATH"}
        if discovery.get("business_rules"):
            required.add("BUSINESS_RULE")
        if any("filter" in item.transformation_type.lower() for item in mapping.transformations):
            required.add("NEGATIVE")
        if any(field.nullable for field in fields.values()):
            required.add("NULL_HANDLING")
        missing_categories = required - categories
        coverage_ok = not missing_categories
        checks.append(SyntheticDataCheck(
            check_name="SCENARIO_COVERAGE", passed=coverage_ok,
            details="Required categories are present." if coverage_ok else f"Missing categories: {sorted(missing_categories)}",
        ))
        passed = all(check.passed for check in checks)
        coverage = len(required & categories) / len(required) if required else 1.0
        return SyntheticDataValidationResult(
            status="PASSED" if passed else "FAILED",
            checks=checks,
            valid_row_count=row_count if schema_ok else 0,
            scenario_coverage=round(coverage, 4),
            schema_coverage=1.0 if schema_ok else 0.0,
        )

    @staticmethod
    def _validate_value(test_id: str, name: str, value: Any, field: Any) -> list[str]:
        errors: list[str] = []
        dtype = (field.data_type or "").lower()
        try:
            if "int" in dtype or "integer" in dtype:
                if isinstance(value, bool) or Decimal(str(value)) != int(value):
                    errors.append(f"{test_id}: {name} is not an integer")
            elif any(token in dtype for token in ("decimal", "numeric", "number", "float", "double")):
                number = Decimal(str(value))
                precision = len(number.as_tuple().digits)
                scale = max(-number.as_tuple().exponent, 0)
                if field.precision is not None and precision > field.precision:
                    errors.append(f"{test_id}: {name} exceeds precision {field.precision}")
                if field.scale is not None and scale > field.scale:
                    errors.append(f"{test_id}: {name} exceeds scale {field.scale}")
            elif any(token in dtype for token in ("char", "string", "varchar")) and not isinstance(value, str):
                errors.append(f"{test_id}: {name} is not a string")
        except (InvalidOperation, ValueError, TypeError):
            errors.append(f"{test_id}: {name} has invalid value {value!r}")
        return errors

class StaticCheck(BaseModel):
    check_name: str
    passed: bool
    severity: Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"] = "HIGH"
    details: str


class StaticValidationResult(BaseModel):
    status: Literal["PASSED", "FAILED"]
    checks: list[StaticCheck]
    passed_count: int
    failed_count: int
    safe_to_execute_tests: bool


class StaticArtifactValidator:
    AGENT_NAME = "STATIC_VALIDATION_AGENT"
    AGENT_VERSION = "2.0.0"
    STAGE_NAME = "STATIC_VALIDATION"
    MODEL_NAME = "DETERMINISTIC_AST"

    def run(self, artifacts: dict[str, Path]) -> StaticValidationResult:
        checks: list[StaticCheck] = []
        required = {"PYSPARK_CODE", "UNIT_TEST", "CONFIGURATION"}
        missing = sorted(required - set(artifacts))
        checks.append(StaticCheck(
            check_name="REQUIRED_ARTIFACTS", passed=not missing,
            details="All required artifacts are present." if not missing else f"Missing: {', '.join(missing)}",
        ))
        contents = {kind: path.read_text(encoding="utf-8") for kind, path in artifacts.items() if path.is_file()}
        for kind in ("PYSPARK_CODE", "UNIT_TEST"):
            content = contents.get(kind, "")
            try:
                ast.parse(content)
                ok = bool(content)
                details = f"{kind} parses as Python." if ok else f"{kind} is empty."
            except SyntaxError as exc:
                ok = False
                details = f"Syntax error at line {exc.lineno}: {exc.msg}"
            checks.append(StaticCheck(check_name=f"{kind}_SYNTAX", passed=ok, details=details))
        try:
            config_ok = isinstance(json.loads(contents.get("CONFIGURATION", "")), dict)
            config_details = "Configuration contains valid JSON."
        except (json.JSONDecodeError, TypeError) as exc:
            config_ok = False
            config_details = f"Invalid configuration JSON: {exc}"
        checks.append(StaticCheck(check_name="CONFIGURATION_JSON", passed=config_ok, details=config_details))

        production = contents.get("PYSPARK_CODE", "").lower()
        forbidden = {
            "sparksession.builder": "production mapping creates a Spark session",
            "spark.stop(": "production mapping stops a Spark session",
            "spark.read": "production mapping performs source I/O",
            ".write.": "production mapping performs target I/O",
            ".writestream": "production mapping performs streaming output",
            "jdbc:": "production mapping contains JDBC configuration",
            "/path/to/": "production mapping contains a placeholder path",
            "todo": "production mapping contains TODO",
        }
        runtime_found = [description for token, description in forbidden.items() if token in production]
        checks.append(StaticCheck(
            check_name="DATABRICKS_RUNTIME_SAFETY", passed=not runtime_found,
            details="No unsafe production runtime patterns found." if not runtime_found else "Found: " + ", ".join(runtime_found),
        ))
        combined = "\n".join(contents.values())
        secret_patterns = [r"OPENAI_API_KEY", r"AZURE_CLIENT_SECRET", r"password\s*=", r'"password"\s*:', r"sk-[A-Za-z0-9_-]{20,}"]
        secret_found = any(re.search(pattern, combined, re.I) for pattern in secret_patterns)
        checks.append(StaticCheck(
            check_name="NO_EMBEDDED_SECRETS", passed=not secret_found,
            details="No secret markers found." if not secret_found else "Potential secret marker found.",
        ))
        dangerous = {"subprocess", "socket", "requests", "httpx", "urllib", "ftplib"}
        imported: set[str] = set()
        try:
            tree = ast.parse(contents.get("UNIT_TEST", ""))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported.update(alias.name.split(".")[0] for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imported.add(node.module.split(".")[0])
        except SyntaxError:
            pass
        prohibited = sorted(imported & dangerous)
        checks.append(StaticCheck(
            check_name="SAFE_TEST_IMPORTS", passed=not prohibited,
            details="No prohibited imports found." if not prohibited else f"Prohibited imports: {', '.join(prohibited)}",
        ))
        failed = sum(not check.passed for check in checks)
        return StaticValidationResult(
            status="PASSED" if failed == 0 else "FAILED",
            checks=checks, passed_count=len(checks) - failed, failed_count=failed,
            safe_to_execute_tests=failed == 0,
        )


class LegacyExecutionResult(BaseModel):
    status: Literal["PASSED", "FAILED"]
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    error_message: str | None = None


class LegacyLogicExecutor:
    AGENT_NAME = "LEGACY_EXECUTION_ADAPTER"
    AGENT_VERSION = "1.0.0"
    STAGE_NAME = "LEGACY_EXECUTION"
    MODEL_NAME = "DETERMINISTIC_INTERPRETER"

    def run(self, mapping: CanonicalMapping, input_rows: list[dict[str, Any]]) -> LegacyExecutionResult:
        try:
            rows = [dict(row) for row in input_rows]
            for transformation in mapping.transformations:
                kind = transformation.transformation_type.strip().lower()
                if kind == "filter":
                    expression = self._property(transformation.properties, "filter condition") or self._property(transformation.properties, "source filter")
                    if not expression:
                        raise RuntimeError(f"Filter {transformation.name} has no condition.")
                    rows = [row for row in rows if self._filter(expression, row)]
                elif kind == "expression":
                    for port in transformation.ports:
                        expression = (port.expression or "").strip()
                        if expression and expression.upper() != port.name.upper():
                            for row in rows:
                                row[port.name] = self._expression(expression, row)
                else:
                    raise RuntimeError(f"Unsupported trusted legacy transformation: {transformation.transformation_type}")
            target_fields = [field.name for dataset in mapping.targets for field in dataset.fields]
            output = [{name: row.get(name) for name in target_fields} for row in rows] if target_fields else rows
            return LegacyExecutionResult(status="PASSED", rows=output, row_count=len(output))
        except Exception as exc:
            return LegacyExecutionResult(status="FAILED", error_message=str(exc)[:4000])

    @staticmethod
    def _property(properties: dict, name: str) -> str:
        return next((str(value).strip() for key, value in (properties or {}).items() if str(key).strip().lower() == name and str(value).strip()), "")

    def _filter(self, expression: str, row: dict) -> bool:
        for operator in (">=", "<=", "!=", "=", ">", "<"):
            if operator in expression:
                left, right = [part.strip() for part in expression.split(operator, 1)]
                actual, expected = self._literal(left, row), self._literal(right, row)
                if actual is None or expected is None:
                    return False
                return {"=": actual == expected, "!=": actual != expected, ">": actual > expected, "<": actual < expected, ">=": actual >= expected, "<=": actual <= expected}[operator]
        raise RuntimeError(f"Unsupported filter expression: {expression}")

    def _expression(self, expression: str, row: dict) -> Any:
        match = re.fullmatch(r"UPPER\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)", expression, re.I)
        if match:
            value = row.get(match.group(1))
            return None if value is None else str(value).upper()
        match = re.fullmatch(r"LOWER\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)", expression, re.I)
        if match:
            value = row.get(match.group(1))
            return None if value is None else str(value).lower()
        if expression in row:
            return row[expression]
        return self._literal(expression, row)

    @staticmethod
    def _literal(token: str, row: dict) -> Any:
        text = token.strip()
        if text in row:
            return row[text]
        if text.upper() in {"NULL", "NONE"}:
            return None
        if len(text) >= 2 and text[0] == text[-1] and text[0] in "\"'":
            return text[1:-1]
        try:
            return Decimal(text) if "." in text else int(text)
        except ValueError:
            return text


class TargetExecutionResult(BaseModel):
    status: Literal["PASSED", "FAILED", "SKIPPED"]
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    stdout: str = ""
    stderr: str = ""
    error_message: str | None = None


class TargetPySparkExecutor:
    AGENT_NAME = "TARGET_EXECUTION_ADAPTER"
    AGENT_VERSION = "1.1.0"
    STAGE_NAME = "TARGET_EXECUTION"
    MODEL_NAME = "ISOLATED_PYSPARK_SUBPROCESS"

    HARNESS = """import importlib.util, json, sys
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, IntegerType, LongType, DoubleType, StringType, BooleanType, StructField, StructType
code_path, input_path, schema_path, output_path = sys.argv[1:]
rows = json.load(open(input_path, encoding='utf-8'))
specification = json.load(open(schema_path, encoding='utf-8'))
def spark_type(field):
    dtype = str(field.get('data_type', 'string')).lower()
    if any(x in dtype for x in ('decimal','numeric','number')): return DecimalType(int(field.get('precision') or 38), int(field.get('scale') or 0))
    if 'bigint' in dtype or 'long' in dtype:
        return LongType()
    if 'int' in dtype or 'integer' in dtype:
        precision = field.get('precision')
        if precision is not None and int(precision) > 9:
            return LongType()
        return IntegerType()
    if 'double' in dtype or 'float' in dtype: return DoubleType()
    if 'bool' in dtype: return BooleanType()
    return StringType()
schema = StructType([StructField(field['name'], spark_type(field), bool(field.get('nullable', True))) for field in specification])
for row in rows:
    for field in specification:
        name, dtype = field['name'], str(field.get('data_type','')).lower()
        if row.get(name) is not None and any(x in dtype for x in ('decimal','numeric','number')): row[name] = Decimal(str(row[name]))
module_spec = importlib.util.spec_from_file_location('generated_mapping', code_path)
module = importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(module)
transform = getattr(module, 'transform', None)
if transform is None: raise RuntimeError('Generated artifact must expose transform(DataFrame).')
spark = SparkSession.builder.master('local[1]').appName('neuflow-validation').getOrCreate()
try:
    output = transform(spark.createDataFrame(rows, schema=schema))
    json.dump([item.asDict(recursive=True) for item in output.collect()], open(output_path, 'w', encoding='utf-8'), default=str)
finally:
    spark.stop()
"""

    def run(self, code_path: Path, mapping: CanonicalMapping, rows: list[dict], timeout: int) -> TargetExecutionResult:
        try:
            with tempfile.TemporaryDirectory(prefix="neuflow-target-") as temp:
                directory = Path(temp)
                input_path, schema_path = directory / "input.json", directory / "schema.json"
                output_path, harness_path = directory / "output.json", directory / "harness.py"
                input_path.write_text(json.dumps(rows, default=str), encoding="utf-8")
                schema = [field.model_dump(mode="json") for dataset in mapping.sources for field in dataset.fields]
                schema_path.write_text(json.dumps(schema, default=str), encoding="utf-8")
                harness_path.write_text(self.HARNESS, encoding="utf-8")
                allowed_keys = {
                    "PATH", "JAVA_HOME", "SPARK_HOME", "HADOOP_HOME",
                    "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "TMPDIR",
                    "USERPROFILE", "HOME", "LOCALAPPDATA", "APPDATA",
                }
                environment = {
                    key: value
                    for key, value in os.environ.items()
                    if key.upper() in allowed_keys
                }
                environment.update({
                    "PYTHONPATH": str(code_path.parent),
                    "PYTHONIOENCODING": "utf-8",
                    "PYTHONDONTWRITEBYTECODE": "1",
                    "PYSPARK_PYTHON": sys.executable,
                    "PYSPARK_DRIVER_PYTHON": sys.executable,
                })
                completed = subprocess.run([sys.executable, str(harness_path), str(code_path), str(input_path), str(schema_path), str(output_path)], capture_output=True, text=True, timeout=timeout, env=environment, check=False)
                if completed.returncode != 0:
                    return TargetExecutionResult(status="FAILED", stdout=completed.stdout[-10000:], stderr=completed.stderr[-10000:], error_message=(completed.stderr or completed.stdout)[-4000:])
                output = json.loads(output_path.read_text(encoding="utf-8"))
                return TargetExecutionResult(status="PASSED", rows=output, row_count=len(output), stdout=completed.stdout[-10000:], stderr=completed.stderr[-10000:])
        except subprocess.TimeoutExpired:
            return TargetExecutionResult(status="FAILED", error_message=f"Target execution exceeded {timeout} seconds.")
        except Exception as exc:
            return TargetExecutionResult(status="FAILED", error_message=str(exc)[:4000])


class StaticCoverageResult(BaseModel):
    source_coverage: float
    target_coverage: float
    target_field_coverage: float
    business_rule_coverage: float
    lineage_coverage: float
    precision_scale_coverage: float
    overall_functional_parity: float
    status: Literal["PASSED", "WARNING"]


class StaticImplementationCoverage:
    AGENT_NAME = "FUNCTIONAL_PARITY_AGENT"
    AGENT_VERSION = "2.0.0"
    STAGE_NAME = "STATIC_IMPLEMENTATION_COVERAGE"
    MODEL_NAME = "DETERMINISTIC_COVERAGE"

    def run(self, mapping: CanonicalMapping, discovery: dict, lineage: dict, code: str, config: str, tests: str, minimum: float) -> StaticCoverageResult:
        searchable = "\n".join((code, config, tests))
        sources = [item.name for item in mapping.sources]
        targets = [item.name for item in mapping.targets]
        fields = [field.name for target in mapping.targets for field in target.fields]
        rules = [item.get("source_expression") for item in discovery.get("business_rules", []) if item.get("source_expression")]
        field_lineage = lineage.get("field_lineage", [])
        source = self._coverage(sources, searchable)
        target = self._coverage(targets, searchable)
        field = self._coverage(fields, code)
        rule = 1.0 if not rules else sum(self._rule_evidence(item, code) for item in rules) / len(rules)
        lineage_score = 1.0 if not fields else min(1.0, len([item for item in field_lineage if item.get("lineage_type") != "UNRESOLVED"]) / len(fields))
        decimals = [field for dataset in [*mapping.sources, *mapping.targets] for field in dataset.fields if (field.data_type or "").lower() in {"decimal", "numeric", "number"} and field.precision is not None and field.scale is not None]
        precision = 1.0 if not decimals else sum(self._precision(item.precision, item.scale, searchable) for item in decimals) / len(decimals)
        overall = round(sum((source, target, field, rule, lineage_score, precision)) / 6, 4)
        return StaticCoverageResult(source_coverage=round(source,4), target_coverage=round(target,4), target_field_coverage=round(field,4), business_rule_coverage=round(rule,4), lineage_coverage=round(lineage_score,4), precision_scale_coverage=round(precision,4), overall_functional_parity=overall, status="PASSED" if overall >= minimum else "WARNING")

    @staticmethod
    def _coverage(items: list[str], text: str) -> float:
        lowered = text.lower()
        return 1.0 if not items else sum(item.lower() in lowered for item in items) / len(items)

    @staticmethod
    def _rule_evidence(expression: str, code: str) -> float:
        target = re.sub(r"\s+", "", code).lower()
        identifiers = [token.lower() for token in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expression) if token.lower() not in {"upper", "lower", "and", "or", "not", "null"}]
        functions = [token.lower() for token in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(", expression)]
        return 1.0 if identifiers and all(item in target for item in identifiers) and all(item in target for item in functions) else 0.0

    @staticmethod
    def _precision(precision: int, scale: int, text: str) -> float:
        patterns = (rf"DecimalType\(\s*{precision}\s*,\s*{scale}\s*\)", rf'"precision"\s*:\s*{precision}', rf'"scale"\s*:\s*{scale}')
        return 1.0 if any(re.search(pattern, text, re.I) for pattern in patterns) else 0.0


def calculate_confidence(match_percentage: float, scenario_coverage: float, business_rule_coverage: float, schema_coverage: float, assumptions: int) -> float:
    score = (match_percentage / 100) * 0.50 + scenario_coverage * 0.20 + business_rule_coverage * 0.15 + schema_coverage * 0.15
    return round(max(0.0, min(score - min(assumptions * 0.03, 0.15), 1.0)), 4)


class ValidationDataStore:
    @property
    def root(self) -> Path:
        path = settings.project_root / getattr(settings, "validation_data_directory_name", "runtime_validation_data")
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write(self, workflow_id: str, mapping_name: str, name: str, payload: Any) -> Path:
        safe = lambda value: "".join(character if character.isalnum() or character in "-_." else "_" for character in value)
        directory = self.root / safe(workflow_id) / safe(mapping_name)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / safe(name)
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        return path


class ValidationAgent:
    AGENT_NAME = "VALIDATION_AGENT"
    AGENT_VERSION = "3.4.0"
    STAGE_NAME = "ZERO_TOUCH_VALIDATION"
    MODEL_NAME = "HYBRID_GPT4O_DETERMINISTIC"

    @staticmethod
    def _expand_simulated_rows(
        base_rows: list[dict[str, Any]],
        comparison_keys: list[str],
        target_row_count: int,
    ) -> list[dict[str, Any]]:
        """Expand semantic rows while guaranteeing unique composite keys."""
        if target_row_count <= 0:
            raise ValueError("Simulation target_row_count must be greater than zero.")
        if not base_rows:
            raise ValueError("At least one semantic synthetic row is required.")
        if not comparison_keys:
            raise ValueError("Comparison keys are required for simulation expansion.")

        expanded = [dict(row) for row in base_rows[:target_row_count]]
        used_keys: set[tuple[str, ...]] = set()
        for row in expanded:
            key = tuple(
                "" if row.get(name) is None else str(row[name])
                for name in comparison_keys
            )
            if "" in key:
                raise ValueError("Semantic synthetic rows contain a null comparison key.")
            if key in used_keys:
                raise ValueError(f"Semantic synthetic rows contain duplicate comparison key {key}.")
            used_keys.add(key)

        numeric_next: dict[str, int] = {}
        for name in comparison_keys:
            values = [
                row.get(name)
                for row in base_rows
                if isinstance(row.get(name), int)
                and not isinstance(row.get(name), bool)
            ]
            if values:
                numeric_next[name] = max(values) + 1

        source_index = 0
        while len(expanded) < target_row_count:
            template = dict(base_rows[source_index % len(base_rows)])
            source_index += 1
            candidate = dict(template)
            attempt = 0
            while True:
                attempt += 1
                for position, name in enumerate(comparison_keys):
                    original = template.get(name)
                    if isinstance(original, int) and not isinstance(original, bool):
                        value = numeric_next.get(name, len(expanded) + 1)
                        candidate[name] = value
                        numeric_next[name] = value + 1
                    else:
                        prefix = "KEY" if original is None else str(original)
                        candidate[name] = (
                            f"{prefix}_{len(expanded):08d}_{position}_{attempt}"
                        )
                candidate_key = tuple(str(candidate[name]) for name in comparison_keys)
                if candidate_key not in used_keys:
                    break
            used_keys.add(candidate_key)
            expanded.append(candidate)
        return expanded

    async def run(self, context: ValidationContext | dict[str, Any]) -> ValidationResult:
        if isinstance(context, dict):
            context = ValidationContext.model_validate(context)
        mapping = context.mapping
        artifacts = self._artifacts(context.artifact_paths)
        stages: list[ValidationStageResult] = []
        tests: list[ValidationTestResult] = []
        files: dict[str, str] = {}

        plan_agent = TestPlanAgent()
        plan = plan_agent.run(mapping, context.discovery)
        stages.append(self._stage(plan_agent, "COMPLETED", plan.model_dump(mode="json")))

        synthetic_agent = SyntheticTestDataGenerator()
        synthetic = await synthetic_agent.run(mapping, context.discovery, context.lineage)
        stages.append(self._stage(synthetic_agent, "COMPLETED", synthetic.model_dump(mode="json")))
        semantic_rows = [
            row.as_dict()
            for scenario in synthetic.scenarios
            for row in scenario.input_rows
        ]
        if context.input_mode == "SIMULATE":
            input_rows = self._expand_simulated_rows(
                base_rows=semantic_rows,
                comparison_keys=synthetic.comparison_keys,
                target_row_count=context.requested_row_count,
            )
        elif context.input_datasets:
            primary_name = mapping.sources[0].name
            input_rows = list(
                context.input_datasets.get(primary_name)
                or next(iter(context.input_datasets.values()))
            )
        else:
            input_rows = semantic_rows

        validator = SyntheticDataValidator()
        synthetic_validation = validator.run(
            mapping,
            synthetic,
            context.discovery,
            physical_rows=input_rows,
        )
        stages.append(self._stage(validator, "COMPLETED", synthetic_validation.model_dump(mode="json")))
        tests.extend(self._synthetic_tests(synthetic, synthetic_validation))
        store = ValidationDataStore()
        input_path = store.write(context.workflow_id, mapping.mapping_name, "synthetic_input.json", input_rows)
        files["synthetic_input"] = str(input_path)

        # Generate an independent reference plan from canonical evidence only.
        # Generated PySpark code is intentionally not provided to this agent.
        reference_plan_agent = ReferencePlanGenerator()
        reference_plan = await reference_plan_agent.run(
            mapping,
            context.discovery,
            context.lineage,
        )
        stages.append(
            self._stage(
                reference_plan_agent,
                "COMPLETED",
                reference_plan.model_dump(mode="json"),
            )
        )

        reference_validator = ReferencePlanValidator()
        reference_validation = reference_validator.run(mapping, reference_plan)
        stages.append(
            self._stage(
                reference_validator,
                "COMPLETED" if reference_validation.safe_to_execute else "FAILED",
                reference_validation.model_dump(mode="json"),
                None
                if reference_validation.safe_to_execute
                else "Reference plan failed deterministic validation.",
            )
        )

        static_agent = StaticArtifactValidator()
        static = static_agent.run(artifacts)
        stages.append(self._stage(static_agent, "COMPLETED", static.model_dump(mode="json")))
        tests.extend(self._static_tests(static))

        unit_agent = UnitTestExecutionAgent()
        unit = unit_agent.run(artifacts["UNIT_TEST"], settings.validation_test_timeout_seconds) if static.safe_to_execute_tests else unit_agent.skipped("Static validation blocked pytest execution.")
        stages.append(self._stage(unit_agent, "COMPLETED", unit.model_dump(mode="json")))
        tests.extend(self._unit_tests(unit))

        code = artifacts["PYSPARK_CODE"].read_text(encoding="utf-8")
        config = artifacts["CONFIGURATION"].read_text(encoding="utf-8")
        unit_text = artifacts["UNIT_TEST"].read_text(encoding="utf-8")
        coverage_agent = StaticImplementationCoverage()
        coverage = coverage_agent.run(mapping, context.discovery, context.lineage, code, config, unit_text, context.minimum_functional_parity)
        stages.append(self._stage(coverage_agent, "COMPLETED", coverage.model_dump(mode="json")))
        tests.extend(self._coverage_tests(coverage, context.minimum_functional_parity))

        reference_executor = RelationalIRExecutor()
        primary_source_name = (
            reference_plan.primary_input
            if reference_plan.primary_input
            else mapping.sources[0].name
        )
        input_datasets = context.input_datasets or {primary_source_name: input_rows}
        reference = reference_executor.run(
            mapping,
            reference_plan,
            reference_validation,
            input_datasets,
        )
        stages.append(
            self._stage(
                reference_executor,
                "COMPLETED" if reference.status == "PASSED" else reference.status,
                reference.model_dump(mode="json"),
                reference.error_message,
            )
        )
        reference_path = None
        if reference.status == "PASSED":
            reference_path = store.write(
                context.workflow_id,
                mapping.mapping_name,
                "reference_output.json",
                reference.rows,
            )
            files["reference_output"] = str(reference_path)

        target_agent = TargetPySparkExecutor()
        target = target_agent.run(artifacts["PYSPARK_CODE"], mapping, input_rows, settings.validation_test_timeout_seconds) if synthetic_validation.status == "PASSED" and static.safe_to_execute_tests else TargetExecutionResult(status="SKIPPED", error_message="Synthetic or static validation blocked target execution.")
        stages.append(self._stage(target_agent, "COMPLETED" if target.status == "PASSED" else target.status, target.model_dump(mode="json"), target.error_message))
        target_path = None
        if target.status == "PASSED":
            target_path = store.write(context.workflow_id, mapping.mapping_name, "target_output.json", target.rows)
            files["target_output"] = str(target_path)

        match: MatchFlowResult | None = None
        comparator = MatchFlowComparator()
        if context.expected_output_rows is not None:
            target_path = store.write(context.workflow_id, mapping.mapping_name, "selected_target_output.json", context.expected_output_rows)
            files["selected_target_output"] = str(target_path)
        if reference_path and target_path:
            match = comparator.compare(
                reference_path,
                target_path,
                reference_plan.comparison_keys,
            )
            stages.append(self._stage(comparator, "COMPLETED", match.model_dump(mode="json")))
        else:
            stages.append(
                self._stage(
                    comparator,
                    "SKIPPED",
                    {},
                    "Reference and target outputs are required.",
                )
            )

        confidence = calculate_confidence(match.match_percentage, synthetic_validation.scenario_coverage, coverage.business_rule_coverage, synthetic_validation.schema_coverage, len(context.discovery.get("assumptions", []))) if match else 0.0
        tests.extend(
            self._data_tests(
                synthetic_validation,
                reference,
                target,
                match,
                confidence,
                context.minimum_functional_parity,
            )
        )
        threshold = float(getattr(settings, "validation_confidence_threshold", 0.90))
        passed = (
            static.status == "PASSED"
            and unit.status == "PASSED"
            and synthetic_validation.status == "PASSED"
            and reference.status == "PASSED"
            and reference.oracle_strength == OracleStrength.STRONG
            and reference.full_reference_coverage
            and not reference.human_review_required
            and target.status == "PASSED"
            and match is not None
            and match.match_percentage
            >= context.minimum_functional_parity * 100
            and confidence >= threshold
        )
        reason, recommendation = self._failure(
            static,
            unit,
            synthetic_validation,
            reference,
            target,
            match,
            confidence,
            threshold,
            context.minimum_functional_parity,
        )
        return ValidationResult(
            mapping_name=mapping.mapping_name,
            status="PASSED" if passed else "FAILED",
            deployable=passed,
            minimum_functional_parity=context.minimum_functional_parity,
            match_percentage=match.match_percentage if match else None,
            rows_compared=match.rows_compared if match else 0,
            confidence_index=confidence,
            reference_execution_mode=reference.reference_execution_mode,
            oracle_strength=reference.oracle_strength,
            full_reference_coverage=reference.full_reference_coverage,
            human_review_required=reference.human_review_required,
            unsupported_constructs=reference.unsupported_constructs,
            failure_reason=None if passed else reason,
            recommendation=None if passed else recommendation,
            test_cases=tests,
            stages=stages,
            data_files=files,
        )

    @staticmethod
    def _artifacts(paths: dict[str, Path]) -> dict[str, Path]:
        required = {"PYSPARK_CODE", "UNIT_TEST", "CONFIGURATION"}
        missing = required - set(paths)
        if missing:
            raise RuntimeError("Missing artifacts: " + ", ".join(sorted(missing)))
        normalized = {key: Path(value) for key, value in paths.items()}
        missing_files = [str(value) for value in normalized.values() if not value.is_file()]
        if missing_files:
            raise RuntimeError("Artifact files not found: " + ", ".join(missing_files))
        return normalized

    @staticmethod
    def _stage(agent: Any, status: str, response: dict, error: str | None = None) -> ValidationStageResult:
        return ValidationStageResult(
            agent_name=agent.AGENT_NAME, agent_version=agent.AGENT_VERSION,
            stage_name=agent.STAGE_NAME, model_name=getattr(agent, "model_name", agent.MODEL_NAME),
            prompt_name=getattr(agent, "prompt_name", None), prompt_version=getattr(agent, "prompt_version", None),
            input_tokens=int(getattr(agent, "input_tokens", 0) or 0), output_tokens=int(getattr(agent, "output_tokens", 0) or 0),
            status=status, response_payload=response, error_message=error,
        )

    @staticmethod
    def _synthetic_tests(generated: SyntheticTestDataResult, validation: SyntheticDataValidationResult) -> list[ValidationTestResult]:
        status = "PASSED" if validation.status == "PASSED" else "FAILED"
        return [ValidationTestResult(test_id=item.test_id, test_name=item.title, category="SYNTHETIC_DATA", status=status, severity=item.severity, expected_result=item.expected_behavior, actual_result="Generated and schema-validated." if status == "PASSED" else "Deterministic validation failed.", details=item.description, evidence_payload={"scenario_category": item.category, "source_expression": item.source_expression, "input_rows": [row.as_dict() for row in item.input_rows]}) for item in generated.scenarios]

    @staticmethod
    def _static_tests(result: StaticValidationResult) -> list[ValidationTestResult]:
        ids = {"REQUIRED_ARTIFACTS":"STATIC-001","PYSPARK_CODE_SYNTAX":"STATIC-002","UNIT_TEST_SYNTAX":"STATIC-003","CONFIGURATION_JSON":"STATIC-004","DATABRICKS_RUNTIME_SAFETY":"STATIC-005","NO_EMBEDDED_SECRETS":"STATIC-006","SAFE_TEST_IMPORTS":"STATIC-007"}
        return [ValidationTestResult(test_id=ids[item.check_name], test_name=item.check_name.replace("_", " ").title(), category="STATIC_VALIDATION", status="PASSED" if item.passed else "FAILED", severity=item.severity, expected_result="Static check passes.", actual_result="PASSED" if item.passed else "FAILED", details=item.details, evidence_payload={"check_name": item.check_name}) for item in result.checks if item.check_name in ids]

    @staticmethod
    def _unit_tests(result: Any) -> list[ValidationTestResult]:
        return [ValidationTestResult(test_id=item.test_id, test_name=item.title, category=item.category, status=item.status, severity="HIGH", expected_result="Generated pytest passes.", actual_result=item.status, details=item.details, evidence_payload=item.evidence, duration_seconds=item.duration_seconds) for item in result.test_cases]

    @staticmethod
    def _coverage_tests(result: StaticCoverageResult, minimum: float) -> list[ValidationTestResult]:
        values = [("PARITY-001","Source coverage",result.source_coverage,1.0),("PARITY-002","Target coverage",result.target_coverage,1.0),("PARITY-003","Target-field coverage",result.target_field_coverage,1.0),("PARITY-004","Business-rule coverage",result.business_rule_coverage,1.0),("PARITY-005","Lineage coverage",result.lineage_coverage,1.0),("PARITY-006","Precision and scale coverage",result.precision_scale_coverage,1.0),("PARITY-007","Overall static implementation coverage",result.overall_functional_parity,minimum)]
        return [ValidationTestResult(test_id=test_id, test_name=name, category="FUNCTIONAL_PARITY", status="PASSED" if value >= threshold else "WARNING", severity="MEDIUM", expected_result=f"Diagnostic coverage >= {threshold:.2f}.", actual_result=f"{value:.4f}", details="Diagnostic only. MatchFlow is authoritative.", evidence_payload={"coverage": value, "threshold": threshold, "authoritative": False}) for test_id,name,value,threshold in values]

    @staticmethod
    def _data_tests(synthetic: SyntheticDataValidationResult, legacy: Any, target: TargetExecutionResult, match: MatchFlowResult | None, confidence: float, minimum: float) -> list[ValidationTestResult]:
        threshold = float(getattr(settings, "validation_confidence_threshold", 0.90))
        failed_synthetic_checks = [
            check.model_dump(mode="json")
            for check in synthetic.checks
            if not check.passed
        ]
        synthetic_details = (
            "Synthetic data is valid."
            if synthetic.status == "PASSED"
            else "; ".join(
                check["details"] for check in failed_synthetic_checks
            )
        )
        rows = [
            ("DATA-001", "Synthetic data validation", synthetic.status == "PASSED", synthetic.status, synthetic_details),
            ("DATA-002", "Reference execution", legacy.status == "PASSED", legacy.status, legacy.error_message or f"Rows: {legacy.row_count}"),
            ("DATA-003", "Target execution", target.status == "PASSED", target.status, target.error_message or f"Rows: {target.row_count}"),
        ]
        if match:
            rows += [("DATA-004","Rows compared",match.rows_compared > 0,str(match.rows_compared),"At least one row compared."),("DATA-005","Missing rows",match.missing_rows == 0,str(match.missing_rows),"No missing rows."),("DATA-006","Additional rows",match.additional_rows == 0,str(match.additional_rows),"No additional rows."),("DATA-007","Different rows",match.different_rows == 0,str(match.different_rows),"No different rows."),("DATA-008","MatchFlow functional parity",match.match_percentage >= minimum * 100,f"{match.match_percentage:.2f}%",f"Parity >= {minimum*100:.2f}%"),("DATA-009","Confidence index",confidence >= threshold,f"{confidence:.4f}",f"Confidence >= {threshold:.2f}")]
        evidence = {
            "synthetic_status": synthetic.status,
            "synthetic_checks": [
                check.model_dump(mode="json") for check in synthetic.checks
            ],
            "synthetic_failed_checks": failed_synthetic_checks,
            "reference_status": legacy.status,
            "reference_error": legacy.error_message,
            "reference_execution_mode": str(legacy.reference_execution_mode),
            "oracle_strength": str(legacy.oracle_strength),
            "full_reference_coverage": legacy.full_reference_coverage,
            "human_review_required": legacy.human_review_required,
            "unsupported_constructs": legacy.unsupported_constructs,
            "target_status": target.status,
            "target_error": target.error_message,
            "confidence_index": confidence,
            "matchflow": match.model_dump(mode="json") if match else None,
        }
        return [ValidationTestResult(test_id=test_id, test_name=name, category="DATA_PARITY", status="PASSED" if passed else "FAILED", severity="CRITICAL" if test_id in {"DATA-003","DATA-008","DATA-009"} else "HIGH", expected_result=details, actual_result=actual, details=details, evidence_payload=evidence) for test_id,name,passed,actual,details in rows]

    @staticmethod
    def _failure(static: StaticValidationResult, unit: Any, synthetic: SyntheticDataValidationResult, legacy: Any, target: TargetExecutionResult, match: MatchFlowResult | None, confidence: float, confidence_threshold: float, minimum: float) -> tuple[str, str]:
        if static.status != "PASSED": return "Static validation failed.", "Regenerate artifacts and resolve static safety failures."
        if unit.status != "PASSED": return "Generated unit tests failed.", "Review pytest evidence and regenerate the conversion."
        if synthetic.status != "PASSED": return "Synthetic data validation failed.", "Regenerate canonical-schema-compliant synthetic data."
        if legacy.status != "PASSED":
            return (
                legacy.error_message or "Reference execution failed.",
                "Review the reference plan, unsupported constructs, and deterministic validation evidence.",
            )
        if legacy.oracle_strength != OracleStrength.STRONG:
            return (
                "Reference execution is provisional and cannot authorize deployment.",
                "Obtain a fully covered relational reference plan or a live legacy execution.",
            )
        if legacy.human_review_required or not legacy.full_reference_coverage:
            return (
                "Reference coverage is incomplete.",
                "Resolve assumptions or unsupported constructs before deployment.",
            )
        if target.status != "PASSED": return target.error_message or "Target execution failed.", "Review target logs and regenerate pure transform code."
        if match is None: return "MatchFlow was not executed.", "Complete both executions before comparison."
        if match.match_percentage < minimum * 100: return f"Parity {match.match_percentage:.2f}% is below {minimum*100:.2f}%.", "Review MatchFlow differences."
        if confidence < confidence_threshold: return f"Confidence {confidence:.4f} is below {confidence_threshold:.2f}.", "Increase scenario/rule coverage or resolve assumptions."
        return "Authoritative validation failed.", "Review DATA_PARITY evidence."
