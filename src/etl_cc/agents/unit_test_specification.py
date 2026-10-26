"""Strict structured unit-test specifications for generic ETL validation."""
from __future__ import annotations

import re
from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from etl_cc.models import CanonicalMapping


def canonical_temporal_family(data_type: str | None) -> Literal["timestamp", "date"] | None:
    """Map canonical/vendor temporal aliases to a stable semantic family.

    Informatica metadata commonly represents a timestamp as ``date/time``.
    This function normalizes separators and recognizes temporal aliases without
    depending on a mapping name or output column name.
    """
    raw = str(data_type or "").strip().casefold()
    normalized = re.sub(r"[^a-z0-9]+", " ", raw).strip()
    compact = normalized.replace(" ", "")

    timestamp_aliases = {
        "datetime",
        "datetime2",
        "dateandtime",
        "timestamp",
        "timestamptz",
        "timestampwithtimezone",
        "timestampwithouttimezone",
        "smalldatetime",
    }
    date_aliases = {"date", "localdate"}

    if compact in timestamp_aliases:
        return "timestamp"
    if compact in date_aliases:
        return "date"
    if "timestamp" in compact or "datetime" in compact:
        return "timestamp"
    if normalized in {"date time", "date and time"}:
        return "timestamp"
    return None


class TestCell(BaseModel):
    """One field/value pair using an OpenAI-compatible closed object schema."""

    model_config = ConfigDict(extra="forbid")
    field_name: str
    value: str | int | float | bool | None = None


class TestRow(BaseModel):
    """One row represented without arbitrary JSON object properties."""

    model_config = ConfigDict(extra="forbid")
    cells: list[TestCell] = Field(min_length=1)

    def as_dict(self) -> dict[str, str | int | float | bool | None]:
        return {cell.field_name: cell.value for cell in self.cells}


class InputDatasetSpecification(BaseModel):
    """Rows for one exact canonical source dataset."""

    model_config = ConfigDict(extra="forbid")
    dataset_name: str
    rows: list[TestRow] = Field(min_length=1)


class RuntimeColumnExpectation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    column_name: str
    expected_type: Literal["timestamp", "date"]
    nullable: bool = False


class ParameterValue(BaseModel):
    model_config = ConfigDict(extra="forbid")
    parameter_name: str
    value: str | int | float | bool | None = None


class UnitTestScenario(BaseModel):
    model_config = ConfigDict(extra="forbid")
    test_id: str
    title: str
    category: Literal[
        "HAPPY_PATH",
        "NEGATIVE",
        "NULL_HANDLING",
        "BOUNDARY",
        "DUPLICATE_KEY",
        "BUSINESS_RULE",
    ]
    input_datasets: list[InputDatasetSpecification] = Field(min_length=1)
    expected_rows: list[TestRow]
    comparison_keys: list[str] = Field(min_length=1)
    deterministic_columns: list[str] = Field(min_length=1)
    parameter_values: list[ParameterValue] = Field(default_factory=list)
    runtime_columns: list[RuntimeColumnExpectation] = Field(default_factory=list)


class UnitTestSpecification(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mapping_name: str
    scenarios: list[UnitTestScenario] = Field(min_length=1, max_length=12)


class UnitTestSpecificationValidator:
    """Validate LLM test semantics against canonical mapping metadata."""

    VERSION = "1.5.0"
    _CLOCK = re.compile(
        r"(?<![A-Za-z0-9_])(?:SYSDATE|SYSTIMESTAMP|CURRENT_TIMESTAMP|"
        r"CURRENT_DATE|LOCALTIMESTAMP|NOW(?:\s*\()?|GETDATE(?:\s*\()?)"
        r"(?![A-Za-z0-9_])",
        re.IGNORECASE,
    )

    @classmethod
    def validate(
        cls,
        specification: UnitTestSpecification,
        mapping: CanonicalMapping,
    ) -> list[str]:
        errors: list[str] = []
        if specification.mapping_name != mapping.mapping_name:
            errors.append(
                f"mapping_name must be {mapping.mapping_name!r}, received "
                f"{specification.mapping_name!r}."
            )

        sources = {dataset.name: dataset for dataset in mapping.sources}
        target_fields = {
            field.name: field
            for target in mapping.targets
            for field in target.fields
        }
        parameters = {
            parameter.name.casefold(): parameter
            for parameter in mapping.parameters
        }
        referenced_parameters = {
            name.casefold()
            for transformation in mapping.transformations
            for port in transformation.ports
            for name in re.findall(
                r"\$([A-Za-z_][A-Za-z0-9_]*)",
                str(port.expression or ""),
            )
        }
        clock_default_parameters = {
            parameter.name
            for parameter in mapping.parameters
            if (
                parameter.name.casefold() in referenced_parameters
                and cls._CLOCK.search(str(parameter.default_value or ""))
            )
        }
        runtime_fields = cls.runtime_fields(mapping)
        seen_ids: set[str] = set()

        for scenario in specification.scenarios:
            prefix = f"Scenario {scenario.test_id}:"
            if scenario.test_id in seen_ids:
                errors.append(f"{prefix} duplicate test_id.")
            seen_ids.add(scenario.test_id)
            parameter_names = [
                item.parameter_name for item in scenario.parameter_values
            ]
            duplicate_parameters = sorted(
                name
                for name in set(parameter_names)
                if parameter_names.count(name) > 1
            )
            unknown_parameters = sorted(
                name
                for name in parameter_names
                if name.casefold() not in parameters
            )
            supplied_parameters = {name.casefold() for name in parameter_names}
            missing_clock_parameters = sorted(
                name
                for name in clock_default_parameters
                if name.casefold() not in supplied_parameters
            )
            if duplicate_parameters:
                errors.append(
                    f"{prefix} duplicate parameter value(s): "
                    f"{', '.join(duplicate_parameters)}."
                )
            if unknown_parameters:
                errors.append(
                    f"{prefix} unknown parameter value(s): "
                    f"{', '.join(unknown_parameters)}."
                )
            if missing_clock_parameters:
                errors.append(
                    f"{prefix} clock-default parameter(s) need fixed test values: "
                    f"{', '.join(missing_clock_parameters)}."
                )
            for parameter_value in scenario.parameter_values:
                parameter = parameters.get(parameter_value.parameter_name.casefold())
                if parameter is None:
                    continue
                temporal_family = canonical_temporal_family(
                    parameter.data_type or parameter.parameter_type
                )
                if temporal_family == "date":
                    try:
                        date.fromisoformat(str(parameter_value.value))
                    except (TypeError, ValueError):
                        errors.append(
                            f"{prefix} parameter {parameter.name} must use an "
                            "ISO date value."
                        )
                elif temporal_family == "timestamp":
                    try:
                        datetime.fromisoformat(str(parameter_value.value))
                    except (TypeError, ValueError):
                        errors.append(
                            f"{prefix} parameter {parameter.name} must use an "
                            "ISO timestamp value."
                        )

            dataset_names = [item.dataset_name for item in scenario.input_datasets]
            duplicates = sorted(
                name for name in set(dataset_names) if dataset_names.count(name) > 1
            )
            if duplicates:
                errors.append(
                    f"{prefix} duplicate source dataset(s): {', '.join(duplicates)}."
                )
            unknown_datasets = sorted(set(dataset_names) - set(sources))
            missing_datasets = sorted(set(sources) - set(dataset_names))
            if unknown_datasets:
                errors.append(
                    f"{prefix} unknown source dataset(s): {', '.join(unknown_datasets)}."
                )
            if missing_datasets:
                errors.append(
                    f"{prefix} missing source dataset(s): {', '.join(missing_datasets)}."
                )

            for input_dataset in scenario.input_datasets:
                dataset = sources.get(input_dataset.dataset_name)
                if dataset is None:
                    continue
                valid_fields = {field.name for field in dataset.fields}
                required_fields = {
                    field.name for field in dataset.fields if not field.nullable
                }
                for index, test_row in enumerate(input_dataset.rows, start=1):
                    row = test_row.as_dict()
                    cell_names = [cell.field_name for cell in test_row.cells]
                    duplicate_fields = sorted(
                        name for name in set(cell_names) if cell_names.count(name) > 1
                    )
                    if duplicate_fields:
                        errors.append(
                            f"{prefix} input {input_dataset.dataset_name} row {index} "
                            f"has duplicate field(s): {', '.join(duplicate_fields)}."
                        )
                    unknown = sorted(set(row) - valid_fields)
                    missing_required = sorted(required_fields - set(row))
                    null_required = sorted(
                        field for field in required_fields
                        if field in row and row[field] is None
                    )
                    if unknown:
                        errors.append(
                            f"{prefix} input {input_dataset.dataset_name} row {index} "
                            f"has unknown field(s): {', '.join(unknown)}."
                        )
                    if missing_required:
                        errors.append(
                            f"{prefix} input {input_dataset.dataset_name} row {index} "
                            f"is missing required field(s): {', '.join(missing_required)}."
                        )
                    if null_required:
                        errors.append(
                            f"{prefix} input {input_dataset.dataset_name} row {index} "
                            f"has null for non-nullable field(s): {', '.join(null_required)}."
                        )

            expected_dicts = [row.as_dict() for row in scenario.expected_rows]
            expected_fields = {key for row in expected_dicts for key in row}
            unknown_expected = sorted(expected_fields - set(target_fields))
            if unknown_expected:
                errors.append(
                    f"{prefix} expected rows contain unknown target field(s): "
                    f"{', '.join(unknown_expected)}."
                )

            deterministic = set(scenario.deterministic_columns)
            unknown_deterministic = sorted(deterministic - set(target_fields))
            prohibited = sorted(deterministic & runtime_fields)
            if unknown_deterministic:
                errors.append(
                    f"{prefix} unknown deterministic column(s): "
                    f"{', '.join(unknown_deterministic)}."
                )
            if prohibited:
                errors.append(
                    f"{prefix} runtime-generated column(s) cannot use exact-value "
                    f"comparison: {', '.join(prohibited)}."
                )
            missing_expected = sorted(
                column for column in deterministic
                if any(column not in row for row in expected_dicts)
            )
            if missing_expected:
                errors.append(
                    f"{prefix} every expected row must contain deterministic "
                    f"column(s): {', '.join(missing_expected)}."
                )

            keys = set(scenario.comparison_keys)
            missing_keys = sorted(keys - deterministic)
            if missing_keys:
                errors.append(
                    f"{prefix} comparison keys must be deterministic columns: "
                    f"{', '.join(missing_keys)}."
                )

            declared_runtime = {item.column_name for item in scenario.runtime_columns}
            unknown_runtime = sorted(declared_runtime - runtime_fields)
            if unknown_runtime:
                errors.append(
                    f"{prefix} column(s) are not grounded in a nondeterministic "
                    f"clock expression: {', '.join(unknown_runtime)}."
                )
            for expectation in scenario.runtime_columns:
                field = target_fields.get(expectation.column_name)
                if field is None:
                    continue
                expected_type = canonical_temporal_family(field.data_type)
                if expected_type is None:
                    errors.append(
                        f"{prefix} runtime column {expectation.column_name} has "
                        f"non-date type {field.data_type!r}."
                    )
                elif expectation.expected_type != expected_type:
                    errors.append(
                        f"{prefix} runtime column {expectation.column_name} must "
                        f"declare type {expected_type!r}."
                    )
            undeclared_runtime = sorted(runtime_fields - declared_runtime)
            if undeclared_runtime:
                errors.append(
                    f"{prefix} missing runtime expectation(s): "
                    f"{', '.join(undeclared_runtime)}."
                )
        return errors

    @classmethod
    def runtime_fields(cls, mapping: CanonicalMapping) -> set[str]:
        """Resolve target fields backed by clock expressions in the mapping.

        Runtime expressions frequently pass through Filter, Router, Joiner, or
        other transformations before reaching a target. Classification must
        therefore follow connector lineage backwards instead of inspecting only
        the final port or matching fields globally by name. A parameter default
        is deliberately not treated as a clock expression: tests can supply a
        fixed parameter value and assert its exact output.
        """
        transformations = {
            transformation.name: transformation
            for transformation in mapping.transformations
        }
        ports = {
            (transformation.name, port.name): port
            for transformation in mapping.transformations
            for port in transformation.ports
        }
        incoming: dict[
            tuple[str, str],
            list[tuple[str, str]],
        ] = {}
        for connector in mapping.connectors:
            if not connector.to_field or not connector.from_field:
                continue
            incoming.setdefault(
                (connector.to_instance, connector.to_field),
                [],
            ).append(
                (connector.from_instance, connector.from_field)
            )

        def is_runtime_origin(
            instance_name: str,
            field_name: str,
            visited: set[tuple[str, str]],
        ) -> bool:
            node = (instance_name, field_name)
            if node in visited:
                return False
            visited = {*visited, node}

            port = ports.get(node)
            if port is not None:
                expression = str(port.expression or "")
                if cls._CLOCK.search(expression):
                    return True

            # A pass-through transformation port has no expression. Continue
            # through every incoming connector for that exact instance/field.
            for upstream_instance, upstream_field in incoming.get(node, []):
                if is_runtime_origin(
                    upstream_instance,
                    upstream_field,
                    visited,
                ):
                    return True
            return False

        runtime_targets: set[str] = set()
        for target in mapping.targets:
            for field in target.fields:
                if is_runtime_origin(target.name, field.name, set()):
                    runtime_targets.add(field.name)

        instance_edges: dict[str, set[str]] = {}
        for connector in mapping.connectors:
            instance_edges.setdefault(connector.from_instance, set()).add(
                connector.to_instance
            )

        def reaches_target(start: str, target: str) -> bool:
            pending = [start]
            visited: set[str] = set()
            while pending:
                instance = pending.pop()
                if instance == target:
                    return True
                if instance in visited:
                    continue
                visited.add(instance)
                pending.extend(instance_edges.get(instance, set()) - visited)
            return False

        output_directions = {
            "OUTPUT",
            "OUT",
            "INOUT",
            "INPUT/OUTPUT",
            "OUTPUT/INPUT",
        }
        for target in mapping.targets:
            for field in target.fields:
                target_node = (target.name, field.name)
                if field.name in runtime_targets or incoming.get(target_node):
                    continue
                for transformation in transformations.values():
                    if mapping.connectors and not reaches_target(
                        transformation.name,
                        target.name,
                    ):
                        continue
                    for port in transformation.ports:
                        if port.name != field.name or str(
                            port.direction or ""
                        ).upper() not in output_directions:
                            continue
                        expression = str(port.expression or "")
                        if cls._CLOCK.search(expression):
                            runtime_targets.add(field.name)
                            break

        return runtime_targets
