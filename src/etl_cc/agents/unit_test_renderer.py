"""Deterministic Pytest renderer for structured ETL unit-test specifications."""
from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from etl_cc.agents.unit_test_specification import (
    UnitTestSpecification,
    canonical_temporal_family,
)
from etl_cc.models import CanonicalField, CanonicalMapping


class UnitTestRenderer:
    VERSION = "1.1.0"

    @classmethod
    def render(
        cls,
        specification: UnitTestSpecification,
        mapping: CanonicalMapping,
        pyspark_file_name: str,
    ) -> str:
        module_name = re.sub(r"\.py$", "", Path(pyspark_file_name).name)
        source_map = {dataset.name: dataset for dataset in mapping.sources}
        blocks = [
            '"""Deterministically rendered regression tests."""',
            "from datetime import date, datetime",
            "from decimal import Decimal",
            "",
            "import pytest",
            "from pyspark.sql import SparkSession",
            "from pyspark.sql import functions as F",
            "from pyspark.sql.types import (BooleanType, DateType, DecimalType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType)",
            f"from {module_name} import transform",
            "",
            "@pytest.fixture(scope=\"session\")",
            "def spark():",
            "    session = (SparkSession.builder.master(\"local[1]\").appName(\"neuflow-unit-test\").getOrCreate())",
            "    yield session",
            "    session.stop()",
        ]
        for index, scenario in enumerate(specification.scenarios, start=1):
            function_name = cls._safe_identifier(
                f"test_{index:03d}_{scenario.test_id}_{scenario.title}"
            )
            blocks.extend(["", f"def {function_name}(spark):"])
            blocks.append("    inputs = {}")
            for input_dataset in scenario.input_datasets:
                dataset_name = input_dataset.dataset_name
                rows = [row.as_dict() for row in input_dataset.rows]
                dataset = source_map[dataset_name]
                fields = dataset.fields
                schema = cls._schema_expression(fields)
                ordered_rows = [
                    tuple(cls._coerce(row.get(field.name), field) for field in fields)
                    for row in rows
                ]
                blocks.append(f"    schema_{cls._safe_identifier(dataset_name)} = {schema}")
                blocks.append(
                    f"    rows_{cls._safe_identifier(dataset_name)} = {cls._python_literal(ordered_rows)}"
                )
                blocks.append(
                    f"    inputs[{dataset_name!r}] = spark.createDataFrame(rows_{cls._safe_identifier(dataset_name)}, schema_{cls._safe_identifier(dataset_name)})"
                )
            blocks.append("    actual = transform(inputs)")
            columns = scenario.deterministic_columns
            blocks.append(f"    deterministic_columns = {columns!r}")
            blocks.append(
                f"    actual_rows = [tuple(row[c] for c in deterministic_columns) for row in actual.select(*deterministic_columns).orderBy(*{scenario.comparison_keys!r}).collect()]"
            )
            expected = [
                tuple(row_dict[column] for column in columns)
                for row_dict in (row.as_dict() for row in scenario.expected_rows)
            ]
            key_indexes = [columns.index(key) for key in scenario.comparison_keys]
            expected.sort(
                key=lambda row: tuple(str(row[index]) for index in key_indexes)
            )
            blocks.append(f"    expected_rows = {cls._python_literal(expected)}")
            blocks.append("    assert actual_rows == expected_rows")
            for runtime in scenario.runtime_columns:
                type_name = "TimestampType" if runtime.expected_type == "timestamp" else "DateType"
                blocks.append(f"    assert {runtime.column_name!r} in actual.columns")
                blocks.append(
                    f"    assert isinstance(actual.schema[{runtime.column_name!r}].dataType, {type_name})"
                )
                if not runtime.nullable:
                    blocks.append(
                        f"    assert actual.filter(F.col({runtime.column_name!r}).isNull()).count() == 0"
                    )
        return "\n".join(blocks) + "\n"

    @staticmethod
    def _safe_identifier(value: str) -> str:
        value = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_").lower()
        if not value or value[0].isdigit():
            value = f"generated_{value}"
        return value

    @classmethod
    def _schema_expression(cls, fields: list[CanonicalField]) -> str:
        rendered = ", ".join(
            f"StructField({field.name!r}, {cls._spark_type(field)}, {bool(field.nullable)!r})"
            for field in fields
        )
        return f"StructType([{rendered}])"

    @staticmethod
    def _spark_type(field: CanonicalField) -> str:
        dtype = str(field.data_type or "").lower()
        temporal_family = canonical_temporal_family(field.data_type)
        if temporal_family == "timestamp":
            return "TimestampType()"
        if temporal_family == "date":
            return "DateType()"
        if "decimal" in dtype or "numeric" in dtype or "number" in dtype:
            return f"DecimalType({field.precision or 38}, {field.scale or 0})"
        if any(token in dtype for token in ("bigint", "long", "int64")):
            return "LongType()"
        if any(token in dtype for token in ("int", "smallint", "tinyint", "integer")):
            return "IntegerType()"
        if any(token in dtype for token in ("double", "float", "real")):
            return "DoubleType()"
        if any(token in dtype for token in ("bool", "boolean")):
            return "BooleanType()"
        return "StringType()"

    @staticmethod
    def _coerce(value: Any, field: CanonicalField) -> Any:
        if value is None:
            return None
        dtype = str(field.data_type or "").lower()
        temporal_family = canonical_temporal_family(field.data_type)
        if temporal_family == "timestamp":
            return value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        if temporal_family == "date":
            return value if isinstance(value, date) else date.fromisoformat(str(value))
        if "decimal" in dtype or "numeric" in dtype or "number" in dtype:
            return Decimal(str(value))
        if any(token in dtype for token in ("bigint", "long", "int", "smallint", "tinyint", "integer")):
            return int(value)
        if any(token in dtype for token in ("double", "float", "real")):
            return float(value)
        if any(token in dtype for token in ("bool", "boolean")):
            if isinstance(value, bool):
                return value
            return str(value).strip().lower() in {"true", "1", "yes", "y"}
        return str(value)

    @staticmethod
    def _python_literal(value: Any) -> str:
        if isinstance(value, Decimal):
            return f"Decimal({str(value)!r})"
        if isinstance(value, datetime):
            return f"datetime.fromisoformat({value.isoformat()!r})"
        if isinstance(value, date):
            return f"date.fromisoformat({value.isoformat()!r})"
        if isinstance(value, tuple):
            body = ", ".join(UnitTestRenderer._python_literal(item) for item in value)
            return f"({body}{',' if len(value) == 1 else ''})"
        if isinstance(value, list):
            return "[" + ", ".join(UnitTestRenderer._python_literal(item) for item in value) + "]"
        if isinstance(value, dict):
            return "{" + ", ".join(f"{k!r}: {UnitTestRenderer._python_literal(v)}" for k, v in value.items()) + "}"
        return repr(value)
