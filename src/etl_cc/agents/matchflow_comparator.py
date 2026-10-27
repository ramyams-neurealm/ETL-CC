"""Deterministic CSV/JSON data comparator for MatchFlow."""

import csv
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class MatchFlowDifference(BaseModel):
    comparison_key: dict[str, str]
    column: str
    legacy_value: str | None
    pyspark_value: str | None
    status: str


class MatchFlowResult(BaseModel):
    rows_compared: int
    matched_rows: int
    different_rows: int
    missing_rows: int
    additional_rows: int
    match_percentage: float
    sample_differences: list[MatchFlowDifference] = Field(default_factory=list)


class MatchFlowComparator:
    AGENT_NAME = "MATCHFLOW_COMPARATOR"
    AGENT_VERSION = "1.2.0"
    STAGE_NAME = "DATA_COMPARISON"
    MODEL_NAME = "DETERMINISTIC_COMPARATOR"

    def compare(
        self,
        legacy_path: Path,
        pyspark_path: Path,
        keys: list[str],
        tolerance: Decimal = Decimal("0"),
        ignored_columns: set[str] | None = None,
    ) -> MatchFlowResult:
        """Compare datasets loaded from CSV or JSON files.

        This method is retained for backward compatibility with existing API
        callers. The comparison itself is delegated to compare_rows().
        """
        return self.compare_rows(
            legacy_rows=self._load(legacy_path),
            pyspark_rows=self._load(pyspark_path),
            keys=keys,
            tolerance=tolerance,
            ignored_columns=ignored_columns,
        )

    def compare_rows(
        self,
        legacy_rows: list[dict[str, Any]],
        pyspark_rows: list[dict[str, Any]],
        keys: list[str],
        tolerance: Decimal = Decimal("0"),
        ignored_columns: set[str] | None = None,
    ) -> MatchFlowResult:
        """Compare two datasets supplied as in-memory row dictionaries."""
        if not keys:
            raise ValueError("At least one comparison key is required.")

        ignored = {
            str(column).strip().casefold()
            for column in (ignored_columns or set())
            if str(column).strip()
        }
        key_names = {str(key).strip().casefold() for key in keys}
        overlapping_keys = sorted(ignored & key_names)
        if overlapping_keys:
            raise ValueError(
                "Comparison key columns cannot be ignored: "
                + ", ".join(overlapping_keys)
            )

        self._validate_rows(legacy_rows, "legacy")
        self._validate_rows(pyspark_rows, "pyspark")
        self._validate_comparison_keys(legacy_rows, keys, "legacy")
        self._validate_comparison_keys(pyspark_rows, keys, "pyspark")

        left = self._index_rows(legacy_rows, keys, "legacy")
        right = self._index_rows(pyspark_rows, keys, "pyspark")

        left_keys = set(left)
        right_keys = set(right)
        shared_keys = sorted(left_keys & right_keys)

        differences: list[MatchFlowDifference] = []
        matched_rows = 0

        for comparison_key in shared_keys:
            legacy_row = left[comparison_key]
            pyspark_row = right[comparison_key]
            columns = sorted(set(legacy_row) | set(pyspark_row))
            row_matches = True

            for column in columns:
                if column in keys:
                    continue
                if str(column).strip().casefold() in ignored:
                    continue

                legacy_value = legacy_row.get(column)
                pyspark_value = pyspark_row.get(column)

                if not self._equal(
                    legacy_value,
                    pyspark_value,
                    tolerance,
                ):
                    row_matches = False
                    if len(differences) < 100:
                        differences.append(
                            MatchFlowDifference(
                                comparison_key=dict(
                                    zip(keys, comparison_key)
                                ),
                                column=column,
                                legacy_value=(
                                    None
                                    if legacy_value is None
                                    else str(legacy_value)
                                ),
                                pyspark_value=(
                                    None
                                    if pyspark_value is None
                                    else str(pyspark_value)
                                ),
                                status="DIFFERENT",
                            )
                        )

            if row_matches:
                matched_rows += 1

        missing_rows = len(left_keys - right_keys)
        additional_rows = len(right_keys - left_keys)
        different_rows = len(shared_keys) - matched_rows
        denominator = max(len(left_keys | right_keys), 1)

        return MatchFlowResult(
            rows_compared=len(shared_keys),
            matched_rows=matched_rows,
            different_rows=different_rows,
            missing_rows=missing_rows,
            additional_rows=additional_rows,
            match_percentage=round(
                matched_rows / denominator * 100,
                4,
            ),
            sample_differences=differences,
        )

    @staticmethod
    def _validate_rows(
        rows: list[dict[str, Any]],
        dataset_name: str,
    ) -> None:
        if not isinstance(rows, list):
            raise ValueError(
                f"The {dataset_name} dataset must be a list of rows."
            )
        if any(not isinstance(row, dict) for row in rows):
            raise ValueError(
                f"Every row in the {dataset_name} dataset must be an object."
            )

    @staticmethod
    def _validate_comparison_keys(
        rows: list[dict[str, Any]],
        keys: list[str],
        dataset_name: str,
    ) -> None:
        missing_keys = sorted(
            {
                key
                for row in rows
                for key in keys
                if key not in row
            }
        )
        if missing_keys:
            raise ValueError(
                f"Comparison key(s) missing from {dataset_name} dataset: "
                + ", ".join(missing_keys)
            )

    @staticmethod
    def _index_rows(
        rows: list[dict[str, Any]],
        keys: list[str],
        dataset_name: str,
    ) -> dict[tuple[str, ...], dict[str, Any]]:
        indexed_rows: dict[tuple[str, ...], dict[str, Any]] = {}

        for row in rows:
            comparison_key = tuple(str(row[key]) for key in keys)
            if comparison_key in indexed_rows:
                key_values = dict(zip(keys, comparison_key))
                raise ValueError(
                    f"Duplicate comparison key in {dataset_name} dataset: "
                    f"{key_values}"
                )
            indexed_rows[comparison_key] = row

        return indexed_rows

    @staticmethod
    def _load(path: Path) -> list[dict[str, Any]]:
        """Load CSV or JSON rows for backward-compatible file comparison."""
        path = Path(path)

        if path.suffix.lower() == ".csv":
            with path.open(
                newline="",
                encoding="utf-8-sig",
            ) as handle:
                return list(csv.DictReader(handle))

        data = json.loads(path.read_text(encoding="utf-8"))
        rows = data if isinstance(data, list) else data.get("rows", [])

        if not isinstance(rows, list):
            raise ValueError(
                "Comparison JSON must be a list or contain a 'rows' list."
            )

        return rows

    @staticmethod
    def _equal(
        legacy_value: Any,
        pyspark_value: Any,
        tolerance: Decimal,
    ) -> bool:
        if legacy_value == pyspark_value:
            return True

        try:
            return (
                abs(
                    Decimal(str(legacy_value))
                    - Decimal(str(pyspark_value))
                )
                <= tolerance
            )
        except (InvalidOperation, TypeError, ValueError):
            return str(legacy_value) == str(pyspark_value)
