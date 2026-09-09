"""Deterministic CSV/JSON data comparator for MatchFlow."""

import csv
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
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
    AGENT_VERSION = "1.0.0"
    STAGE_NAME = "DATA_COMPARISON"
    MODEL_NAME = "DETERMINISTIC_COMPARATOR"

    def compare(self, legacy_path: Path, pyspark_path: Path, keys: list[str], tolerance: Decimal = Decimal("0")) -> MatchFlowResult:
        legacy = self._load(legacy_path)
        pyspark = self._load(pyspark_path)
        left = {tuple(str(row.get(key, "")) for key in keys): row for row in legacy}
        right = {tuple(str(row.get(key, "")) for key in keys): row for row in pyspark}
        shared = sorted(set(left) & set(right))
        differences = []
        matched = 0
        for key in shared:
            columns = sorted(set(left[key]) | set(right[key]))
            row_ok = True
            for column in columns:
                if column in keys:
                    continue
                a, b = left[key].get(column), right[key].get(column)
                if not self._equal(a, b, tolerance):
                    row_ok = False
                    if len(differences) < 100:
                        differences.append(MatchFlowDifference(comparison_key=dict(zip(keys, key)), column=column, legacy_value=None if a is None else str(a), pyspark_value=None if b is None else str(b), status="DIFFERENT"))
            matched += row_ok
        missing = len(set(left) - set(right))
        additional = len(set(right) - set(left))
        different = len(shared) - matched
        denominator = max(len(set(left) | set(right)), 1)
        return MatchFlowResult(rows_compared=len(shared), matched_rows=matched, different_rows=different, missing_rows=missing, additional_rows=additional, match_percentage=round(matched / denominator * 100, 4), sample_differences=differences)

    @staticmethod
    def _load(path: Path):
        if path.suffix.lower() == ".csv":
            with path.open(newline="", encoding="utf-8-sig") as handle:
                return list(csv.DictReader(handle))
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else data.get("rows", [])

    @staticmethod
    def _equal(a, b, tolerance):
        if a == b:
            return True
        try:
            return abs(Decimal(str(a)) - Decimal(str(b))) <= tolerance
        except (InvalidOperation, TypeError):
            return str(a) == str(b)
