"""Deterministic, explainable ETL mapping complexity analysis."""

from collections import Counter
import re
from typing import Any, Literal

from pydantic import BaseModel, Field

from etl_cc.models import CanonicalMapping

ComplexityLevel = Literal["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]
EvidenceStatus = Literal["OBSERVED", "INFERRED", "UNAVAILABLE"]

HIGH_RISK_TYPES = {
    "java transformation",
    "custom transformation",
    "external procedure",
    "stored procedure",
    "sql transformation",
    "update strategy",
}
MEDIUM_RISK_TYPES = {
    "lookup",
    "lookup procedure",
    "joiner",
    "aggregator",
    "sorter",
    "rank",
    "router",
    "transaction control",
}
UNSUPPORTED_TYPES = {
    "java transformation",
    "custom transformation",
    "external procedure",
}


class FactorEvidence(BaseModel):
    value: int | bool | None
    status: EvidenceStatus
    evidence: list[str] = Field(default_factory=list)


class ComplexityBreakdown(BaseModel):
    version: str = "complexity-v1"
    score: int = Field(default=0, ge=0, le=100)
    level: ComplexityLevel = "LOW"
    factors: dict[str, FactorEvidence] = Field(default_factory=dict)
    contributions: dict[str, int] = Field(default_factory=dict)
    risk_factors: list[dict[str, Any]] = Field(default_factory=list)
    reasons: list[str] = Field(default_factory=list)
    reason: str = ""
    evidence_coverage: float = Field(default=0.0, ge=0.0, le=1.0)


def derive_deterministic_risks(mapping: CanonicalMapping) -> list[dict[str, Any]]:
    """Return the discovery agent's existing deterministic risk records."""
    risks: list[dict[str, Any]] = []
    for transformation in mapping.transformations:
        normalized = transformation.transformation_type.lower()
        if normalized in HIGH_RISK_TYPES:
            risks.append(
                {
                    "risk_type": "COMPLEX_TRANSFORMATION",
                    "severity": "HIGH",
                    "source_object": transformation.name,
                    "description": (
                        f"{transformation.transformation_type} requires detailed "
                        "behavioral review during PySpark conversion."
                    ),
                    "recommendation": (
                        "Review transformation properties, expressions, error handling, "
                        "state behavior, and target-side effects before conversion."
                    ),
                }
            )
        elif normalized in MEDIUM_RISK_TYPES:
            risks.append(
                {
                    "risk_type": "SEMANTIC_PARITY",
                    "severity": "MEDIUM",
                    "source_object": transformation.name,
                    "description": (
                        f"{transformation.transformation_type} behavior must be "
                        "preserved in the generated implementation."
                    ),
                    "recommendation": (
                        "Validate null handling, duplicate behavior, ordering, cache or "
                        "join semantics, and rejected records."
                    ),
                }
            )

        for key, value in (transformation.properties or {}).items():
            key_text = str(key).lower()
            if str(value).strip() and any(
                marker in key_text
                for marker in ("sql query", "sql override", "pre sql", "post sql")
            ):
                risks.append(
                    {
                        "risk_type": "CUSTOM_SQL",
                        "severity": "HIGH",
                        "source_object": transformation.name,
                        "description": f"Custom SQL is configured in property {key}.",
                        "recommendation": (
                            "Review SQL dialect, pushdown behavior, side effects, and "
                            "target-platform compatibility."
                        ),
                    }
                )
    return risks


def unsupported_constructs(mapping: CanonicalMapping) -> list[str]:
    """Return the current unsupported-type set using its exact matching rules."""
    return sorted(
        {
            transformation.transformation_type
            for transformation in mapping.transformations
            if transformation.transformation_type.lower() in UNSUPPORTED_TYPES
        }
    )


def normalize_transformation_category(transformation_type: str | None) -> str:
    """Map common vendor labels to stable categories for explanation only."""
    raw_type = re.sub(
        r"(?<=[a-z0-9])(?=[A-Z])",
        " ",
        transformation_type or "",
    )
    normalized = " ".join(re.sub(r"[_-]+", " ", raw_type.lower()).split())
    if not normalized or normalized == "unknown":
        return "UNKNOWN"
    if "stored procedure" in normalized or "external procedure" in normalized:
        return "STORED_PROCEDURE"
    if "lookup" in normalized:
        return "LOOKUP"
    if "join" in normalized or "merge" in normalized:
        return "JOIN"
    if "aggregat" in normalized or "group by" in normalized:
        return "AGGREGATION"
    if "filter" in normalized or "selection" in normalized:
        return "FILTER"
    if "sort" in normalized or "rank" in normalized:
        return "SORT"
    if "sql" in normalized:
        return "SQL"
    if any(token in normalized for token in ("java", "python", "custom", "script")):
        return "CUSTOM_CODE"
    return "OTHER"


class ComplexityAnalyzer:
    """Collect parser evidence and score it using the unchanged v1 algorithm."""

    VERSION = "complexity-v1"

    def analyze(self, mapping: CanonicalMapping) -> ComplexityBreakdown:
        risks = derive_deterministic_risks(mapping)
        unsupported = unsupported_constructs(mapping)
        score, level, reasons, contributions = self.calculate_score(
            mapping,
            risks,
            unsupported,
        )
        factors = self._collect_factors(mapping, unsupported)
        factor_values = list(factors.values())
        evidence_coverage = (
            sum(item.status != "UNAVAILABLE" for item in factor_values) / len(factor_values)
            if factor_values
            else 0.0
        )
        risk_factors = [
            {
                "code": risk["risk_type"],
                "label": risk["risk_type"].replace("_", " ").title(),
                **risk,
            }
            for risk in risks
        ]
        for key, code, label, severity, minimum in (
            ("lookups", "MULTIPLE_LOOKUPS", "Multiple lookups", "MEDIUM", 2),
            (
                "aggregations",
                "MULTIPLE_AGGREGATIONS",
                "Multiple aggregations",
                "MEDIUM",
                2,
            ),
            (
                "stored_procedures",
                "STORED_PROCEDURE",
                "Stored procedure",
                "HIGH",
                1,
            ),
            ("custom_code", "CUSTOM_CODE", "Custom code", "HIGH", 1),
        ):
            count = factors[key].value
            if isinstance(count, int) and count >= minimum:
                risk_factors.append(
                    {
                        "code": code,
                        "label": label,
                        "severity": severity,
                        "count": count,
                        "score_note": "No points added beyond the existing score contributions.",
                    }
                )
        dynamic_sql = factors["dynamic_sql"]
        if dynamic_sql.value is True:
            risk_factors.append(
                {
                    "code": "DYNAMIC_SQL",
                    "label": "Dynamic SQL",
                    "severity": "HIGH",
                    "count": 1,
                    "evidence": dynamic_sql.evidence,
                    "score_note": "No points added beyond the existing score contributions.",
                }
            )

        return ComplexityBreakdown(
            version=self.VERSION,
            score=score,
            level=level,
            factors=factors,
            contributions=contributions,
            risk_factors=risk_factors,
            reasons=reasons,
            reason=self._explain(score, level, contributions, reasons),
            evidence_coverage=round(evidence_coverage, 3),
        )

    @classmethod
    def calculate_score(
        cls,
        mapping: CanonicalMapping,
        risks: list[Any],
        unsupported: list[str],
    ) -> tuple[int, ComplexityLevel, list[str], dict[str, int]]:
        """Apply existing score weights, caps, and thresholds exactly."""
        score = 10
        reasons: list[str] = []

        transformation_count = len(mapping.transformations)
        transformation_points = min(transformation_count * 2, 24)
        score += transformation_points
        if transformation_count:
            reasons.append(f"Contains {transformation_count} transformations.")

        additional_datasets = max(len(mapping.sources) + len(mapping.targets) - 2, 0)
        dataset_points = min(additional_datasets * 3, 12)
        score += dataset_points
        if additional_datasets:
            reasons.append("Uses multiple source or target datasets.")

        medium_count = sum(cls._risk_value(risk, "severity") == "MEDIUM" for risk in risks)
        high_count = sum(
            cls._risk_value(risk, "severity") in {"HIGH", "CRITICAL"}
            for risk in risks
        )
        medium_points = min(medium_count * 5, 20)
        high_points = min(high_count * 10, 30)
        score += medium_points + high_points
        if medium_count:
            reasons.append(f"Contains {medium_count} medium-risk constructs.")
        if high_count:
            reasons.append(f"Contains {high_count} high-risk constructs.")

        parameter_points = min(len(mapping.parameters) * 2, 8)
        dependency_points = min(len(mapping.dependencies) * 4, 12)
        unsupported_points = min(len(unsupported) * 15, 30)
        score += parameter_points + dependency_points + unsupported_points
        score = min(score, 100)

        contributions = {
            "base": 10,
            "transformations": transformation_points,
            "additional_datasets": dataset_points,
            "medium_risk": medium_points,
            "high_risk": high_points,
            "parameters": parameter_points,
            "dependencies": dependency_points,
            "unsupported_constructs": unsupported_points,
        }
        return score, cls.classify_score(score), reasons, contributions

    @staticmethod
    def classify_score(score: int) -> ComplexityLevel:
        if score <= 30:
            return "LOW"
        if score <= 55:
            return "MEDIUM"
        if score <= 80:
            return "HIGH"
        return "VERY_HIGH"

    @staticmethod
    def _risk_value(risk: Any, key: str) -> Any:
        if isinstance(risk, dict):
            return risk.get(key)
        return getattr(risk, key, None)

    @classmethod
    def _collect_factors(
        cls,
        mapping: CanonicalMapping,
        unsupported: list[str],
    ) -> dict[str, FactorEvidence]:
        transformations = mapping.transformations
        categories = Counter(
            normalize_transformation_category(item.transformation_type)
            for item in transformations
        )
        category_status: EvidenceStatus = (
            "INFERRED" if categories["UNKNOWN"] else "OBSERVED"
        )
        category_evidence = ["canonical.transformations.transformation_type"]
        if categories["UNKNOWN"]:
            category_evidence.append(
                f"{categories['UNKNOWN']} transformation type(s) are unknown"
            )

        metadata = mapping.source_metadata or {}
        stage_count: int | None = None
        stage_status: EvidenceStatus = "UNAVAILABLE"
        stage_evidence: list[str] = []
        if isinstance(metadata.get("stage_count"), int):
            stage_count = metadata["stage_count"]
            stage_status = "OBSERVED"
            stage_evidence = ["source_metadata.stage_count"]
        elif isinstance(metadata.get("stage_inventory"), list):
            stage_count = len(metadata["stage_inventory"])
            stage_status = "OBSERVED"
            stage_evidence = ["source_metadata.stage_inventory"]

        factors: dict[str, FactorEvidence] = {
            "source_count": FactorEvidence(
                value=len(mapping.sources), status="OBSERVED", evidence=["canonical.sources"]
            ),
            "target_count": FactorEvidence(
                value=len(mapping.targets), status="OBSERVED", evidence=["canonical.targets"]
            ),
            "stage_count": FactorEvidence(
                value=stage_count, status=stage_status, evidence=stage_evidence
            ),
            "transformation_count": FactorEvidence(
                value=len(transformations),
                status="OBSERVED",
                evidence=["canonical.transformations"],
            ),
        }
        for name, category in (
            ("lookups", "LOOKUP"),
            ("joins", "JOIN"),
            ("aggregations", "AGGREGATION"),
            ("filters", "FILTER"),
            ("sorts", "SORT"),
            ("stored_procedures", "STORED_PROCEDURE"),
            ("custom_code", "CUSTOM_CODE"),
            ("sql", "SQL"),
            ("other_transformations", "OTHER"),
            ("unknown_transformations", "UNKNOWN"),
        ):
            factors[name] = FactorEvidence(
                value=categories[category],
                status=category_status,
                evidence=category_evidence,
            )

        factors.update(
            {
                "parameters": FactorEvidence(
                    value=len(mapping.parameters),
                    status="OBSERVED",
                    evidence=["canonical.parameters"],
                ),
                "dependencies": FactorEvidence(
                    value=len(mapping.dependencies),
                    status="OBSERVED",
                    evidence=["canonical.dependencies"],
                ),
                "unsupported_constructs": FactorEvidence(
                    value=len(unsupported),
                    status="OBSERVED",
                    evidence=["canonical.transformations.transformation_type"],
                ),
            }
        )

        dynamic_values: list[bool] = []
        dynamic_evidence: list[str] = []
        for transformation in transformations:
            for key, value in (transformation.properties or {}).items():
                raw_key = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", str(key))
                normalized_key = " ".join(
                    re.sub(r"[_-]+", " ", raw_key.lower()).split()
                )
                if normalized_key in {"dynamic sql", "is dynamic sql", "dynamic query"}:
                    parsed = cls._parse_boolean(value)
                    if parsed is not None:
                        dynamic_values.append(parsed)
                        dynamic_evidence.append(
                            f"transformation[{transformation.name}].properties[{key}]"
                        )

        factors["dynamic_sql"] = FactorEvidence(
            value=any(dynamic_values) if dynamic_values else None,
            status="OBSERVED" if dynamic_values else "UNAVAILABLE",
            evidence=dynamic_evidence,
        )
        return factors

    @staticmethod
    def _parse_boolean(value: Any) -> bool | None:
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in {"true", "yes", "1", "enabled"}:
            return True
        if normalized in {"false", "no", "0", "disabled"}:
            return False
        return None

    @staticmethod
    def _explain(
        score: int,
        level: str,
        contributions: dict[str, int],
        reasons: list[str],
    ) -> str:
        labels = {
            "base": "base",
            "transformations": "transformation volume",
            "additional_datasets": "additional datasets",
            "medium_risk": "medium-risk constructs",
            "high_risk": "high-risk constructs",
            "parameters": "parameters",
            "dependencies": "dependencies",
            "unsupported_constructs": "unsupported constructs",
        }
        drivers = [
            f"{labels[key]} contributed {value} points"
            for key, value in contributions.items()
            if key != "base" and value
        ]
        if not drivers:
            return f"The mapping scores {score}/100 and is classified {level}."
        explanation = f"The mapping scores {score}/100 ({level}). " + "; ".join(drivers) + "."
        if reasons:
            explanation += " " + " ".join(reasons)
        return explanation