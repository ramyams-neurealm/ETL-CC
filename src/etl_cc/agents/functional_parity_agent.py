"""Deterministic functional parity scoring against canonical mapping evidence."""

from pydantic import BaseModel
from etl_cc.models import CanonicalMapping


class FunctionalParityResult(BaseModel):
    source_coverage: float
    target_coverage: float
    target_field_coverage: float
    business_rule_coverage: float
    lineage_coverage: float
    precision_scale_coverage: float
    overall_functional_parity: float
    status: str


class FunctionalParityAgent:
    AGENT_NAME = "FUNCTIONAL_PARITY_AGENT"
    AGENT_VERSION = "1.0.0"
    STAGE_NAME = "FUNCTIONAL_PARITY"
    MODEL_NAME = "DETERMINISTIC_COVERAGE"

    def run(self, mapping: CanonicalMapping, discovery: dict, lineage: dict, code: str, minimum: float) -> FunctionalParityResult:
        sources = [item.name for item in mapping.sources]
        targets = [item.name for item in mapping.targets]
        fields = [field.name for target in mapping.targets for field in target.fields]
        rules = [item.get("source_expression") for item in discovery.get("business_rules", []) if item.get("source_expression")]
        field_lineage = lineage.get("field_lineage", [])
        source_coverage = self._coverage(sources, code)
        target_coverage = self._coverage(targets, code)
        field_coverage = self._coverage(fields, code)
        rule_coverage = 1.0 if not rules else sum(self._expression_evidence(rule, code) for rule in rules) / len(rules)
        lineage_coverage = 1.0 if not fields else min(1.0, len([item for item in field_lineage if item.get("lineage_type") != "UNRESOLVED"]) / len(fields))
        decimals = [field for dataset in [*mapping.sources, *mapping.targets] for field in dataset.fields if field.data_type.lower() in {"decimal", "numeric"} and field.precision is not None and field.scale is not None]
        precision = 1.0 if not decimals else sum(f"{field.precision}, {field.scale}" in code or f"{field.precision},{field.scale}" in code for field in decimals) / len(decimals)
        values = [source_coverage, target_coverage, field_coverage, rule_coverage, lineage_coverage, precision]
        overall = round(sum(values) / len(values), 4)
        return FunctionalParityResult(source_coverage=round(source_coverage, 4), target_coverage=round(target_coverage, 4), target_field_coverage=round(field_coverage, 4), business_rule_coverage=round(rule_coverage, 4), lineage_coverage=round(lineage_coverage, 4), precision_scale_coverage=round(precision, 4), overall_functional_parity=overall, status="PASSED" if overall >= minimum else "FAILED")

    @staticmethod
    def _coverage(items, text):
        return 1.0 if not items else sum(item in text for item in items) / len(items)

    @staticmethod
    def _expression_evidence(expression, code):
        tokens = [token for token in expression.replace("'", " ").replace('"', " ").replace("=", " ").split() if len(token) > 2]
        return 1.0 if tokens and all(token.lower() in code.lower() for token in tokens) else 0.0
