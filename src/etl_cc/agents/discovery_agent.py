"""
Discovery Agent for the ETL Migration Command Center.

The agent consumes one canonical ETL mapping after connector discovery. It
uses deterministic rules to derive technical facts, business-rule evidence,
prerequisites, migration risks, unsupported constructs, and a migration
complexity score. The agent does not parse Informatica XML and does not access
repository credentials.
"""

from collections import Counter
import json
from typing import Any, Literal

from pydantic import BaseModel, Field

from etl_cc.key_vault_service import DynamicChatOpenAI
from etl_cc.models import CanonicalMapping


class DiscoveryBusinessRule(BaseModel):
    """Human-readable rule traced to an exact source transformation."""

    rule_type: str
    description: str
    source_object: str
    source_expression: str | None = None
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)


class DiscoveryPrerequisite(BaseModel):
    """Dataset, parameter, connection, or runtime dependency."""

    prerequisite_type: str
    name: str
    required: bool = True
    description: str


class DiscoveryRisk(BaseModel):
    """Migration risk supported by mapping metadata."""

    risk_type: str
    severity: Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    description: str
    source_object: str | None = None
    recommendation: str


class DiscoveryAssumption(BaseModel):
    """Explicit assumption that requires confirmation or human review."""

    description: str
    confidence: float = Field(ge=0.0, le=1.0)


class TransformationSummary(BaseModel):
    """Deterministic technical counts derived from the canonical mapping."""

    source_count: int
    target_count: int
    transformation_count: int
    connector_count: int
    session_count: int
    workflow_count: int
    parameter_count: int
    transformation_types: dict[str, int]


class LLMBusinessRule(BaseModel):
    """GPT-4o interpretation grounded in exact mapping evidence."""
    rule_type: str
    description: str
    source_object: str
    source_expression: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)


class LLMSemanticRisk(BaseModel):
    risk_type: str
    severity: Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    description: str
    source_object: str | None = None
    recommendation: str


class DiscoverySemanticAnalysis(BaseModel):
    business_purpose: str
    business_rules: list[LLMBusinessRule] = Field(default_factory=list)
    semantic_risks: list[LLMSemanticRisk] = Field(default_factory=list)
    assumptions: list[DiscoveryAssumption] = Field(default_factory=list)
    migration_recommendations: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    human_review_required: bool


class DiscoveryAnalysis(BaseModel):
    """Structured Discovery Agent output persisted by the discovery worker."""

    business_purpose: str
    business_rules: list[DiscoveryBusinessRule] = Field(default_factory=list)
    prerequisites: list[DiscoveryPrerequisite] = Field(default_factory=list)
    transformation_summary: TransformationSummary
    complexity: Literal["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]
    complexity_score: int = Field(ge=0, le=100)
    complexity_reasons: list[str] = Field(default_factory=list)
    risks: list[DiscoveryRisk] = Field(default_factory=list)
    unsupported_constructs: list[str] = Field(default_factory=list)
    assumptions: list[DiscoveryAssumption] = Field(default_factory=list)
    migration_recommendations: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    human_review_required: bool


class DiscoveryAgent:
    """Analyze one CanonicalMapping without re-reading source XML."""

    AGENT_NAME = "DISCOVERY_AGENT"
    AGENT_VERSION = "2.0.0"
    STAGE_NAME = "DISCOVERY_ENRICHMENT"
    MODEL_NAME = "gpt-4o"
    PROMPT_NAME = "DISCOVERY_SEMANTIC_ANALYSIS"
    PROMPT_VERSION = "1.0.0"

    def __init__(self) -> None:
        self.model_name = "gpt-4o"
        self.prompt_name = self.PROMPT_NAME
        self.prompt_version = self.PROMPT_VERSION
        self.input_tokens = 0
        self.output_tokens = 0

    _HIGH_RISK_TYPES = {
        "java transformation",
        "custom transformation",
        "external procedure",
        "stored procedure",
        "sql transformation",
        "update strategy",
    }
    _MEDIUM_RISK_TYPES = {
        "lookup",
        "lookup procedure",
        "joiner",
        "aggregator",
        "sorter",
        "rank",
        "router",
        "transaction control",
    }
    _UNSUPPORTED_TYPES = {
        "java transformation",
        "custom transformation",
        "external procedure",
    }

    async def run(self, mapping: CanonicalMapping) -> DiscoveryAnalysis:
        """Combine deterministic facts with mandatory GPT-4o semantic analysis."""
        types = Counter(
            transformation.transformation_type
            for transformation in mapping.transformations
        )
        summary = TransformationSummary(
            source_count=len(mapping.sources),
            target_count=len(mapping.targets),
            transformation_count=len(mapping.transformations),
            connector_count=len(mapping.connectors),
            session_count=len(mapping.sessions),
            workflow_count=len(mapping.workflows),
            parameter_count=len(mapping.parameters),
            transformation_types=dict(sorted(types.items())),
        )
        deterministic_rules = self._derive_business_rules(mapping)
        prerequisites = self._derive_prerequisites(mapping)
        deterministic_risks = self._derive_risks(mapping)
        unsupported = self._unsupported_constructs(mapping)
        score, level, reasons = self._calculate_complexity(
            mapping,
            deterministic_risks,
            unsupported,
        )

        semantic = await self._run_llm(
            mapping=mapping,
            summary=summary,
            deterministic_rules=deterministic_rules,
            deterministic_risks=deterministic_risks,
            unsupported=unsupported,
            complexity=level,
            complexity_score=score,
        )

        semantic_rules = [
            DiscoveryBusinessRule(**item.model_dump())
            for item in semantic.business_rules
        ]
        semantic_risks = [
            DiscoveryRisk(**item.model_dump())
            for item in semantic.semantic_risks
        ]
        rules = semantic_rules or deterministic_rules
        risks = self._merge_risks(deterministic_risks, semantic_risks)
        human_review = (
            semantic.human_review_required
            or bool(unsupported)
            or any(risk.severity in {"HIGH", "CRITICAL"} for risk in risks)
        )

        return DiscoveryAnalysis(
            business_purpose=semantic.business_purpose,
            business_rules=rules,
            prerequisites=prerequisites,
            transformation_summary=summary,
            complexity=level,
            complexity_score=score,
            complexity_reasons=reasons,
            risks=risks,
            unsupported_constructs=unsupported,
            assumptions=semantic.assumptions,
            migration_recommendations=semantic.migration_recommendations,
            confidence=semantic.confidence,
            human_review_required=human_review,
        )

    async def _run_llm(
        self,
        *,
        mapping: CanonicalMapping,
        summary: TransformationSummary,
        deterministic_rules: list[DiscoveryBusinessRule],
        deterministic_risks: list[DiscoveryRisk],
        unsupported: list[str],
        complexity: str,
        complexity_score: int,
    ) -> DiscoverySemanticAnalysis:
        system_prompt = """You are the Discovery Agent for an ETL migration system.
Use only the supplied canonical mapping evidence. Do not invent datasets, fields,
transformations, expressions, dependencies, or runtime behavior. Preserve each
source_expression exactly as supplied. Generate a concise business purpose,
business-friendly rule explanations, semantic risks, assumptions, and migration
recommendations. Identify uncertainty through confidence and human review fields.
Return only the requested structured output."""
        evidence = {
            "mapping_name": mapping.mapping_name,
            "description": mapping.description,
            "sources": [item.model_dump(mode="json") for item in mapping.sources],
            "targets": [item.model_dump(mode="json") for item in mapping.targets],
            "transformations": [
                item.model_dump(mode="json") for item in mapping.transformations
            ],
            "parameters": [item.model_dump(mode="json") for item in mapping.parameters],
            "technical_summary": summary.model_dump(mode="json"),
            "deterministic_business_rule_evidence": [
                item.model_dump(mode="json") for item in deterministic_rules
            ],
            "deterministic_risks": [
                item.model_dump(mode="json") for item in deterministic_risks
            ],
            "unsupported_constructs": unsupported,
            "deterministic_complexity": complexity,
            "deterministic_complexity_score": complexity_score,
        }
        llm = DynamicChatOpenAI(
            operation_name=f"discovery:{mapping.source_object_key}",
        )
        response = await llm.with_structured_output(
            DiscoverySemanticAnalysis,
            include_raw=True,
        ).ainvoke(
            [
                ("system", system_prompt),
                ("human", json.dumps(evidence, sort_keys=True, default=str)),
            ]
        )
        parsed = response.get("parsed") if isinstance(response, dict) else response
        raw = response.get("raw") if isinstance(response, dict) else None
        if parsed is None:
            raise RuntimeError("GPT-4o returned no valid structured Discovery result.")

        usage = getattr(raw, "usage_metadata", None) or {}
        response_metadata = getattr(raw, "response_metadata", None) or {}
        token_usage = response_metadata.get("token_usage", {})
        self.input_tokens = int(
            usage.get("input_tokens")
            or token_usage.get("prompt_tokens")
            or 0
        )
        self.output_tokens = int(
            usage.get("output_tokens")
            or token_usage.get("completion_tokens")
            or 0
        )
        self.model_name = (
            response_metadata.get("model_name")
            or response_metadata.get("model")
            or llm.model
        )
        return parsed

    @staticmethod
    def _merge_risks(
        deterministic: list[DiscoveryRisk],
        semantic: list[DiscoveryRisk],
    ) -> list[DiscoveryRisk]:
        result: list[DiscoveryRisk] = []
        seen: set[tuple[str, str | None, str]] = set()
        for risk in [*deterministic, *semantic]:
            key = (risk.risk_type, risk.source_object, risk.description)
            if key not in seen:
                seen.add(key)
                result.append(risk)
        return result

    def _derive_business_purpose(
        self,
        mapping: CanonicalMapping,
    ) -> tuple[str, list[DiscoveryAssumption]]:
        if mapping.description and mapping.description.strip():
            return mapping.description.strip(), []

        source_names = ", ".join(item.name for item in mapping.sources) or "source data"
        target_names = ", ".join(item.name for item in mapping.targets) or "target data"
        purpose = (
            f"Processes {source_names} and loads the resulting data into "
            f"{target_names} through mapping {mapping.mapping_name}."
        )
        assumption = DiscoveryAssumption(
            description=(
                "The business purpose was inferred from mapping, source, and target "
                "names because the source mapping description is empty."
            ),
            confidence=0.65,
        )
        return purpose, [assumption]

    def _derive_business_rules(
        self,
        mapping: CanonicalMapping,
    ) -> list[DiscoveryBusinessRule]:
        """Derive meaningful rules from transformation properties and ports."""
        rules: list[DiscoveryBusinessRule] = []
        seen: set[tuple[str, str, str]] = set()

        def add_rule(
            rule_type: str,
            description: str,
            source_object: str,
            expression: str,
        ) -> None:
            normalized_expression = expression.strip()
            key = (rule_type, source_object, normalized_expression)
            if normalized_expression and key not in seen:
                seen.add(key)
                rules.append(
                    DiscoveryBusinessRule(
                        rule_type=rule_type,
                        description=description,
                        source_object=source_object,
                        source_expression=normalized_expression,
                        confidence=1.0,
                    )
                )

        for transformation in mapping.transformations:
            transformation_type = transformation.transformation_type.lower().strip()
            properties = transformation.properties or {}

            property_rules = (
                ("filter condition", "FILTER"),
                ("source filter", "FILTER"),
                ("lookup condition", "LOOKUP"),
                ("join condition", "JOIN"),
                ("group by ports", "AGGREGATION"),
                ("update strategy expression", "UPDATE_STRATEGY"),
            )
            for property_name, rule_type in property_rules:
                value = next(
                    (
                        str(property_value).strip()
                        for key, property_value in properties.items()
                        if str(key).strip().lower() == property_name
                        and str(property_value).strip()
                    ),
                    "",
                )
                if value:
                    descriptions = {
                        "FILTER": f"Records are retained when '{value}' is true.",
                        "JOIN": f"Datasets are joined using '{value}'.",
                        "LOOKUP": f"Lookup matching uses '{value}'.",
                        "AGGREGATION": f"Aggregation groups records using '{value}'.",
                        "UPDATE_STRATEGY": f"Target row action is selected using '{value}'.",
                    }
                    add_rule(
                        rule_type,
                        descriptions[rule_type],
                        transformation.name,
                        value,
                    )

            for port in transformation.ports:
                expression = (port.expression or "").strip()
                if not expression:
                    continue

                # A direct pass-through is technical wiring, not a business rule.
                if expression.upper() == port.name.strip().upper():
                    continue

                if "filter" in transformation_type:
                    rule_type = "FILTER"
                    description = f"Records are retained when '{expression}' is true."
                elif "lookup" in transformation_type:
                    rule_type = "LOOKUP"
                    description = f"Lookup output is derived using '{expression}'."
                elif "aggregator" in transformation_type:
                    rule_type = "AGGREGATION"
                    description = f"Aggregated output is calculated using '{expression}'."
                elif "router" in transformation_type:
                    rule_type = "ROUTING"
                    description = f"Records are routed using '{expression}'."
                elif "update strategy" in transformation_type:
                    rule_type = "UPDATE_STRATEGY"
                    description = f"Target row action is selected using '{expression}'."
                else:
                    rule_type = "DERIVATION"
                    description = f"Field {port.name} is derived using '{expression}'."

                add_rule(
                    rule_type,
                    description,
                    transformation.name,
                    expression,
                )

        return rules

    def _derive_prerequisites(
        self,
        mapping: CanonicalMapping,
    ) -> list[DiscoveryPrerequisite]:
        prerequisites: list[DiscoveryPrerequisite] = []
        seen: set[tuple[str, str]] = set()

        def add(kind: str, name: str, description: str) -> None:
            key = (kind, name)
            if name and key not in seen:
                seen.add(key)
                prerequisites.append(
                    DiscoveryPrerequisite(
                        prerequisite_type=kind,
                        name=name,
                        description=description,
                    )
                )

        for source in mapping.sources:
            add(
                "SOURCE_DATASET",
                source.name,
                f"Source dataset {source.name} must be available and readable.",
            )
            if source.connection_name:
                add(
                    "SOURCE_CONNECTION",
                    source.connection_name,
                    f"Source connection {source.connection_name} must be configured.",
                )

        for target in mapping.targets:
            add(
                "TARGET_DATASET",
                target.name,
                f"Target dataset {target.name} must exist or be creatable.",
            )
            if target.connection_name:
                add(
                    "TARGET_CONNECTION",
                    target.connection_name,
                    f"Target connection {target.connection_name} must be configured.",
                )

        for parameter in mapping.parameters:
            add(
                "PARAMETER",
                parameter.name,
                f"Runtime parameter {parameter.name} must be resolved before execution.",
            )

        for dependency in mapping.dependencies:
            add(
                "UPSTREAM_MAPPING",
                dependency.upstream_object_key,
                (
                    f"Upstream object {dependency.upstream_object_key} must complete "
                    "before this mapping when the dependency is active."
                ),
            )

        return prerequisites

    def _derive_risks(self, mapping: CanonicalMapping) -> list[DiscoveryRisk]:
        risks: list[DiscoveryRisk] = []
        for transformation in mapping.transformations:
            normalized = transformation.transformation_type.lower()
            if normalized in self._HIGH_RISK_TYPES:
                risks.append(
                    DiscoveryRisk(
                        risk_type="COMPLEX_TRANSFORMATION",
                        severity="HIGH",
                        source_object=transformation.name,
                        description=(
                            f"{transformation.transformation_type} requires detailed "
                            "behavioral review during PySpark conversion."
                        ),
                        recommendation=(
                            "Review transformation properties, expressions, error handling, "
                            "state behavior, and target-side effects before conversion."
                        ),
                    )
                )
            elif normalized in self._MEDIUM_RISK_TYPES:
                risks.append(
                    DiscoveryRisk(
                        risk_type="SEMANTIC_PARITY",
                        severity="MEDIUM",
                        source_object=transformation.name,
                        description=(
                            f"{transformation.transformation_type} behavior must be "
                            "preserved in the generated implementation."
                        ),
                        recommendation=(
                            "Validate null handling, duplicate behavior, ordering, cache or "
                            "join semantics, and rejected records."
                        ),
                    )
                )

            properties: dict[str, Any] = transformation.properties or {}
            for key, value in properties.items():
                key_text = str(key).lower()
                value_text = str(value).strip()
                if value_text and any(
                    marker in key_text
                    for marker in ("sql query", "sql override", "pre sql", "post sql")
                ):
                    risks.append(
                        DiscoveryRisk(
                            risk_type="CUSTOM_SQL",
                            severity="HIGH",
                            source_object=transformation.name,
                            description=f"Custom SQL is configured in property {key}.",
                            recommendation=(
                                "Review SQL dialect, pushdown behavior, side effects, and "
                                "target-platform compatibility."
                            ),
                        )
                    )
        return risks

    def _unsupported_constructs(self, mapping: CanonicalMapping) -> list[str]:
        constructs = {
            transformation.transformation_type
            for transformation in mapping.transformations
            if transformation.transformation_type.lower() in self._UNSUPPORTED_TYPES
        }
        return sorted(constructs)

    def _calculate_complexity(
        self,
        mapping: CanonicalMapping,
        risks: list[DiscoveryRisk],
        unsupported: list[str],
    ) -> tuple[int, Literal["LOW", "MEDIUM", "HIGH", "VERY_HIGH"], list[str]]:
        score = 10
        reasons: list[str] = []

        transformation_count = len(mapping.transformations)
        score += min(transformation_count * 2, 24)
        if transformation_count:
            reasons.append(f"Contains {transformation_count} transformations.")

        additional_datasets = max(len(mapping.sources) + len(mapping.targets) - 2, 0)
        score += min(additional_datasets * 3, 12)
        if additional_datasets:
            reasons.append("Uses multiple source or target datasets.")

        medium_count = sum(risk.severity == "MEDIUM" for risk in risks)
        high_count = sum(risk.severity in {"HIGH", "CRITICAL"} for risk in risks)
        score += min(medium_count * 5, 20)
        score += min(high_count * 10, 30)
        if medium_count:
            reasons.append(f"Contains {medium_count} medium-risk constructs.")
        if high_count:
            reasons.append(f"Contains {high_count} high-risk constructs.")

        score += min(len(mapping.parameters) * 2, 8)
        score += min(len(mapping.dependencies) * 4, 12)
        score += min(len(unsupported) * 15, 30)
        score = min(score, 100)

        if score <= 30:
            level: Literal["LOW", "MEDIUM", "HIGH", "VERY_HIGH"] = "LOW"
        elif score <= 55:
            level = "MEDIUM"
        elif score <= 80:
            level = "HIGH"
        else:
            level = "VERY_HIGH"
        return score, level, reasons

    @staticmethod
    def _calculate_confidence(
        mapping: CanonicalMapping,
        rules: list[DiscoveryBusinessRule],
    ) -> float:
        score = 0.55
        if mapping.description:
            score += 0.15
        if mapping.sources:
            score += 0.08
        if mapping.targets:
            score += 0.08
        if mapping.transformations:
            score += 0.08
        if rules:
            score += 0.06
        return round(min(score, 1.0), 2)
