import asyncio
from types import SimpleNamespace

from etl_cc import services
from etl_cc.agents.discovery_agent import DiscoveryAgent
from etl_cc.complexity import ComplexityAnalyzer, normalize_transformation_category
from etl_cc.models import (
    CanonicalDataset,
    CanonicalDependency,
    CanonicalMapping,
    CanonicalParameter,
    CanonicalTransformation,
)


def _mapping(
    transformation_types=(),
    *,
    source_count=1,
    target_count=1,
    parameter_count=0,
    dependency_count=0,
    source_metadata=None,
):
    return CanonicalMapping(
        source_vendor="TEST",
        source_object_key="folder/mapping/test",
        mapping_name="test_mapping",
        sources=[
            CanonicalDataset(name=f"SRC_{index}", dataset_type="SOURCE")
            for index in range(source_count)
        ],
        targets=[
            CanonicalDataset(name=f"TGT_{index}", dataset_type="TARGET")
            for index in range(target_count)
        ],
        transformations=[
            CanonicalTransformation(
                name=f"TRANS_{index}",
                transformation_type=transformation_type,
                properties=(
                    {"DynamicSQL": "true"}
                    if normalize_transformation_category(transformation_type) == "SQL"
                    else {}
                ),
            )
            for index, transformation_type in enumerate(transformation_types)
        ],
        parameters=[
            CanonicalParameter(name=f"PARAM_{index}")
            for index in range(parameter_count)
        ],
        dependencies=[
            CanonicalDependency(
                upstream_object_key=f"upstream/{index}",
                downstream_object_key="folder/mapping/test",
                dependency_source="TEST",
            )
            for index in range(dependency_count)
        ],
        source_metadata=source_metadata or {},
    )


def test_complexity_score_boundaries_preserve_existing_levels():
    assert ComplexityAnalyzer.classify_score(30) == "LOW"
    assert ComplexityAnalyzer.classify_score(31) == "MEDIUM"
    assert ComplexityAnalyzer.classify_score(55) == "MEDIUM"
    assert ComplexityAnalyzer.classify_score(56) == "HIGH"
    assert ComplexityAnalyzer.classify_score(80) == "HIGH"
    assert ComplexityAnalyzer.classify_score(81) == "VERY_HIGH"


def test_complexity_contribution_caps_match_existing_formula():
    mapping = _mapping(
        [
            *(["Lookup"] * 5),
            "Java Transformation",
            "Custom Transformation",
            "External Procedure",
            *(["Expression"] * 12),
        ],
        source_count=6,
        target_count=6,
        parameter_count=8,
        dependency_count=4,
    )

    result = ComplexityAnalyzer().analyze(mapping)

    assert result.score == 100
    assert result.contributions == {
        "base": 10,
        "transformations": 24,
        "additional_datasets": 12,
        "medium_risk": 20,
        "high_risk": 30,
        "parameters": 8,
        "dependencies": 12,
        "unsupported_constructs": 30,
    }


def test_unsupported_construct_scoring_preserves_existing_overlap():
    result = ComplexityAnalyzer().analyze(_mapping(["Java Transformation"]))

    assert result.score == 37
    assert result.level == "MEDIUM"
    assert result.factors["unsupported_constructs"].value == 1
    assert result.contributions["high_risk"] == 10
    assert result.contributions["unsupported_constructs"] == 15
    assert result.risk_factors[0]["code"] == "COMPLEX_TRANSFORMATION"


def test_complexity_breakdown_normalizes_types_and_unknown_metadata():
    mapping = _mapping(
        [
            "Lookup",
            "PxJoiner",
            "Aggregator",
            "Filter",
            "Sorter",
            "StoredProcedure",
            "Java Transformation",
            "SQLTransformation",
            "Unknown",
        ],
        source_metadata={"stage_inventory": [{"name": "stage1"}]},
    )

    result = ComplexityAnalyzer().analyze(mapping)

    assert result.version == "complexity-v1"
    assert result.factors["lookups"].value == 1
    assert result.factors["joins"].value == 1
    assert result.factors["aggregations"].value == 1
    assert result.factors["filters"].value == 1
    assert result.factors["sorts"].value == 1
    assert result.factors["stored_procedures"].value == 1
    assert result.factors["custom_code"].value == 1
    assert result.factors["sql"].value == 1
    assert result.factors["stage_count"].value == 1
    assert result.factors["dynamic_sql"].value is True
    assert result.factors["dynamic_sql"].status == "OBSERVED"
    assert any(item["code"] == "DYNAMIC_SQL" for item in result.risk_factors)
    assert result.reason.startswith("The mapping scores ")
    assert 0 < result.evidence_coverage <= 1


def test_missing_stage_and_dynamic_sql_metadata_is_not_assumed_false():
    result = ComplexityAnalyzer().analyze(_mapping(["Lookup"]))

    assert result.factors["stage_count"].value is None
    assert result.factors["stage_count"].status == "UNAVAILABLE"
    assert result.factors["dynamic_sql"].value is None
    assert result.factors["dynamic_sql"].status == "UNAVAILABLE"


def test_agent_legacy_complexity_helper_matches_analyzer():
    mapping = _mapping(["Lookup", "Java Transformation"])
    agent = DiscoveryAgent()
    risks = agent._derive_risks(mapping)
    unsupported = agent._unsupported_constructs(mapping)
    result = ComplexityAnalyzer().analyze(mapping)

    assert agent._calculate_complexity(mapping, risks, unsupported) == (
        result.score,
        result.level,
        result.reasons,
    )


def _discovery_row(breakdown=None):
    return SimpleNamespace(
        id=17,
        repository_id=4,
        source_object_key="folder/mapping/test",
        object_name="test_mapping",
        folder_path="folder",
        business_purpose="Loads test data",
        business_rules=[],
        prerequisites=[],
        complexity="HIGH",
        complexity_score=72,
        complexity_breakdown=breakdown or {},
        complexity_version=None,
        has_dependency_cycle=False,
        discovery_status="ANALYZED",
        source_definition={},
    )


class _DiscoverySession:
    def __init__(self, row, audit):
        self.row = row
        self.audit = audit

    async def get(self, _model, _key):
        return self.row

    async def scalar(self, _statement):
        return self.audit


def test_discovery_api_serializes_new_breakdown_and_keeps_old_fields():
    details = {
        "version": "complexity-v1",
        "score": 72,
        "level": "HIGH",
        "factors": {"lookups": {"value": 5, "status": "OBSERVED", "evidence": []}},
        "contributions": {"base": 10},
        "risk_factors": [],
        "reasons": ["legacy-compatible reason"],
        "reason": "Transformation volume drove the score.",
        "evidence_coverage": 0.9,
    }
    row = _discovery_row(details)
    audit = SimpleNamespace(
        response_payload={
            "complexity_version": "complexity-v1",
            "complexity_details": details,
            "complexity_reasons": ["legacy-compatible reason"],
            "risks": [],
            "unsupported_constructs": [],
            "human_review_required": False,
        },
        model_name="gpt-4o",
        prompt_name="discovery",
        prompt_version="1",
        input_tokens=1,
        output_tokens=1,
        id=23,
        completed_at=None,
    )

    response = asyncio.run(
        services.get_mapping_discovery_details(_DiscoverySession(row, audit), row.id)
    )
    serialized = response.model_dump(mode="json")

    assert serialized["complexity"] == "HIGH"
    assert serialized["complexity_score"] == 72
    assert serialized["complexity_version"] == "complexity-v1"
    assert serialized["complexity_details"] == details
    assert serialized["complexity_reasons"] == ["legacy-compatible reason"]
    assert "human_review_required" in serialized


def test_discovery_api_builds_breakdown_for_legacy_rows():
    row = _discovery_row()
    audit = SimpleNamespace(
        response_payload={
            "complexity": "HIGH",
            "complexity_score": 72,
            "complexity_reasons": ["Contains 4 transformations."],
            "transformation_summary": {
                "source_count": 1,
                "target_count": 1,
                "transformation_count": 4,
                "parameter_count": 2,
                "transformation_types": {"Lookup": 2, "Aggregator": 2},
            },
            "risks": [],
            "unsupported_constructs": [],
            "human_review_required": False,
        },
        model_name="gpt-4o",
        prompt_name="discovery",
        prompt_version="1",
        input_tokens=0,
        output_tokens=0,
        id=23,
        completed_at=None,
    )

    response = asyncio.run(
        services.get_mapping_discovery_details(_DiscoverySession(row, audit), row.id)
    )

    assert response.complexity_version == "legacy"
    assert response.complexity_details["factors"]["lookups"]["value"] == 2
    assert response.complexity_details["factors"]["aggregations"]["value"] == 2
    assert response.complexity_details["reason"] == "Contains 4 transformations."


def test_discovery_summary_counts_very_high_complexity():
    rows = [
        SimpleNamespace(
            id=31,
            complexity="VERY_HIGH",
            discovery_status="ANALYZED",
            has_dependency_cycle=False,
        )
    ]

    class FakeSession:
        async def get(self, _model, _key):
            return SimpleNamespace(id=4)

        async def scalars(self, _statement):
            return SimpleNamespace(all=lambda: rows)

        async def scalar(self, _statement):
            return None

    summary = asyncio.run(services.get_discovery_summary(FakeSession(), 4))

    assert summary.complexity_distribution.VERY_HIGH == 1
    assert summary.complexity_distribution.UNKNOWN == 0