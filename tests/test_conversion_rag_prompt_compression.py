import json

from etl_cc.agents.conversion_agent import ConversionAgent
from etl_cc.agents.conversion_agent import ConversionResult, GeneratedFile
from etl_cc.agents.rag_retrieval_agent import (
    RAGRetrievalResult,
    RetrievedKnowledge,
)


def _retrieved_knowledge(knowledge_id: int, code: str) -> RetrievedKnowledge:
    content = json.dumps(
        {
            "source_platform": "AB_INITIO",
            "target_platform": "DATABRICKS",
            "mapping_name": f"mapping_{knowledge_id}",
            "complexity": "MEDIUM",
            "transformation_count": 1,
            "transformation_types": ["REFORMAT"],
            "business_rules": [
                {
                    "rule_type": "FILTER",
                    "description": "Copy active customer records.",
                }
            ],
            "lineage": {"field_lineage": [{"source": "A", "target": "B"}]},
            "generated_code": code,
            "validation_results": {
                "status": "PASSED",
                "large_payload": "validation-data-" * 1000,
            },
            "confidence_score": 0.95,
        }
    )
    return RetrievedKnowledge(
        knowledge_id=knowledge_id,
        knowledge_type="SUCCESSFUL_CONVERSION",
        title=f"mapping_{knowledge_id}",
        content=content,
        relevance_score=0.95,
        vector_similarity=0.95,
        metadata_score=0.8,
        metadata_payload={
            "source_vendor": "AB_INITIO",
            "target_framework": "PYSPARK",
            "transformation_types": ["REFORMAT"],
        },
    )


def test_500k_knowledge_record_is_compacted_below_retrieval_context_cap():
    large_generated_code = (
        "def transform(inputs):\n    return inputs\n"
        + ("# generated-code-padding\n" * 22_000)
        + "UNIQUE_FULL_CODE_MUST_NOT_ENTER_PROMPT"
    )
    rag = RAGRetrievalResult(
        mapping_name="current_mapping",
        status="CONTRIBUTED",
        embedding_model="text-embedding-3-small",
        retrieved_items=[
            _retrieved_knowledge(index, large_generated_code)
            for index in range(1, 4)
        ],
        candidate_count=3,
        eligible_knowledge_count=3,
        retrieval_count=3,
        accepted_count=3,
        confidence=0.95,
        average_similarity=0.9,
        contributed_to_prompt=True,
    )
    agent = ConversionAgent("AB_INITIO")

    compact_rag, context_tokens, excluded_counts = (
        agent._compact_rag_for_prompt(rag)
    )
    compact_prompt_context = json.dumps(compact_rag, sort_keys=True)

    assert len(large_generated_code) > 500_000
    assert (
        json.loads(rag.retrieved_items[0].content)["generated_code"]
        == large_generated_code
    )
    assert context_tokens <= agent.max_retrieval_context_tokens == 8000
    assert len(compact_rag["retrieved_items"]) == 3
    assert "UNIQUE_FULL_CODE_MUST_NOT_ENTER_PROMPT" not in compact_prompt_context
    assert "validation-data-" not in compact_prompt_context
    assert '"lineage"' not in compact_prompt_context
    assert "business_rule_summaries" in compact_prompt_context
    assert "transformation_types" in compact_prompt_context
    assert "generated_code_summary" in compact_prompt_context
    assert excluded_counts["retrieved_generated_code_tokens_excluded"] > 8000
    assert excluded_counts["retrieved_validation_artifact_tokens_excluded"] > 0
    assert excluded_counts["retrieved_lineage_tokens_excluded"] > 0


def test_compaction_discards_lowest_ranked_items_until_under_hard_cap():
    rule_heavy_content = json.dumps(
        {
            "source_platform": "AB_INITIO",
            "target_platform": "DATABRICKS",
            "mapping_name": "rule_heavy",
            "complexity": "HIGH",
            "transformation_count": 2,
            "transformation_types": ["FILTER", "REFORMAT"],
            "business_rules": [
                {"description": f"business rule {i} " + ("details " * 120)}
                for i in range(20)
            ],
            "generated_code": "def transform(inputs):\n    return inputs\n",
        }
    )
    rag = RAGRetrievalResult(
        mapping_name="current_mapping",
        status="CONTRIBUTED",
        embedding_model="text-embedding-3-small",
        retrieved_items=[
            RetrievedKnowledge(
                knowledge_id=index,
                knowledge_type="SUCCESSFUL_CONVERSION",
                title="rule-heavy",
                content=rule_heavy_content,
                relevance_score=0.9,
                vector_similarity=0.9,
                metadata_score=0.8,
            )
            for index in range(100)
        ],
        candidate_count=100,
        eligible_knowledge_count=100,
        retrieval_count=100,
        accepted_count=100,
        confidence=0.9,
        average_similarity=0.9,
        contributed_to_prompt=True,
    )

    compact_rag, context_tokens, _ = ConversionAgent(
        "AB_INITIO"
    )._compact_rag_for_prompt(rag)

    assert context_tokens <= 8000
    assert len(compact_rag["retrieved_items"]) < 100


def test_prompt_token_measurements_are_persistable_in_conversion_audit_payload():
    system_prompt = "Convert the canonical mapping."
    baseline_evidence = '{"canonical_mapping":{"mapping_name":"orders"}}'
    rag_evidence = (
        '{"canonical_mapping":{"mapping_name":"orders"},'
        '"approved_knowledge":[{"summary":"known successful pattern"}]}'
    )

    baseline_tokens, actual_tokens = ConversionAgent._prompt_token_counts(
        system_prompt,
        baseline_evidence,
        rag_evidence,
    )
    result = ConversionResult(
        mapping_name="orders",
        target_platform="DATABRICKS",
        target_framework="PYSPARK",
        generated_files=[
            GeneratedFile(
                artifact_type="CONFIGURATION",
                file_name="config.json",
                content="{}",
                media_type="application/json",
            )
        ],
        confidence=0.9,
        baseline_prompt_tokens=baseline_tokens,
        actual_prompt_tokens=actual_tokens,
        retrieved_context_tokens=17,
    )

    audit_payload = result.model_dump(mode="json")

    assert baseline_tokens > 0
    assert actual_tokens > baseline_tokens
    assert audit_payload["baseline_prompt_tokens"] == baseline_tokens
    assert audit_payload["actual_prompt_tokens"] == actual_tokens
    assert audit_payload["retrieved_context_tokens"] == 17
