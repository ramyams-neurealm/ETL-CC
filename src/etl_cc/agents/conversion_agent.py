"""GPT-4o conversion agent for Informatica-to-Databricks PySpark migration."""

import json
import re
from typing import Literal

from pydantic import BaseModel, Field

from etl_cc.key_vault_service import DynamicChatOpenAI
from etl_cc.models import CanonicalMapping
from etl_cc.agents.rag_retrieval_agent import RAGRetrievalResult


class GeneratedFile(BaseModel):
    artifact_type: Literal["PYSPARK_CODE", "UNIT_TEST", "CONFIGURATION"]
    file_name: str
    content: str
    media_type: str


class ConversionResult(BaseModel):
    mapping_name: str
    target_platform: Literal["DATABRICKS"]
    target_framework: Literal["PYSPARK"]
    generated_files: list[GeneratedFile]
    assumptions: list[str] = Field(default_factory=list)
    manual_actions: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)


class ConversionAgent:
    AGENT_NAME = "CONVERSION_AGENT"
    AGENT_VERSION = "1.0.0"
    STAGE_NAME = "CODE_CONVERSION"
    MODEL_NAME = "gpt-4o"
    PROMPT_NAME = "INFORMATICA_TO_DATABRICKS_PYSPARK"
    PROMPT_VERSION = "1.0.0"

    def __init__(self) -> None:
        self.model_name = self.MODEL_NAME
        self.prompt_name = self.PROMPT_NAME
        self.prompt_version = self.PROMPT_VERSION
        self.input_tokens = 0
        self.output_tokens = 0

    async def run(
        self,
        *,
        mapping: CanonicalMapping,
        discovery: dict,
        lineage: dict,
        dependencies: dict,
        rag: RAGRetrievalResult,
        critique_feedback: list[dict] | None = None,
    ) -> ConversionResult:
        evidence = {
            "canonical_mapping": mapping.model_dump(mode="json"),
            "discovery": discovery,
            "lineage": lineage,
            "dependencies": dependencies,
            "approved_knowledge": rag.model_dump(mode="json"),
            "critique_feedback": critique_feedback or [],
            "requirements": {
                "target_platform": "DATABRICKS",
                "target_framework": "PYSPARK",
                "files": ["PYSPARK_CODE", "UNIT_TEST", "CONFIGURATION"],
            },
        }
        prompt = """You convert one Informatica mapping into production-oriented
Databricks PySpark. Use only supplied evidence. Preserve filters, expressions,
field names, decimal precision, null behavior, and source-to-target semantics.
Do not include credentials, secrets, markdown fences, TODO markers, ellipses, or
invented datasets. Produce exactly one Python implementation, one pytest file,
and one JSON configuration file. The Python must parse as valid Python. Return
only the requested structured output."""
        llm = DynamicChatOpenAI(
            operation_name=f"conversion:{mapping.source_object_key}"
        )
        response = await llm.with_structured_output(
            ConversionResult,
            include_raw=True,
        ).ainvoke([
            ("system", prompt),
            ("human", json.dumps(evidence, sort_keys=True, default=str)),
        ])
        parsed = response.get("parsed") if isinstance(response, dict) else response
        raw = response.get("raw") if isinstance(response, dict) else None
        if parsed is None:
            raise RuntimeError("GPT-4o returned no valid structured conversion result.")
        for item in parsed.generated_files:
            item.content = self._strip_fences(item.content)
        self._capture_usage(raw)
        return parsed

    @staticmethod
    def _strip_fences(content: str) -> str:
        value = content.strip()
        value = re.sub(r"^```(?:python|json)?\s*", "", value, flags=re.I)
        value = re.sub(r"\s*```$", "", value)
        return value.strip() + "\n"

    def _capture_usage(self, raw) -> None:
        usage = getattr(raw, "usage_metadata", None) or {}
        metadata = getattr(raw, "response_metadata", None) or {}
        token_usage = metadata.get("token_usage", {})
        self.input_tokens = int(usage.get("input_tokens") or token_usage.get("prompt_tokens") or 0)
        self.output_tokens = int(usage.get("output_tokens") or token_usage.get("completion_tokens") or 0)
        self.model_name = metadata.get("model_name") or metadata.get("model") or self.MODEL_NAME
