"""Parser for IBM DataStage DSX exports."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
from pathlib import Path
import re
import shlex
from typing import Any, Iterable

from etl_cc.models import (
    CanonicalConnector,
    CanonicalDataset,
    CanonicalField,
    CanonicalMapping,
    CanonicalParameter,
    CanonicalPort,
    CanonicalTransformation,
    MappingSummary,
)


logger = logging.getLogger(__name__)


class DataStageParserError(ValueError):
    """Raised when a DSX export cannot be parsed or validated."""


@dataclass
class _DSXNode:
    kind: str
    values: dict[str, str] = field(default_factory=dict)
    children: list["_DSXNode"] = field(default_factory=list)

    @property
    def name(self) -> str | None:
        for key in ("NAME", "JOBNAME", "STAGENAME", "IDENTIFIER", "ID"):
            value = self.values.get(key)
            if value:
                return value
        return None


class DataStageParser:
    """Convert DataStage DSX jobs into the shared canonical mapping model."""

    _BEGIN = re.compile(r"^BEGIN\s+([A-Za-z0-9_]+)\s*$", re.IGNORECASE)
    _END = re.compile(r"^END(?:\s+([A-Za-z0-9_]+))?\s*$", re.IGNORECASE)
    _KEY_VALUE = re.compile(
        r"^([A-Za-z_][A-Za-z0-9_.-]*)\s*(?:=|\s)\s*(.*?)\s*;?$"
    )
    _JOB_KINDS = {"DSJOB", "DSJOBDEF"}
    _STAGE_KINDS = {"DSSTAGE"}
    _LINK_KINDS = {"DSLINK"}
    _PARAMETER_KINDS = {"DSPARAMETER"}
    _SEQUENCE_KINDS = {"DSSEQUENCE"}

    def parse(
        self,
        content: bytes | str | Path,
        source_reference: str = "upload.dsx",
        selected_keys: set[str] | None = None,
        source_vendor: str = "DATASTAGE_DSX",
    ) -> list[CanonicalMapping]:
        """Parse all or selected DSX jobs into canonical mappings."""
        root = self._parse_document(self._read_content(content))
        jobs = self.extract_jobs(root)
        results: list[CanonicalMapping] = []
        for job in jobs:
            mapping = self._build_mapping(
                job, source_reference, source_vendor
            )
            if selected_keys is None or mapping.source_object_key in selected_keys:
                results.append(mapping)
        if not jobs:
            raise DataStageParserError("The DSX export contains no DSJOB blocks.")
        logger.info("Parsed %d DataStage jobs from %s", len(results), source_reference)
        return results

    def list_mappings_bytes(
        self, content: bytes, source_reference: str
    ) -> list[MappingSummary]:
        """List DataStage jobs as selectable mapping summaries."""
        root = self._parse_document(content)
        return [
            MappingSummary(
                source_object_key=self._job_key(job),
                mapping_name=self._job_name(job),
                folder_name=self._job_folder(job),
                object_type="JOB",
                source_reference=source_reference,
            )
            for job in self.extract_jobs(root)
        ]

    def list_mappings(
        self, file_path: Path, source_reference: str
    ) -> list[MappingSummary]:
        """List DataStage jobs from a DSX file."""
        return self.list_mappings_bytes(file_path.read_bytes(), source_reference)

    def parse_selected_bytes(
        self,
        content: bytes,
        selected_keys: set[str] | None,
        source_vendor: str,
        source_reference: str,
    ) -> list[CanonicalMapping]:
        """Parse selected DataStage jobs from uploaded bytes."""
        return self.parse(content, source_reference, selected_keys, source_vendor)

    def parse_selected(
        self,
        file_path: Path,
        selected_keys: set[str] | None,
        source_vendor: str,
        source_reference: str,
    ) -> list[CanonicalMapping]:
        """Parse selected DataStage jobs from a file."""
        return self.parse(file_path, source_reference, selected_keys, source_vendor)

    def extract_jobs(self, root: Iterable[_DSXNode]) -> list[_DSXNode]:
        """Return top-level DSJOB blocks, including nested DSX records."""
        jobs = [node for node in root if node.kind in self._JOB_KINDS]
        return jobs

    def extract_stages(self, job: _DSXNode) -> list[dict[str, Any]]:
        """Extract stages and their column metadata from one job."""
        stages: list[dict[str, Any]] = []
        for node in self._descendants(job):
            if not self._is_stage(node):
                continue
            stage_type = self._first(node, "STAGETYPE", "TYPE", "OLETYPE") or "UNKNOWN"
            stages.append(
                {
                    "name": self._node_name(node),
                    "stage_type": stage_type,
                    "properties": dict(node.values),
                    "columns": self._extract_columns(node),
                    "role": self._stage_role(stage_type, node.values),
                }
            )
        return self._unique_by_name(stages)

    def extract_links(self, job: _DSXNode) -> list[dict[str, Any]]:
        """Extract stage links and optional column mappings."""
        links: list[dict[str, Any]] = []
        for node in self._descendants(job):
            if not self._is_link(node):
                continue
            links.append(
                {
                    "from_instance": self._first(
                        node, "FROMSTAGE", "FROMINSTANCE", "FROM"
                    ) or "UNKNOWN",
                    "to_instance": self._first(
                        node, "TOSTAGE", "TOINSTANCE", "TO"
                    ) or "UNKNOWN",
                    "from_field": self._first(
                        node, "FROMFIELD", "FROMCOLUMN", "SOURCECOLUMN"
                    ),
                    "to_field": self._first(
                        node, "TOFIELD", "TOCOLUMN", "TARGETCOLUMN"
                    ),
                    "properties": dict(node.values),
                }
            )
        return links

    def extract_transformers(self, job: _DSXNode) -> list[dict[str, Any]]:
        """Extract transformer derivations from stage and column records."""
        transformers: list[dict[str, Any]] = []
        for stage in self.extract_stages(job):
            if stage["role"] == "SOURCE" or stage["role"] == "TARGET":
                continue
            derivations = [
                column
                for column in stage["columns"]
                if column.get("expression")
            ]
            if derivations or "TRANSFORM" in stage["stage_type"].upper():
                transformers.append(
                    {
                        "name": stage["name"],
                        "transformation_type": stage["stage_type"],
                        "derivations": derivations,
                        "properties": stage["properties"],
                    }
                )
        return transformers

    def extract_parameters(self, job: _DSXNode) -> list[dict[str, Any]]:
        """Extract job and DSRECORD parameter definitions."""
        parameters: list[dict[str, Any]] = []
        for node in self._descendants(job):
            if not self._is_parameter(node):
                continue
            parameters.append(
                {
                    "name": self._node_name(node),
                    "parameter_type": "DSPARAMETER",
                    "data_type": self._first(node, "DATATYPE", "TYPE"),
                    "default_value": self._first(
                        node, "DEFAULTVALUE", "DEFAULT", "VALUE"
                    ),
                    "scope": self._first(node, "SCOPE") or "JOB",
                    "properties": dict(node.values),
                }
            )
        return parameters

    def extract_sequences(self, job: _DSXNode) -> list[dict[str, Any]]:
        """Extract sequence jobs and their activities."""
        sequences: list[dict[str, Any]] = []
        for node in self._descendants(job):
            if node.kind not in self._SEQUENCE_KINDS and "SEQUENCE" not in self._upper_type(node):
                continue
            activities = [
                dict(activity.values, name=self._node_name(activity))
                for activity in self._descendants(node)
                if "ACTIVITY" in activity.kind or "ACTIVITY" in self._upper_type(activity)
            ]
            sequences.append(
                {
                    "name": self._node_name(node),
                    "properties": dict(node.values),
                    "activities": activities,
                }
            )
        return sequences

    def extract_constraints(self, job: _DSXNode) -> list[dict[str, Any]]:
        """Extract stage and job constraints without losing DSX attributes."""
        return self._extract_constraints(job)

    def extract_activities(self, job: _DSXNode) -> list[dict[str, Any]]:
        """Extract sequence activities and their execution attributes."""
        return self._activities(job)

    def build_lineage(self, links: list[dict[str, Any]]) -> list[CanonicalConnector]:
        """Normalize DSX links into the canonical connector contract."""
        return [
            CanonicalConnector(
                from_instance=item.get("from_instance", "UNKNOWN"),
                to_instance=item.get("to_instance", "UNKNOWN"),
                from_field=item.get("from_field"),
                to_field=item.get("to_field"),
            )
            for item in links
        ]

    def _build_mapping(
        self, job: _DSXNode, source_reference: str, source_vendor: str
    ) -> CanonicalMapping:
        stages = self.extract_stages(job)
        links = self.extract_links(job)
        sequences = self.extract_sequences(job)
        parameters = self.extract_parameters(job)
        transformations = self.extract_transformers(job)
        mapping_name = self._job_name(job)
        source_stages = [stage for stage in stages if stage["role"] == "SOURCE"]
        target_stages = [stage for stage in stages if stage["role"] == "TARGET"]

        return CanonicalMapping(
            source_vendor=source_vendor,
            source_object_key=self._job_key(job),
            mapping_name=mapping_name,
            folder_name=self._job_folder(job),
            description=self._first(job, "DESCRIPTION", "DESC"),
            sources=[self._dataset(stage, "SOURCE") for stage in source_stages],
            targets=[self._dataset(stage, "TARGET") for stage in target_stages],
            transformations=[self._transformation(item) for item in transformations],
            connectors=self.build_lineage(links),
            sessions=self._activities(job),
            workflows=sequences,
            parameters=[CanonicalParameter(**{
                key: value for key, value in item.items()
                if key in {"name", "parameter_type", "data_type", "default_value", "scope"}
            }) for item in parameters],
            source_metadata={
                "object_type": "JOB",
                "job_attributes": dict(job.values),
                "source_reference": source_reference,
                "constraints": self._extract_constraints(job),
                "activities": self._activities(job),
                "sequences": sequences,
                "stage_count": len(stages),
                "link_count": len(links),
            },
        )

    def _dataset(self, stage: dict[str, Any], dataset_type: str) -> CanonicalDataset:
        return CanonicalDataset(
            name=stage["name"],
            dataset_type=dataset_type,
            fields=[CanonicalField(
                name=column.get("name", "UNKNOWN"),
                data_type=column.get("data_type") or "UNKNOWN",
                precision=self._integer(column.get("precision")),
                scale=self._integer(column.get("scale")),
                nullable=self._nullable(column.get("nullable")),
            ) for column in stage["columns"]],
            properties=stage["properties"],
        )

    def _transformation(self, item: dict[str, Any]) -> CanonicalTransformation:
        return CanonicalTransformation(
            name=item["name"],
            transformation_type=item["transformation_type"],
            ports=[CanonicalPort(
                name=column.get("name", "UNKNOWN"),
                direction=column.get("direction") or "OUTPUT",
                data_type=column.get("data_type"),
                expression=column.get("expression"),
                properties=column.get("properties", {}),
            ) for column in item["derivations"]],
            properties=item["properties"],
        )

    def _extract_columns(self, node: _DSXNode) -> list[dict[str, Any]]:
        columns: list[dict[str, Any]] = []
        for child in self._descendants(node):
            if child is node or not self._is_column(child):
                continue
            columns.append({
                "name": self._node_name(child),
                "data_type": self._first(child, "DATATYPE", "TYPE"),
                "precision": self._first(child, "PRECISION", "LENGTH"),
                "scale": self._first(child, "SCALE"),
                "nullable": self._first(child, "NULLABLE"),
                "direction": self._first(child, "PORTTYPE", "DIRECTION"),
                "expression": self._first(
                    child, "DERIVATION", "EXPRESSION", "DERIVATIONEXPRESSION"
                ),
                "properties": dict(child.values),
            })
        return self._unique_by_name(columns)

    def _extract_constraints(self, job: _DSXNode) -> list[dict[str, Any]]:
        return [dict(node.values, name=self._node_name(node))
                for node in self._descendants(job)
                if "CONSTRAINT" in node.kind or "CONSTRAINT" in self._upper_type(node)]

    def _activities(self, job: _DSXNode) -> list[dict[str, Any]]:
        return [dict(node.values, name=self._node_name(node))
                for node in self._descendants(job)
                if "ACTIVITY" in node.kind or "ACTIVITY" in self._upper_type(node)]

    @classmethod
    def _parse_document(cls, content: bytes | str) -> list[_DSXNode]:
        if isinstance(content, bytes):
            text = content.decode("utf-8-sig", errors="replace")
        else:
            text = content
        roots: list[_DSXNode] = []
        stack: list[_DSXNode] = []
        for line_number, raw_line in enumerate(text.splitlines(), 1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            begin = cls._BEGIN.match(line)
            if begin:
                node = _DSXNode(begin.group(1).upper())
                if stack:
                    stack[-1].children.append(node)
                else:
                    roots.append(node)
                stack.append(node)
                continue
            end = cls._END.match(line)
            if end:
                if not stack:
                    raise DataStageParserError(f"Unexpected {line} at line {line_number}.")
                expected = end.group(1)
                if expected and stack[-1].kind != expected.upper():
                    raise DataStageParserError(
                        f"Mismatched {line}; expected END {stack[-1].kind} at line {line_number}."
                    )
                stack.pop()
                continue
            if not stack:
                continue
            match = cls._KEY_VALUE.match(line)
            if not match:
                logger.debug("Ignoring unrecognised DSX line %d", line_number)
                continue
            key, raw_value = match.groups()
            stack[-1].values[key.upper()] = cls._value(raw_value)
        if stack:
            raise DataStageParserError(f"Unclosed DSX block: {stack[-1].kind}.")
        return roots

    @staticmethod
    def _read_content(content: bytes | str | Path) -> bytes | str:
        if isinstance(content, Path):
            try:
                return content.read_bytes()
            except OSError as exc:
                raise DataStageParserError("Unable to read the DSX file.") from exc
        if not content:
            raise DataStageParserError("The DSX file is empty.")
        return content

    @staticmethod
    def _value(value: str) -> str:
        try:
            values = shlex.split(value, posix=True)
        except ValueError:
            return value.strip().strip('"')
        return values[0] if len(values) == 1 else " ".join(values)

    @staticmethod
    def _descendants(node: _DSXNode) -> Iterable[_DSXNode]:
        for child in node.children:
            yield child
            yield from DataStageParser._descendants(child)

    @classmethod
    def _first(cls, node: _DSXNode, *keys: str) -> str | None:
        for key in keys:
            value = node.values.get(key.upper())
            if value:
                return value
        return None

    @classmethod
    def _node_name(cls, node: _DSXNode) -> str:
        return node.name or cls._first(node, "VALUE") or "UNKNOWN"

    @classmethod
    def _job_name(cls, job: _DSXNode) -> str:
        return cls._node_name(job)

    @classmethod
    def _job_folder(cls, job: _DSXNode) -> str | None:
        return cls._first(job, "FOLDER", "PROJECT", "CATEGORY")

    @classmethod
    def _job_key(cls, job: _DSXNode) -> str:
        return f"{cls._job_folder(job) or 'DEFAULT'}/DSJOB/{cls._job_name(job)}"

    @classmethod
    def _upper_type(cls, node: _DSXNode) -> str:
        return " ".join(
            value.upper() for key, value in node.values.items()
            if key in {"TYPE", "OLETYPE", "RECORDTYPE", "CLASS"}
        )

    @classmethod
    def _is_stage(cls, node: _DSXNode) -> bool:
        return node.kind in cls._STAGE_KINDS or "STAGE" in cls._upper_type(node)

    @classmethod
    def _is_link(cls, node: _DSXNode) -> bool:
        return node.kind in cls._LINK_KINDS or "LINK" in cls._upper_type(node)

    @classmethod
    def _is_parameter(cls, node: _DSXNode) -> bool:
        return node.kind in cls._PARAMETER_KINDS or "PARAMETER" in cls._upper_type(node)

    @classmethod
    def _is_column(cls, node: _DSXNode) -> bool:
        return node.kind in {"DSCOLUMN", "DSFIELD", "DSPORT", "DSOUTPUT"} or any(
            key in node.values for key in ("DATATYPE", "DERIVATION", "EXPRESSION")
        )

    @staticmethod
    def _stage_role(stage_type: str, values: dict[str, str]) -> str:
        text = f"{stage_type} {values.get('ROLE', '')}".upper()
        if any(token in text for token in ("SEQUENTIALFILE", "ODBC", "JDBC", "SOURCE", "INPUT")):
            return "SOURCE"
        if any(token in text for token in ("TARGET", "OUTPUT", "DATASET")):
            return "TARGET"
        return "TRANSFORM"

    @staticmethod
    def _nullable(value: str | None) -> bool:
        return (value or "YES").strip().upper() not in {"NO", "FALSE", "0", "NOT NULL"}

    @staticmethod
    def _integer(value: Any) -> int | None:
        try:
            return int(value) if value not in (None, "") else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _unique_by_name(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in items:
            name = str(item.get("name", "UNKNOWN"))
            if name not in seen:
                result.append(item)
                seen.add(name)
        return result
