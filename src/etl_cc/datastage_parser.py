# """Parser for IBM DataStage DSX exports."""

# from __future__ import annotations

# from dataclasses import dataclass, field
# import logging
# from pathlib import Path
# import re
# import shlex
# from typing import Any, Iterable

# from etl_cc.models import (
#     CanonicalConnector,
#     CanonicalDataset,
#     CanonicalField,
#     CanonicalMapping,
#     CanonicalParameter,
#     CanonicalPort,
#     CanonicalTransformation,
#     MappingSummary,
# )


# logger = logging.getLogger(__name__)


# class DataStageParserError(ValueError):
#     """Raised when a DSX export cannot be parsed or validated."""


# @dataclass
# class _DSXNode:
#     kind: str
#     values: dict[str, str] = field(default_factory=dict)
#     children: list["_DSXNode"] = field(default_factory=list)

#     @property
#     def name(self) -> str | None:
#         for key in ("NAME", "JOBNAME", "STAGENAME", "IDENTIFIER", "ID"):
#             value = self.values.get(key)
#             if value:
#                 return value
#         return None


# class DataStageParser:
#     """Convert DataStage DSX jobs into the shared canonical mapping model."""

#     _BEGIN = re.compile(r"^BEGIN\s+([A-Za-z0-9_]+)\s*$", re.IGNORECASE)
#     _END = re.compile(r"^END(?:\s+([A-Za-z0-9_]+))?\s*$", re.IGNORECASE)
#     _KEY_VALUE = re.compile(
#         r"^([A-Za-z_][A-Za-z0-9_.-]*)\s*(?:=|\s)\s*(.*?)\s*;?$"
#     )
#     _JOB_KINDS = {"DSJOB", "DSJOBDEF"}
#     _STAGE_KINDS = {"DSSTAGE"}
#     _LINK_KINDS = {"DSLINK"}
#     _PARAMETER_KINDS = {"DSPARAMETER"}
#     _SEQUENCE_KINDS = {"DSSEQUENCE"}

#     def parse(
#         self,
#         content: bytes | str | Path,
#         source_reference: str = "upload.dsx",
#         selected_keys: set[str] | None = None,
#         source_vendor: str = "DATASTAGE_DSX",
#     ) -> list[CanonicalMapping]:
#         """Parse all or selected DSX jobs into canonical mappings."""
#         root = self._parse_document(self._read_content(content))
#         jobs = self.extract_jobs(root)
#         results: list[CanonicalMapping] = []
#         for job in jobs:
#             mapping = self._build_mapping(
#                 job, source_reference, source_vendor
#             )
#             if selected_keys is None or mapping.source_object_key in selected_keys:
#                 results.append(mapping)
#         if not jobs:
#             raise DataStageParserError("The DSX export contains no DSJOB blocks.")
#         logger.info("Parsed %d DataStage jobs from %s", len(results), source_reference)
#         return results

#     def list_mappings_bytes(
#         self, content: bytes, source_reference: str
#     ) -> list[MappingSummary]:
#         """List DataStage jobs as selectable mapping summaries."""
#         root = self._parse_document(content)
#         return [
#             MappingSummary(
#                 source_object_key=self._job_key(job),
#                 mapping_name=self._job_name(job),
#                 folder_name=self._job_folder(job),
#                 object_type="JOB",
#                 source_reference=source_reference,
#             )
#             for job in self.extract_jobs(root)
#         ]

#     def list_mappings(
#         self, file_path: Path, source_reference: str
#     ) -> list[MappingSummary]:
#         """List DataStage jobs from a DSX file."""
#         return self.list_mappings_bytes(file_path.read_bytes(), source_reference)

#     def parse_selected_bytes(
#         self,
#         content: bytes,
#         selected_keys: set[str] | None,
#         source_vendor: str,
#         source_reference: str,
#     ) -> list[CanonicalMapping]:
#         """Parse selected DataStage jobs from uploaded bytes."""
#         return self.parse(content, source_reference, selected_keys, source_vendor)

#     def parse_selected(
#         self,
#         file_path: Path,
#         selected_keys: set[str] | None,
#         source_vendor: str,
#         source_reference: str,
#     ) -> list[CanonicalMapping]:
#         """Parse selected DataStage jobs from a file."""
#         return self.parse(file_path, source_reference, selected_keys, source_vendor)

#     def extract_jobs(self, root: Iterable[_DSXNode]) -> list[_DSXNode]:
#         """Return top-level DSJOB blocks, including nested DSX records."""
#         jobs = [node for node in root if node.kind in self._JOB_KINDS]
#         return jobs

#     def extract_stages(self, job: _DSXNode) -> list[dict[str, Any]]:
#         """Extract stages and their column metadata from one job."""
#         stages: list[dict[str, Any]] = []
#         for node in self._descendants(job):
#             if not self._is_stage(node):
#                 continue
#             stage_type = self._first(node, "STAGETYPE", "OLETYPE", "TYPE") or "UNKNOWN"
#             stages.append(
#                 {
#                     "name": self._node_name(node),
#                     "stage_type": stage_type,
#                     "properties": dict(node.values),
#                     "columns": self._extract_columns(node),
#                     "role": self._stage_role(stage_type, node.values),
#                 }
#             )
#         return self._unique_by_name(stages)

#     def extract_links(self, job: _DSXNode) -> list[dict[str, Any]]:
#         """Extract stage links and optional column mappings."""
#         links: list[dict[str, Any]] = []
#         for node in self._descendants(job):
#             if not self._is_link(node):
#                 continue
#             links.append(
#                 {
#                     "from_instance": self._first(
#                         node, "FROMSTAGE", "FROMINSTANCE", "FROM"
#                     ) or "UNKNOWN",
#                     "to_instance": self._first(
#                         node, "TOSTAGE", "TOINSTANCE", "TO"
#                     ) or "UNKNOWN",
#                     "from_field": self._first(
#                         node, "FROMFIELD", "FROMCOLUMN", "SOURCECOLUMN"
#                     ),
#                     "to_field": self._first(
#                         node, "TOFIELD", "TOCOLUMN", "TARGETCOLUMN"
#                     ),
#                     "properties": dict(node.values),
#                 }
#             )
#         return links

#     def extract_transformers(self, job: _DSXNode) -> list[dict[str, Any]]:
#         """Extract transformer derivations from stage and column records."""
#         transformers: list[dict[str, Any]] = []
#         for stage in self.extract_stages(job):
#             if stage["role"] != "TRANSFORM":
#                 continue
#             derivations = [
#                 column
#                 for column in stage["columns"]
#                 if column.get("expression")
#             ]
#             transformers.append(
#                 {
#                     "name": stage["name"],
#                     "transformation_type": stage["stage_type"],
#                     "derivations": derivations,
#                     "properties": stage["properties"],
#                 }
#             )
#         return transformers

#     def extract_parameters(self, job: _DSXNode) -> list[dict[str, Any]]:
#         """Extract job and DSRECORD parameter definitions."""
#         parameters: list[dict[str, Any]] = []
#         for node in self._descendants(job):
#             if not self._is_parameter(node):
#                 continue
#             parameters.append(
#                 {
#                     "name": self._node_name(node),
#                     "parameter_type": "DSPARAMETER",
#                     "data_type": self._first(node, "DATATYPE", "TYPE"),
#                     "default_value": self._first(
#                         node, "DEFAULTVALUE", "DEFAULT", "VALUE"
#                     ),
#                     "scope": self._first(node, "SCOPE") or "JOB",
#                     "properties": dict(node.values),
#                 }
#             )
#         return parameters

#     def extract_sequences(self, job: _DSXNode) -> list[dict[str, Any]]:
#         """Extract sequence jobs and their activities."""
#         sequences: list[dict[str, Any]] = []
#         for node in self._descendants(job):
#             if node.kind not in self._SEQUENCE_KINDS and "SEQUENCE" not in self._upper_type(node):
#                 continue
#             activities = [
#                 dict(activity.values, name=self._node_name(activity))
#                 for activity in self._descendants(node)
#                 if "ACTIVITY" in activity.kind or "ACTIVITY" in self._upper_type(activity)
#             ]
#             sequences.append(
#                 {
#                     "name": self._node_name(node),
#                     "properties": dict(node.values),
#                     "activities": activities,
#                 }
#             )
#         return sequences

#     def extract_constraints(self, job: _DSXNode) -> list[dict[str, Any]]:
#         """Extract stage and job constraints without losing DSX attributes."""
#         return self._extract_constraints(job)

#     def extract_activities(self, job: _DSXNode) -> list[dict[str, Any]]:
#         """Extract sequence activities and their execution attributes."""
#         return self._activities(job)

#     def build_lineage(self, links: list[dict[str, Any]]) -> list[CanonicalConnector]:
#         """Normalize DSX links into the canonical connector contract."""
#         return [
#             CanonicalConnector(
#                 from_instance=item.get("from_instance", "UNKNOWN"),
#                 to_instance=item.get("to_instance", "UNKNOWN"),
#                 from_field=item.get("from_field"),
#                 to_field=item.get("to_field"),
#             )
#             for item in links
#         ]

#     def _build_mapping(
#         self, job: _DSXNode, source_reference: str, source_vendor: str
#     ) -> CanonicalMapping:
#         stages = self.extract_stages(job)
#         links = self.extract_links(job)
#         sequences = self.extract_sequences(job)
#         parameters = self.extract_parameters(job)
#         transformations = self.extract_transformers(job)
#         mapping_name = self._job_name(job)
#         source_stages = [stage for stage in stages if stage["role"] == "SOURCE"]
#         target_stages = [stage for stage in stages if stage["role"] == "TARGET"]

#         return CanonicalMapping(
#             source_vendor=source_vendor,
#             source_object_key=self._job_key(job),
#             mapping_name=mapping_name,
#             folder_name=self._job_folder(job),
#             description=self._first(job, "DESCRIPTION", "DESC"),
#             sources=[self._dataset(stage, "SOURCE") for stage in source_stages],
#             targets=[self._dataset(stage, "TARGET") for stage in target_stages],
#             transformations=[self._transformation(item) for item in transformations],
#             connectors=self.build_lineage(links),
#             sessions=self._activities(job),
#             workflows=sequences,
#             parameters=[CanonicalParameter(**{
#                 key: value for key, value in item.items()
#                 if key in {"name", "parameter_type", "data_type", "default_value", "scope"}
#             }) for item in parameters],
#             source_metadata={
#                 "object_type": "JOB",
#                 "job_attributes": dict(job.values),
#                 "source_reference": source_reference,
#                 "constraints": self._extract_constraints(job),
#                 "activities": self._activities(job),
#                 "sequences": sequences,
#                 "stage_count": len(stages),
#                 "stage_inventory": [
#                     {
#                         "name": stage["name"],
#                         "stage_type": stage["stage_type"],
#                         "role": stage["role"],
#                     }
#                     for stage in stages
#                 ],
#                 "link_count": len(links),
#             },
#         )

#     def _dataset(self, stage: dict[str, Any], dataset_type: str) -> CanonicalDataset:
#         return CanonicalDataset(
#             name=stage["name"],
#             dataset_type=dataset_type,
#             fields=[CanonicalField(
#                 name=column.get("name", "UNKNOWN"),
#                 data_type=column.get("data_type") or "UNKNOWN",
#                 precision=self._integer(column.get("precision")),
#                 scale=self._integer(column.get("scale")),
#                 nullable=self._nullable(column.get("nullable")),
#             ) for column in stage["columns"]],
#             properties=stage["properties"],
#         )

#     def _transformation(self, item: dict[str, Any]) -> CanonicalTransformation:
#         return CanonicalTransformation(
#             name=item["name"],
#             transformation_type=item["transformation_type"],
#             ports=[CanonicalPort(
#                 name=column.get("name", "UNKNOWN"),
#                 direction=column.get("direction") or "OUTPUT",
#                 data_type=column.get("data_type"),
#                 expression=column.get("expression"),
#                 properties=column.get("properties", {}),
#             ) for column in item["derivations"]],
#             properties=item["properties"],
#         )

#     def _extract_columns(self, node: _DSXNode) -> list[dict[str, Any]]:
#         columns: list[dict[str, Any]] = []
#         for child in self._descendants(node):
#             if child is node or not self._is_column(child):
#                 continue
#             columns.append({
#                 "name": self._node_name(child),
#                 "data_type": self._first(child, "DATATYPE", "TYPE"),
#                 "precision": self._first(child, "PRECISION", "LENGTH"),
#                 "scale": self._first(child, "SCALE"),
#                 "nullable": self._first(child, "NULLABLE"),
#                 "direction": self._first(child, "PORTTYPE", "DIRECTION"),
#                 "expression": self._first(
#                     child, "DERIVATION", "EXPRESSION", "DERIVATIONEXPRESSION"
#                 ),
#                 "properties": dict(child.values),
#             })
#         return self._unique_by_name(columns)

#     def _extract_constraints(self, job: _DSXNode) -> list[dict[str, Any]]:
#         return [dict(node.values, name=self._node_name(node))
#                 for node in self._descendants(job)
#                 if "CONSTRAINT" in node.kind or "CONSTRAINT" in self._upper_type(node)]

#     def _activities(self, job: _DSXNode) -> list[dict[str, Any]]:
#         return [dict(node.values, name=self._node_name(node))
#                 for node in self._descendants(job)
#                 if "ACTIVITY" in node.kind or "ACTIVITY" in self._upper_type(node)]

#     @classmethod
#     def _parse_document(cls, content: bytes | str) -> list[_DSXNode]:
#         if isinstance(content, bytes):
#             text = content.decode("utf-8-sig", errors="replace")
#         else:
#             text = content
#         roots: list[_DSXNode] = []
#         stack: list[_DSXNode] = []
#         for line_number, raw_line in enumerate(text.splitlines(), 1):
#             line = raw_line.strip()
#             if not line or line.startswith("#"):
#                 continue
#             begin = cls._BEGIN.match(line)
#             if begin:
#                 node = _DSXNode(begin.group(1).upper())
#                 if stack:
#                     stack[-1].children.append(node)
#                 else:
#                     roots.append(node)
#                 stack.append(node)
#                 continue
#             end = cls._END.match(line)
#             if end:
#                 if not stack:
#                     raise DataStageParserError(f"Unexpected {line} at line {line_number}.")
#                 expected = end.group(1)
#                 if expected and stack[-1].kind != expected.upper():
#                     raise DataStageParserError(
#                         f"Mismatched {line}; expected END {stack[-1].kind} at line {line_number}."
#                     )
#                 stack.pop()
#                 continue
#             if not stack:
#                 continue
#             match = cls._KEY_VALUE.match(line)
#             if not match:
#                 logger.debug("Ignoring unrecognised DSX line %d", line_number)
#                 continue
#             key, raw_value = match.groups()
#             stack[-1].values[key.upper()] = cls._value(raw_value)
#         if stack:
#             raise DataStageParserError(f"Unclosed DSX block: {stack[-1].kind}.")
#         return roots

#     @staticmethod
#     def _read_content(content: bytes | str | Path) -> bytes | str:
#         if isinstance(content, Path):
#             try:
#                 return content.read_bytes()
#             except OSError as exc:
#                 raise DataStageParserError("Unable to read the DSX file.") from exc
#         if not content:
#             raise DataStageParserError("The DSX file is empty.")
#         return content

#     @staticmethod
#     def _value(value: str) -> str:
#         try:
#             values = shlex.split(value, posix=True)
#         except ValueError:
#             return value.strip().strip('"')
#         return values[0] if len(values) == 1 else " ".join(values)

#     @staticmethod
#     def _descendants(node: _DSXNode) -> Iterable[_DSXNode]:
#         for child in node.children:
#             yield child
#             yield from DataStageParser._descendants(child)

#     @classmethod
#     def _first(cls, node: _DSXNode, *keys: str) -> str | None:
#         for key in keys:
#             value = node.values.get(key.upper())
#             if value:
#                 return value
#         return None

#     @classmethod
#     def _node_name(cls, node: _DSXNode) -> str:
#         return node.name or cls._first(node, "VALUE") or "UNKNOWN"

#     @classmethod
#     def _job_name(cls, job: _DSXNode) -> str:
#         return cls._node_name(job)

#     @classmethod
#     def _job_folder(cls, job: _DSXNode) -> str | None:
#         return cls._first(job, "FOLDER", "PROJECT", "CATEGORY")

#     @classmethod
#     def _job_key(cls, job: _DSXNode) -> str:
#         return f"{cls._job_folder(job) or 'DEFAULT'}/DSJOB/{cls._job_name(job)}"

#     @classmethod
#     def _upper_type(cls, node: _DSXNode) -> str:
#         return " ".join(
#             value.upper() for key, value in node.values.items()
#             if key in {"TYPE", "OLETYPE", "STAGETYPE", "RECORDTYPE", "CLASS"}
#         )

#     @classmethod
#     def _is_stage(cls, node: _DSXNode) -> bool:
#         if node.kind in cls._STAGE_KINDS:
#             return True
#         if node.kind == "DSRECORD":
#             return bool(
#                 cls._first(node, "IDENTIFIER", "STAGENAME", "NAME")
#                 and cls._first(node, "OLETYPE", "STAGETYPE", "TYPE")
#             )
#         return "STAGE" in cls._upper_type(node)

#     @classmethod
#     def _is_link(cls, node: _DSXNode) -> bool:
#         return node.kind in cls._LINK_KINDS or "LINK" in cls._upper_type(node)

#     @classmethod
#     def _is_parameter(cls, node: _DSXNode) -> bool:
#         return node.kind in cls._PARAMETER_KINDS or "PARAMETER" in cls._upper_type(node)

#     @classmethod
#     def _is_column(cls, node: _DSXNode) -> bool:
#         return node.kind in {"DSCOLUMN", "DSFIELD", "DSPORT", "DSOUTPUT"} or any(
#             key in node.values for key in ("DATATYPE", "DERIVATION", "EXPRESSION")
#         )

#     @staticmethod
#     def _stage_role(stage_type: str, values: dict[str, str]) -> str:
#         stage_type_text = stage_type.upper()
#         direction = " ".join(
#             values.get(key, "")
#             for key in (
#                 "ROLE", "STAGEDIRECTION", "DIRECTION", "OPERATION", "MODE",
#                 "ACCESSMODE",
#             )
#         ).upper()
#         if any(token in direction for token in ("SOURCE", "INPUT", "READ")):
#             return "SOURCE"
#         if any(token in direction for token in ("TARGET", "OUTPUT", "WRITE")):
#             return "TARGET"
#         if any(token in stage_type_text for token in ("SEQUENTIALFILE", "FLATFILE")):
#             return "SOURCE"
#         if "DATASET" in stage_type_text:
#             return "TARGET"
#         if any(token in stage_type_text for token in (
#             "TRANSFORM", "AGGREGATOR", "JOINER", "SORTER", "LOOKUP",
#             "FILTER", "ROUTER", "FUNNEL", "PIVOT",
#         )):
#             return "TRANSFORM"
#         if "CONNECTOR" in stage_type_text or any(token in stage_type_text for token in (
#             "ORACLE", "ODBC", "JDBC", "DB2", "TERADATA",
#         )):
#             return "CONNECTOR"
#         if any(token in stage_type_text for token in ("SOURCE", "INPUT")):
#             return "SOURCE"
#         if any(token in stage_type_text for token in ("TARGET", "OUTPUT")):
#             return "TARGET"
#         return "UNKNOWN"

#     @staticmethod
#     def _nullable(value: str | None) -> bool:
#         return (value or "YES").strip().upper() not in {"NO", "FALSE", "0", "NOT NULL"}

#     @staticmethod
#     def _integer(value: Any) -> int | None:
#         try:
#             return int(value) if value not in (None, "") else None
#         except (TypeError, ValueError):
#             return None

#     @staticmethod
#     def _unique_by_name(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
#         result: list[dict[str, Any]] = []
#         seen: set[str] = set()
#         for item in items:
#             name = str(item.get("name", "UNKNOWN"))
#             if name not in seen:
#                 result.append(item)
#                 seen.add(name)
#         return result
#---------------------------------------------------------------
# """Parser for IBM DataStage DSX exports with functional parity to Informatica XML parser."""

# from __future__ import annotations

# from dataclasses import dataclass, field
# import logging
# from pathlib import Path
# import re
# import shlex
# from typing import Any, Iterable

# from etl_cc.models import (
#     CanonicalConnector,
#     CanonicalDataset,
#     CanonicalField,
#     CanonicalMapping,
#     CanonicalParameter,
#     CanonicalPort,
#     CanonicalTransformation,
#     MappingSummary,
# )

# logger = logging.getLogger(__name__)


# class DataStageParserError(ValueError):
#     """Raised when a DSX export cannot be parsed or validated."""


# @dataclass
# class _DSXNode:
#     kind: str
#     values: dict[str, str] = field(default_factory=dict)
#     children: list["_DSXNode"] = field(default_factory=list)

#     @property
#     def name(self) -> str | None:
#         for key in ("NAME", "JOBNAME", "STAGENAME", "IDENTIFIER", "ID"):
#             value = self.values.get(key)
#             if value:
#                 return value
#         return None


# class DataStageParser:
#     """Convert DataStage DSX jobs into the shared canonical mapping model."""

#     VALID_PORT_TYPES = {"INPUT", "OUTPUT", "INPUT/OUTPUT", "VARIABLE", "RETURN"}

#     _BEGIN = re.compile(r"^BEGIN\s+([A-Za-z0-9_]+)\s*$", re.IGNORECASE)
#     _END = re.compile(r"^END(?:\s+([A-Za-z0-9_]+))?\s*$", re.IGNORECASE)
#     _KEY_VALUE = re.compile(
#         r"^([A-Za-z_][A-Za-z0-9_.-]*)\s*(?:=|\s)\s*(.*?)\s*;?$"
#     )
#     _JOB_KINDS = {"DSJOB", "DSJOBDEF"}
#     _STAGE_KINDS = {"DSSTAGE"}
#     _LINK_KINDS = {"DSLINK"}
#     _PARAMETER_KINDS = {"DSPARAMETER"}
#     _SEQUENCE_KINDS = {"DSSEQUENCE"}

#     # -------------------------------------------------------------------------
#     # Public Entrypoints (Parity with InformaticaXMLParser)
#     # -------------------------------------------------------------------------

#     def list_mappings_bytes(
#         self, content: bytes, source_reference: str
#     ) -> list[MappingSummary]:
#         """List DataStage jobs as selectable mapping summaries from raw bytes."""
#         root = self._parse_document(content)
#         return [
#             MappingSummary(
#                 source_object_key=self._job_key(job),
#                 mapping_name=self._job_name(job),
#                 folder_name=self._job_folder(job),
#                 object_type="JOB",
#                 source_reference=source_reference,
#             )
#             for job in self.extract_jobs(root)
#         ]

#     def list_mappings(
#         self, file_path: Path, source_reference: str
#     ) -> list[MappingSummary]:
#         """List DataStage jobs from a DSX file path."""
#         return self.list_mappings_bytes(file_path.read_bytes(), source_reference)

#     def parse_selected_bytes(
#         self,
#         content: bytes,
#         selected_keys: set[str] | None,
#         source_vendor: str,
#         source_reference: str,
#     ) -> list[CanonicalMapping]:
#         """Parse selected DataStage jobs directly from uploaded bytes."""
#         return self.parse(content, source_reference, selected_keys, source_vendor)

#     def parse_selected(
#         self,
#         file_path: Path,
#         selected_keys: set[str] | None,
#         source_vendor: str,
#         source_reference: str,
#     ) -> list[CanonicalMapping]:
#         """Parse selected DataStage jobs from a file path."""
#         return self.parse(file_path, source_reference, selected_keys, source_vendor)

#     def parse(
#         self,
#         content: bytes | str | Path,
#         source_reference: str = "upload.dsx",
#         selected_keys: set[str] | None = None,
#         source_vendor: str = "DATASTAGE_DSX",
#     ) -> list[CanonicalMapping]:
#         """Parse all or selected DSX jobs into canonical mappings."""
#         root = self._parse_document(self._read_content(content))
#         jobs = self.extract_jobs(root)
#         if not jobs:
#             raise DataStageParserError("The DSX export contains no DSJOB blocks.")

#         results: list[CanonicalMapping] = []
#         for job in jobs:
#             mapping = self._build_mapping(
#                 job, source_reference, source_vendor
#             )
#             if selected_keys is None or mapping.source_object_key in selected_keys:
#                 results.append(mapping)

#         logger.info("Parsed %d DataStage jobs from %s", len(results), source_reference)
#         return results

#     # -------------------------------------------------------------------------
#     # Granular DSX Extraction Methods
#     # -------------------------------------------------------------------------

#     def extract_jobs(self, root: Iterable[_DSXNode]) -> list[_DSXNode]:
#         """Return top-level DSJOB blocks, including nested DSX records."""
#         return [node for node in root if node.kind in self._JOB_KINDS]

#     def extract_stages(self, job: _DSXNode) -> list[dict[str, Any]]:
#         """Extract stages and their column metadata from one job."""
#         stages: list[dict[str, Any]] = []
#         for node in self._descendants(job):
#             if not self._is_stage(node):
#                 continue
#             stage_type = self._first(node, "STAGETYPE", "OLETYPE", "TYPE") or "UNKNOWN"
#             stages.append(
#                 {
#                     "name": self._node_name(node),
#                     "stage_type": stage_type,
#                     "properties": dict(node.values),
#                     "columns": self._extract_columns(node),
#                     "role": self._stage_role(stage_type, node.values),
#                 }
#             )
#         return self._unique_by_name(stages)

#     def extract_links(self, job: _DSXNode) -> list[dict[str, Any]]:
#         """Extract stage links and connector mappings."""
#         links: list[dict[str, Any]] = []
#         for node in self._descendants(job):
#             if not self._is_link(node):
#                 continue
#             links.append(
#                 {
#                     "from_instance": self._first(
#                         node, "FROMSTAGE", "FROMINSTANCE", "FROM"
#                     ) or "UNKNOWN",
#                     "to_instance": self._first(
#                         node, "TOSTAGE", "TOINSTANCE", "TO"
#                     ) or "UNKNOWN",
#                     "from_field": self._first(
#                         node, "FROMFIELD", "FROMCOLUMN", "SOURCECOLUMN"
#                     ),
#                     "to_field": self._first(
#                         node, "TOFIELD", "TOCOLUMN", "TARGETCOLUMN"
#                     ),
#                     "properties": dict(node.values),
#                 }
#             )
#         return links

#     def extract_transformers(self, job: _DSXNode) -> list[dict[str, Any]]:
#         """Extract all transformer stages, capturing all ports (parity with Informatica)."""
#         transformers: list[dict[str, Any]] = []
#         for stage in self.extract_stages(job):
#             if stage["role"] != "TRANSFORM":
#                 continue
#             transformers.append(
#                 {
#                     "name": stage["name"],
#                     "transformation_type": stage["stage_type"],
#                     "columns": stage["columns"],
#                     "properties": stage["properties"],
#                 }
#             )
#         return transformers

#     def extract_parameters(self, job: _DSXNode) -> list[dict[str, Any]]:
#         """Extract job and DSRECORD parameter definitions."""
#         parameters: list[dict[str, Any]] = []
#         for node in self._descendants(job):
#             if not self._is_parameter(node):
#                 continue
#             parameters.append(
#                 {
#                     "name": self._node_name(node),
#                     "parameter_type": "DSPARAMETER",
#                     "data_type": self._first(node, "DATATYPE", "TYPE"),
#                     "default_value": self._first(
#                         node, "DEFAULTVALUE", "DEFAULT", "VALUE"
#                     ),
#                     "scope": self._first(node, "SCOPE") or "JOB",
#                     "properties": dict(node.values),
#                 }
#             )
#         return parameters

#     def extract_sequences(self, job: _DSXNode) -> list[dict[str, Any]]:
#         """Extract sequence jobs and activities."""
#         sequences: list[dict[str, Any]] = []
#         for node in self._descendants(job):
#             if node.kind not in self._SEQUENCE_KINDS and "SEQUENCE" not in self._upper_type(node):
#                 continue
#             activities = [
#                 dict(activity.values, name=self._node_name(activity))
#                 for activity in self._descendants(node)
#                 if "ACTIVITY" in activity.kind or "ACTIVITY" in self._upper_type(activity)
#             ]
#             sequences.append(
#                 {
#                     "name": self._node_name(node),
#                     "properties": dict(node.values),
#                     "activities": activities,
#                 }
#             )
#         return sequences

#     def build_lineage(self, links: list[dict[str, Any]]) -> list[CanonicalConnector]:
#         """Normalize DSX links into canonical connectors."""
#         return [
#             CanonicalConnector(
#                 from_instance=item.get("from_instance", "UNKNOWN"),
#                 to_instance=item.get("to_instance", "UNKNOWN"),
#                 from_field=item.get("from_field"),
#                 to_field=item.get("to_field"),
#             )
#             for item in links
#         ]

#     # -------------------------------------------------------------------------
#     # Canonical Builders
#     # -------------------------------------------------------------------------

#     def _build_mapping(
#         self, job: _DSXNode, source_reference: str, source_vendor: str
#     ) -> CanonicalMapping:
#         stages = self.extract_stages(job)
#         links = self.extract_links(job)
#         sequences = self.extract_sequences(job)
#         parameters = self.extract_parameters(job)
#         transformations = self.extract_transformers(job)
#         mapping_name = self._job_name(job)

#         source_stages = [stage for stage in stages if stage["role"] == "SOURCE"]
#         target_stages = [stage for stage in stages if stage["role"] == "TARGET"]

#         source_instance_names = sorted([stage["name"] for stage in source_stages])
#         target_instance_names = sorted([stage["name"] for stage in target_stages])

#         return CanonicalMapping(
#             source_vendor=source_vendor,
#             source_object_key=self._job_key(job),
#             mapping_name=mapping_name,
#             folder_name=self._job_folder(job),
#             description=self._first(job, "DESCRIPTION", "DESC"),
#             sources=[self._dataset(stage, "SOURCE") for stage in source_stages],
#             targets=[self._dataset(stage, "TARGET") for stage in target_stages],
#             transformations=[self._transformation(item, mapping_name) for item in transformations],
#             connectors=self.build_lineage(links),
#             sessions=self._activities(job),
#             workflows=sequences,
#             parameters=[
#                 CanonicalParameter(**{
#                     key: value for key, value in item.items()
#                     if key in {"name", "parameter_type", "data_type", "default_value", "scope"}
#                 })
#                 for item in parameters
#             ],
#             source_metadata={
#                 "object_type": "JOB",
#                 "mapping_attributes": dict(job.values),
#                 "job_attributes": dict(job.values),
#                 "source_reference": source_reference,
#                 "source_instance_names": source_instance_names,
#                 "target_instance_names": target_instance_names,
#                 "stage_count": len(stages),
#                 "stage_inventory": [
#                     {
#                         "name": stage["name"],
#                         "stage_type": stage["stage_type"],
#                         "role": stage["role"],
#                     }
#                     for stage in stages
#                 ],
#                 "link_count": len(links),
#                 "constraints": self._extract_constraints(job),
#                 "activities": self._activities(job),
#                 "sequences": sequences,
#             },
#         )

#     def _dataset(self, stage: dict[str, Any], dataset_type: str) -> CanonicalDataset:
#         return CanonicalDataset(
#             name=stage["name"],
#             dataset_type=dataset_type,
#             fields=[
#                 CanonicalField(
#                     name=column.get("name", "UNKNOWN"),
#                     data_type=column.get("data_type") or "UNKNOWN",
#                     precision=self._safe_int(column.get("precision")),
#                     scale=self._safe_int(column.get("scale")),
#                     nullable=self._is_nullable(column.get("nullable")),
#                 )
#                 for column in stage["columns"]
#             ],
#             properties=stage["properties"],
#         )

#     def _transformation(self, item: dict[str, Any], mapping_name: str) -> CanonicalTransformation:
#         return CanonicalTransformation(
#             name=item["name"],
#             transformation_type=item["transformation_type"],
#             ports=[
#                 CanonicalPort(
#                     name=column.get("name", "UNKNOWN"),
#                     direction=self._normalize_port_type(
#                         column.get("direction"),
#                         mapping_name=mapping_name,
#                         stage_name=item["name"],
#                         column_name=column.get("name", "UNKNOWN"),
#                     ),
#                     data_type=column.get("data_type"),
#                     expression=column.get("expression"),
#                     properties=column.get("properties", {}),
#                 )
#                 for column in item["columns"]
#             ],
#             properties=item["properties"],
#         )

#     def _extract_columns(self, node: _DSXNode) -> list[dict[str, Any]]:
#         columns: list[dict[str, Any]] = []
#         for child in self._descendants(node):
#             if child is node or not self._is_column(child):
#                 continue
#             columns.append({
#                 "name": self._node_name(child),
#                 "data_type": self._first(child, "DATATYPE", "TYPE"),
#                 "precision": self._first(child, "PRECISION", "LENGTH"),
#                 "scale": self._first(child, "SCALE"),
#                 "nullable": self._first(child, "NULLABLE"),
#                 "direction": self._first(child, "PORTTYPE", "DIRECTION", "USAGETYPE"),
#                 "expression": self._first(
#                     child, "DERIVATION", "EXPRESSION", "DERIVATIONEXPRESSION"
#                 ),
#                 "properties": dict(child.values),
#             })
#         return self._unique_by_name(columns)

#     def _extract_constraints(self, job: _DSXNode) -> list[dict[str, Any]]:
#         return [
#             dict(node.values, name=self._node_name(node))
#             for node in self._descendants(job)
#             if "CONSTRAINT" in node.kind or "CONSTRAINT" in self._upper_type(node)
#         ]

#     def _activities(self, job: _DSXNode) -> list[dict[str, Any]]:
#         return [
#             dict(node.values, name=self._node_name(node))
#             for node in self._descendants(job)
#             if "ACTIVITY" in node.kind or "ACTIVITY" in self._upper_type(node)
#         ]

#     # -------------------------------------------------------------------------
#     # Validation & Normalization Helpers (Parity with Informatica)
#     # -------------------------------------------------------------------------

#     def _normalize_port_type(
#         self,
#         value: str | None,
#         mapping_name: str = "",
#         stage_name: str = "",
#         column_name: str = "",
#     ) -> str:
#         """Normalize and validate port direction against VALID_PORT_TYPES."""
#         val = (value or "").strip().upper()
#         if not val:
#             return "OUTPUT"

#         if val in {"IN", "INPUT", "READ"}:
#             normalized = "INPUT"
#         elif val in {"OUT", "OUTPUT", "WRITE"}:
#             normalized = "OUTPUT"
#         elif val in {"INOUT", "IN/OUT", "INPUT/OUTPUT"}:
#             normalized = "INPUT/OUTPUT"
#         elif val in {"VAR", "VARIABLE", "STAGEVAR"}:
#             normalized = "VARIABLE"
#         elif val in {"RET", "RETURN"}:
#             normalized = "RETURN"
#         else:
#             normalized = val

#         if normalized not in self.VALID_PORT_TYPES:
#             logger.warning(
#                 "Unrecognized port type '%s' on %s.%s.%s. Defaulting to 'OUTPUT'.",
#                 value, mapping_name, stage_name, column_name,
#             )
#             return "OUTPUT"
#         return normalized

#     @staticmethod
#     def _is_nullable(value: str | None) -> bool:
#         """Normalize nullability identical to Informatica parser logic."""
#         normalized = (value or "YES").strip().upper().replace("_", " ")
#         return normalized not in {"NO", "NOTNULL", "NOT NULL", "FALSE", "0"}

#     @staticmethod
#     def _safe_int(value: Any) -> int | None:
#         """Convert string/number to int safely without failing parsing."""
#         try:
#             return int(value) if value not in (None, "") else None
#         except (TypeError, ValueError):
#             return None

#     # -------------------------------------------------------------------------
#     # DSX Parsing Core
#     # -------------------------------------------------------------------------

#     @classmethod
#     def _parse_document(cls, content: bytes | str) -> list[_DSXNode]:
#         if isinstance(content, bytes):
#             text = content.decode("utf-8-sig", errors="replace")
#         else:
#             text = content
#         roots: list[_DSXNode] = []
#         stack: list[_DSXNode] = []
#         for line_number, raw_line in enumerate(text.splitlines(), 1):
#             line = raw_line.strip()
#             if not line or line.startswith("#"):
#                 continue
#             begin = cls._BEGIN.match(line)
#             if begin:
#                 node = _DSXNode(begin.group(1).upper())
#                 if stack:
#                     stack[-1].children.append(node)
#                 else:
#                     roots.append(node)
#                 stack.append(node)
#                 continue
#             end = cls._END.match(line)
#             if end:
#                 if not stack:
#                     raise DataStageParserError(f"Unexpected {line} at line {line_number}.")
#                 expected = end.group(1)
#                 if expected and stack[-1].kind != expected.upper():
#                     raise DataStageParserError(
#                         f"Mismatched {line}; expected END {stack[-1].kind} at line {line_number}."
#                     )
#                 stack.pop()
#                 continue
#             if not stack:
#                 continue
#             match = cls._KEY_VALUE.match(line)
#             if not match:
#                 logger.debug("Ignoring unrecognised DSX line %d", line_number)
#                 continue
#             key, raw_value = match.groups()
#             stack[-1].values[key.upper()] = cls._value(raw_value)
#         if stack:
#             raise DataStageParserError(f"Unclosed DSX block: {stack[-1].kind}.")
#         return roots

#     @staticmethod
#     def _read_content(content: bytes | str | Path) -> bytes | str:
#         if isinstance(content, Path):
#             try:
#                 return content.read_bytes()
#             except OSError as exc:
#                 raise DataStageParserError("Unable to read the DSX file.") from exc
#         if not content:
#             raise DataStageParserError("The DSX file is empty.")
#         return content

#     @staticmethod
#     def _value(value: str) -> str:
#         try:
#             values = shlex.split(value, posix=True)
#         except ValueError:
#             return value.strip().strip('"')
#         return values[0] if len(values) == 1 else " ".join(values)

#     @staticmethod
#     def _descendants(node: _DSXNode) -> Iterable[_DSXNode]:
#         for child in node.children:
#             yield child
#             yield from DataStageParser._descendants(child)

#     @classmethod
#     def _first(cls, node: _DSXNode, *keys: str) -> str | None:
#         for key in keys:
#             value = node.values.get(key.upper())
#             if value:
#                 return value
#         return None

#     @classmethod
#     def _node_name(cls, node: _DSXNode) -> str:
#         return node.name or cls._first(node, "VALUE") or "UNKNOWN"

#     @classmethod
#     def _job_name(cls, job: _DSXNode) -> str:
#         return cls._node_name(job)

#     @classmethod
#     def _job_folder(cls, job: _DSXNode) -> str | None:
#         return cls._first(job, "FOLDER", "PROJECT", "CATEGORY")

#     @classmethod
#     def _job_key(cls, job: _DSXNode) -> str:
#         return f"{cls._job_folder(job) or 'DEFAULT'}/DSJOB/{cls._job_name(job)}"

#     @classmethod
#     def _upper_type(cls, node: _DSXNode) -> str:
#         return " ".join(
#             value.upper() for key, value in node.values.items()
#             if key in {"TYPE", "OLETYPE", "STAGETYPE", "RECORDTYPE", "CLASS"}
#         )

#     @classmethod
#     def _is_stage(cls, node: _DSXNode) -> bool:
#         if node.kind in cls._STAGE_KINDS:
#             return True
#         if node.kind == "DSRECORD":
#             return bool(
#                 cls._first(node, "IDENTIFIER", "STAGENAME", "NAME")
#                 and cls._first(node, "OLETYPE", "STAGETYPE", "TYPE")
#             )
#         return "STAGE" in cls._upper_type(node)

#     @classmethod
#     def _is_link(cls, node: _DSXNode) -> bool:
#         return node.kind in cls._LINK_KINDS or "LINK" in cls._upper_type(node)

#     @classmethod
#     def _is_parameter(cls, node: _DSXNode) -> bool:
#         return node.kind in cls._PARAMETER_KINDS or "PARAMETER" in cls._upper_type(node)

#     @classmethod
#     def _is_column(cls, node: _DSXNode) -> bool:
#         return node.kind in {"DSCOLUMN", "DSFIELD", "DSPORT", "DSOUTPUT"} or any(
#             key in node.values for key in ("DATATYPE", "DERIVATION", "EXPRESSION")
#         )

#     @staticmethod
#     def _stage_role(stage_type: str, values: dict[str, str]) -> str:
#         # Extract the stage name or identifier
#         name = (
#             values.get("NAME")
#             or values.get("STAGENAME")
#             or values.get("IDENTIFIER")
#             or ""
#         ).upper()
#         stage_type_text = stage_type.upper()      
#         # 1. First, check stage name conventions (Resolves SRC_EMPLOYEE and TGT_EMPLOYEE)
#         if name.startswith("SRC_") or name.endswith("_SRC") or "SOURCE" in name:
#             return "SOURCE"
#         if name.startswith("TGT_") or name.endswith("_TGT") or "TARGET" in name:
#             return "TARGET"      
#         # 2. Check stage direction properties
#         direction = " ".join(
#             values.get(key, "")
#             for key in (
#                 "ROLE", "STAGEDIRECTION", "DIRECTION", "OPERATION", "MODE",
#                 "ACCESSMODE",
#             )
#         ).upper()
#         if any(token in direction for token in ("SOURCE", "INPUT", "READ")):
#             return "SOURCE"
#         if any(token in direction for token in ("TARGET", "OUTPUT", "WRITE")):
#             return "TARGET"      
#         # 3. Check stage types
#         if any(token in stage_type_text for token in ("SEQUENTIALFILE", "FLATFILE")):
#             return "SOURCE"
#         if "DATASET" in stage_type_text:
#             return "TARGET"
#         if any(token in stage_type_text for token in (
#             "TRANSFORM", "AGGREGATOR", "JOINER", "SORTER", "LOOKUP",
#             "FILTER", "ROUTER", "FUNNEL", "PIVOT",
#         )):
#             return "TRANSFORM"
#         if "CONNECTOR" in stage_type_text or any(token in stage_type_text for token in (
#             "ORACLE", "ODBC", "JDBC", "DB2", "TERADATA",
#         )):
#             return "CONNECTOR"
#         if any(token in stage_type_text for token in ("SOURCE", "INPUT")):
#             return "SOURCE"
#         if any(token in stage_type_text for token in ("TARGET", "OUTPUT")):
#             return "TARGET"      
#         return "UNKNOWN"
    
#     @staticmethod
#     def _unique_by_name(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
#         result: list[dict[str, Any]] = []
#         seen: set[str] = set()
#         for item in items:
#             name = str(item.get("name", "UNKNOWN"))
#             if name not in seen:
#                 result.append(item)
#                 seen.add(name)
#         return result
#------------------------------------------------------

"""Parser for IBM DataStage DSX exports with functional parity to Informatica XML parser."""

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
    """Convert authentic IBM InfoSphere DataStage DSX exports and legacy DSX fixtures into CanonicalMapping."""

    VALID_PORT_TYPES = {"INPUT", "OUTPUT", "INPUT/OUTPUT", "VARIABLE", "RETURN"}

    # Standard DataStage ODBC SqlType numeric code mappings
    SQL_TYPE_MAP: dict[str, str] = {
        "-7": "BIT",
        "-6": "TINYINT",
        "-5": "BIGINT",
        "-4": "LONGVARBINARY",
        "-3": "VARBINARY",
        "-2": "BINARY",
        "-1": "LONGVARCHAR",
        "1": "CHAR",
        "2": "NUMERIC",
        "3": "DECIMAL",
        "4": "INTEGER",
        "5": "SMALLINT",
        "6": "FLOAT",
        "7": "REAL",
        "8": "DOUBLE",
        "9": "DATE",
        "10": "TIME",
        "11": "TIMESTAMP",
        "12": "VARCHAR",
        "91": "DATE",
        "92": "TIME",
        "93": "TIMESTAMP",
    }

    _BEGIN = re.compile(r"^BEGIN\s+([A-Za-z0-9_]+)\s*$", re.IGNORECASE)
    _END = re.compile(r"^END(?:\s+([A-Za-z0-9_]+))?\s*$", re.IGNORECASE)
    _MULTILINE_START = re.compile(
        r"^([A-Za-z_][A-Za-z0-9_.-]*)\s*=\s*=\+=\+=\+=\s*$"
    )
    _KEY_VALUE = re.compile(
        r"^([A-Za-z_][A-Za-z0-9_.-]*)\s*(?:=|\s)\s*(.*?)\s*;?$"
    )
    _DSX_ENCODED_TOKENS = re.compile(r"\\\([123]\)")

    _JOB_KINDS = {"DSJOB", "DSJOBDEF"}
    _STAGE_KINDS = {"DSSTAGE"}
    _LINK_KINDS = {"DSLINK"}
    _PARAMETER_KINDS = {"DSPARAMETER"}
    _SEQUENCE_KINDS = {"DSSEQUENCE"}

    _NON_STAGE_OLE_TYPES = {
        "CJOBDEFN",
        "CCONTAINERVIEW",
        "CCUSTOMINPUT",
        "CCUSTOMOUTPUT",
        "CTRXINPUT",
        "CTRXOUTPUT",
        "CSTDINPUT",
        "CSTDOUTPUT",
        "CMETATABLE",
        "CJSACTIVITY",
        "CJSEXECJOBACTIVITY",
    }

    _INPUT_PIN_OLE_TYPES = {"CCUSTOMINPUT", "CTRXINPUT", "CSTDINPUT"}
    _OUTPUT_PIN_OLE_TYPES = {"CCUSTOMOUTPUT", "CTRXOUTPUT", "CSTDOUTPUT"}

    # -------------------------------------------------------------------------
    # Public Entrypoints (Parity with InformaticaXMLParser)
    # -------------------------------------------------------------------------

    def list_mappings_bytes(
        self, content: bytes, source_reference: str
    ) -> list[MappingSummary]:
        """List DataStage jobs as selectable mapping summaries from raw bytes."""
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
        """List DataStage jobs from a DSX file path."""
        return self.list_mappings_bytes(file_path.read_bytes(), source_reference)

    def parse_selected_bytes(
        self,
        content: bytes,
        selected_keys: set[str] | None,
        source_vendor: str,
        source_reference: str,
    ) -> list[CanonicalMapping]:
        """Parse selected DataStage jobs directly from uploaded bytes."""
        return self.parse(content, source_reference, selected_keys, source_vendor)

    def parse_selected(
        self,
        file_path: Path,
        selected_keys: set[str] | None,
        source_vendor: str,
        source_reference: str,
    ) -> list[CanonicalMapping]:
        """Parse selected DataStage jobs from a file path."""
        return self.parse(file_path, source_reference, selected_keys, source_vendor)

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
        if not jobs:
            raise DataStageParserError("The DSX export contains no DSJOB blocks.")

        results: list[CanonicalMapping] = []
        for job in jobs:
            mapping = self._build_mapping(
                job, source_reference, source_vendor
            )
            if selected_keys is None or mapping.source_object_key in selected_keys:
                results.append(mapping)

        logger.info("Parsed %d DataStage jobs from %s", len(results), source_reference)
        return results

    # -------------------------------------------------------------------------
    # Granular DSX Extraction Methods
    # -------------------------------------------------------------------------

    def extract_jobs(self, root: Iterable[_DSXNode]) -> list[_DSXNode]:
        """Return top-level DSJOB blocks."""
        return [node for node in root if node.kind in self._JOB_KINDS]

    def extract_stages(self, job: _DSXNode) -> list[dict[str, Any]]:
        """Extract stages, pin-backed properties, and columns from one job."""
        pin_index = self._index_pins(job)
        view_stage_types = self._container_stage_types(job)
        stages: list[dict[str, Any]] = []

        for node in self._descendants(job):
            if not self._is_stage(node):
                continue

            stage_id = self._first(node, "IDENTIFIER", "ID")
            stage_name = self._first(node, "NAME", "STAGENAME") or stage_id or "UNKNOWN"
            stage_type = (
                self._first(node, "STAGETYPE", "OLETYPE", "TYPE")
                or (view_stage_types.get(stage_id or "") if stage_id else None)
                or "UNKNOWN"
            )

            # Merge stage attributes, nested CCustomProperty subrecords, and attached pin properties
            merged_props = self._extract_stage_properties(node, pin_index)

            # Collect columns from direct children, attached pins, and stage variables
            columns = self._extract_stage_columns(node, pin_index)

            stages.append(
                {
                    "id": stage_id or stage_name,
                    "name": stage_name,
                    "stage_type": stage_type,
                    "properties": merged_props,
                    "columns": columns,
                    "role": self._stage_role(stage_type, merged_props),
                }
            )
        return self._unique_by_name(stages)

    def extract_links(self, job: _DSXNode) -> list[dict[str, Any]]:
        """Extract stage links from standard pin Partner wiring and legacy DSLINK records."""
        links: list[dict[str, Any]] = []

        # 1. Legacy/custom explicit DSLINK blocks
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

        # 2. Standard DSX pin-to-pin wiring (OutputPins + Partner "StageId|PinId")
        stage_id_to_name: dict[str, str] = {}
        for node in self._descendants(job):
            if self._is_stage(node):
                sid = self._first(node, "IDENTIFIER", "ID")
                sname = self._first(node, "NAME", "STAGENAME") or sid
                if sid and sname:
                    stage_id_to_name[sid] = sname

        pin_index = self._index_pins(job)
        for pin_id, pin_node in pin_index.items():
            ole_type = (pin_node.values.get("OLETYPE") or "").upper()
            if ole_type not in self._OUTPUT_PIN_OLE_TYPES:
                continue

            partner = pin_node.values.get("PARTNER", "")
            if "|" not in partner:
                continue

            target_stage_id, _ = partner.split("|", 1)
            source_stage_id = self._stage_id_from_pin_id(pin_id)
            from_stage = stage_id_to_name.get(source_stage_id, source_stage_id)
            to_stage = stage_id_to_name.get(target_stage_id.strip(), target_stage_id.strip())

            pin_columns = self._extract_columns(pin_node, default_direction="OUTPUT")
            if pin_columns:
                for col in pin_columns:
                    col_name = col.get("name")
                    source_col = col.get("source_column")
                    from_field = (
                        source_col.split(".", 1)[-1]
                        if source_col and "." in source_col
                        else col_name
                    )
                    links.append(
                        {
                            "from_instance": from_stage,
                            "to_instance": to_stage,
                            "from_field": from_field,
                            "to_field": col_name,
                            "properties": dict(pin_node.values),
                        }
                    )
            else:
                links.append(
                    {
                        "from_instance": from_stage,
                        "to_instance": to_stage,
                        "from_field": None,
                        "to_field": None,
                        "properties": dict(pin_node.values),
                    }
                )

        return links

    def extract_transformers(self, job: _DSXNode) -> list[dict[str, Any]]:
        """Extract all transformer/processing stages and their ports."""
        transformers: list[dict[str, Any]] = []
        for stage in self.extract_stages(job):
            if stage["role"] != "TRANSFORM":
                continue
            transformers.append(
                {
                    "name": stage["name"],
                    "transformation_type": stage["stage_type"],
                    "columns": stage["columns"],
                    "properties": stage["properties"],
                }
            )
        return transformers

    def extract_parameters(self, job: _DSXNode) -> list[dict[str, Any]]:
        """Extract parameters from standard CParameters DSSUBRECORDs and legacy DSPARAMETERs."""
        parameters: list[dict[str, Any]] = []
        seen: set[str] = set()

        # 1. Standard DSX: Parameters "CParameters" -> BEGIN DSSUBRECORD inside ROOT
        root_rec = self._root_record(job)
        if root_rec:
            for sub in root_rec.children:
                if sub.kind != "DSSUBRECORD":
                    continue
                if any(k in sub.values for k in ("DEFAULT", "DEFAULTVALUE", "PARAMTYPE", "PROMPT")):
                    param_name = self._node_name(sub)
                    if param_name not in seen:
                        seen.add(param_name)
                        parameters.append(
                            {
                                "name": param_name,
                                "parameter_type": "DSPARAMETER",
                                "data_type": self._first(sub, "DATATYPE", "PARAMTYPE", "TYPE"),
                                "default_value": self._first(
                                    sub, "DEFAULT", "DEFAULTVALUE", "VALUE"
                                ),
                                "scope": self._first(sub, "SCOPE") or "JOB",
                                "properties": dict(sub.values),
                            }
                        )

        # 2. Legacy/custom DSPARAMETER nodes
        for node in self._descendants(job):
            if not self._is_parameter(node):
                continue
            param_name = self._node_name(node)
            if param_name in seen:
                continue
            seen.add(param_name)
            parameters.append(
                {
                    "name": param_name,
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
        """Extract sequence jobs and activities."""
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

    def build_lineage(self, links: list[dict[str, Any]]) -> list[CanonicalConnector]:
        """Normalize DSX links into canonical connectors."""
        return [
            CanonicalConnector(
                from_instance=item.get("from_instance", "UNKNOWN"),
                to_instance=item.get("to_instance", "UNKNOWN"),
                from_field=item.get("from_field"),
                to_field=item.get("to_field"),
            )
            for item in links
        ]

    # -------------------------------------------------------------------------
    # Canonical Builders
    # -------------------------------------------------------------------------

    def _build_mapping(
        self, job: _DSXNode, source_reference: str, source_vendor: str
    ) -> CanonicalMapping:
        root_record = self._root_record(job)
        stages = self.extract_stages(job)
        links = self.extract_links(job)
        sequences = self.extract_sequences(job)
        parameters = self.extract_parameters(job)
        transformations = self.extract_transformers(job)
        mapping_name = self._job_name(job)

        source_stages = [stage for stage in stages if stage["role"] == "SOURCE"]
        target_stages = [stage for stage in stages if stage["role"] == "TARGET"]

        source_instance_names = sorted([stage["name"] for stage in source_stages])
        target_instance_names = sorted([stage["name"] for stage in target_stages])

        job_attrs = dict(job.values)
        if root_record:
            job_attrs.update(root_record.values)

        description = (
            (self._first(root_record, "DESCRIPTION", "DESC") if root_record else None)
            or self._first(job, "DESCRIPTION", "DESC")
        )

        return CanonicalMapping(
            source_vendor=source_vendor,
            source_object_key=self._job_key(job),
            mapping_name=mapping_name,
            folder_name=self._job_folder(job),
            description=description,
            sources=[self._dataset(stage, "SOURCE") for stage in source_stages],
            targets=[self._dataset(stage, "TARGET") for stage in target_stages],
            transformations=[self._transformation(item, mapping_name) for item in transformations],
            connectors=self.build_lineage(links),
            sessions=self._activities(job),
            workflows=sequences,
            parameters=[
                CanonicalParameter(**{
                    key: value for key, value in item.items()
                    if key in {"name", "parameter_type", "data_type", "default_value", "scope"}
                })
                for item in parameters
            ],
            source_metadata={
                "object_type": "JOB",
                "mapping_attributes": job_attrs,
                "job_attributes": job_attrs,
                "source_reference": source_reference,
                "source_instance_names": source_instance_names,
                "target_instance_names": target_instance_names,
                "stage_count": len(stages),
                "stage_inventory": [
                    {
                        "name": stage["name"],
                        "stage_type": stage["stage_type"],
                        "role": stage["role"],
                    }
                    for stage in stages
                ],
                "link_count": len(links),
                "constraints": self._extract_constraints(job),
                "activities": self._activities(job),
                "sequences": sequences,
            },
        )

    def _dataset(self, stage: dict[str, Any], dataset_type: str) -> CanonicalDataset:
        return CanonicalDataset(
            name=stage["name"],
            dataset_type=dataset_type,
            fields=[
                CanonicalField(
                    name=column.get("name", "UNKNOWN"),
                    data_type=column.get("data_type") or "UNKNOWN",
                    precision=self._safe_int(column.get("precision")),
                    scale=self._safe_int(column.get("scale")),
                    nullable=self._is_nullable(column.get("nullable")),
                )
                for column in stage["columns"]
            ],
            properties=stage["properties"],
        )

    def _transformation(self, item: dict[str, Any], mapping_name: str) -> CanonicalTransformation:
        return CanonicalTransformation(
            name=item["name"],
            transformation_type=item["transformation_type"],
            ports=[
                CanonicalPort(
                    name=column.get("name", "UNKNOWN"),
                    direction=self._normalize_port_type(
                        column.get("direction"),
                        mapping_name=mapping_name,
                        stage_name=item["name"],
                        column_name=column.get("name", "UNKNOWN"),
                    ),
                    data_type=column.get("data_type"),
                    expression=column.get("expression"),
                    properties=column.get("properties", {}),
                )
                for column in item["columns"]
            ],
            properties=item["properties"],
        )

    # -------------------------------------------------------------------------
    # Standard DSX Pin, ContainerView & Subrecord Helpers
    # -------------------------------------------------------------------------

    @classmethod
    def _root_record(cls, job: _DSXNode) -> _DSXNode | None:
        """Return the CJobDefn (Identifier 'ROOT') DSRECORD inside a DSJOB, if present."""
        for node in cls._descendants(job):
            if node.kind == "DSRECORD" and (
                (node.values.get("OLETYPE") or "").upper() == "CJOBDEFN"
                or (node.values.get("IDENTIFIER") or "").upper() == "ROOT"
            ):
                return node
        return None

    @classmethod
    def _container_stage_types(cls, job: _DSXNode) -> dict[str, str]:
        """Map stage IDs ('V0S6') to StageTypeIDs ('PxSequentialFile') from CContainerView."""
        mapping: dict[str, str] = {}
        for node in cls._descendants(job):
            if node.kind != "DSRECORD":
                continue
            if (node.values.get("OLETYPE") or "").upper() != "CCONTAINERVIEW":
                continue
            stage_ids = [s.strip() for s in node.values.get("STAGELIST", "").split("|") if s.strip()]
            type_ids = [t.strip() for t in node.values.get("STAGETYPEIDS", "").split("|") if t.strip()]
            for sid, tid in zip(stage_ids, type_ids):
                mapping[sid] = tid
        return mapping

    def _index_pins(self, job: _DSXNode) -> dict[str, _DSXNode]:
        """Map pin identifiers (e.g. 'V0S6P3') to their DSRECORD pin nodes."""
        pins: dict[str, _DSXNode] = {}
        for node in self._descendants(job):
            if node.kind != "DSRECORD":
                continue
            ole_type = (node.values.get("OLETYPE") or "").upper()
            if ole_type in self._INPUT_PIN_OLE_TYPES or ole_type in self._OUTPUT_PIN_OLE_TYPES:
                pin_id = self._first(node, "IDENTIFIER", "ID")
                if pin_id:
                    pins[pin_id] = node
        return pins

    @staticmethod
    def _stage_id_from_pin_id(pin_id: str) -> str:
        """Derive stage identifier 'V0S6' from standard pin identifier 'V0S6P3'."""
        match = re.match(r"^(V\d+S\d+)P\d+$", pin_id, re.IGNORECASE)
        return match.group(1) if match else pin_id

    @classmethod
    def _extract_stage_properties(
        cls, stage_node: _DSXNode, pin_index: dict[str, _DSXNode]
    ) -> dict[str, str]:
        """Flatten stage attributes, nested CCustomProperty subrecords, and pin properties."""
        props = dict(stage_node.values)

        def _collect_subrecord_props(target_node: _DSXNode) -> None:
            for child in target_node.children:
                if child.kind != "DSSUBRECORD":
                    continue
                # Skip column subrecords
                if any(k in child.values for k in ("SQLTYPE", "DATATYPE", "DERIVATION")):
                    continue
                prop_name = (child.values.get("NAME") or "").strip()
                prop_val = child.values.get("VALUE")
                if prop_name and prop_val is not None and prop_val.strip() != "":
                    props[prop_name.upper()] = cls._clean_dsx_escaped(prop_val)

        _collect_subrecord_props(stage_node)

        for pin_attr in ("INPUTPINS", "OUTPUTPINS"):
            raw_pins = stage_node.values.get(pin_attr, "")
            for pin_id in (p.strip() for p in raw_pins.split("|") if p.strip()):
                pin_node = pin_index.get(pin_id)
                if not pin_node:
                    continue
                _collect_subrecord_props(pin_node)
                constraint = pin_node.values.get("CONSTRAINT")
                if constraint:
                    props["CONSTRAINT"] = constraint

        return props

    def _extract_stage_columns(
        self, stage_node: _DSXNode, pin_index: dict[str, _DSXNode]
    ) -> list[dict[str, Any]]:
        """Extract columns from stage children, attached InputPins/OutputPins, and StageVars."""
        columns = self._extract_columns(stage_node)

        # Extract CTransformerStage StageVars ("CStageVar" subrecords)
        for child in stage_node.children:
            if child.kind == "DSSUBRECORD" and (
                "EXPRESSION" in child.values or "PARSEDEXPRESSION" in child.values
            ) and "SQLTYPE" in child.values:
                var_name = self._node_name(child)
                if not var_name.startswith("$"):
                    columns.append({
                        "name": var_name,
                        "data_type": self._normalize_sql_type(self._first(child, "SQLTYPE", "DATATYPE")),
                        "precision": self._first(child, "PRECISION", "LENGTH"),
                        "scale": self._first(child, "COLSCALE", "SCALE"),
                        "nullable": "1",
                        "direction": "VARIABLE",
                        "expression": self._first(child, "EXPRESSION", "PARSEDEXPRESSION"),
                        "source_column": self._first(child, "SOURCECOLUMN"),
                        "properties": dict(child.values),
                    })

        for pin_attr, direction in (("INPUTPINS", "INPUT"), ("OUTPUTPINS", "OUTPUT")):
            raw_pins = stage_node.values.get(pin_attr, "")
            for pin_id in (p.strip() for p in raw_pins.split("|") if p.strip()):
                pin_node = pin_index.get(pin_id)
                if not pin_node:
                    continue
                pin_cols = self._extract_columns(pin_node, default_direction=direction)
                # If an input pin has no inline columns, resolve from its Partner output pin
                if not pin_cols and direction == "INPUT":
                    partner = pin_node.values.get("PARTNER", "")
                    if "|" in partner:
                        _, partner_pin_id = partner.split("|", 1)
                        partner_pin = pin_index.get(partner_pin_id.strip())
                        if partner_pin:
                            pin_cols = self._extract_columns(
                                partner_pin, default_direction="INPUT"
                            )
                columns.extend(pin_cols)

        # Deduplicate while preserving distinct (name, direction) ports
        result: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for col in columns:
            key = (str(col.get("name", "UNKNOWN")), str(col.get("direction") or "OUTPUT"))
            if key not in seen:
                seen.add(key)
                result.append(col)
        return result

    def _extract_columns(
        self, node: _DSXNode, default_direction: str | None = None
    ) -> list[dict[str, Any]]:
        columns: list[dict[str, Any]] = []
        for child in self._descendants(node):
            if child is node or not self._is_column(child):
                continue
            raw_type = self._first(child, "DATATYPE", "SQLTYPE", "TYPE")
            columns.append({
                "name": self._node_name(child),
                "data_type": self._normalize_sql_type(raw_type),
                "precision": self._first(child, "PRECISION", "LENGTH"),
                "scale": self._first(child, "SCALE", "COLSCALE"),
                "nullable": self._first(child, "NULLABLE"),
                "direction": (
                    self._first(child, "PORTTYPE", "DIRECTION", "USAGETYPE")
                    or default_direction
                ),
                "expression": self._first(
                    child, "DERIVATION", "EXPRESSION", "DERIVATIONEXPRESSION"
                ),
                "source_column": self._first(child, "SOURCECOLUMN"),
                "properties": dict(child.values),
            })
        return self._unique_by_name(columns)

    @classmethod
    def _normalize_sql_type(cls, value: str | None) -> str | None:
        if not value:
            return None
        cleaned = value.strip()
        return cls.SQL_TYPE_MAP.get(cleaned, cleaned.upper())

    def _extract_constraints(self, job: _DSXNode) -> list[dict[str, Any]]:
        constraints: list[dict[str, Any]] = []
        for node in self._descendants(job):
            if "CONSTRAINT" in node.kind or "CONSTRAINT" in self._upper_type(node):
                constraints.append(dict(node.values, name=self._node_name(node)))
            elif "CONSTRAINT" in node.values and node.values["CONSTRAINT"].strip():
                constraints.append({
                    "name": self._node_name(node),
                    "constraint": node.values["CONSTRAINT"],
                })
        return constraints

    def _activities(self, job: _DSXNode) -> list[dict[str, Any]]:
        return [
            dict(node.values, name=self._node_name(node))
            for node in self._descendants(job)
            if "ACTIVITY" in node.kind or "ACTIVITY" in self._upper_type(node)
        ]

    # -------------------------------------------------------------------------
    # Validation & Normalization Helpers (Parity with Informatica)
    # -------------------------------------------------------------------------

    def _normalize_port_type(
        self,
        value: str | None,
        mapping_name: str = "",
        stage_name: str = "",
        column_name: str = "",
    ) -> str:
        """Normalize and validate port direction against VALID_PORT_TYPES."""
        val = (value or "").strip().upper()
        if not val:
            return "OUTPUT"

        if val in {"IN", "INPUT", "READ"}:
            normalized = "INPUT"
        elif val in {"OUT", "OUTPUT", "WRITE"}:
            normalized = "OUTPUT"
        elif val in {"INOUT", "IN/OUT", "INPUT/OUTPUT"}:
            normalized = "INPUT/OUTPUT"
        elif val in {"VAR", "VARIABLE", "STAGEVAR"}:
            normalized = "VARIABLE"
        elif val in {"RET", "RETURN"}:
            normalized = "RETURN"
        else:
            normalized = val

        if normalized not in self.VALID_PORT_TYPES:
            logger.warning(
                "Unrecognized port type '%s' on %s.%s.%s. Defaulting to 'OUTPUT'.",
                value, mapping_name, stage_name, column_name,
            )
            return "OUTPUT"
        return normalized

    @staticmethod
    def _is_nullable(value: str | None) -> bool:
        """Normalize nullability identical to Informatica parser logic."""
        normalized = (value or "YES").strip().upper().replace("_", " ")
        return normalized not in {"NO", "NOTNULL", "NOT NULL", "FALSE", "0"}

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        """Convert string/number to int safely without failing parsing."""
        try:
            return int(value) if value not in (None, "") else None
        except (TypeError, ValueError):
            return None

    # -------------------------------------------------------------------------
    # DSX Parsing Core
    # -------------------------------------------------------------------------

    @classmethod
    def _parse_document(cls, content: bytes | str) -> list[_DSXNode]:
        if isinstance(content, bytes):
            text = content.decode("utf-8-sig", errors="replace")
        else:
            text = content

        roots: list[_DSXNode] = []
        stack: list[_DSXNode] = []
        multiline_key: str | None = None
        multiline_buffer: list[str] = []

        for line_number, raw_line in enumerate(text.splitlines(), 1):
            stripped = raw_line.strip()

            # Handle DataStage =+=+=+= ... =+=+=+= multi-line blocks (OrchestrateCode, TrxGenCode, Schema)
            if multiline_key is not None:
                if stripped == "=+=+=+=":
                    if stack:
                        stack[-1].values[multiline_key] = "\n".join(multiline_buffer)
                    multiline_key = None
                    multiline_buffer = []
                else:
                    multiline_buffer.append(raw_line)
                continue

            if not stripped or stripped.startswith("#"):
                continue

            begin = cls._BEGIN.match(stripped)
            if begin:
                node = _DSXNode(begin.group(1).upper())
                if stack:
                    stack[-1].children.append(node)
                else:
                    roots.append(node)
                stack.append(node)
                continue

            end = cls._END.match(stripped)
            if end:
                if not stack:
                    raise DataStageParserError(f"Unexpected {stripped} at line {line_number}.")
                expected = end.group(1)
                if expected and stack[-1].kind != expected.upper():
                    raise DataStageParserError(
                        f"Mismatched {stripped}; expected END {stack[-1].kind} at line {line_number}."
                    )
                stack.pop()
                continue

            if not stack:
                continue

            ml_start = cls._MULTILINE_START.match(stripped)
            if ml_start:
                multiline_key = ml_start.group(1).upper()
                multiline_buffer = []
                continue

            match = cls._KEY_VALUE.match(stripped)
            if not match:
                logger.debug("Ignoring unrecognised DSX line %d", line_number)
                continue
            key, raw_value = match.groups()
            stack[-1].values[key.upper()] = cls._value(raw_value)

        if multiline_key is not None:
            raise DataStageParserError(f"Unclosed multi-line DSX block: {multiline_key}.")
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

    @classmethod
    def _clean_dsx_escaped(cls, text: str) -> str:
        """Decode DataStage \\(1)\\(2)\\(3) delimited property payloads into readable values."""
        if not cls._DSX_ENCODED_TOKENS.search(text):
            return text
        parts = [
            p.strip()
            for p in cls._DSX_ENCODED_TOKENS.split(text)
            if p.strip() and p.strip() != "0"
        ]
        # Strip leading property-name echo (e.g. ['file', 'C:\\path\\file.txt'] -> 'C:\\path\\file.txt')
        if len(parts) >= 2 and parts[0].lower() in {"file", "key", "countfield", "writemethod"}:
            return ", ".join(parts[1:])
        return " ".join(parts)

    @staticmethod
    def _value(value: str) -> str:
        stripped = value.strip()
        if stripped.startswith('"') and stripped.endswith('"') and len(stripped) >= 2:
            inner = stripped[1:-1]
            # Preserve Windows backslashes inside DataStage quoted strings
            return inner.replace('\\"', '"').replace("\\\\", "\\")
        try:
            values = shlex.split(value, posix=True)
        except ValueError:
            return stripped.strip('"')
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
        root_rec = cls._root_record(job)
        if root_rec:
            name = cls._first(root_rec, "NAME", "JOBNAME")
            if name:
                return name
        return cls._node_name(job)

    @classmethod
    def _job_folder(cls, job: _DSXNode) -> str | None:
        root_rec = cls._root_record(job)
        if root_rec:
            folder = cls._first(root_rec, "CATEGORY", "FOLDER", "PROJECT")
            if folder:
                return folder
        return cls._first(job, "CATEGORY", "FOLDER", "PROJECT")

    @classmethod
    def _job_key(cls, job: _DSXNode) -> str:
        return f"{cls._job_folder(job) or 'DEFAULT'}/DSJOB/{cls._job_name(job)}"

    @classmethod
    def _upper_type(cls, node: _DSXNode) -> str:
        return " ".join(
            value.upper() for key, value in node.values.items()
            if key in {"TYPE", "OLETYPE", "STAGETYPE", "RECORDTYPE", "CLASS"}
        )

    @classmethod
    def _is_stage(cls, node: _DSXNode) -> bool:
        if node.kind in cls._STAGE_KINDS:
            return True
        if node.kind == "DSRECORD":
            ole_type = (node.values.get("OLETYPE") or "").upper()
            ident = (node.values.get("IDENTIFIER") or "").upper()
            if ole_type in cls._NON_STAGE_OLE_TYPES or ident == "ROOT":
                return False
            return bool(
                cls._first(node, "IDENTIFIER", "STAGENAME", "NAME")
                and cls._first(node, "OLETYPE", "STAGETYPE", "TYPE")
            )
        return "STAGE" in cls._upper_type(node)

    @classmethod
    def _is_link(cls, node: _DSXNode) -> bool:
        return node.kind in cls._LINK_KINDS

    @classmethod
    def _is_parameter(cls, node: _DSXNode) -> bool:
        return node.kind in cls._PARAMETER_KINDS

    @classmethod
    def _is_column(cls, node: _DSXNode) -> bool:
        if node.kind in {"DSCOLUMN", "DSFIELD", "DSPORT", "DSOUTPUT"}:
            return True
        if node.kind == "DSSUBRECORD":
            return any(
                key in node.values
                for key in ("SQLTYPE", "DATATYPE", "DERIVATION")
            )
        return False

    @staticmethod
    def _stage_role(stage_type: str, values: dict[str, str]) -> str:
        name = (
            values.get("NAME")
            or values.get("STAGENAME")
            or values.get("IDENTIFIER")
            or ""
        ).upper()
        stage_type_text = stage_type.upper()

        # 1. Explicit pin wiring in standard DSX (OutputPins only = SOURCE; InputPins only = TARGET)
        has_inputs = bool((values.get("INPUTPINS") or "").strip())
        has_outputs = bool((values.get("OUTPUTPINS") or "").strip())
        if has_outputs and not has_inputs:
            return "SOURCE"
        if has_inputs and not has_outputs:
            return "TARGET"

        # 2. Stage naming conventions
        if name.startswith("SRC_") or name.endswith("_SRC") or "SOURCE" in name:
            return "SOURCE"
        if name.startswith("TGT_") or name.endswith("_TGT") or "TARGET" in name:
            return "TARGET"

        # 3. Stage direction properties
        direction = " ".join(
            values.get(key, "")
            for key in (
                "ROLE", "STAGEDIRECTION", "DIRECTION", "OPERATION", "MODE",
                "ACCESSMODE",
            )
        ).upper()
        if any(token in direction for token in ("SOURCE", "INPUT", "READ")):
            return "SOURCE"
        if any(token in direction for token in ("TARGET", "OUTPUT", "WRITE")):
            return "TARGET"

        # 4. Stage types
        if any(token in stage_type_text for token in (
            "TRANSFORM", "AGGREGATOR", "JOINER", "JOIN", "SORTER", "SORT",
            "LOOKUP", "FILTER", "ROUTER", "FUNNEL", "PIVOT", "MODIFY", "COPY",
            "HEAD", "TAIL", "REMOVE", "SURROGATE",
        )):
            return "TRANSFORM"
        if any(token in stage_type_text for token in ("SEQUENTIALFILE", "FLATFILE")):
            return "SOURCE"
        if "DATASET" in stage_type_text:
            return "TARGET"
        if "CONNECTOR" in stage_type_text or any(token in stage_type_text for token in (
            "ORACLE", "ODBC", "JDBC", "DB2", "TERADATA",
        )):
            return "CONNECTOR"
        if any(token in stage_type_text for token in ("SOURCE", "INPUT")):
            return "SOURCE"
        if any(token in stage_type_text for token in ("TARGET", "OUTPUT")):
            return "TARGET"
        return "UNKNOWN"

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