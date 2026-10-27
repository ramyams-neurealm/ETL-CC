"""Parser for normalized Ab Initio graph exports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

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


class AbInitioGraphParserError(ValueError):
    """Raised when an Ab Initio graph export is invalid."""


class AbInitioGraphParser:
    """Convert a portable Ab Initio graph export into canonical mappings."""

    FORMAT = "AB_INITIO_GRAPH_JSON"

    def _load(self, content: bytes) -> dict[str, Any]:
        try:
            payload = json.loads(content.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AbInitioGraphParserError("The file is not valid UTF-8 JSON.") from exc
        if not isinstance(payload, dict) or payload.get("format") != self.FORMAT:
            raise AbInitioGraphParserError(
                f"Expected an {self.FORMAT} export."
            )
        if not isinstance(payload.get("graphs"), list):
            raise AbInitioGraphParserError("The export must contain a graphs array.")
        return payload

    @staticmethod
    def _field(item: dict[str, Any]) -> CanonicalField:
        return CanonicalField(
            name=str(item["name"]),
            data_type=str(item.get("data_type", "UNKNOWN")),
            precision=item.get("precision"),
            scale=item.get("scale"),
            nullable=bool(item.get("nullable", True)),
        )

    @classmethod
    def _dataset(cls, item: dict[str, Any], dataset_type: str) -> CanonicalDataset:
        return CanonicalDataset(
            name=str(item["name"]),
            dataset_type=dataset_type,
            connection_name=item.get("connection_name"),
            fields=[cls._field(field) for field in item.get("fields", [])],
            properties=dict(item.get("properties", {})),
        )

    @staticmethod
    def _key(project: str, graph_name: str) -> str:
        return f"{project}/{graph_name}"

    def list_mappings_bytes(
        self,
        content: bytes,
        source_reference: str,
    ) -> list[MappingSummary]:
        payload = self._load(content)
        project = str(payload.get("project", "default"))
        result: list[MappingSummary] = []
        for graph in payload["graphs"]:
            if not isinstance(graph, dict) or not graph.get("name"):
                continue
            name = str(graph["name"])
            result.append(
                MappingSummary(
                    source_object_key=self._key(project, name),
                    mapping_name=name,
                    folder_name=project,
                    source_reference=source_reference,
                )
            )
        return result

    def parse_selected_bytes(
        self,
        content: bytes,
        selected_keys: set[str] | None,
        source_vendor: str,
        source_reference: str,
    ) -> list[CanonicalMapping]:
        payload = self._load(content)
        project = str(payload.get("project", "default"))
        result: list[CanonicalMapping] = []
        for graph in payload["graphs"]:
            if not isinstance(graph, dict) or not graph.get("name"):
                continue
            name = str(graph["name"])
            key = self._key(project, name)
            if selected_keys is not None and key not in selected_keys:
                continue
            result.append(
                self._parse_graph(
                    project,
                    graph,
                    key,
                    source_vendor,
                    source_reference,
                )
            )
        return result

    def parse_file(
        self,
        file_path: Path,
        selected_keys: set[str] | None = None,
        source_vendor: str = "AB_INITIO",
    ) -> list[CanonicalMapping]:
        return self.parse_selected_bytes(
            file_path.read_bytes(),
            selected_keys,
            source_vendor,
            file_path.name,
        )

    def _parse_graph(
        self,
        project: str,
        graph: dict[str, Any],
        key: str,
        source_vendor: str,
        source_reference: str,
    ) -> CanonicalMapping:
        datasets = graph.get("datasets", {})
        sources = [self._dataset(item, "SOURCE") for item in datasets.get("sources", [])]
        targets = [self._dataset(item, "TARGET") for item in datasets.get("targets", [])]
        transformations = [
            CanonicalTransformation(
                name=str(item["name"]),
                transformation_type=str(item.get("type", "UNKNOWN")),
                ports=[CanonicalPort(**port) for port in item.get("ports", [])],
                properties=dict(item.get("properties", {})),
            )
            for item in graph.get("components", [])
        ]
        connectors = [
            CanonicalConnector(
                from_instance=str(item["from_instance"]),
                to_instance=str(item["to_instance"]),
                from_field=item.get("from_field"),
                to_field=item.get("to_field"),
            )
            for item in [*graph.get("edges", []), *graph.get("field_links", [])]
        ]
        parameters = [CanonicalParameter(**item) for item in graph.get("parameters", [])]
        return CanonicalMapping(
            source_vendor=source_vendor,
            source_object_key=key,
            mapping_name=str(graph["name"]),
            folder_name=project,
            description=graph.get("description"),
            sources=sources,
            targets=targets,
            transformations=transformations,
            connectors=connectors,
            parameters=parameters,
            source_metadata={
                "source_reference": source_reference,
                "format": self.FORMAT,
                "format_version": graph.get("format_version"),
                "graph_version": graph.get("version"),
                "project": project,
                "native_graph": graph,
                "unsupported_constructs": graph.get("unsupported_constructs", []),
            },
        )
