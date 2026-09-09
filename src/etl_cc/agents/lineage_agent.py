"""
Deterministic Lineage Agent for the ETL Migration Command Center.

The agent consumes one CanonicalMapping and builds component-level and
field-level lineage from canonical connectors. It does not parse XML, call an
LLM, infer links absent from the canonical mapping, or assign repository-level
migration waves.

Migration-wave assignment belongs exclusively to DependencyPlanner because a
single mapping cannot determine its position within a repository dependency
graph.
"""

from collections import defaultdict
from typing import Literal

import networkx as nx
from pydantic import BaseModel, Field

from etl_cc.models import CanonicalMapping


class LineageNode(BaseModel):
    """One source, transformation, or target component in a mapping graph."""

    id: str
    name: str
    node_type: Literal["SOURCE", "TRANSFORMATION", "TARGET"]
    transformation_type: str | None = None


class LineageEdge(BaseModel):
    """A grouped component-level edge built from canonical connectors."""

    from_instance: str
    to_instance: str
    field_count: int
    field_mappings: list[dict[str, str | None]] = Field(default_factory=list)


class FieldLineagePath(BaseModel):
    """Resolved lineage information for one target field."""

    target_instance: str
    target_field: str
    source_instances: list[str] = Field(default_factory=list)
    source_fields: list[str] = Field(default_factory=list)
    paths: list[list[str]] = Field(default_factory=list)
    derivation_expressions: list[str] = Field(default_factory=list)
    lineage_type: Literal["DIRECT", "DERIVED", "MIXED", "UNRESOLVED"]


class LineageWarning(BaseModel):
    """A deterministic warning produced for incomplete lineage metadata."""

    warning_type: str
    description: str
    instance_name: str | None = None
    field_name: str | None = None


class LineageAnalysis(BaseModel):
    """Structured mapping-level lineage result."""

    mapping_name: str
    component_nodes: list[LineageNode] = Field(default_factory=list)
    component_edges: list[LineageEdge] = Field(default_factory=list)
    field_lineage: list[FieldLineagePath] = Field(default_factory=list)
    dependencies: list[dict] = Field(default_factory=list)
    component_cycles: list[list[str]] = Field(default_factory=list)
    has_dependency_cycle: bool = False

    # Intentionally None at mapping level. DependencyPlanner is the only
    # component that assigns repository-level migration waves.
    migration_wave: int | None = None

    warnings: list[LineageWarning] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    human_review_required: bool


class LineageAgent:
    """Build exact mapping-level lineage from one CanonicalMapping."""

    AGENT_NAME = "LINEAGE_AGENT"
    AGENT_VERSION = "1.1.0"
    STAGE_NAME = "LINEAGE_ANALYSIS"
    MODEL_NAME = "DETERMINISTIC_NETWORKX"

    async def run(self, mapping: CanonicalMapping) -> LineageAnalysis:
        """Build component and field lineage deterministically."""
        component_graph = nx.DiGraph()
        field_graph = nx.DiGraph()
        derivation_expressions: dict[str, str] = {}

        component_nodes = self._component_nodes(mapping)
        for node in component_nodes:
            component_graph.add_node(
                node.id,
                node_type=node.node_type,
            )

        source_fields = {
            f"{dataset.name}.{field.name}"
            for dataset in mapping.sources
            for field in dataset.fields
        }
        target_fields = {
            f"{dataset.name}.{field.name}"
            for dataset in mapping.targets
            for field in dataset.fields
        }

        for dataset in [*mapping.sources, *mapping.targets]:
            for field in dataset.fields:
                field_graph.add_node(f"{dataset.name}.{field.name}")

        for transformation in mapping.transformations:
            for port in transformation.ports:
                field_node = f"{transformation.name}.{port.name}"
                field_graph.add_node(field_node)

                expression = (port.expression or "").strip()
                if expression and expression.upper() != port.name.strip().upper():
                    derivation_expressions[field_node] = expression

        grouped_connectors: dict[
            tuple[str, str],
            list[dict[str, str | None]],
        ] = defaultdict(list)

        for connector in mapping.connectors:
            component_graph.add_edge(
                connector.from_instance,
                connector.to_instance,
            )
            grouped_connectors[
                (connector.from_instance, connector.to_instance)
            ].append(
                {
                    "from_field": connector.from_field,
                    "to_field": connector.to_field,
                }
            )

            if connector.from_field and connector.to_field:
                field_graph.add_edge(
                    f"{connector.from_instance}.{connector.from_field}",
                    f"{connector.to_instance}.{connector.to_field}",
                )

        component_edges = [
            LineageEdge(
                from_instance=from_instance,
                to_instance=to_instance,
                field_count=len(field_mappings),
                field_mappings=field_mappings,
            )
            for (
                from_instance,
                to_instance,
            ), field_mappings in sorted(grouped_connectors.items())
        ]

        component_cycles = [
            self._close_cycle(list(cycle))
            for cycle in nx.simple_cycles(component_graph)
        ]

        field_lineage, warnings = self._trace_targets(
            field_graph=field_graph,
            source_fields=source_fields,
            target_fields=target_fields,
            expressions=derivation_expressions,
        )

        dependencies = [
            item.model_dump(mode="json")
            for item in mapping.dependencies
        ]
        has_component_cycle = bool(component_cycles)
        unresolved_count = sum(
            item.lineage_type == "UNRESOLVED"
            for item in field_lineage
        )
        target_count = max(len(target_fields), 1)
        confidence = round(
            max(0.0, 1.0 - (unresolved_count / target_count)),
            2,
        )
        human_review_required = (
            has_component_cycle
            or bool(warnings)
            or confidence < 1.0
        )

        return LineageAnalysis(
            mapping_name=mapping.mapping_name,
            component_nodes=component_nodes,
            component_edges=component_edges,
            field_lineage=field_lineage,
            dependencies=dependencies,
            component_cycles=component_cycles,
            has_dependency_cycle=has_component_cycle,
            migration_wave=None,
            warnings=warnings,
            confidence=confidence,
            human_review_required=human_review_required,
        )

    @staticmethod
    def _close_cycle(cycle: list[str]) -> list[str]:
        """Return a cycle with its starting node repeated for display."""
        return cycle + [cycle[0]] if cycle else cycle

    @staticmethod
    def _component_nodes(mapping: CanonicalMapping) -> list[LineageNode]:
        """Build unique semantic component nodes in mapping order."""
        nodes: list[LineageNode] = []
        seen: set[str] = set()

        for source in mapping.sources:
            if source.name in seen:
                continue
            seen.add(source.name)
            nodes.append(
                LineageNode(
                    id=source.name,
                    name=source.name,
                    node_type="SOURCE",
                )
            )

        for transformation in mapping.transformations:
            if transformation.name in seen:
                continue
            seen.add(transformation.name)
            nodes.append(
                LineageNode(
                    id=transformation.name,
                    name=transformation.name,
                    node_type="TRANSFORMATION",
                    transformation_type=(
                        transformation.transformation_type
                    ),
                )
            )

        for target in mapping.targets:
            if target.name in seen:
                continue
            seen.add(target.name)
            nodes.append(
                LineageNode(
                    id=target.name,
                    name=target.name,
                    node_type="TARGET",
                )
            )

        return nodes

    @staticmethod
    def _trace_targets(
        field_graph: nx.DiGraph,
        source_fields: set[str],
        target_fields: set[str],
        expressions: dict[str, str],
    ) -> tuple[list[FieldLineagePath], list[LineageWarning]]:
        """Trace each target field to canonical source fields or derivations."""
        results: list[FieldLineagePath] = []
        warnings: list[LineageWarning] = []

        for target_node in sorted(target_fields):
            target_instance, target_field = target_node.split(".", 1)

            if target_node not in field_graph:
                results.append(
                    FieldLineagePath(
                        target_instance=target_instance,
                        target_field=target_field,
                        lineage_type="UNRESOLVED",
                    )
                )
                warnings.append(
                    LineageWarning(
                        warning_type="TARGET_FIELD_NOT_CONNECTED",
                        description=f"No connector reaches {target_node}.",
                        instance_name=target_instance,
                        field_name=target_field,
                    )
                )
                continue

            ancestors = nx.ancestors(field_graph, target_node)
            upstream_sources = sorted(
                ancestors.intersection(source_fields)
            )

            paths: list[list[str]] = []
            for source_node in upstream_sources:
                try:
                    paths.append(
                        nx.shortest_path(
                            field_graph,
                            source_node,
                            target_node,
                        )
                    )
                except nx.NetworkXNoPath:
                    continue

            relevant_nodes = set(ancestors) | {target_node}
            derivations = sorted(
                {
                    expression
                    for node, expression in expressions.items()
                    if node in relevant_nodes
                }
            )

            if paths and derivations:
                lineage_type = "MIXED"
            elif paths:
                lineage_type = "DIRECT"
            elif derivations:
                lineage_type = "DERIVED"
            else:
                lineage_type = "UNRESOLVED"
                warnings.append(
                    LineageWarning(
                        warning_type="FIELD_LINEAGE_UNRESOLVED",
                        description=(
                            "Lineage could not be resolved for "
                            f"{target_node}."
                        ),
                        instance_name=target_instance,
                        field_name=target_field,
                    )
                )

            results.append(
                FieldLineagePath(
                    target_instance=target_instance,
                    target_field=target_field,
                    source_instances=sorted(
                        {
                            item.split(".", 1)[0]
                            for item in upstream_sources
                        }
                    ),
                    source_fields=sorted(
                        {
                            item.split(".", 1)[1]
                            for item in upstream_sources
                        }
                    ),
                    paths=paths,
                    derivation_expressions=derivations,
                    lineage_type=lineage_type,
                )
            )

        return results, warnings
