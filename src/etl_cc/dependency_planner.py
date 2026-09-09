"""Repository-level dependency planning for selected canonical mappings."""

from collections import defaultdict
from typing import Any

import networkx as nx
from pydantic import BaseModel, Field

from etl_cc.models import CanonicalMapping


class MappingDependency(BaseModel):
    upstream_object_key: str
    downstream_object_key: str
    dependency_source: str = "TARGET_SOURCE_MATCH"
    matched_dataset: str
    confidence: float = 1.0


class MappingPlan(BaseModel):
    source_object_key: str
    upstream_mapping_keys: list[str] = Field(default_factory=list)
    downstream_mapping_keys: list[str] = Field(default_factory=list)
    migration_wave: int | None = None
    has_dependency_cycle: bool = False


class DependencyPlan(BaseModel):
    dependencies: list[MappingDependency] = Field(default_factory=list)
    mapping_plans: dict[str, MappingPlan] = Field(default_factory=dict)
    migration_waves: dict[str, list[str]] = Field(default_factory=dict)
    cycles: list[list[str]] = Field(default_factory=list)
    unresolved_datasets: list[dict[str, Any]] = Field(default_factory=list)
    has_dependency_cycle: bool = False
    human_review_required: bool = False


class DependencyPlanner:
    """Match mapping targets to mapping sources and assign migration waves."""

    AGENT_NAME = "DEPENDENCY_PLANNER"
    AGENT_VERSION = "1.0.0"
    STAGE_NAME = "DEPENDENCY_PLANNING"
    MODEL_NAME = "DETERMINISTIC_NETWORKX"

    async def run(self, mappings: list[CanonicalMapping]) -> DependencyPlan:
        graph = nx.DiGraph()
        by_key = {mapping.source_object_key: mapping for mapping in mappings}
        graph.add_nodes_from(by_key)

        producers: dict[str, set[str]] = defaultdict(set)
        consumers: dict[str, set[str]] = defaultdict(set)
        for mapping in mappings:
            for target in mapping.targets:
                producers[self._dataset_key(target.name)].add(mapping.source_object_key)
            for source in mapping.sources:
                consumers[self._dataset_key(source.name)].add(mapping.source_object_key)

        dependencies: list[MappingDependency] = []
        seen: set[tuple[str, str, str]] = set()
        for dataset in sorted(set(producers).intersection(consumers)):
            for upstream in sorted(producers[dataset]):
                for downstream in sorted(consumers[dataset]):
                    if upstream == downstream:
                        continue
                    edge_key = (upstream, downstream, dataset)
                    if edge_key in seen:
                        continue
                    seen.add(edge_key)
                    graph.add_edge(upstream, downstream, matched_dataset=dataset)
                    dependencies.append(
                        MappingDependency(
                            upstream_object_key=upstream,
                            downstream_object_key=downstream,
                            matched_dataset=dataset,
                        )
                    )

        cycles = [self._closed_cycle(cycle) for cycle in nx.simple_cycles(graph)]
        cyclic_nodes = {node for cycle in cycles for node in cycle}
        waves = self._assign_waves(graph, cyclic_nodes)

        mapping_plans: dict[str, MappingPlan] = {}
        grouped_waves: dict[str, list[str]] = defaultdict(list)
        for mapping_key in sorted(graph.nodes):
            wave = waves.get(mapping_key)
            if wave is not None:
                grouped_waves[str(wave)].append(mapping_key)
            mapping_plans[mapping_key] = MappingPlan(
                source_object_key=mapping_key,
                upstream_mapping_keys=sorted(graph.predecessors(mapping_key)),
                downstream_mapping_keys=sorted(graph.successors(mapping_key)),
                migration_wave=wave,
                has_dependency_cycle=mapping_key in cyclic_nodes,
            )

        unresolved = []
        produced = set(producers)
        for dataset, mapping_keys in sorted(consumers.items()):
            if dataset not in produced:
                unresolved.append(
                    {
                        "dataset": dataset,
                        "consumer_mapping_keys": sorted(mapping_keys),
                        "reason": "No selected mapping produces this source dataset.",
                    }
                )

        return DependencyPlan(
            dependencies=dependencies,
            mapping_plans=mapping_plans,
            migration_waves=dict(sorted(grouped_waves.items(), key=lambda item: int(item[0]))),
            cycles=cycles,
            unresolved_datasets=unresolved,
            has_dependency_cycle=bool(cycles),
            human_review_required=bool(cycles),
        )

    @staticmethod
    def _dataset_key(name: str) -> str:
        return name.strip().upper()

    @staticmethod
    def _closed_cycle(cycle: list[str]) -> list[str]:
        return cycle + [cycle[0]] if cycle else cycle

    @staticmethod
    def _assign_waves(graph: nx.DiGraph, cyclic_nodes: set[str]) -> dict[str, int]:
        acyclic_nodes = [node for node in graph.nodes if node not in cyclic_nodes]
        acyclic = graph.subgraph(acyclic_nodes).copy()
        waves: dict[str, int] = {}
        for node in nx.topological_sort(acyclic):
            predecessors = list(acyclic.predecessors(node))
            waves[node] = 1 if not predecessors else max(waves[item] for item in predecessors) + 1
        return waves
