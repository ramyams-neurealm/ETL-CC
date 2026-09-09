import networkx as nx

def calculate_migration_waves(dependencies: list[tuple[str, str]]) -> list[list[str]]:
    graph = nx.DiGraph(dependencies)
    if not nx.is_directed_acyclic_graph(graph):
        raise ValueError("Circular mapping dependency detected")
    waves = []
    while graph.nodes:
        wave = sorted(node for node, degree in graph.in_degree() if degree == 0)
        waves.append(wave)
        graph.remove_nodes_from(wave)
    return waves
