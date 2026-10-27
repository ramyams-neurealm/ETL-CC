import json, sys, requests

SUPPORTED = {"FILTER", "REFORMAT", "JOIN", "SORT", "ROLLUP", "DEDUP"}  # example list, replace with the project's real one

def summarize(graph):
    ds = graph["datasets"]
    comps = graph.get("components", [])
    types = [c["type"] for c in comps]
    unsupported = [t for t in types if t not in SUPPORTED]
    ports = [p for c in comps for p in c.get("ports", [])]
    calls = sum(1 for p in ports if "(" in p.get("expression", ""))
    joins = sum(1 for t in types if t == "JOIN")
    return (
        f"Mapping reads {len(ds['sources'])} source table and writes {len(ds['targets'])} target table. "
        f"It has {len(comps)} components of types {', '.join(types)}. "
        + ("All component types are standard and supported. " if not unsupported
           else f"Unsupported component types: {', '.join(unsupported)}. ")
        + f"{calls} output fields use a function call. "
        + ("No joins. " if joins == 0 else f"{joins} joins. ")
        + "No custom code."
    )

def main():
    with open(sys.argv[1], encoding="utf-8") as source_file:
        data = json.load(source_file)
    for graph in data["graphs"]:
        summary = summarize(graph)
        print(graph["name"], "\nSUMMARY:", summary)
        response = requests.post(
            "http://127.0.0.1:8100/decide",
            json={"text": summary},
            timeout=30,
        )
        print("DECISION:", response.json())


if __name__ == "__main__":
    main()