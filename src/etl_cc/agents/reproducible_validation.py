"""Reproducible synthetic validation helpers."""
from __future__ import annotations
import hashlib, json, random
from copy import deepcopy
from typing import Any
GENERATOR_VERSION = "2.0.0"
def canonical_hash(value: Any) -> str:
    if hasattr(value, "model_dump"): value=value.model_dump(mode="json")
    raw=json.dumps(value,sort_keys=True,separators=(",",":"),default=str).encode()
    return hashlib.sha256(raw).hexdigest()
def stable_seed(mapping: Any, requested_seed: int|None=None) -> int:
    if requested_seed is not None: return int(requested_seed)&((1<<63)-1)
    raw=f"{canonical_hash(mapping)}:{GENERATOR_VERSION}".encode()
    return int.from_bytes(hashlib.sha256(raw).digest()[:8],"big")&((1<<63)-1)
def dataset_hashes(datasets: dict[str,list[dict[str,Any]]]) -> dict[str,str]:
    return {name:canonical_hash(rows) for name,rows in sorted(datasets.items())}
def clone_rows(rows: list[dict[str,Any]]) -> list[dict[str,Any]]: return deepcopy(rows)
