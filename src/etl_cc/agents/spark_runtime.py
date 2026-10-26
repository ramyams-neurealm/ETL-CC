"""Helpers for configuring local Spark subprocess environments."""
from __future__ import annotations

import os
from pathlib import Path


def add_java_home_to_path(environment: dict[str, str]) -> None:
    """Make JAVA_HOME's Java executable discoverable to Spark child processes."""
    java_home = environment.get("JAVA_HOME")
    if not java_home:
        return

    java_bin = Path(java_home) / "bin"
    executable_names = ("java.exe", "java") if os.name == "nt" else ("java",)
    if not any((java_bin / name).is_file() for name in executable_names):
        return

    current_path = environment.get("PATH", "")
    normalized_java_bin = os.path.normcase(os.path.abspath(java_bin))
    path_entries = current_path.split(os.pathsep) if current_path else []
    if any(
        os.path.normcase(os.path.abspath(entry)) == normalized_java_bin
        for entry in path_entries
        if entry
    ):
        return

    environment["PATH"] = os.pathsep.join(
        [str(java_bin), *path_entries]
    )
