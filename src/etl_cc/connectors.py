"""Source connectors for PowerCenter live repositories and GitHub XML exports."""

import asyncio
import base64
import os
import re
from abc import ABC, abstractmethod
from pathlib import Path
from urllib.parse import quote, urlparse

import httpx

from etl_cc.config import settings
from etl_cc.informatica_parser import InformaticaXMLParser
from etl_cc.models import CanonicalMapping, MappingSummary


class ETLConnectorError(Exception):
    pass


class ETLConnectionError(ETLConnectorError):
    pass


class ETLAuthenticationError(ETLConnectorError):
    pass


class ETLRepositoryNotFoundError(ETLConnectorError):
    pass


class ETLMetadataError(ETLConnectorError):
    pass


class MappingSource(ABC):
    @abstractmethod
    async def list_mappings(self) -> list[MappingSummary]:
        pass

    @abstractmethod
    async def get_mapping_details(self, keys: list[str] | None) -> list[CanonicalMapping]:
        pass


class PowerCenterSource(MappingSource):
    """Use pmrep to list and export PowerCenter mappings."""

    PASSWORD_ENV = "ETL_CC_PMREP_PASSWORD"

    def __init__(self, config: dict, password: str):
        self.config = config
        self.password = password
        self.parser = InformaticaXMLParser()

    async def _run(self, args: list[str]) -> str:
        env = os.environ.copy()
        env[self.PASSWORD_ENV] = self.password
        try:
            process = await asyncio.create_subprocess_exec(
                settings.informatica_pmrep_path,
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), settings.informatica_command_timeout_seconds
            )
        except FileNotFoundError as exc:
            raise ETLConnectionError("pmrep is not installed or its path is incorrect.") from exc
        except asyncio.TimeoutError as exc:
            process.kill()
            await process.communicate()
            raise ETLConnectionError("The PowerCenter command timed out.") from exc
        message = (stderr or stdout).decode(errors="replace").strip()
        if process.returncode != 0:
            lowered = message.lower()
            if "password" in lowered or "login" in lowered or "authentication" in lowered:
                raise ETLAuthenticationError(message)
            raise ETLConnectionError(message or "PowerCenter command failed.")
        return stdout.decode(errors="replace")

    def _connect_args(self) -> list[str]:
        return [
            "connect", "-r", self.config["repository_name"],
            "-h", self.config["host"], "-o", str(self.config["port"]),
            "-n", self.config["username"], "-s", self.config.get("security_domain", "Native"),
            "-X", self.PASSWORD_ENV,
        ]

    async def test(self) -> None:
        await self._run(self._connect_args())

    @staticmethod
    def _names(output: str) -> list[str]:
        result: list[str] = []
        for raw in output.splitlines():
            line = raw.strip().strip('"')
            if not line or line.startswith("-") or line.lower().startswith("informatica"):
                continue
            candidate = re.split(r"\t+|\s{2,}", line)[-1].strip('" ')
            if candidate and candidate.lower() not in {"name", "folder name", "object name"}:
                result.append(candidate)
        return list(dict.fromkeys(result))

    async def list_mappings(self) -> list[MappingSummary]:
        await self.test()
        folders = self._names(await self._run(["listobjects", "-o", "folder"]))
        mappings: list[MappingSummary] = []
        for folder in folders:
            await self.test()
            output = await self._run(["listobjects", "-o", "mapping", "-f", folder])
            for name in self._names(output):
                mappings.append(
                    MappingSummary(
                        source_object_key=f"{folder}/MAPPING/{name}",
                        mapping_name=name,
                        folder_name=folder,
                    )
                )
        return mappings

    async def get_mapping_details(self, keys: list[str] | None) -> list[CanonicalMapping]:
        if not keys:
            keys = [item.source_object_key for item in await self.list_mappings()]
        export_root = settings.export_directory
        export_root.mkdir(parents=True, exist_ok=True)
        results: list[CanonicalMapping] = []
        for index, key in enumerate(keys, 1):
            try:
                folder, name = key.split("/MAPPING/", 1)
            except ValueError as exc:
                raise ETLMetadataError(f"Invalid mapping key: {key}") from exc
            output_file = export_root / f"pc_{index}_{name}.xml"
            await self.test()
            await self._run([
                "objectexport", "-n", name, "-o", "mapping", "-f", folder,
                "-u", str(output_file),
            ])
            results.extend(
                self.parser.parse_selected(
                    output_file, {key}, "INFORMATICA_POWERCENTER", str(output_file)
                )
            )
            output_file.unlink(missing_ok=True)
        return results


class GitHubSource(MappingSource):
    """Read Informatica XML exports from a GitHub repository through its API."""

    def __init__(self, config: dict, token: str | None, local_directory: Path):
        self.config = config
        self.token = token
        self.local_directory = local_directory
        self.parser = InformaticaXMLParser()

    def _coordinates(self) -> tuple[str, str]:
        parsed = urlparse(self.config["repository_url"])
        parts = parsed.path.strip("/").removesuffix(".git").split("/")
        if parsed.netloc.lower() != "github.com" or len(parts) != 2:
            raise ETLConnectionError("repository_url must be a GitHub owner/repository URL.")
        return parts[0], parts[1]

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/vnd.github+json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    async def download_xml_files(self) -> list[Path]:
        owner, repo = self._coordinates()
        branch = self.config.get("branch", "main")
        folder = self.config.get("folder_path", "").strip("/")
        url = f"https://api.github.com/repos/{quote(owner)}/{quote(repo)}/contents"
        if folder:
            url += f"/{quote(folder, safe='/')}"
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            response = await client.get(url, params={"ref": branch}, headers=self._headers())
            if response.status_code in {401, 403}:
                raise ETLAuthenticationError("GitHub authentication or repository access failed.")
            if response.status_code == 404:
                raise ETLRepositoryNotFoundError("GitHub repository, branch, or folder was not found.")
            response.raise_for_status()
            items = response.json()
            if not isinstance(items, list):
                items = [items]
            xml_items = [
                item for item in items
                if item.get("type") == "file" and item.get("name", "").lower().endswith(".xml")
            ]
            if not xml_items:
                raise ETLMetadataError("No Informatica XML files were found in the configured folder.")
            self.local_directory.mkdir(parents=True, exist_ok=True)
            files: list[Path] = []
            for item in xml_items:
                file_response = await client.get(item["url"], headers=self._headers())
                file_response.raise_for_status()
                content = file_response.json().get("content", "")
                data = base64.b64decode(content)
                path = self.local_directory / Path(item["name"]).name
                path.write_bytes(data)
                files.append(path)
            return files

    async def list_mappings(self) -> list[MappingSummary]:
        files = await self.download_xml_files()
        result: list[MappingSummary] = []
        for file_path in files:
            result.extend(self.parser.list_mappings(file_path, file_path.name))
        return result

    async def get_mapping_details(self, keys: list[str] | None) -> list[CanonicalMapping]:
        files = list(self.local_directory.glob("*.xml")) or await self.download_xml_files()
        selected = set(keys) if keys else None
        result: list[CanonicalMapping] = []
        for file_path in files:
            result.extend(
                self.parser.parse_selected(
                    file_path, selected, "INFORMATICA_GITHUB", file_path.name
                )
            )
        return result
