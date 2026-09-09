"""Reusable secure parser for Informatica PowerCenter XML exports."""

from pathlib import Path
from typing import Any

from defusedxml import ElementTree as SafeElementTree

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


class InformaticaXMLParserError(ValueError):
    """Raised when an uploaded or downloaded file is not valid Informatica XML."""


class InformaticaXMLParser:
    """List and parse mappings from a PowerCenter XML export."""

    SOURCE_INSTANCE_TYPES = {
        "SOURCE",
        "SOURCE DEFINITION",
        "SOURCE QUALIFIER",
    }
    TARGET_INSTANCE_TYPES = {
        "TARGET",
        "TARGET DEFINITION",
    }

    @staticmethod
    def _tag(element: Any) -> str:
        """Return an XML tag without a namespace and in uppercase form."""
        return str(element.tag).split("}")[-1].upper()

    @staticmethod
    def _safe_int(value: str | None) -> int | None:
        """Convert an optional XML number to int without failing parsing."""
        try:
            return int(value) if value not in (None, "") else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _attrs(element: Any) -> dict[str, str]:
        """Return all XML attributes as a plain string dictionary."""
        return {
            str(key): str(value)
            for key, value in element.attrib.items()
        }

    @staticmethod
    def _is_nullable(value: str | None) -> bool:
        """Normalize common PowerCenter nullability values."""
        normalized = (value or "YES").strip().upper().replace("_", " ")
        return normalized not in {
            "NO",
            "NOTNULL",
            "NOT NULL",
            "FALSE",
            "0",
        }

    @staticmethod
    def _normalize_instance_type(value: str | None) -> str:
        """Normalize an Informatica instance type for role comparison."""
        return " ".join((value or "").strip().upper().replace("_", " ").split())

    @staticmethod
    def _instance_reference(instance: Any) -> str | None:
        """Return the definition referenced by an INSTANCE element."""
        return (
            instance.attrib.get("TRANSFORMATION_NAME")
            or instance.attrib.get("NAME")
        )

    def _transformation_properties(self, element: Any) -> dict[str, str]:
        """Combine transformation attributes and nested TABLEATTRIBUTE values."""
        properties = self._attrs(element)
        for child in element:
            if self._tag(child) != "TABLEATTRIBUTE":
                continue
            name = child.attrib.get("NAME")
            if name:
                properties[name] = child.attrib.get("VALUE", "")
        return properties

    def _root(self, file_path: Path):
        """Securely parse and validate a PowerCenter XML document."""
        try:
            root = SafeElementTree.parse(file_path).getroot()
        except Exception as exc:
            raise InformaticaXMLParserError(
                "The file is not valid XML."
            ) from exc

        if self._tag(root) != "POWERMART":
            raise InformaticaXMLParserError(
                "The XML root must be POWERMART for a PowerCenter export."
            )
        return root

    def list_mappings(
        self,
        file_path: Path,
        source_reference: str,
    ) -> list[MappingSummary]:
        """Return selectable mapping summaries from one XML export."""
        root = self._root(file_path)
        mappings: list[MappingSummary] = []

        for folder in (
            item for item in root.iter()
            if self._tag(item) == "FOLDER"
        ):
            folder_name = folder.attrib.get("NAME", "UNKNOWN")
            for mapping in (
                item for item in folder
                if self._tag(item) == "MAPPING"
            ):
                mapping_name = mapping.attrib.get("NAME")
                if not mapping_name:
                    continue
                mappings.append(
                    MappingSummary(
                        source_object_key=(
                            f"{folder_name}/MAPPING/{mapping_name}"
                        ),
                        mapping_name=mapping_name,
                        folder_name=folder_name,
                        source_reference=source_reference,
                    )
                )

        return mappings

    def parse_selected(
        self,
        file_path: Path,
        selected_keys: set[str] | None,
        source_vendor: str,
        source_reference: str,
    ) -> list[CanonicalMapping]:
        """Parse selected mappings, or all mappings when selection is None."""
        root = self._root(file_path)
        results: list[CanonicalMapping] = []

        for folder in (
            item for item in root.iter()
            if self._tag(item) == "FOLDER"
        ):
            folder_name = folder.attrib.get("NAME", "UNKNOWN")
            sources_by_name = {
                item.attrib.get("NAME", "UNKNOWN"): item
                for item in folder
                if self._tag(item) == "SOURCE"
            }
            targets_by_name = {
                item.attrib.get("NAME", "UNKNOWN"): item
                for item in folder
                if self._tag(item) == "TARGET"
            }

            for mapping in (
                item for item in folder
                if self._tag(item) == "MAPPING"
            ):
                mapping_name = mapping.attrib.get("NAME")
                if not mapping_name:
                    continue

                source_object_key = (
                    f"{folder_name}/MAPPING/{mapping_name}"
                )
                if (
                    selected_keys is not None
                    and source_object_key not in selected_keys
                ):
                    continue

                results.append(
                    self._parse_mapping(
                        folder=folder,
                        mapping=mapping,
                        folder_name=folder_name,
                        mapping_name=mapping_name,
                        key=source_object_key,
                        sources_by_name=sources_by_name,
                        targets_by_name=targets_by_name,
                        source_vendor=source_vendor,
                        source_reference=source_reference,
                    )
                )

        return results

    def _parse_mapping(
        self,
        folder: Any,
        mapping: Any,
        folder_name: str,
        mapping_name: str,
        key: str,
        sources_by_name: dict[str, Any],
        targets_by_name: dict[str, Any],
        source_vendor: str,
        source_reference: str,
    ) -> CanonicalMapping:
        """Convert one MAPPING element into the canonical mapping contract."""
        instances = [
            item
            for item in mapping
            if self._tag(item) == "INSTANCE"
        ]

        source_instance_names = {
            reference
            for item in instances
            if self._normalize_instance_type(item.attrib.get("TYPE"))
            in self.SOURCE_INSTANCE_TYPES
            if (reference := self._instance_reference(item))
        }

        target_instance_names = {
            reference
            for item in instances
            if self._normalize_instance_type(item.attrib.get("TYPE"))
            in self.TARGET_INSTANCE_TYPES
            if (reference := self._instance_reference(item))
        }

        sources = [
            CanonicalDataset(
                name=name,
                dataset_type="SOURCE",
                fields=[
                    CanonicalField(
                        name=field.attrib.get("NAME", "UNKNOWN"),
                        data_type=field.attrib.get(
                            "DATATYPE",
                            "UNKNOWN",
                        ),
                        precision=self._safe_int(
                            field.attrib.get("PRECISION")
                        ),
                        scale=self._safe_int(
                            field.attrib.get("SCALE")
                        ),
                        nullable=self._is_nullable(
                            field.attrib.get("NULLABLE")
                        ),
                    )
                    for field in element
                    if self._tag(field) == "SOURCEFIELD"
                ],
                properties=self._attrs(element),
            )
            for name, element in sources_by_name.items()
            if name in source_instance_names
        ]

        targets = [
            CanonicalDataset(
                name=name,
                dataset_type="TARGET",
                fields=[
                    CanonicalField(
                        name=field.attrib.get("NAME", "UNKNOWN"),
                        data_type=field.attrib.get(
                            "DATATYPE",
                            "UNKNOWN",
                        ),
                        precision=self._safe_int(
                            field.attrib.get("PRECISION")
                        ),
                        scale=self._safe_int(
                            field.attrib.get("SCALE")
                        ),
                        nullable=self._is_nullable(
                            field.attrib.get("NULLABLE")
                        ),
                    )
                    for field in element
                    if self._tag(field) == "TARGETFIELD"
                ],
                properties=self._attrs(element),
            )
            for name, element in targets_by_name.items()
            if name in target_instance_names
        ]

        transformations = [
            CanonicalTransformation(
                name=item.attrib.get("NAME", "UNKNOWN"),
                transformation_type=item.attrib.get(
                    "TYPE",
                    "UNKNOWN",
                ),
                ports=[
                    CanonicalPort(
                        name=field.attrib.get("NAME", "UNKNOWN"),
                        direction=field.attrib.get(
                            "PORTTYPE",
                            "UNKNOWN",
                        ),
                        data_type=field.attrib.get("DATATYPE"),
                        expression=field.attrib.get("EXPRESSION"),
                        properties=self._attrs(field),
                    )
                    for field in item
                    if self._tag(field) == "TRANSFORMFIELD"
                ],
                properties=self._transformation_properties(item),
            )
            for item in mapping
            if self._tag(item) == "TRANSFORMATION"
        ]

        connectors = [
            CanonicalConnector(
                from_instance=item.attrib.get(
                    "FROMINSTANCE",
                    "UNKNOWN",
                ),
                to_instance=item.attrib.get(
                    "TOINSTANCE",
                    "UNKNOWN",
                ),
                from_field=item.attrib.get("FROMFIELD"),
                to_field=item.attrib.get("TOFIELD"),
            )
            for item in mapping
            if self._tag(item) == "CONNECTOR"
        ]

        parameters = [
            CanonicalParameter(
                name=item.attrib.get("NAME", "UNKNOWN"),
                parameter_type=self._tag(item),
                data_type=item.attrib.get("DATATYPE"),
                default_value=item.attrib.get("DEFAULTVALUE"),
                scope="MAPPING",
            )
            for item in mapping
            if self._tag(item)
            in {"MAPPINGVARIABLE", "MAPPINGPARAMETER"}
        ]

        sessions = [
            self._attrs(item)
            for item in folder.iter()
            if self._tag(item) == "SESSION"
            and item.attrib.get("MAPPINGNAME") == mapping_name
        ]

        workflows = [
            self._attrs(item)
            for item in folder.iter()
            if self._tag(item) == "WORKFLOW"
            and any(
                self._tag(descendant) == "SESSION"
                and descendant.attrib.get("MAPPINGNAME") == mapping_name
                for descendant in item.iter()
            )
        ]

        return CanonicalMapping(
            source_vendor=source_vendor,
            source_object_key=key,
            mapping_name=mapping_name,
            folder_name=folder_name,
            description=mapping.attrib.get("DESCRIPTION"),
            sources=sources,
            targets=targets,
            transformations=transformations,
            connectors=connectors,
            sessions=sessions,
            workflows=workflows,
            parameters=parameters,
            source_metadata={
                "mapping_attributes": self._attrs(mapping),
                "source_reference": source_reference,
                "source_instance_names": sorted(
                    source_instance_names
                ),
                "target_instance_names": sorted(
                    target_instance_names
                ),
            },
        )
