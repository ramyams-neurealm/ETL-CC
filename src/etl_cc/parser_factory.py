"""Select source parsers while preserving the shared canonical contracts."""

from etl_cc.datastage_parser import DataStageParser
from etl_cc.informatica_parser import InformaticaXMLParser


class ParserFactoryError(ValueError):
    """Raised when no parser is registered for a source selection."""


def get_parser(product_code: str, method_code: str):
    """Return the parser for an uploaded or exported source."""
    key = (product_code.upper(), method_code.upper())
    if key in {
        ("INFORMATICA", "XML_UPLOAD"),
        ("INFORMATICA", "GITHUB"),
        ("INFORMATICA", "POWERCENTER"),
    }:
        return InformaticaXMLParser()
    if key in {
        ("DATASTAGE", "DSX_UPLOAD"),
        ("DATASTAGE", "XML_UPLOAD"),
    }:
        return DataStageParser()
    raise ParserFactoryError(
        f"No parser is registered for {product_code}/{method_code}."
    )
