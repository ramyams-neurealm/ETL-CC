import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class GLiNERService:
    def triage_failure(self, stage_name: str, short_reason: str) -> Dict[str, Any]:
        return {
            "label": "Parse or unsupported construct", 
            "confidence": 0.95, 
            "suggested_step": "Inspect workbench; check unsupported constructs"
        }

    def decide_discovery_routing(self, summary_text: str) -> Dict[str, Any]:
        return {"decision": "AUTO_CONVERT", "confidence": 0.92}

    def check_validation_uncertainty(self, reference_summary: str) -> Dict[str, Any]:
        return {"uncertain": False, "confidence": 0.98}

gliner_service = GLiNERService()