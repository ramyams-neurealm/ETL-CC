"""GLiNER decision adapter for discovery, failure triage, and validation."""

from typing import Any

from etl_cc.agents.gliner_decision_client import gliner_decide
from etl_cc.config import settings

class GLiNERService:
    """Use the configured GLiNER HTTP service without blocking async workers."""

    def _decide(self, text: str) -> dict[str, Any] | None:
        if not settings.enable_gliner:
            return None
        return gliner_decide(
            text,
            settings.gliner_service_url,
            settings.gliner_timeout_seconds,
        )

    def triage_failure(self, stage_name: str, short_reason: str) -> dict[str, Any]:
        """Return an advisory routing label and a stage-appropriate next step."""
        result = self._decide(
            "Classify this ETL workflow failure for routing. "
            f"Stage: {stage_name}. Failure: {short_reason}"
        )
        if result is None:
            return {
                "label": "UNKNOWN",
                "confidence": 0.0,
                "suggested_step": "Review the failure reason and worker logs manually.",
            }

        label = result["label"]
        next_steps = {
            "AUTO_CONVERT": (
                "Review the failure reason, fix its cause, then retry the workflow."
            ),
            "HUMAN_REVIEW": (
                "Inspect the failure reason and affected mapping manually."
            ),
            "BLOCK": (
                "Resolve the blocking failure before retrying the workflow."
            ),
        }
        return {
            "label": label,
            "confidence": float(result["confidence"]),
            "suggested_step": next_steps[label],
        }

    def decide_discovery_routing(self, summary_text: str) -> dict[str, Any]:
        """Classify mapping readiness; failures and low confidence require review."""
        result = self._decide(summary_text)
        if result is None:
            return {"decision": "HUMAN_REVIEW", "confidence": 0.0}

        decision = result["label"]
        confidence = float(result["confidence"])
        if (
            decision == "AUTO_CONVERT"
            and confidence < settings.gliner_auto_convert_threshold
        ):
            decision = "HUMAN_REVIEW"
        return {"decision": decision, "confidence": confidence}

    def check_validation_uncertainty(
        self,
        reference_summary: str,
    ) -> dict[str, Any]:
        """Treat non-auto decisions and unavailable advice as validation uncertainty."""
        if not settings.enable_gliner:
            return {
                "uncertain": False,
                "confidence": 0.0,
                "decision": "DISABLED",
            }
        result = self._decide(
            "Assess whether this validation evidence is uncertain and needs human "
            f"review. {reference_summary}"
        )
        if result is None:
            return {
                "uncertain": True,
                "confidence": 0.0,
                "decision": "UNAVAILABLE",
            }

        confidence = float(result["confidence"])
        uncertain = (
            result["label"] != "AUTO_CONVERT"
            or confidence < settings.gliner_auto_convert_threshold
        )
        return {
            "uncertain": uncertain,
            "confidence": confidence,
            "decision": result["label"],
        }


gliner_service = GLiNERService()
