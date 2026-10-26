"""Advisory GLiNER decision client. Never raises: returns None on any problem."""
import json
import logging
import math
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)
LABELS = {"AUTO_CONVERT", "HUMAN_REVIEW", "BLOCK"}


def gliner_decide(
    text: str,
    url: str,
    timeout: float = 30.0,
) -> dict[str, Any] | None:
    """Return {"label": ..., "confidence": ...}, or None if unavailable."""
    try:
        req = urllib.request.Request(
            url.rstrip("/") + "/decide",
            data=json.dumps({"text": text}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            out = json.loads(resp.read().decode("utf-8"))
        if not isinstance(out, dict):
            raise ValueError("GLiNER response must be a JSON object.")
        label = out.get("label")
        confidence = out.get("confidence")
        if (
            label not in LABELS
            or isinstance(confidence, bool)
            or not isinstance(confidence, (int, float))
            or not math.isfinite(confidence)
            or not 0.0 <= confidence <= 1.0
        ):
            raise ValueError("GLiNER response has an invalid label or confidence.")
        return {"label": label, "confidence": float(confidence)}
    except Exception as exc:  # tunnel closed, server down, bad reply: carry on without it
        logger.warning("GLiNER decision unavailable: %s", exc)
    return None


if __name__ == "__main__":
    print(
        gliner_decide(
            "Simple mapping: one source table, one filter, one reformat, "
            "one target table. Standard components only.",
            "http://127.0.0.1:8100",
        )
    )