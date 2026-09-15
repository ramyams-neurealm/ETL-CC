"""Deterministic ETL migration economics calculations.

Measured mapping metadata is converted into estimates using one centralized,
versioned policy. Policy coefficients are business assumptions, while the
repository-specific result is calculated from actual Discovery metadata.
"""

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class DiscoveryEconomicsPolicy:
    """Versioned business policy used for deterministic effort estimates."""

    policy_name: str = "DISCOVERY_ECONOMICS"
    policy_version: str = "1.0.0"
    currency: str = "USD"

    base_hours_per_mapping: float = 2.0
    hours_per_complexity_point: float = 0.25
    hours_per_transformation: float = 1.0
    hours_per_dependency: float = 1.5
    human_review_hours: float = 4.0
    blocked_mapping_hours: float = 8.0

    hourly_labor_cost: float = 100.0
    automation_savings_percentage: float = 70.0


POLICY = DiscoveryEconomicsPolicy()


def _dependency_count(mapping: dict[str, Any]) -> int:
    dependencies = mapping.get("dependencies")
    if isinstance(dependencies, list):
        return len(dependencies)
    if isinstance(dependencies, dict):
        items = dependencies.get("items")
        if isinstance(items, list):
            return len(items)
        upstream = dependencies.get("upstream_mapping_keys")
        if isinstance(upstream, list):
            return len(upstream)
    return 0


def calculate_mapping_manual_hours(
    mapping: dict[str, Any],
    policy: DiscoveryEconomicsPolicy = POLICY,
) -> float:
    """Estimate manual effort for one analyzed mapping."""

    complexity_score = max(float(mapping.get("complexity_score") or 0), 0.0)
    transformation_count = max(int(mapping.get("transformation_count") or 0), 0)
    dependency_count = _dependency_count(mapping)

    hours = (
        policy.base_hours_per_mapping
        + complexity_score * policy.hours_per_complexity_point
        + transformation_count * policy.hours_per_transformation
        + dependency_count * policy.hours_per_dependency
    )

    if bool(mapping.get("human_review_required")):
        hours += policy.human_review_hours
    if mapping.get("status") == "BLOCKED":
        hours += policy.blocked_mapping_hours

    return round(hours, 2)


def calculate_discovery_economics(
    mappings: list[dict[str, Any]],
    actual_input_tokens: int,
    actual_output_tokens: int,
    policy: DiscoveryEconomicsPolicy = POLICY,
) -> dict[str, Any]:
    """Calculate repository Discovery effort and cost estimates."""

    analyzed = [
        mapping
        for mapping in mappings
        if mapping.get("discovery_status") == "ANALYZED"
    ]
    input_tokens = max(int(actual_input_tokens or 0), 0)
    output_tokens = max(int(actual_output_tokens or 0), 0)
    total_tokens = input_tokens + output_tokens

    if not analyzed:
        return {
            "manual_hours_required": 0.0,
            "manual_cost": 0.0,
            "automated_effort_hours": 0.0,
            "effort_saved": 0.0,
            "cost_saved": 0.0,
            "actual_input_tokens": input_tokens,
            "actual_output_tokens": output_tokens,
            "actual_total_tokens": total_tokens,
            "token_utilization_saved": None,
            "calculation_status": "NO_ANALYZED_MAPPINGS",
            "calculated_mapping_count": 0,
            "calculation_policy": asdict(policy),
        }

    mapping_hours = {
        str(mapping.get("etl_object_id")): calculate_mapping_manual_hours(
            mapping, policy
        )
        for mapping in analyzed
    }
    manual_hours = sum(mapping_hours.values())
    savings_ratio = policy.automation_savings_percentage / 100.0
    effort_saved = manual_hours * savings_ratio
    automated_effort = manual_hours - effort_saved
    manual_cost = manual_hours * policy.hourly_labor_cost
    cost_saved = effort_saved * policy.hourly_labor_cost

    return {
        "manual_hours_required": round(manual_hours, 2),
        "manual_cost": round(manual_cost, 2),
        "automated_effort_hours": round(automated_effort, 2),
        "effort_saved": round(effort_saved, 2),
        "cost_saved": round(cost_saved, 2),
        "actual_input_tokens": input_tokens,
        "actual_output_tokens": output_tokens,
        "actual_total_tokens": total_tokens,
        "token_utilization_saved": None,
        "calculation_status": "CALCULATED",
        "calculated_mapping_count": len(analyzed),
        "mapping_manual_hours": mapping_hours,
        "calculation_policy": asdict(policy),
    }


def calculate_conversion_economics(
    mapping: dict[str, Any],
    agent_duration_seconds: float,
    policy: DiscoveryEconomicsPolicy = POLICY,
) -> dict[str, Any]:
    """Calculate mapping-level conversion economics from measured metadata."""
    manual_hours = calculate_mapping_manual_hours(mapping, policy)
    agent_effort_hours = max(float(agent_duration_seconds or 0), 0.0) / 3600.0
    effort_saved = max(manual_hours - agent_effort_hours, 0.0)
    manual_cost = manual_hours * policy.hourly_labor_cost
    agent_labor_equivalent_cost = agent_effort_hours * policy.hourly_labor_cost
    cost_saved = max(manual_cost - agent_labor_equivalent_cost, 0.0)
    automation_rate = (
        (effort_saved / manual_hours) * 100.0 if manual_hours else 0.0
    )
    return {
        "manual_hours": round(manual_hours, 2),
        "manual_rate": policy.hourly_labor_cost,
        "manual_cost": round(manual_cost, 2),
        "agent_effort_hours": round(agent_effort_hours, 4),
        "agent_labor_equivalent_cost": round(agent_labor_equivalent_cost, 2),
        "effort_saved_hours": round(effort_saved, 2),
        "cost_saved": round(cost_saved, 2),
        "automation_rate_percentage": round(automation_rate, 2),
        "currency": policy.currency,
        "calculation_status": "CALCULATED",
        "calculation_policy": asdict(policy),
    }
