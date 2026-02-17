"""Cluster policies API endpoints."""

import json
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from ..core import Dependency, logger
from ..models import (
    AutoScaleConfig,
    ClusterPolicyDetail,
    ClusterPolicySummary,
    ClusterSource,
    ClusterState,
    ClusterSummary,
    PolicyUsage,
)

router = APIRouter(prefix="/api/policies", tags=["policies"])


def _ms_to_datetime(ms: int | None) -> datetime | None:
    """Convert milliseconds timestamp to datetime."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


@router.get("", response_model=list[ClusterPolicySummary])
def list_policies(ws: Dependency.Client) -> list[ClusterPolicySummary]:
    """List all cluster policies in the workspace."""
    logger.info("Listing cluster policies")

    try:
        policies = list(ws.cluster_policies.list())

        summaries = []
        for policy in policies:
            summaries.append(ClusterPolicySummary(
                policy_id=policy.policy_id,
                name=policy.name or "Unnamed Policy",
                definition=policy.definition,
                description=policy.description,
                creator_user_name=policy.creator_user_name,
                created_at_timestamp=_ms_to_datetime(policy.created_at_timestamp),
                is_default=policy.is_default or False,
            ))

        # Sort by name
        summaries.sort(key=lambda p: p.name)

        logger.info(f"Found {len(summaries)} cluster policies")
        return summaries

    except Exception as e:
        logger.error(f"Failed to list cluster policies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{policy_id}", response_model=ClusterPolicyDetail)
def get_policy(policy_id: str, ws: Dependency.Client) -> ClusterPolicyDetail:
    """Get detailed information about a specific cluster policy."""
    logger.info(f"Getting cluster policy {policy_id}")

    try:
        policy = ws.cluster_policies.get(policy_id)

        # Parse definition JSON
        definition_json = {}
        if policy.definition:
            try:
                definition_json = json.loads(policy.definition)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse policy definition JSON for {policy_id}")

        return ClusterPolicyDetail(
            policy_id=policy.policy_id,
            name=policy.name or "Unnamed Policy",
            definition=policy.definition,
            description=policy.description,
            creator_user_name=policy.creator_user_name,
            created_at_timestamp=_ms_to_datetime(policy.created_at_timestamp),
            is_default=policy.is_default or False,
            definition_json=definition_json,
            max_clusters_per_user=policy.max_clusters_per_user,
            policy_family_id=policy.policy_family_id,
            policy_family_definition_overrides=policy.policy_family_definition_overrides,
        )

    except Exception as e:
        logger.error(f"Failed to get cluster policy {policy_id}: {e}")
        raise HTTPException(status_code=404, detail=f"Policy not found: {policy_id}")


@router.get("/{policy_id}/usage", response_model=PolicyUsage)
def get_policy_usage(policy_id: str, ws: Dependency.Client) -> PolicyUsage:
    """Get clusters using a specific policy."""
    logger.info(f"Getting usage for cluster policy {policy_id}")

    try:
        # Get policy details first
        policy = ws.cluster_policies.get(policy_id)

        # Get all clusters and filter by policy
        clusters = list(ws.clusters.list())
        policy_clusters = [c for c in clusters if c.policy_id == policy_id]

        # Convert to summaries
        cluster_summaries = []
        for cluster in policy_clusters:
            autoscale = None
            if cluster.autoscale:
                autoscale = AutoScaleConfig(
                    min_workers=cluster.autoscale.min_workers,
                    max_workers=cluster.autoscale.max_workers,
                )

            # Map state
            state = ClusterState.UNKNOWN
            if cluster.state:
                try:
                    state = ClusterState(cluster.state.value)
                except ValueError:
                    pass

            # Map source
            source = None
            if cluster.cluster_source:
                try:
                    source = ClusterSource(cluster.cluster_source.value)
                except ValueError:
                    pass

            cluster_summaries.append(ClusterSummary(
                cluster_id=cluster.cluster_id,
                cluster_name=cluster.cluster_name or "Unnamed Cluster",
                state=state,
                creator_user_name=cluster.creator_user_name,
                node_type_id=cluster.node_type_id,
                driver_node_type_id=cluster.driver_node_type_id,
                num_workers=cluster.num_workers,
                autoscale=autoscale,
                spark_version=cluster.spark_version,
                cluster_source=source,
                start_time=_ms_to_datetime(cluster.start_time),
                last_activity_time=_ms_to_datetime(getattr(cluster, 'last_activity_time', None)),
            ))

        return PolicyUsage(
            policy_id=policy_id,
            policy_name=policy.name or "Unnamed Policy",
            cluster_count=len(cluster_summaries),
            clusters=cluster_summaries,
        )

    except Exception as e:
        logger.error(f"Failed to get policy usage for {policy_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
