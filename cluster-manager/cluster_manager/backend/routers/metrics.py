"""Metrics and analytics API endpoints."""

from datetime import datetime, timezone

from databricks.sdk.service.compute import State
from fastapi import APIRouter

from ..core import Dependency, logger
from ..models import (
    ClusterMetricsSummary,
    ClusterState,
    IdleClusterAlert,
    OptimizationRecommendation,
)

router = APIRouter(prefix="/api/metrics", tags=["metrics"])


def _state_to_enum(state: State | None) -> ClusterState:
    """Convert SDK State to our ClusterState enum."""
    if state is None:
        return ClusterState.UNKNOWN
    try:
        return ClusterState(state.value)
    except ValueError:
        return ClusterState.UNKNOWN


def _ms_to_datetime(ms: int | None) -> datetime | None:
    """Convert milliseconds timestamp to datetime."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


@router.get("/summary", response_model=ClusterMetricsSummary)
def get_metrics_summary(ws: Dependency.Client) -> ClusterMetricsSummary:
    """Get a summary of cluster metrics across the workspace.

    Returns counts of clusters by state and estimated hourly DBU usage.
    """
    logger.info("Getting metrics summary")

    clusters = list(ws.clusters.list())

    total_clusters = len(clusters)
    running_clusters = 0
    pending_clusters = 0
    terminated_clusters = 0
    total_running_workers = 0
    estimated_hourly_dbu = 0.0

    for cluster in clusters:
        state = _state_to_enum(cluster.state)

        if state == ClusterState.RUNNING:
            running_clusters += 1
            # Count workers
            if cluster.num_workers:
                total_running_workers += cluster.num_workers
            elif cluster.autoscale:
                # Use current number or average of min/max
                total_running_workers += (cluster.autoscale.min_workers + cluster.autoscale.max_workers) // 2

            # Estimate DBU (rough: 1 DBU per node per hour)
            workers = cluster.num_workers or 0
            if cluster.autoscale:
                workers = (cluster.autoscale.min_workers + cluster.autoscale.max_workers) / 2
            estimated_hourly_dbu += (workers + 1)  # +1 for driver

        elif state == ClusterState.PENDING:
            pending_clusters += 1
        elif state == ClusterState.TERMINATED:
            terminated_clusters += 1

    return ClusterMetricsSummary(
        total_clusters=total_clusters,
        running_clusters=running_clusters,
        pending_clusters=pending_clusters,
        terminated_clusters=terminated_clusters,
        total_running_workers=total_running_workers,
        estimated_hourly_dbu=estimated_hourly_dbu,
    )


@router.get("/idle-clusters", response_model=list[IdleClusterAlert])
def get_idle_clusters(ws: Dependency.Client) -> list[IdleClusterAlert]:
    """Get clusters that are running but have been idle for too long.

    A cluster is considered idle if it has been running with no activity
    for more than 30 minutes.
    """
    logger.info("Getting idle clusters")

    clusters = list(ws.clusters.list())
    alerts = []
    now = datetime.now(timezone.utc)

    # Idle threshold: 30 minutes
    idle_threshold_minutes = 30

    for cluster in clusters:
        if cluster.state != State.RUNNING:
            continue

        # Check last activity time
        last_activity = _ms_to_datetime(getattr(cluster, 'last_activity_time', None))
        if last_activity is None:
            # Use start time if no activity recorded
            last_activity = _ms_to_datetime(cluster.start_time)

        if last_activity is None:
            continue

        idle_duration = int((now - last_activity).total_seconds() / 60)

        if idle_duration >= idle_threshold_minutes:
            # Calculate wasted DBU
            workers = cluster.num_workers or 0
            if cluster.autoscale:
                workers = (cluster.autoscale.min_workers + cluster.autoscale.max_workers) / 2
            dbu_per_hour = workers + 1  # +1 for driver
            wasted_dbu = dbu_per_hour * (idle_duration / 60)

            # Determine recommendation
            auto_terminate = getattr(cluster, 'autotermination_minutes', None)
            if auto_terminate is None or auto_terminate == 0:
                recommendation = "Configure auto-termination to prevent idle costs"
            else:
                recommendation = "Consider stopping this cluster to save costs"

            alerts.append(IdleClusterAlert(
                cluster_id=cluster.cluster_id,
                cluster_name=cluster.cluster_name or "Unnamed Cluster",
                idle_duration_minutes=idle_duration,
                estimated_wasted_dbu=round(wasted_dbu, 2),
                recommendation=recommendation,
            ))

    # Sort by wasted DBU (highest first)
    alerts.sort(key=lambda a: a.estimated_wasted_dbu, reverse=True)

    logger.info(f"Found {len(alerts)} idle clusters")
    return alerts


@router.get("/recommendations", response_model=list[OptimizationRecommendation])
def get_recommendations(ws: Dependency.Client) -> list[OptimizationRecommendation]:
    """Get optimization recommendations for clusters.

    Analyzes cluster configurations and usage patterns to suggest improvements.
    """
    logger.info("Getting optimization recommendations")

    clusters = list(ws.clusters.list())
    recommendations = []
    now = datetime.now(timezone.utc)

    for cluster in clusters:
        cluster_name = cluster.cluster_name or "Unnamed Cluster"

        # Check 1: No auto-termination configured
        auto_terminate = getattr(cluster, 'autotermination_minutes', None)
        if cluster.state == State.RUNNING and (auto_terminate is None or auto_terminate == 0):
            recommendations.append(OptimizationRecommendation(
                cluster_id=cluster.cluster_id,
                cluster_name=cluster_name,
                issue="No auto-termination configured",
                recommendation="Set auto-termination to 30-120 minutes to prevent idle costs",
                potential_savings="Up to $50-200/month depending on usage",
                priority="high",
            ))

        # Check 2: Large fixed-size cluster (could use autoscaling)
        if cluster.num_workers and cluster.num_workers >= 10 and cluster.autoscale is None:
            recommendations.append(OptimizationRecommendation(
                cluster_id=cluster.cluster_id,
                cluster_name=cluster_name,
                issue=f"Large fixed-size cluster ({cluster.num_workers} workers)",
                recommendation="Consider enabling autoscaling to match workload demand",
                potential_savings="10-40% cost reduction during low-demand periods",
                priority="medium",
            ))

        # Check 3: Check if cluster has been running for very long
        if cluster.state == State.RUNNING and cluster.start_time:
            start = _ms_to_datetime(cluster.start_time)
            if start:
                running_hours = (now - start).total_seconds() / 3600
                if running_hours > 24:
                    recommendations.append(OptimizationRecommendation(
                        cluster_id=cluster.cluster_id,
                        cluster_name=cluster_name,
                        issue=f"Cluster running for {int(running_hours)} hours",
                        recommendation="Verify this cluster is actively needed; consider jobs clusters for batch workloads",
                        potential_savings="Varies by workload pattern",
                        priority="medium" if running_hours < 72 else "high",
                    ))

        # Check 4: Wide autoscale range
        if cluster.autoscale:
            range_size = cluster.autoscale.max_workers - cluster.autoscale.min_workers
            if range_size > 20:
                recommendations.append(OptimizationRecommendation(
                    cluster_id=cluster.cluster_id,
                    cluster_name=cluster_name,
                    issue=f"Wide autoscale range ({cluster.autoscale.min_workers}-{cluster.autoscale.max_workers} workers)",
                    recommendation="Review if this range is necessary; consider tighter bounds for predictable workloads",
                    potential_savings="More predictable costs and faster scaling",
                    priority="low",
                ))

        # Check 5: Old Spark version
        if cluster.spark_version:
            # Check if using an older DBR version (simplified check)
            version_parts = cluster.spark_version.split(".")
            if version_parts and version_parts[0].isdigit():
                major_version = int(version_parts[0])
                if major_version < 13:  # DBR 13+ recommended as of 2024
                    recommendations.append(OptimizationRecommendation(
                        cluster_id=cluster.cluster_id,
                        cluster_name=cluster_name,
                        issue=f"Using older Databricks Runtime: {cluster.spark_version}",
                        recommendation="Consider upgrading to a newer runtime for better performance and features",
                        potential_savings="Up to 20% performance improvement with newer runtimes",
                        priority="low",
                    ))

    # Sort by priority (high first)
    priority_order = {"high": 0, "medium": 1, "low": 2}
    recommendations.sort(key=lambda r: priority_order.get(r.priority, 99))

    logger.info(f"Generated {len(recommendations)} recommendations")
    return recommendations
