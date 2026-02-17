"""Cluster management API endpoints."""

from datetime import datetime, timezone
from typing import Annotated

from databricks.sdk.service.compute import ClusterDetails, State
from fastapi import APIRouter, HTTPException, Query

from ..core import Dependency, logger
from ..models import (
    AutoScaleConfig,
    ClusterActionResponse,
    ClusterDetail,
    ClusterEvent,
    ClusterEventsResponse,
    ClusterSource,
    ClusterState,
    ClusterSummary,
)

router = APIRouter(prefix="/api/clusters", tags=["clusters"])


def _state_to_enum(state: State | None) -> ClusterState:
    """Convert SDK State to our ClusterState enum."""
    if state is None:
        return ClusterState.UNKNOWN
    try:
        return ClusterState(state.value)
    except ValueError:
        return ClusterState.UNKNOWN


def _source_to_enum(source: str | None) -> ClusterSource | None:
    """Convert SDK source string to our ClusterSource enum."""
    if source is None:
        return None
    try:
        return ClusterSource(source)
    except ValueError:
        return None


def _ms_to_datetime(ms: int | None) -> datetime | None:
    """Convert milliseconds timestamp to datetime."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _calculate_uptime_minutes(cluster: ClusterDetails) -> int:
    """Calculate cluster uptime in minutes."""
    if cluster.state not in [State.RUNNING, State.RESIZING, State.RESTARTING]:
        return 0
    if cluster.start_time is None:
        return 0
    start = _ms_to_datetime(cluster.start_time)
    if start is None:
        return 0
    now = datetime.now(timezone.utc)
    return int((now - start).total_seconds() / 60)


def _estimate_dbu_per_hour(cluster: ClusterDetails) -> float:
    """Estimate DBU per hour based on cluster configuration.

    This is a rough estimate. Actual DBU rates depend on instance types.
    """
    if cluster.state not in [State.RUNNING, State.RESIZING]:
        return 0.0

    # Base estimate: 1 DBU per worker per hour (rough approximation)
    num_workers = cluster.num_workers or 0
    if cluster.autoscale:
        # Use average of min/max for autoscaling clusters
        num_workers = (cluster.autoscale.min_workers + cluster.autoscale.max_workers) / 2

    # Add 1 for driver
    return (num_workers + 1) * 1.0


def _cluster_to_summary(cluster: ClusterDetails) -> ClusterSummary:
    """Convert SDK ClusterDetails to our ClusterSummary model."""
    autoscale = None
    if cluster.autoscale:
        autoscale = AutoScaleConfig(
            min_workers=cluster.autoscale.min_workers,
            max_workers=cluster.autoscale.max_workers,
        )

    return ClusterSummary(
        cluster_id=cluster.cluster_id,
        cluster_name=cluster.cluster_name or "Unnamed Cluster",
        state=_state_to_enum(cluster.state),
        creator_user_name=cluster.creator_user_name,
        node_type_id=cluster.node_type_id,
        driver_node_type_id=cluster.driver_node_type_id,
        num_workers=cluster.num_workers,
        autoscale=autoscale,
        spark_version=cluster.spark_version,
        cluster_source=_source_to_enum(cluster.cluster_source.value if cluster.cluster_source else None),
        start_time=_ms_to_datetime(cluster.start_time),
        last_activity_time=_ms_to_datetime(getattr(cluster, 'last_activity_time', None)),
        uptime_minutes=_calculate_uptime_minutes(cluster),
        estimated_dbu_per_hour=_estimate_dbu_per_hour(cluster),
    )


def _cluster_to_detail(cluster: ClusterDetails) -> ClusterDetail:
    """Convert SDK ClusterDetails to our ClusterDetail model."""
    summary = _cluster_to_summary(cluster)

    # Use getattr for optional attributes that may not exist on all cluster types
    disk_spec = getattr(cluster, 'disk_spec', None)
    enable_elastic_disk = getattr(cluster, 'enable_elastic_disk', None)

    return ClusterDetail(
        **summary.model_dump(),
        terminated_time=_ms_to_datetime(cluster.terminated_time),
        termination_reason=cluster.termination_reason.message if cluster.termination_reason else None,
        state_message=cluster.state_message,
        default_tags=cluster.default_tags or {},
        custom_tags=cluster.custom_tags or {},
        aws_attributes=cluster.aws_attributes.as_dict() if cluster.aws_attributes else None,
        azure_attributes=cluster.azure_attributes.as_dict() if cluster.azure_attributes else None,
        gcp_attributes=cluster.gcp_attributes.as_dict() if cluster.gcp_attributes else None,
        spark_conf=cluster.spark_conf or {},
        spark_env_vars=cluster.spark_env_vars or {},
        init_scripts=[s.as_dict() for s in (cluster.init_scripts or [])],
        cluster_log_conf=cluster.cluster_log_conf.as_dict() if cluster.cluster_log_conf else None,
        policy_id=cluster.policy_id,
        enable_elastic_disk=enable_elastic_disk,
        disk_spec=disk_spec.as_dict() if disk_spec else None,
        single_user_name=cluster.single_user_name,
        data_security_mode=cluster.data_security_mode.value if cluster.data_security_mode else None,
    )


@router.get("", response_model=list[ClusterSummary])
def list_clusters(
    ws: Dependency.Client,
    state: Annotated[ClusterState | None, Query(description="Filter by cluster state")] = None,
    limit: Annotated[int, Query(ge=1, le=500, description="Maximum number of clusters to return")] = 100,
) -> list[ClusterSummary]:
    """List clusters in the workspace.

    Returns a summary of each cluster including state, configuration, and estimated DBU usage.
    Limited to first N clusters for performance (default 100).
    """
    logger.info(f"Listing clusters - limit: {limit}")

    try:
        # Iterate over clusters with limit for performance
        clusters = []
        for i, cluster in enumerate(ws.clusters.list()):
            clusters.append(cluster)
            if i + 1 >= limit:
                logger.info(f"Reached limit of {limit} clusters")
                break

        logger.info(f"Retrieved {len(clusters)} clusters")
        summaries = [_cluster_to_summary(c) for c in clusters]

        # Filter by state if provided
        if state:
            summaries = [s for s in summaries if s.state == state]

        # Sort by state (running first) then by name
        state_order = {
            ClusterState.RUNNING: 0,
            ClusterState.PENDING: 1,
            ClusterState.RESTARTING: 2,
            ClusterState.RESIZING: 3,
            ClusterState.TERMINATING: 4,
            ClusterState.TERMINATED: 5,
            ClusterState.ERROR: 6,
            ClusterState.UNKNOWN: 7,
        }
        summaries.sort(key=lambda s: (state_order.get(s.state, 99), s.cluster_name))

        logger.info(f"Returning {len(summaries)} clusters")
        return summaries
    except Exception as e:
        error_msg = f"Failed to list clusters: {e}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


@router.get("/{cluster_id}", response_model=ClusterDetail)
def get_cluster(
    cluster_id: str,
    ws: Dependency.Client,
) -> ClusterDetail:
    """Get detailed information about a specific cluster."""
    logger.info(f"Getting cluster {cluster_id}")

    try:
        cluster = ws.clusters.get(cluster_id)
        return _cluster_to_detail(cluster)
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        logger.error(f"Failed to get cluster {cluster_id}: [{error_type}] {error_msg}")
        # Return more detailed error to help debug
        raise HTTPException(
            status_code=404,
            detail=f"Cluster not found: {cluster_id}. Error: {error_msg}"
        )


@router.post("/{cluster_id}/start", response_model=ClusterActionResponse)
def start_cluster(
    cluster_id: str,
    ws: Dependency.Client,
) -> ClusterActionResponse:
    """Start a terminated or stopped cluster.

    The cluster will transition to PENDING state and then to RUNNING.
    """
    logger.info(f"Starting cluster {cluster_id}")

    try:
        # Get current state first
        cluster = ws.clusters.get(cluster_id)
        if cluster.state == State.RUNNING:
            return ClusterActionResponse(
                success=True,
                message="Cluster is already running",
                cluster_id=cluster_id,
            )
        if cluster.state not in [State.TERMINATED, State.ERROR]:
            return ClusterActionResponse(
                success=False,
                message=f"Cannot start cluster in state: {cluster.state.value}",
                cluster_id=cluster_id,
            )

        ws.clusters.start(cluster_id)
        return ClusterActionResponse(
            success=True,
            message="Cluster start initiated",
            cluster_id=cluster_id,
        )
    except Exception as e:
        logger.error(f"Failed to start cluster {cluster_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{cluster_id}/stop", response_model=ClusterActionResponse)
def stop_cluster(
    cluster_id: str,
    ws: Dependency.Client,
) -> ClusterActionResponse:
    """Stop a running cluster.

    SAFE MODE: This is a non-destructive action. The cluster configuration is preserved
    and can be started again later.

    Note: This permanently terminates the cluster. Use with caution.
    Any running jobs will be interrupted.
    """
    logger.info(f"Stopping cluster {cluster_id}")

    try:
        # Get current state first
        cluster = ws.clusters.get(cluster_id)
        if cluster.state == State.TERMINATED:
            return ClusterActionResponse(
                success=True,
                message="Cluster is already stopped",
                cluster_id=cluster_id,
            )
        if cluster.state not in [State.RUNNING, State.PENDING, State.RESIZING, State.RESTARTING]:
            return ClusterActionResponse(
                success=False,
                message=f"Cannot stop cluster in state: {cluster.state.value}",
                cluster_id=cluster_id,
            )

        # Use permanent_delete=False to keep cluster configuration
        ws.clusters.delete(cluster_id)
        return ClusterActionResponse(
            success=True,
            message="Cluster stop initiated",
            cluster_id=cluster_id,
        )
    except Exception as e:
        logger.error(f"Failed to stop cluster {cluster_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{cluster_id}/events", response_model=ClusterEventsResponse)
def get_cluster_events(
    cluster_id: str,
    ws: Dependency.Client,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> ClusterEventsResponse:
    """Get recent events for a cluster."""
    logger.info(f"Getting events for cluster {cluster_id}")

    try:
        events_response = ws.clusters.events(
            cluster_id=cluster_id,
            limit=limit,
        )

        events = []
        for event in events_response.events or []:
            events.append(ClusterEvent(
                cluster_id=cluster_id,
                timestamp=_ms_to_datetime(event.timestamp) or datetime.now(timezone.utc),
                event_type=event.type.value if event.type else "UNKNOWN",
                details=event.details.as_dict() if event.details else {},
            ))

        return ClusterEventsResponse(
            events=events,
            next_page_token=events_response.next_page.end_time if events_response.next_page else None,
            total_count=events_response.total_count or len(events),
        )
    except Exception as e:
        logger.error(f"Failed to get events for cluster {cluster_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
