"""Cluster optimization and utilization analysis API endpoints."""

from datetime import datetime, timedelta, timezone
from typing import Annotated

from databricks.sdk.service.compute import State
from databricks.sdk.service.sql import (
    Disposition,
    Format,
    StatementState,
)
from fastapi import APIRouter, HTTPException, Query

from ..core import Dependency, logger
from ..models import (
    ClusterType,
    ClusterUtilizationMetric,
    JobClusterRecommendation,
    MetricsCollectionResponse,
    OversizedClusterAnalysis,
    OptimizationSummary,
    ScheduleOptimizationRecommendation,
    UserConsolidationRecommendation,
)

router = APIRouter(prefix="/api/optimization", tags=["optimization"])


def _execute_sql(ws, warehouse_id: str, sql: str) -> list[dict]:
    """Execute a SQL statement and return results as a list of dicts."""
    logger.info(f"Executing SQL: {sql[:100]}...")

    response = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        format=Format.JSON_ARRAY,
        disposition=Disposition.INLINE,
        wait_timeout="30s",
    )

    if response.status.state == StatementState.FAILED:
        error_msg = response.status.error.message if response.status.error else "Unknown error"
        logger.error(f"SQL execution failed: {error_msg}")
        raise HTTPException(status_code=500, detail=f"SQL execution failed: {error_msg}")

    if response.status.state != StatementState.SUCCEEDED:
        raise HTTPException(
            status_code=500,
            detail=f"SQL execution did not succeed: {response.status.state.value}"
        )

    if not response.result or not response.result.data_array:
        return []

    columns = [col.name for col in response.manifest.schema.columns] if response.manifest else []

    results = []
    for row in response.result.data_array:
        row_dict = {}
        for i, col_name in enumerate(columns):
            row_dict[col_name] = row[i] if i < len(row) else None
        results.append(row_dict)

    return results


def _get_warehouse_id(ws, config) -> str:
    """Get SQL warehouse ID from config or find a suitable one."""
    if config.sql_warehouse_id:
        return config.sql_warehouse_id

    warehouses = list(ws.warehouses.list())
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            logger.info(f"Using warehouse: {wh.name} ({wh.id})")
            return wh.id

    if warehouses:
        logger.info(f"Using warehouse: {warehouses[0].name} ({warehouses[0].id})")
        return warehouses[0].id

    raise HTTPException(
        status_code=500,
        detail="No SQL warehouse available. Configure CLUSTER_MANAGER_SQL_WAREHOUSE_ID"
    )


def _list_clusters_limited(ws, limit: int = 100) -> list:
    """List clusters with a limit to avoid timeout on large workspaces."""
    clusters = []
    for i, cluster in enumerate(ws.clusters.list()):
        clusters.append(cluster)
        if i + 1 >= limit:
            logger.info(f"Reached cluster limit of {limit}")
            break
    return clusters


def _classify_cluster(cluster) -> ClusterType:
    """Classify cluster type based on source."""
    source = cluster.cluster_source
    if source is None:
        return ClusterType.INTERACTIVE

    source_value = source.value if hasattr(source, 'value') else str(source)

    if source_value == "JOB":
        return ClusterType.JOB
    elif source_value == "SQL":
        return ClusterType.SQL
    elif source_value in ["PIPELINE", "PIPELINE_MAINTENANCE"]:
        return ClusterType.PIPELINE
    elif source_value == "MODELS":
        return ClusterType.MODELS
    else:
        return ClusterType.INTERACTIVE


def _calculate_efficiency(actual_dbu: float, workers: int, uptime_hours: float) -> float:
    """Calculate cluster efficiency score (0-100)."""
    potential_dbu = (workers + 1) * uptime_hours  # +1 for driver
    if potential_dbu <= 0:
        return 0.0
    return min(100.0, (actual_dbu / potential_dbu) * 100)


def _ensure_metrics_table(ws, config) -> bool:
    """Ensure the metrics table exists in Unity Catalog."""
    warehouse_id = _get_warehouse_id(ws, config)

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {config.metrics_catalog}.{config.metrics_schema}.cluster_utilization_metrics (
        cluster_id STRING,
        cluster_name STRING,
        metric_date DATE,
        cluster_type STRING,
        worker_count INT,
        potential_dbu_per_hour DOUBLE,
        actual_dbu DOUBLE,
        uptime_hours DOUBLE,
        efficiency_score DOUBLE,
        job_run_count INT,
        unique_users INT,
        is_oversized BOOLEAN,
        is_underutilized BOOLEAN,
        collected_at TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (metric_date)
    """

    try:
        _execute_sql(ws, warehouse_id, create_sql)
        return True
    except Exception as e:
        logger.warning(f"Could not create metrics table: {e}")
        return False


@router.post("/collect-metrics", response_model=MetricsCollectionResponse)
def collect_metrics(
    ws: Dependency.Client,
    config: Dependency.Config,
) -> MetricsCollectionResponse:
    """Collect and persist daily cluster utilization metrics.

    This endpoint should be called daily (e.g., via a scheduled job) to
    populate the metrics table with historical utilization data.
    """
    logger.info("Starting metrics collection")

    try:
        warehouse_id = _get_warehouse_id(ws, config)

        # Ensure table exists
        table_created = _ensure_metrics_table(ws, config)

        # Get all clusters (limited to avoid timeout)
        clusters = _list_clusters_limited(ws, limit=200)
        logger.info(f"Processing {len(clusters)} clusters")

        # Get yesterday's date
        yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)

        # Get billing data for yesterday
        billing_sql = f"""
        SELECT
            usage_metadata.cluster_id as cluster_id,
            SUM(usage_quantity) as total_dbu
        FROM system.billing.usage
        WHERE usage_date = '{yesterday}'
            AND usage_metadata.cluster_id IS NOT NULL
        GROUP BY usage_metadata.cluster_id
        """

        billing_data = {}
        try:
            results = _execute_sql(ws, warehouse_id, billing_sql)
            billing_data = {r['cluster_id']: float(r.get('total_dbu') or 0) for r in results}
        except Exception as e:
            logger.warning(f"Could not fetch billing data: {e}")

        # Get job run counts for job clusters
        job_runs = {}
        try:
            yesterday_start = int(datetime.combine(yesterday, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
            yesterday_end = int(datetime.combine(yesterday, datetime.max.time(), tzinfo=timezone.utc).timestamp() * 1000)

            for run in ws.jobs.list_runs(
                start_time_from=yesterday_start,
                start_time_to=yesterday_end,
            ):
                # Check for existing_cluster_id in cluster_spec
                if hasattr(run, 'cluster_spec') and run.cluster_spec:
                    cluster_id = getattr(run.cluster_spec, 'existing_cluster_id', None)
                    if cluster_id:
                        job_runs[cluster_id] = job_runs.get(cluster_id, 0) + 1
        except Exception as e:
            logger.warning(f"Could not fetch job runs: {e}")

        # Calculate metrics for each cluster
        metrics = []
        for cluster in clusters:
            cluster_type = _classify_cluster(cluster)

            # Get worker count
            workers = cluster.num_workers or 0
            if cluster.autoscale:
                workers = (cluster.autoscale.min_workers + cluster.autoscale.max_workers) // 2

            potential_dbu_per_hour = workers + 1  # +1 for driver

            # Get actual DBU from billing
            actual_dbu = billing_data.get(cluster.cluster_id, 0)

            # Estimate uptime (rough: assume 8 hours if we have DBU data)
            uptime_hours = 8.0 if actual_dbu > 0 else 0.0

            # Calculate efficiency
            efficiency = _calculate_efficiency(actual_dbu, workers, uptime_hours)

            is_oversized = efficiency < config.oversized_threshold and efficiency > 0
            is_underutilized = efficiency < config.underutilized_threshold and efficiency > 0

            metrics.append(ClusterUtilizationMetric(
                cluster_id=cluster.cluster_id,
                cluster_name=cluster.cluster_name or "Unnamed Cluster",
                metric_date=datetime.combine(yesterday, datetime.min.time(), tzinfo=timezone.utc),
                cluster_type=cluster_type,
                worker_count=workers,
                potential_dbu_per_hour=potential_dbu_per_hour,
                actual_dbu=actual_dbu,
                uptime_hours=uptime_hours,
                efficiency_score=round(efficiency, 2),
                job_run_count=job_runs.get(cluster.cluster_id) if cluster_type == ClusterType.JOB else None,
                unique_users=None,
                is_oversized=is_oversized,
                is_underutilized=is_underutilized,
            ))

        # Persist metrics to Delta table
        persisted = False
        if table_created and metrics:
            try:
                now = datetime.now(timezone.utc).isoformat()
                values = []
                for m in metrics:
                    job_count = str(m.job_run_count) if m.job_run_count is not None else "NULL"
                    users = str(m.unique_users) if m.unique_users is not None else "NULL"
                    values.append(
                        f"('{m.cluster_id}', '{m.cluster_name}', '{yesterday}', "
                        f"'{m.cluster_type.value}', {m.worker_count}, {m.potential_dbu_per_hour}, "
                        f"{m.actual_dbu}, {m.uptime_hours}, {m.efficiency_score}, "
                        f"{job_count}, {users}, {str(m.is_oversized).lower()}, "
                        f"{str(m.is_underutilized).lower()}, '{now}')"
                    )

                insert_sql = f"""
                INSERT INTO {config.metrics_catalog}.{config.metrics_schema}.cluster_utilization_metrics
                VALUES {', '.join(values)}
                """
                _execute_sql(ws, warehouse_id, insert_sql)
                persisted = True
                logger.info(f"Persisted {len(metrics)} metrics to Delta table")
            except Exception as e:
                logger.warning(f"Could not persist metrics: {e}")

        return MetricsCollectionResponse(
            success=True,
            message=f"Collected metrics for {len(clusters)} clusters",
            clusters_processed=len(clusters),
            metrics_persisted=persisted,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to collect metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary", response_model=OptimizationSummary)
def get_optimization_summary(
    ws: Dependency.Client,
    config: Dependency.Config,
) -> OptimizationSummary:
    """Get summary of optimization opportunities across all clusters."""
    logger.info("Getting optimization summary")

    clusters = _list_clusters_limited(ws, limit=100)

    oversized_count = 0
    underutilized_count = 0
    total_savings = 0.0
    recommendations_count = 0

    for cluster in clusters:
        if cluster.state != State.RUNNING:
            continue

        workers = cluster.num_workers or 0
        if cluster.autoscale:
            workers = (cluster.autoscale.min_workers + cluster.autoscale.max_workers) // 2

        # Check for missing auto-termination
        auto_terminate = getattr(cluster, 'autotermination_minutes', None)
        if auto_terminate is None or auto_terminate == 0:
            recommendations_count += 1
            # Estimate 2 hours of idle time per day at $0.15/DBU
            total_savings += (workers + 1) * 2 * 0.15 * 30

        # Check for large clusters (potentially oversized)
        if workers >= 10:
            underutilized_count += 1
            recommendations_count += 1
            # Assume 30% could be saved
            potential_monthly_dbu = (workers + 1) * 8 * 30 * 0.3
            total_savings += potential_monthly_dbu * 0.15

        # Check for very large clusters
        if workers >= 20:
            oversized_count += 1

    return OptimizationSummary(
        total_clusters_analyzed=len(clusters),
        oversized_clusters=oversized_count,
        underutilized_clusters=underutilized_count,
        total_potential_monthly_savings=round(total_savings, 2),
        recommendations_count=recommendations_count,
        last_analysis_time=datetime.now(timezone.utc),
    )


@router.get("/oversized-clusters", response_model=list[OversizedClusterAnalysis])
def get_oversized_clusters(
    ws: Dependency.Client,
    config: Dependency.Config,
    min_workers: Annotated[int, Query(ge=1)] = 10,
) -> list[OversizedClusterAnalysis]:
    """Get clusters that are potentially oversized based on configuration.

    Analyzes clusters with >= min_workers and estimates potential savings.
    """
    logger.info(f"Getting oversized clusters (min_workers={min_workers})")

    clusters = _list_clusters_limited(ws, limit=100)
    oversized = []

    for cluster in clusters:
        workers = cluster.num_workers or 0
        if cluster.autoscale:
            workers = (cluster.autoscale.max_workers + cluster.autoscale.min_workers) // 2

        if workers < min_workers:
            continue

        cluster_type = _classify_cluster(cluster)

        # Estimate efficiency (without historical data, assume 50%)
        avg_efficiency = 50.0
        avg_daily_dbu = (workers + 1) * 8  # Assume 8 hours/day

        # Calculate recommended workers based on estimated efficiency
        recommended = max(2, int(workers * (avg_efficiency / 100)))

        # Calculate savings
        worker_reduction = workers - recommended
        daily_dbu_savings = worker_reduction * 8  # hours
        monthly_cost_savings = daily_dbu_savings * 30 * 0.15  # $0.15/DBU

        oversized.append(OversizedClusterAnalysis(
            cluster_id=cluster.cluster_id,
            cluster_name=cluster.cluster_name or "Unnamed Cluster",
            cluster_type=cluster_type,
            current_workers=workers,
            avg_efficiency_score=avg_efficiency,
            avg_daily_dbu=avg_daily_dbu,
            recommended_workers=recommended,
            potential_dbu_savings=round(daily_dbu_savings, 2),
            potential_cost_savings=round(monthly_cost_savings, 2),
        ))

    # Sort by potential savings
    oversized.sort(key=lambda x: x.potential_cost_savings, reverse=True)

    logger.info(f"Found {len(oversized)} potentially oversized clusters")
    return oversized


@router.get("/job-recommendations", response_model=list[JobClusterRecommendation])
def get_job_recommendations(
    ws: Dependency.Client,
    config: Dependency.Config,
) -> list[JobClusterRecommendation]:
    """Get recommendations for moving jobs to oversized clusters.

    Identifies small job clusters that could use larger, underutilized clusters.
    """
    logger.info("Getting job cluster recommendations")

    clusters = _list_clusters_limited(ws, limit=100)
    recommendations = []

    # Find large interactive clusters (potential targets)
    large_clusters = []
    small_job_clusters = []

    for cluster in clusters:
        cluster_type = _classify_cluster(cluster)
        workers = cluster.num_workers or 0
        if cluster.autoscale:
            workers = (cluster.autoscale.min_workers + cluster.autoscale.max_workers) // 2

        if cluster_type == ClusterType.INTERACTIVE and workers >= 10:
            large_clusters.append(cluster)
        elif cluster_type == ClusterType.JOB and workers < 5:
            small_job_clusters.append(cluster)

    # Generate recommendations
    for target in large_clusters[:3]:  # Top 3 large clusters
        target_workers = target.num_workers or 0
        if target.autoscale:
            target_workers = (target.autoscale.min_workers + target.autoscale.max_workers) // 2

        matching_jobs = [c for c in small_job_clusters[:5]]  # First 5 small job clusters
        if matching_jobs:
            recommendations.append(JobClusterRecommendation(
                source_cluster_id=matching_jobs[0].cluster_id,
                source_cluster_name=matching_jobs[0].cluster_name or "Unnamed",
                target_cluster_id=target.cluster_id,
                target_cluster_name=target.cluster_name or "Unnamed",
                job_count=len(matching_jobs),
                reason=f"Target cluster has {target_workers} workers with capacity to spare",
                estimated_savings="$50-200/month by consolidating job clusters",
            ))

    logger.info(f"Generated {len(recommendations)} job recommendations")
    return recommendations


@router.get("/schedule-recommendations", response_model=list[ScheduleOptimizationRecommendation])
def get_schedule_recommendations(
    ws: Dependency.Client,
    config: Dependency.Config,
) -> list[ScheduleOptimizationRecommendation]:
    """Get recommendations for optimizing cluster start/stop schedules.

    Identifies clusters without auto-termination or with suboptimal settings.
    """
    logger.info("Getting schedule optimization recommendations")

    clusters = _list_clusters_limited(ws, limit=100)
    recommendations = []

    for cluster in clusters:
        auto_terminate = getattr(cluster, 'autotermination_minutes', None)

        # Only recommend for running or recently used clusters
        if cluster.state not in [State.RUNNING, State.TERMINATED]:
            continue

        workers = cluster.num_workers or 0
        if cluster.autoscale:
            workers = (cluster.autoscale.min_workers + cluster.autoscale.max_workers) // 2

        # Skip very small clusters
        if workers < 2:
            continue

        if auto_terminate is None or auto_terminate == 0:
            # No auto-termination configured
            recommendations.append(ScheduleOptimizationRecommendation(
                cluster_id=cluster.cluster_id,
                cluster_name=cluster.cluster_name or "Unnamed Cluster",
                current_auto_terminate_minutes=auto_terminate,
                recommended_auto_terminate_minutes=60,
                avg_idle_time_per_day_minutes=120.0,  # Estimate
                peak_usage_hours=[9, 10, 11, 14, 15, 16],  # Business hours
                reason="No auto-termination configured. Recommended: 60 minutes to prevent idle costs.",
            ))
        elif auto_terminate > 120:
            # Auto-termination too long
            recommendations.append(ScheduleOptimizationRecommendation(
                cluster_id=cluster.cluster_id,
                cluster_name=cluster.cluster_name or "Unnamed Cluster",
                current_auto_terminate_minutes=auto_terminate,
                recommended_auto_terminate_minutes=60,
                avg_idle_time_per_day_minutes=float(auto_terminate - 60),
                peak_usage_hours=[9, 10, 11, 14, 15, 16],
                reason=f"Auto-termination of {auto_terminate} minutes is long. Consider reducing to 60-90 minutes.",
            ))

    # Sort by estimated idle time
    recommendations.sort(key=lambda x: x.avg_idle_time_per_day_minutes, reverse=True)

    logger.info(f"Generated {len(recommendations)} schedule recommendations")
    return recommendations


@router.get("/cluster/{cluster_id}/history", response_model=list[ClusterUtilizationMetric])
def get_cluster_history(
    cluster_id: str,
    ws: Dependency.Client,
    config: Dependency.Config,
    days: Annotated[int, Query(ge=1, le=90)] = 30,
) -> list[ClusterUtilizationMetric]:
    """Get utilization history for a specific cluster.

    Returns daily metrics for the specified number of days.
    """
    logger.info(f"Getting {days}-day history for cluster {cluster_id}")

    try:
        warehouse_id = _get_warehouse_id(ws, config)

        sql = f"""
        SELECT *
        FROM {config.metrics_catalog}.{config.metrics_schema}.cluster_utilization_metrics
        WHERE cluster_id = '{cluster_id}'
            AND metric_date >= CURRENT_DATE - INTERVAL {days} DAY
        ORDER BY metric_date DESC
        """

        results = _execute_sql(ws, warehouse_id, sql)

        metrics = []
        for row in results:
            metric_date = row.get('metric_date')
            if isinstance(metric_date, str):
                metric_date = datetime.fromisoformat(metric_date.replace('Z', '+00:00'))

            metrics.append(ClusterUtilizationMetric(
                cluster_id=row.get('cluster_id', cluster_id),
                cluster_name=row.get('cluster_name', 'Unknown'),
                metric_date=metric_date or datetime.now(timezone.utc),
                cluster_type=ClusterType(row.get('cluster_type', 'INTERACTIVE')),
                worker_count=int(row.get('worker_count') or 0),
                potential_dbu_per_hour=float(row.get('potential_dbu_per_hour') or 0),
                actual_dbu=float(row.get('actual_dbu') or 0),
                uptime_hours=float(row.get('uptime_hours') or 0),
                efficiency_score=float(row.get('efficiency_score') or 0),
                job_run_count=int(row['job_run_count']) if row.get('job_run_count') else None,
                unique_users=int(row['unique_users']) if row.get('unique_users') else None,
                is_oversized=bool(row.get('is_oversized')),
                is_underutilized=bool(row.get('is_underutilized')),
            ))

        logger.info(f"Found {len(metrics)} historical records for cluster {cluster_id}")
        return metrics

    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"Could not fetch cluster history: {e}")
        return []
