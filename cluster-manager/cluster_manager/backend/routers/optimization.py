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
    AutoscalingIssueType,
    AutoscalingRecommendation,
    AutoscalingSeverity,
    ClusterAutoscalingAnalysis,
    ClusterCostAnalysis,
    ClusterNodeTypeAnalysis,
    ClusterSparkConfigAnalysis,
    ClusterType,
    ClusterUtilizationMetric,
    CostOptimizationCategory,
    CostOptimizationRecommendation,
    CostRecommendationSeverity,
    JobClusterRecommendation,
    NodeTypeCategory,
    NodeTypeIssueType,
    NodeTypeRecommendation,
    NodeTypeSeverity,
    NodeTypeSpec,
    OversizedClusterAnalysis,
    OptimizationSummary,
    ScheduleOptimizationRecommendation,
    SparkConfigImpact,
    SparkConfigRecommendation,
    SparkConfigSeverity,
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
    """Get recommendations for optimizing cluster usage patterns.

    Identifies opportunities to:
    1. Consolidate clusters from the same user
    2. Convert always-on clusters to job clusters
    3. Use serverless compute for sporadic workloads
    """
    logger.info("Getting job cluster recommendations")

    clusters = _list_clusters_limited(ws, limit=100)
    recommendations = []

    # Group clusters by creator
    clusters_by_user: dict[str, list] = {}
    large_interactive = []
    always_on_clusters = []

    for cluster in clusters:
        creator = cluster.creator_user_name or "unknown"
        if creator not in clusters_by_user:
            clusters_by_user[creator] = []
        clusters_by_user[creator].append(cluster)

        workers = cluster.num_workers or 0
        if cluster.autoscale:
            workers = (cluster.autoscale.min_workers + cluster.autoscale.max_workers) // 2

        cluster_type = _classify_cluster(cluster)

        # Track large interactive clusters
        if cluster_type == ClusterType.INTERACTIVE and workers >= 4:
            large_interactive.append(cluster)

        # Track clusters without auto-termination (always-on risk)
        auto_terminate = getattr(cluster, 'autotermination_minutes', None)
        if auto_terminate is None or auto_terminate == 0:
            if cluster.state == State.RUNNING and workers >= 2:
                always_on_clusters.append(cluster)

    # Recommendation 1: Users with multiple clusters could consolidate
    for user, user_clusters in clusters_by_user.items():
        if len(user_clusters) >= 3:
            # User has 3+ clusters - recommend consolidation
            running = [c for c in user_clusters if c.state == State.RUNNING]
            terminated = [c for c in user_clusters if c.state == State.TERMINATED]

            if len(running) >= 2:
                # Multiple running clusters from same user
                source = running[0]
                target = running[1]
                recommendations.append(JobClusterRecommendation(
                    source_cluster_id=source.cluster_id,
                    source_cluster_name=source.cluster_name or "Unnamed",
                    target_cluster_id=target.cluster_id,
                    target_cluster_name=target.cluster_name or "Unnamed",
                    job_count=len(running),
                    reason=f"User {user.split('@')[0]} has {len(running)} running clusters. Consider consolidating workloads.",
                    estimated_savings="$100-500/month by reducing duplicate clusters",
                ))
            elif terminated and running:
                # Mix of running and terminated - recommend cleanup
                source = terminated[0]
                target = running[0]
                recommendations.append(JobClusterRecommendation(
                    source_cluster_id=source.cluster_id,
                    source_cluster_name=source.cluster_name or "Unnamed",
                    target_cluster_id=target.cluster_id,
                    target_cluster_name=target.cluster_name or "Unnamed",
                    job_count=len(terminated),
                    reason=f"User has {len(terminated)} terminated clusters that could be cleaned up or consolidated.",
                    estimated_savings="Simplified management, reduced clutter",
                ))

        if len(recommendations) >= 5:
            break

    # Recommendation 2: Always-on clusters should use job clusters
    for cluster in always_on_clusters[:3]:
        if len(recommendations) >= 8:
            break

        workers = cluster.num_workers or 0
        if cluster.autoscale:
            workers = (cluster.autoscale.min_workers + cluster.autoscale.max_workers) // 2

        # Estimate monthly cost for always-on
        monthly_dbu = (workers + 1) * 24 * 30  # DBUs per month
        monthly_cost = monthly_dbu * 0.15  # Rough estimate

        recommendations.append(JobClusterRecommendation(
            source_cluster_id=cluster.cluster_id,
            source_cluster_name=cluster.cluster_name or "Unnamed",
            target_cluster_id=cluster.cluster_id,
            target_cluster_name="Serverless or Job Cluster",
            job_count=1,
            reason=f"Running 24/7 without auto-terminate (~${monthly_cost:.0f}/mo). Consider serverless or job clusters for workloads.",
            estimated_savings=f"Up to ${monthly_cost * 0.7:.0f}/month with on-demand compute",
        ))

    # Recommendation 3: Similar clusters that could be shared
    if len(recommendations) < 5 and len(large_interactive) >= 2:
        # Find clusters with similar configurations
        for i, c1 in enumerate(large_interactive[:5]):
            if len(recommendations) >= 8:
                break
            for c2 in large_interactive[i + 1:6]:
                if c1.node_type_id == c2.node_type_id and c1.spark_version == c2.spark_version:
                    recommendations.append(JobClusterRecommendation(
                        source_cluster_id=c1.cluster_id,
                        source_cluster_name=c1.cluster_name or "Unnamed",
                        target_cluster_id=c2.cluster_id,
                        target_cluster_name=c2.cluster_name or "Unnamed",
                        job_count=2,
                        reason=f"Similar config (same node type & runtime). Consider sharing one cluster.",
                        estimated_savings="$50-300/month by sharing resources",
                    ))
                    break

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


def _is_photon_runtime(spark_version: str | None) -> bool:
    """Check if the Spark version indicates Photon is enabled."""
    if not spark_version:
        return False
    version_lower = spark_version.lower()
    return "photon" in version_lower or "-photon-" in version_lower


def _get_spark_conf_value(spark_conf: dict, key: str) -> str | None:
    """Get a Spark configuration value, handling various formats."""
    if not spark_conf:
        return None
    return spark_conf.get(key)


def _analyze_cluster_spark_config(cluster) -> ClusterSparkConfigAnalysis:
    """Analyze Spark configuration for a single cluster and generate recommendations."""
    cluster_id = cluster.cluster_id
    cluster_name = cluster.cluster_name or "Unnamed Cluster"
    spark_version = cluster.spark_version
    spark_conf = getattr(cluster, 'spark_conf', {}) or {}

    is_photon = _is_photon_runtime(spark_version)
    recommendations = []

    # --- AQE (Adaptive Query Execution) Analysis ---

    # Check if AQE is enabled (default is true in DBR 7.3+)
    aqe_enabled_str = _get_spark_conf_value(spark_conf, "spark.sql.adaptive.enabled")
    aqe_enabled = None
    if aqe_enabled_str is not None:
        aqe_enabled = aqe_enabled_str.lower() == "true"

    if aqe_enabled_str is not None and aqe_enabled_str.lower() == "false":
        recommendations.append(SparkConfigRecommendation(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            setting="spark.sql.adaptive.enabled",
            current_value="false",
            recommended_value="true",
            impact=SparkConfigImpact.PERFORMANCE,
            severity=SparkConfigSeverity.HIGH,
            reason="AQE (Adaptive Query Execution) is disabled. AQE automatically optimizes query plans at runtime, improving performance for joins, aggregations, and skewed data.",
            documentation_link="https://docs.databricks.com/en/optimizations/aqe.html",
        ))

    # Check AQE coalesce partitions
    aqe_coalesce = _get_spark_conf_value(spark_conf, "spark.sql.adaptive.coalescePartitions.enabled")
    if aqe_coalesce is not None and aqe_coalesce.lower() == "false":
        recommendations.append(SparkConfigRecommendation(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            setting="spark.sql.adaptive.coalescePartitions.enabled",
            current_value="false",
            recommended_value="true",
            impact=SparkConfigImpact.PERFORMANCE,
            severity=SparkConfigSeverity.MEDIUM,
            reason="AQE partition coalescing is disabled. This feature reduces the number of partitions after shuffles, improving performance for small datasets.",
            documentation_link="https://docs.databricks.com/en/optimizations/aqe.html",
        ))

    # Check AQE skew join optimization
    aqe_skew = _get_spark_conf_value(spark_conf, "spark.sql.adaptive.skewJoin.enabled")
    if aqe_skew is not None and aqe_skew.lower() == "false":
        recommendations.append(SparkConfigRecommendation(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            setting="spark.sql.adaptive.skewJoin.enabled",
            current_value="false",
            recommended_value="true",
            impact=SparkConfigImpact.PERFORMANCE,
            severity=SparkConfigSeverity.MEDIUM,
            reason="AQE skew join handling is disabled. This feature automatically splits skewed partitions to prevent data skew from slowing down joins.",
            documentation_link="https://docs.databricks.com/en/optimizations/aqe.html",
        ))

    # --- Shuffle Partitions Analysis ---

    shuffle_partitions = _get_spark_conf_value(spark_conf, "spark.sql.shuffle.partitions")
    if shuffle_partitions is not None:
        try:
            partitions_int = int(shuffle_partitions)
            if partitions_int > 2000:
                recommendations.append(SparkConfigRecommendation(
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    setting="spark.sql.shuffle.partitions",
                    current_value=shuffle_partitions,
                    recommended_value="200 (default) or use AQE auto-coalesce",
                    impact=SparkConfigImpact.PERFORMANCE,
                    severity=SparkConfigSeverity.MEDIUM,
                    reason=f"Shuffle partitions set to {partitions_int}, which is very high. This can cause excessive task overhead and slow down small-to-medium queries. Consider using AQE to auto-tune partitions.",
                    documentation_link="https://docs.databricks.com/en/optimizations/aqe.html",
                ))
            elif partitions_int < 10 and partitions_int > 0:
                recommendations.append(SparkConfigRecommendation(
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    setting="spark.sql.shuffle.partitions",
                    current_value=shuffle_partitions,
                    recommended_value="200 (default) or use AQE auto-coalesce",
                    impact=SparkConfigImpact.PERFORMANCE,
                    severity=SparkConfigSeverity.LOW,
                    reason=f"Shuffle partitions set to only {partitions_int}. This may limit parallelism for large datasets. Consider using AQE to auto-tune partitions based on data size.",
                    documentation_link="https://docs.databricks.com/en/optimizations/aqe.html",
                ))
        except ValueError:
            pass

    # --- Broadcast Join Analysis ---

    broadcast_threshold = _get_spark_conf_value(spark_conf, "spark.sql.autoBroadcastJoinThreshold")
    if broadcast_threshold is not None:
        if broadcast_threshold == "-1" or broadcast_threshold == "0":
            recommendations.append(SparkConfigRecommendation(
                cluster_id=cluster_id,
                cluster_name=cluster_name,
                setting="spark.sql.autoBroadcastJoinThreshold",
                current_value=broadcast_threshold,
                recommended_value="10485760 (10MB default)",
                impact=SparkConfigImpact.PERFORMANCE,
                severity=SparkConfigSeverity.MEDIUM,
                reason="Auto broadcast join is disabled. Broadcast joins can significantly speed up joins with small tables by avoiding shuffles. Consider enabling unless you have specific memory constraints.",
                documentation_link="https://docs.databricks.com/en/optimizations/broadcast-join.html",
            ))

    # --- Photon Analysis ---

    # Check if this is a SQL/analytics workload that could benefit from Photon
    cluster_source = getattr(cluster, 'cluster_source', None)
    source_value = cluster_source.value if hasattr(cluster_source, 'value') else str(cluster_source) if cluster_source else None

    if not is_photon and source_value in ["SQL", "UI", "API"]:
        # Potentially could benefit from Photon
        recommendations.append(SparkConfigRecommendation(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            setting="Runtime Version",
            current_value=spark_version,
            recommended_value="Photon-enabled runtime (e.g., 14.3.x-photon-scala2.12)",
            impact=SparkConfigImpact.PERFORMANCE,
            severity=SparkConfigSeverity.LOW,
            reason="Cluster is not using Photon runtime. Photon can provide 2-8x speedup for SQL and DataFrame workloads with no code changes. Consider upgrading for analytics-heavy workloads.",
            documentation_link="https://docs.databricks.com/en/runtime/photon.html",
        ))

    # --- Memory Configuration Analysis ---

    # Check driver memory
    driver_memory = _get_spark_conf_value(spark_conf, "spark.driver.memory")
    executor_memory = _get_spark_conf_value(spark_conf, "spark.executor.memory")

    if driver_memory and executor_memory:
        try:
            # Parse memory values (e.g., "4g", "8192m")
            def parse_memory_gb(mem_str: str) -> float:
                mem_str = mem_str.lower().strip()
                if mem_str.endswith("g"):
                    return float(mem_str[:-1])
                elif mem_str.endswith("m"):
                    return float(mem_str[:-1]) / 1024
                elif mem_str.endswith("k"):
                    return float(mem_str[:-1]) / (1024 * 1024)
                return float(mem_str) / (1024 * 1024 * 1024)

            driver_gb = parse_memory_gb(driver_memory)
            executor_gb = parse_memory_gb(executor_memory)

            if driver_gb < executor_gb * 0.5:
                recommendations.append(SparkConfigRecommendation(
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    setting="spark.driver.memory",
                    current_value=driver_memory,
                    recommended_value=f"At least {executor_memory} (match executor memory)",
                    impact=SparkConfigImpact.RELIABILITY,
                    severity=SparkConfigSeverity.MEDIUM,
                    reason=f"Driver memory ({driver_memory}) is significantly smaller than executor memory ({executor_memory}). This can cause OOM errors when collecting results or broadcasting data.",
                    documentation_link="https://docs.databricks.com/en/compute/configure.html",
                ))
        except (ValueError, AttributeError):
            pass

    # --- Delta Lake Optimization ---

    delta_auto_optimize = _get_spark_conf_value(spark_conf, "spark.databricks.delta.autoOptimize.enabled")
    if delta_auto_optimize is not None and delta_auto_optimize.lower() == "false":
        recommendations.append(SparkConfigRecommendation(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            setting="spark.databricks.delta.autoOptimize.enabled",
            current_value="false",
            recommended_value="true",
            impact=SparkConfigImpact.PERFORMANCE,
            severity=SparkConfigSeverity.LOW,
            reason="Delta auto-optimize is disabled. Auto-optimize automatically compacts small files during writes, improving read performance for downstream queries.",
            documentation_link="https://docs.databricks.com/en/delta/tune-file-size.html",
        ))

    # --- Dynamic Allocation ---

    dynamic_allocation = _get_spark_conf_value(spark_conf, "spark.dynamicAllocation.enabled")
    if dynamic_allocation is not None and dynamic_allocation.lower() == "false":
        # Check if this is a cluster without autoscale
        autoscale = getattr(cluster, 'autoscale', None)
        if autoscale is None:
            recommendations.append(SparkConfigRecommendation(
                cluster_id=cluster_id,
                cluster_name=cluster_name,
                setting="spark.dynamicAllocation.enabled",
                current_value="false",
                recommended_value="true",
                impact=SparkConfigImpact.COST,
                severity=SparkConfigSeverity.LOW,
                reason="Dynamic allocation is disabled on a fixed-size cluster. Consider enabling to allow Spark to adjust executors based on workload, or use cluster autoscaling.",
                documentation_link="https://docs.databricks.com/en/compute/configure.html",
            ))

    return ClusterSparkConfigAnalysis(
        cluster_id=cluster_id,
        cluster_name=cluster_name,
        spark_version=spark_version,
        is_photon_enabled=is_photon,
        aqe_enabled=aqe_enabled if aqe_enabled is not None else True,  # Default is True in modern DBR
        total_issues=len(recommendations),
        recommendations=recommendations,
    )


@router.get("/spark-config-recommendations", response_model=list[ClusterSparkConfigAnalysis])
def get_spark_config_recommendations(
    ws: Dependency.Client,
    config: Dependency.Config,
    include_no_issues: Annotated[bool, Query()] = False,
) -> list[ClusterSparkConfigAnalysis]:
    """Analyze Spark configurations across all clusters and provide recommendations.

    Checks for:
    - AQE (Adaptive Query Execution) settings
    - Shuffle partition configuration
    - Broadcast join thresholds
    - Photon runtime usage
    - Memory configuration
    - Delta Lake optimization settings
    - Dynamic allocation

    Args:
        include_no_issues: If True, include clusters with no configuration issues.
    """
    logger.info("Analyzing Spark configurations for all clusters")

    clusters = _list_clusters_limited(ws, limit=100)
    analyses = []

    for cluster in clusters:
        try:
            analysis = _analyze_cluster_spark_config(cluster)

            # Only include if there are issues or user wants all clusters
            if analysis.total_issues > 0 or include_no_issues:
                analyses.append(analysis)

        except Exception as e:
            logger.warning(f"Could not analyze cluster {cluster.cluster_id}: {e}")
            continue

    # Sort by number of issues (most issues first)
    analyses.sort(key=lambda x: x.total_issues, reverse=True)

    logger.info(f"Analyzed {len(clusters)} clusters, {len(analyses)} have configuration recommendations")
    return analyses


def _detect_cloud_provider(cluster) -> str:
    """Detect cloud provider from cluster attributes."""
    if getattr(cluster, 'aws_attributes', None):
        return "aws"
    elif getattr(cluster, 'azure_attributes', None):
        return "azure"
    elif getattr(cluster, 'gcp_attributes', None):
        return "gcp"
    return "unknown"


def _analyze_cluster_cost(cluster) -> ClusterCostAnalysis:
    """Analyze cost optimization opportunities for a cluster."""
    cluster_id = cluster.cluster_id
    cluster_name = cluster.cluster_name or "Unnamed Cluster"
    cloud_provider = _detect_cloud_provider(cluster)

    recommendations = []

    # Get worker count
    num_workers = cluster.num_workers or 0
    if cluster.autoscale:
        num_workers = (cluster.autoscale.min_workers + cluster.autoscale.max_workers) // 2

    # Extract cloud-specific attributes
    aws_attrs = getattr(cluster, 'aws_attributes', None)
    azure_attrs = getattr(cluster, 'azure_attributes', None)
    gcp_attrs = getattr(cluster, 'gcp_attributes', None)

    uses_spot = False
    spot_bid_price = None
    first_on_demand = None
    availability_zone = None
    ebs_volume_type = None

    # --- AWS-specific analysis ---
    if aws_attrs:
        availability = getattr(aws_attrs, 'availability', None)
        if availability:
            availability_str = availability.value if hasattr(availability, 'value') else str(availability)
            uses_spot = availability_str in ["SPOT", "SPOT_WITH_FALLBACK"]

        spot_bid_price = getattr(aws_attrs, 'spot_bid_price_percent', None)
        first_on_demand = getattr(aws_attrs, 'first_on_demand', None)
        availability_zone = getattr(aws_attrs, 'zone_id', None)
        ebs_volume_type = getattr(aws_attrs, 'ebs_volume_type', None)
        if ebs_volume_type and hasattr(ebs_volume_type, 'value'):
            ebs_volume_type = ebs_volume_type.value

        # Check if not using spot instances
        if not uses_spot and num_workers >= 2:
            cluster_type = _classify_cluster(cluster)
            # Recommend spot for non-critical workloads
            if cluster_type in [ClusterType.INTERACTIVE, ClusterType.JOB]:
                recommendations.append(CostOptimizationRecommendation(
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    category=CostOptimizationCategory.SPOT_INSTANCES,
                    current_state="On-Demand instances only",
                    recommendation="Use Spot instances with fallback to On-Demand",
                    estimated_savings_percent=60.0,
                    severity=CostRecommendationSeverity.HIGH,
                    reason="Spot instances can reduce compute costs by up to 70% compared to On-Demand. For fault-tolerant workloads, use SPOT_WITH_FALLBACK to automatically switch to On-Demand if Spot capacity is unavailable.",
                    implementation_steps=[
                        "Edit cluster configuration",
                        "Under Advanced Options > Instances, set Availability to 'Spot with fallback'",
                        "Set first_on_demand to 1 (keeps driver on On-Demand for stability)",
                        "Save and restart cluster"
                    ],
                ))

        # Check first_on_demand ratio
        if uses_spot and first_on_demand is not None and num_workers > 0:
            on_demand_ratio = first_on_demand / (num_workers + 1)  # +1 for driver
            if on_demand_ratio > 0.5:
                recommendations.append(CostOptimizationRecommendation(
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    category=CostOptimizationCategory.SPOT_INSTANCES,
                    current_state=f"{first_on_demand} On-Demand nodes out of {num_workers + 1} total",
                    recommendation="Reduce first_on_demand to 1 (driver only)",
                    estimated_savings_percent=30.0,
                    severity=CostRecommendationSeverity.MEDIUM,
                    reason=f"Currently {int(on_demand_ratio * 100)}% of nodes are On-Demand. For most workloads, only the driver needs On-Demand for stability. Workers can safely use Spot instances.",
                    implementation_steps=[
                        "Edit cluster configuration",
                        "Under Advanced Options > Instances, set first_on_demand to 1",
                        "This keeps driver stable while workers use cost-effective Spot instances"
                    ],
                ))

        # Check EBS volume type
        if ebs_volume_type and ebs_volume_type == "GENERAL_PURPOSE_SSD":
            cluster_type = _classify_cluster(cluster)
            if cluster_type == ClusterType.JOB:
                recommendations.append(CostOptimizationRecommendation(
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    category=CostOptimizationCategory.STORAGE,
                    current_state=f"EBS Volume Type: {ebs_volume_type}",
                    recommendation="Consider THROUGHPUT_OPTIMIZED_HDD for batch jobs",
                    estimated_savings_percent=15.0,
                    severity=CostRecommendationSeverity.LOW,
                    reason="For batch/ETL jobs that don't require low-latency storage, Throughput Optimized HDD can reduce storage costs while maintaining good sequential read/write performance.",
                    implementation_steps=[
                        "Edit cluster configuration",
                        "Under Advanced Options > Instances, change EBS Volume Type",
                        "Select Throughput Optimized HDD for batch workloads"
                    ],
                ))

    # --- Azure-specific analysis ---
    elif azure_attrs:
        availability = getattr(azure_attrs, 'availability', None)
        if availability:
            availability_str = availability.value if hasattr(availability, 'value') else str(availability)
            uses_spot = availability_str in ["SPOT_AZURE", "SPOT_WITH_FALLBACK_AZURE"]

        first_on_demand = getattr(azure_attrs, 'first_on_demand', None)

        if not uses_spot and num_workers >= 2:
            cluster_type = _classify_cluster(cluster)
            if cluster_type in [ClusterType.INTERACTIVE, ClusterType.JOB]:
                recommendations.append(CostOptimizationRecommendation(
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    category=CostOptimizationCategory.SPOT_INSTANCES,
                    current_state="On-Demand VMs only",
                    recommendation="Use Azure Spot VMs with fallback",
                    estimated_savings_percent=60.0,
                    severity=CostRecommendationSeverity.HIGH,
                    reason="Azure Spot VMs can reduce compute costs by up to 90% compared to On-Demand. For fault-tolerant workloads, use Spot with fallback to automatically switch to On-Demand if Spot capacity is unavailable.",
                    implementation_steps=[
                        "Edit cluster configuration",
                        "Under Azure Options, set Availability to 'Spot with fallback'",
                        "Set first_on_demand to 1 for driver stability"
                    ],
                ))

    # --- GCP-specific analysis ---
    elif gcp_attrs:
        use_preemptible = getattr(gcp_attrs, 'use_preemptible_executors', False)
        uses_spot = use_preemptible

        if not uses_spot and num_workers >= 2:
            cluster_type = _classify_cluster(cluster)
            if cluster_type in [ClusterType.INTERACTIVE, ClusterType.JOB]:
                recommendations.append(CostOptimizationRecommendation(
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    category=CostOptimizationCategory.SPOT_INSTANCES,
                    current_state="Standard VMs only",
                    recommendation="Use Preemptible VMs for workers",
                    estimated_savings_percent=60.0,
                    severity=CostRecommendationSeverity.HIGH,
                    reason="GCP Preemptible VMs can reduce compute costs by up to 80%. For Spark workloads that can tolerate interruptions, preemptible workers provide significant cost savings.",
                    implementation_steps=[
                        "Edit cluster configuration",
                        "Under GCP Options, enable 'Use preemptible executors'",
                        "Keep driver as standard VM for stability"
                    ],
                ))

    # --- Node Type Analysis (all clouds) ---
    node_type = cluster.node_type_id
    driver_node_type = cluster.driver_node_type_id or node_type

    # Check for expensive GPU instances on non-ML workloads
    if node_type:
        node_type_lower = node_type.lower()
        cluster_type = _classify_cluster(cluster)

        # Check if using GPU for non-ML workload
        if any(gpu in node_type_lower for gpu in ['p3', 'p4', 'g4', 'g5', 'gpu', 'a10', 'v100', 'a100', 't4']):
            if cluster_type not in [ClusterType.MODELS]:
                recommendations.append(CostOptimizationRecommendation(
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    category=CostOptimizationCategory.NODE_TYPE,
                    current_state=f"Node type: {node_type} (GPU instance)",
                    recommendation="Use non-GPU instances for non-ML workloads",
                    estimated_savings_percent=70.0,
                    severity=CostRecommendationSeverity.HIGH,
                    reason="This cluster uses GPU instances but doesn't appear to be an ML workload. GPU instances are 3-10x more expensive than comparable CPU instances. Consider switching to memory or compute-optimized instances.",
                    implementation_steps=[
                        "Review if workload actually requires GPU",
                        "For SQL/ETL workloads, use r5/r6i (memory-optimized) or c5/c6i (compute-optimized)",
                        "Edit cluster and select appropriate instance type"
                    ],
                ))

        # Check for very large instances that might be oversized
        if any(size in node_type_lower for size in ['24xlarge', '16xlarge', '12xlarge', 'metal']):
            if num_workers <= 2:
                recommendations.append(CostOptimizationRecommendation(
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    category=CostOptimizationCategory.NODE_TYPE,
                    current_state=f"Node type: {node_type} (very large instance)",
                    recommendation="Consider smaller instances with more workers",
                    estimated_savings_percent=20.0,
                    severity=CostRecommendationSeverity.MEDIUM,
                    reason="Using very large instances with few workers can be less cost-effective and provide less parallelism than smaller instances with more workers. Consider scaling out instead of scaling up.",
                    implementation_steps=[
                        "Evaluate workload parallelism requirements",
                        "Consider using smaller instances (4xlarge/8xlarge) with more workers",
                        "This often provides better cost/performance ratio for distributed workloads"
                    ],
                ))

    # --- Autoscaling Analysis ---
    autoscale = cluster.autoscale
    if autoscale:
        min_workers = autoscale.min_workers
        max_workers = autoscale.max_workers

        # Check for wide autoscale range that might not be efficient
        if max_workers - min_workers > 20 and min_workers > 5:
            recommendations.append(CostOptimizationRecommendation(
                cluster_id=cluster_id,
                cluster_name=cluster_name,
                category=CostOptimizationCategory.AUTOSCALING,
                current_state=f"Autoscale: {min_workers} to {max_workers} workers",
                recommendation="Consider reducing min_workers",
                estimated_savings_percent=25.0,
                severity=CostRecommendationSeverity.MEDIUM,
                reason=f"High minimum workers ({min_workers}) means paying for capacity even during low-usage periods. Consider reducing min_workers to 1-2 and letting autoscaling add capacity as needed.",
                implementation_steps=[
                    "Analyze actual usage patterns",
                    "Reduce min_workers to 1-2 for interactive clusters",
                    "Keep max_workers for peak capacity",
                    "Use auto-termination to stop idle clusters"
                ],
            ))
    elif num_workers >= 4:
        # Fixed-size cluster that could benefit from autoscaling
        recommendations.append(CostOptimizationRecommendation(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            category=CostOptimizationCategory.AUTOSCALING,
            current_state=f"Fixed size: {num_workers} workers",
            recommendation="Enable autoscaling to optimize costs",
            estimated_savings_percent=30.0,
            severity=CostRecommendationSeverity.MEDIUM,
            reason="Fixed-size clusters pay for full capacity even during low-usage periods. Autoscaling can reduce costs by scaling down when not needed and scaling up for peak demand.",
            implementation_steps=[
                "Edit cluster configuration",
                "Enable autoscaling with min_workers=1",
                f"Set max_workers={num_workers} to maintain current peak capacity",
                "This reduces idle costs while preserving performance"
            ],
        ))

    # Calculate total potential savings
    total_savings = sum(r.estimated_savings_percent for r in recommendations)
    # Cap at 90% (can't save more than that)
    total_savings = min(90.0, total_savings)

    return ClusterCostAnalysis(
        cluster_id=cluster_id,
        cluster_name=cluster_name,
        cloud_provider=cloud_provider,
        node_type_id=node_type,
        driver_node_type_id=driver_node_type,
        num_workers=num_workers,
        uses_spot_instances=uses_spot,
        spot_bid_price=spot_bid_price,
        first_on_demand=first_on_demand,
        availability_zone=availability_zone,
        ebs_volume_type=ebs_volume_type,
        total_recommendations=len(recommendations),
        total_potential_savings_percent=round(total_savings, 1),
        recommendations=recommendations,
    )


@router.get("/cost-recommendations", response_model=list[ClusterCostAnalysis])
def get_cost_recommendations(
    ws: Dependency.Client,
    config: Dependency.Config,
    include_no_issues: Annotated[bool, Query()] = False,
) -> list[ClusterCostAnalysis]:
    """Analyze cost optimization opportunities across all clusters.

    Checks for:
    - Spot/Preemptible instance usage
    - On-Demand vs Spot mix ratio
    - Node type appropriateness
    - Storage type optimization
    - Autoscaling configuration

    Args:
        include_no_issues: If True, include clusters with no cost recommendations.
    """
    logger.info("Analyzing cost optimization for all clusters")

    clusters = _list_clusters_limited(ws, limit=100)
    analyses = []

    for cluster in clusters:
        try:
            analysis = _analyze_cluster_cost(cluster)

            # Only include if there are recommendations or user wants all clusters
            if analysis.total_recommendations > 0 or include_no_issues:
                analyses.append(analysis)

        except Exception as e:
            logger.warning(f"Could not analyze cluster {cluster.cluster_id} for cost: {e}")
            continue

    # Sort by potential savings (highest first)
    analyses.sort(key=lambda x: x.total_potential_savings_percent, reverse=True)

    logger.info(f"Analyzed {len(clusters)} clusters, {len(analyses)} have cost recommendations")
    return analyses


def _analyze_cluster_autoscaling(cluster) -> ClusterAutoscalingAnalysis:
    """Analyze autoscaling configuration for a cluster and generate recommendations."""
    cluster_id = cluster.cluster_id
    cluster_name = cluster.cluster_name or "Unnamed Cluster"
    cluster_type = _classify_cluster(cluster)

    recommendations = []
    autoscale = cluster.autoscale
    auto_terminate = getattr(cluster, 'autotermination_minutes', None)

    # Get current workers
    current_workers = cluster.num_workers or 0

    has_autoscaling = autoscale is not None
    min_workers = None
    max_workers = None
    autoscale_range = None
    range_ratio = None

    if autoscale:
        min_workers = autoscale.min_workers
        max_workers = autoscale.max_workers
        autoscale_range = max_workers - min_workers
        range_ratio = max_workers / min_workers if min_workers > 0 else None
        current_workers = (min_workers + max_workers) // 2

        # --- Issue 1: Wide Range Detection ---
        # If max >> min (ratio > 5x), it suggests uncertainty about actual needs
        if range_ratio and range_ratio >= 5 and autoscale_range >= 10:
            recommendations.append(AutoscalingRecommendation(
                cluster_id=cluster_id,
                cluster_name=cluster_name,
                issue_type=AutoscalingIssueType.WIDE_RANGE,
                current_config=f"Autoscale: {min_workers} to {max_workers} workers (range: {autoscale_range}, ratio: {range_ratio:.1f}x)",
                recommendation="Narrow the autoscale range based on actual usage patterns",
                estimated_savings_percent=20.0,
                severity=AutoscalingSeverity.MEDIUM,
                reason=f"Autoscale range is very wide ({range_ratio:.1f}x ratio). This suggests uncertainty about workload requirements. A very wide range can lead to slow scale-up times and unpredictable costs. Consider analyzing actual usage to set tighter bounds.",
                implementation_steps=[
                    "Review cluster metrics to understand actual peak usage",
                    f"If typical usage is {min_workers + autoscale_range // 4}-{min_workers + autoscale_range // 2} workers, adjust max accordingly",
                    "Consider setting max_workers to 2-3x typical usage for burst capacity",
                    "Monitor for throttling after adjustment"
                ],
            ))

        # --- Issue 2: Narrow Range Detection ---
        # If max ≈ min (range <= 2 and both >= 4), might as well use fixed size
        if autoscale_range <= 2 and min_workers >= 4:
            recommendations.append(AutoscalingRecommendation(
                cluster_id=cluster_id,
                cluster_name=cluster_name,
                issue_type=AutoscalingIssueType.NARROW_RANGE,
                current_config=f"Autoscale: {min_workers} to {max_workers} workers (range: {autoscale_range})",
                recommendation="Consider using fixed-size cluster or widening the range",
                estimated_savings_percent=5.0,
                severity=AutoscalingSeverity.LOW,
                reason=f"Autoscale range is very narrow ({autoscale_range} workers). The overhead of autoscaling may not be worth it for such a small range. Consider either a fixed-size cluster (simpler, more predictable) or widening the range to get real benefit from autoscaling.",
                implementation_steps=[
                    "Evaluate if workload actually varies",
                    f"For stable workloads: use fixed {max_workers} workers",
                    f"For variable workloads: consider expanding range (e.g., {min_workers // 2} to {max_workers * 2})",
                    "Fixed-size clusters have faster startup (no scaling delay)"
                ],
            ))

        # --- Issue 3: High Minimum Workers ---
        # High min_workers means paying for capacity even during idle periods
        if min_workers >= 8:
            # Estimate savings: assume 50% of time the cluster is at minimum
            idle_savings = (min_workers - 2) / min_workers * 50  # % time at min × potential reduction
            severity = AutoscalingSeverity.HIGH if min_workers >= 16 else AutoscalingSeverity.MEDIUM

            recommendations.append(AutoscalingRecommendation(
                cluster_id=cluster_id,
                cluster_name=cluster_name,
                issue_type=AutoscalingIssueType.HIGH_MINIMUM,
                current_config=f"min_workers: {min_workers}",
                recommendation=f"Reduce min_workers to 1-2 and rely on autoscaling",
                estimated_savings_percent=round(idle_savings, 1),
                severity=severity,
                reason=f"High minimum workers ({min_workers}) means paying for significant capacity even during low-usage periods. Unless your workload requires constant high capacity, reducing min_workers can significantly reduce idle costs while autoscaling handles peak demand.",
                implementation_steps=[
                    "Analyze when peak usage actually occurs",
                    "For interactive clusters: set min_workers=1 or 2",
                    "For job clusters: consider min_workers=0 (scale from zero)",
                    f"Keep max_workers={max_workers} for peak capacity",
                    "Combine with auto-termination for further savings"
                ],
            ))
        elif min_workers >= 4 and cluster_type == ClusterType.INTERACTIVE:
            # Even 4+ min workers can be wasteful for interactive clusters
            recommendations.append(AutoscalingRecommendation(
                cluster_id=cluster_id,
                cluster_name=cluster_name,
                issue_type=AutoscalingIssueType.HIGH_MINIMUM,
                current_config=f"min_workers: {min_workers}",
                recommendation="Reduce min_workers for interactive cluster",
                estimated_savings_percent=15.0,
                severity=AutoscalingSeverity.LOW,
                reason=f"Interactive clusters often have variable usage patterns. With min_workers={min_workers}, you pay for this capacity even when users aren't active. Reducing to 1-2 workers lets autoscaling handle demand while reducing idle costs.",
                implementation_steps=[
                    "Set min_workers=1 for interactive clusters",
                    "Enable auto-termination (60-120 min) for fully idle periods",
                    f"max_workers={max_workers} ensures capacity for peak times"
                ],
            ))

        # --- Issue 4: Inefficient Range for Cluster Type ---
        # Job clusters should consider scale-from-zero
        if cluster_type == ClusterType.JOB and min_workers > 0:
            recommendations.append(AutoscalingRecommendation(
                cluster_id=cluster_id,
                cluster_name=cluster_name,
                issue_type=AutoscalingIssueType.INEFFICIENT_RANGE,
                current_config=f"Job cluster with min_workers={min_workers}",
                recommendation="Consider min_workers=0 for job clusters",
                estimated_savings_percent=25.0,
                severity=AutoscalingSeverity.MEDIUM,
                reason="Job clusters typically run on-demand workloads. Setting min_workers=0 allows the cluster to scale to zero when not running jobs, eliminating idle costs completely. Jobs will trigger scale-up automatically.",
                implementation_steps=[
                    "Edit autoscale configuration",
                    "Set min_workers=0 to enable scale-to-zero",
                    "Jobs will automatically scale up workers as needed",
                    "Consider job clusters for sporadic workloads"
                ],
            ))

    else:
        # No autoscaling - fixed size cluster
        if current_workers >= 4:
            recommendations.append(AutoscalingRecommendation(
                cluster_id=cluster_id,
                cluster_name=cluster_name,
                issue_type=AutoscalingIssueType.NO_AUTOSCALING,
                current_config=f"Fixed size: {current_workers} workers",
                recommendation="Enable autoscaling to reduce idle costs",
                estimated_savings_percent=35.0,
                severity=AutoscalingSeverity.HIGH,
                reason=f"Fixed-size clusters with {current_workers} workers pay for full capacity continuously. Autoscaling can significantly reduce costs by scaling down during low-usage periods while maintaining capacity for peak demand.",
                implementation_steps=[
                    "Edit cluster configuration",
                    "Enable autoscaling with min_workers=1",
                    f"Set max_workers={current_workers} to maintain peak capacity",
                    "Also enable auto-termination (60-120 min) for full idle periods"
                ],
            ))

        # Check for missing auto-termination (significant for any cluster)
        if auto_terminate is None or auto_terminate == 0:
            if current_workers >= 2:
                recommendations.append(AutoscalingRecommendation(
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    issue_type=AutoscalingIssueType.INEFFICIENT_RANGE,
                    current_config="Auto-termination: disabled",
                    recommendation="Enable auto-termination to stop idle clusters",
                    estimated_savings_percent=40.0,
                    severity=AutoscalingSeverity.HIGH,
                    reason="Without auto-termination, clusters run 24/7 even when completely idle. Enabling auto-termination (e.g., 60-120 minutes) automatically stops clusters after periods of inactivity, eliminating idle costs.",
                    implementation_steps=[
                        "Edit cluster configuration",
                        "Set autotermination_minutes to 60-120",
                        "Cluster will automatically stop after idle period",
                        "Start-up time is typically 2-5 minutes when needed"
                    ],
                ))

    # Handle auto-termination for autoscaled clusters too
    if has_autoscaling and (auto_terminate is None or auto_terminate == 0):
        recommendations.append(AutoscalingRecommendation(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            issue_type=AutoscalingIssueType.INEFFICIENT_RANGE,
            current_config="Autoscaling enabled but no auto-termination",
            recommendation="Enable auto-termination for complete cost optimization",
            estimated_savings_percent=20.0,
            severity=AutoscalingSeverity.MEDIUM,
            reason="While autoscaling reduces costs during low-usage, without auto-termination the cluster still runs at min_workers when completely idle. Enable auto-termination to stop the cluster entirely during extended idle periods.",
            implementation_steps=[
                "Set autotermination_minutes to 60-120",
                "Cluster will terminate after inactivity",
                "Combined with autoscaling: scales down first, then terminates if fully idle"
            ],
        ))

    # Calculate total potential savings (cap at 80%)
    total_savings = sum(r.estimated_savings_percent for r in recommendations)
    total_savings = min(80.0, total_savings)

    return ClusterAutoscalingAnalysis(
        cluster_id=cluster_id,
        cluster_name=cluster_name,
        cluster_type=cluster_type,
        has_autoscaling=has_autoscaling,
        min_workers=min_workers,
        max_workers=max_workers,
        current_workers=current_workers,
        autoscale_range=autoscale_range,
        range_ratio=round(range_ratio, 2) if range_ratio else None,
        auto_terminate_minutes=auto_terminate,
        total_issues=len(recommendations),
        total_potential_savings_percent=round(total_savings, 1),
        recommendations=recommendations,
    )


@router.get("/autoscaling-recommendations", response_model=list[ClusterAutoscalingAnalysis])
def get_autoscaling_recommendations(
    ws: Dependency.Client,
    config: Dependency.Config,
    include_no_issues: Annotated[bool, Query()] = False,
) -> list[ClusterAutoscalingAnalysis]:
    """Analyze autoscaling configuration across all clusters and provide recommendations.

    Checks for:
    - Wide autoscale ranges (suggests uncertainty)
    - Narrow autoscale ranges (consider fixed size)
    - High minimum workers (wasteful during idle)
    - Fixed-size clusters that could benefit from autoscaling
    - Missing auto-termination configuration
    - Inefficient configurations for cluster type

    Args:
        include_no_issues: If True, include clusters with no autoscaling issues.
    """
    logger.info("Analyzing autoscaling configurations for all clusters")

    clusters = _list_clusters_limited(ws, limit=100)
    analyses = []

    for cluster in clusters:
        try:
            analysis = _analyze_cluster_autoscaling(cluster)

            # Only include if there are issues or user wants all clusters
            if analysis.total_issues > 0 or include_no_issues:
                analyses.append(analysis)

        except Exception as e:
            logger.warning(f"Could not analyze cluster {cluster.cluster_id} for autoscaling: {e}")
            continue

    # Sort by potential savings (highest first)
    analyses.sort(key=lambda x: x.total_potential_savings_percent, reverse=True)

    logger.info(f"Analyzed {len(clusters)} clusters, {len(analyses)} have autoscaling recommendations")
    return analyses


# --- Node Type Instance Patterns ---
# Used to classify instance types by category

AWS_INSTANCE_PATTERNS = {
    NodeTypeCategory.MEMORY_OPTIMIZED: ["r5", "r6i", "r6g", "r7i", "r7g", "x1", "x2"],
    NodeTypeCategory.COMPUTE_OPTIMIZED: ["c5", "c6i", "c6g", "c7i", "c7g"],
    NodeTypeCategory.GENERAL_PURPOSE: ["m5", "m6i", "m6g", "m7i", "m7g"],
    NodeTypeCategory.GPU: ["p3", "p4", "p5", "g4", "g5", "g6"],
    NodeTypeCategory.STORAGE_OPTIMIZED: ["i3", "i4i", "d2", "d3", "h1"],
}

AZURE_INSTANCE_PATTERNS = {
    NodeTypeCategory.MEMORY_OPTIMIZED: ["Standard_E", "Standard_M", "Standard_D.*s_v"],
    NodeTypeCategory.COMPUTE_OPTIMIZED: ["Standard_F"],
    NodeTypeCategory.GENERAL_PURPOSE: ["Standard_D", "Standard_A"],
    NodeTypeCategory.GPU: ["Standard_NC", "Standard_ND", "Standard_NV"],
    NodeTypeCategory.STORAGE_OPTIMIZED: ["Standard_L"],
}

GCP_INSTANCE_PATTERNS = {
    NodeTypeCategory.MEMORY_OPTIMIZED: ["n2-highmem", "n2d-highmem", "m1-", "m2-", "m3-"],
    NodeTypeCategory.COMPUTE_OPTIMIZED: ["c2-", "c2d-", "c3-", "h3-"],
    NodeTypeCategory.GENERAL_PURPOSE: ["n1-", "n2-standard", "n2d-standard", "e2-"],
    NodeTypeCategory.GPU: ["a2-", "g2-"],
    NodeTypeCategory.STORAGE_OPTIMIZED: ["n2-"],
}


def _parse_node_type(node_type: str | None, cloud_provider: str) -> NodeTypeSpec:
    """Parse a node type string and extract its properties."""
    if not node_type:
        return NodeTypeSpec(
            instance_type="unknown",
            category=NodeTypeCategory.UNKNOWN,
        )

    node_type_lower = node_type.lower()
    category = NodeTypeCategory.UNKNOWN
    generation = None
    size = None
    vcpus = None
    memory_gb = None
    gpu_count = None

    # Select patterns based on cloud provider
    if cloud_provider == "aws":
        patterns = AWS_INSTANCE_PATTERNS
    elif cloud_provider == "azure":
        patterns = AZURE_INSTANCE_PATTERNS
    elif cloud_provider == "gcp":
        patterns = GCP_INSTANCE_PATTERNS
    else:
        patterns = AWS_INSTANCE_PATTERNS  # Default to AWS patterns

    # Determine category
    for cat, prefixes in patterns.items():
        for prefix in prefixes:
            prefix_lower = prefix.lower()
            if node_type_lower.startswith(prefix_lower) or prefix_lower in node_type_lower:
                category = cat
                break
        if category != NodeTypeCategory.UNKNOWN:
            break

    # Extract AWS-specific info
    if cloud_provider == "aws":
        # Parse generation (e.g., r5 -> 5, c6i -> 6i)
        import re
        gen_match = re.search(r'[a-z](\d+[a-z]?)', node_type_lower)
        if gen_match:
            generation = gen_match.group(1)

        # Parse size (e.g., xlarge, 2xlarge, 4xlarge)
        size_match = re.search(r'\.(\d*x?large|metal)', node_type_lower)
        if size_match:
            size = size_match.group(1)

        # Estimate vCPUs from size (rough mapping)
        size_vcpu_map = {
            "large": 2, "xlarge": 4, "2xlarge": 8, "4xlarge": 16,
            "8xlarge": 32, "12xlarge": 48, "16xlarge": 64,
            "24xlarge": 96, "metal": 192,
        }
        if size in size_vcpu_map:
            vcpus = size_vcpu_map[size]

        # Check for GPU instances
        if category == NodeTypeCategory.GPU:
            if "p4" in node_type_lower or "p5" in node_type_lower:
                gpu_count = 8  # p4d.24xlarge, p5.48xlarge
            elif "g5" in node_type_lower:
                gpu_count = 4 if "12xlarge" in node_type_lower else 1
            elif "g4" in node_type_lower:
                gpu_count = 4 if "12xlarge" in node_type_lower else 1
            elif "p3" in node_type_lower:
                gpu_count = 8 if "16xlarge" in node_type_lower else 4

    elif cloud_provider == "azure":
        # Parse Azure instance info
        import re
        # Azure format: Standard_E16s_v3, Standard_NC6
        size_match = re.search(r'(\d+)', node_type)
        if size_match:
            vcpus = int(size_match.group(1))

        gen_match = re.search(r'_v(\d+)', node_type)
        if gen_match:
            generation = f"v{gen_match.group(1)}"

        if category == NodeTypeCategory.GPU:
            if "NC" in node_type:
                gpu_match = re.search(r'NC(\d+)', node_type)
                if gpu_match:
                    gpu_count = int(gpu_match.group(1)) // 6  # NC6 = 1 GPU, NC24 = 4 GPUs

    elif cloud_provider == "gcp":
        # Parse GCP instance info
        import re
        # GCP format: n2-standard-8, a2-highgpu-1g
        vcpu_match = re.search(r'-(\d+)$', node_type)
        if vcpu_match:
            vcpus = int(vcpu_match.group(1))

        if category == NodeTypeCategory.GPU:
            if "highgpu" in node_type_lower:
                gpu_match = re.search(r'highgpu-(\d+)', node_type_lower)
                if gpu_match:
                    gpu_count = int(gpu_match.group(1))

    return NodeTypeSpec(
        instance_type=node_type,
        category=category,
        vcpus=vcpus,
        memory_gb=memory_gb,
        gpu_count=gpu_count,
        generation=generation,
        size=size,
    )


def _analyze_cluster_node_type(cluster) -> ClusterNodeTypeAnalysis:
    """Analyze node type configuration for a cluster and generate recommendations."""
    cluster_id = cluster.cluster_id
    cluster_name = cluster.cluster_name or "Unnamed Cluster"
    cluster_type = _classify_cluster(cluster)
    cloud_provider = _detect_cloud_provider(cluster)

    worker_node_type = cluster.node_type_id
    driver_node_type = cluster.driver_node_type_id or worker_node_type

    # Parse node type specs
    worker_spec = _parse_node_type(worker_node_type, cloud_provider)
    driver_spec = _parse_node_type(driver_node_type, cloud_provider)

    uses_same_driver_worker = driver_node_type == worker_node_type

    # Get worker count
    num_workers = cluster.num_workers or 0
    if cluster.autoscale:
        num_workers = (cluster.autoscale.min_workers + cluster.autoscale.max_workers) // 2

    recommendations = []

    # --- Issue 1: Oversized Driver ---
    # Driver larger than workers (often unnecessary)
    if driver_spec.vcpus and worker_spec.vcpus:
        if driver_spec.vcpus > worker_spec.vcpus * 2:
            recommendations.append(NodeTypeRecommendation(
                cluster_id=cluster_id,
                cluster_name=cluster_name,
                issue_type=NodeTypeIssueType.OVERSIZED_DRIVER,
                current_config=f"Driver: {driver_node_type} ({driver_spec.vcpus} vCPUs), Workers: {worker_node_type} ({worker_spec.vcpus} vCPUs)",
                recommended_config=f"Match driver to worker: {worker_node_type}",
                estimated_savings_percent=15.0,
                severity=NodeTypeSeverity.MEDIUM,
                reason=f"Driver ({driver_spec.vcpus} vCPUs) is significantly larger than workers ({worker_spec.vcpus} vCPUs). Unless collecting large datasets to driver, matching driver to worker size is more cost-effective. The driver mainly coordinates tasks and handles collect() operations.",
                implementation_steps=[
                    "Evaluate if driver needs extra capacity (large collect(), broadcast variables)",
                    f"If not, set driver_node_type_id to {worker_node_type}",
                    "This reduces driver cost while maintaining worker performance"
                ],
            ))

    # --- Issue 2: GPU for Non-ML Workloads ---
    if worker_spec.category == NodeTypeCategory.GPU:
        if cluster_type not in [ClusterType.MODELS]:
            # Check if Photon (which uses GPU) is indicated
            spark_version = cluster.spark_version or ""
            is_photon = "photon" in spark_version.lower()

            if not is_photon:
                recommendations.append(NodeTypeRecommendation(
                    cluster_id=cluster_id,
                    cluster_name=cluster_name,
                    issue_type=NodeTypeIssueType.GPU_UNDERUTILIZED,
                    current_config=f"GPU instance: {worker_node_type}",
                    recommended_config="Use memory or compute-optimized instances",
                    estimated_savings_percent=70.0,
                    severity=NodeTypeSeverity.HIGH,
                    reason=f"GPU instances ({worker_node_type}) are used but cluster doesn't appear to be ML-focused and isn't using Photon. GPUs are 3-10x more expensive than CPU instances. For SQL/ETL workloads, memory-optimized (r-series) or compute-optimized (c-series) instances are more cost-effective.",
                    implementation_steps=[
                        "Confirm workload doesn't require GPU (ML training, deep learning)",
                        "For SQL/analytics: use Photon with standard instances",
                        "For ETL: use r5/r6i (memory-optimized) or m5/m6i (general purpose)",
                        "GPU savings of 70%+ are typical when switching to CPU instances"
                    ],
                ))

    # --- Issue 3: Legacy Instance Generation ---
    if worker_spec.generation:
        gen_num = worker_spec.generation[0] if worker_spec.generation else None
        if gen_num and gen_num.isdigit() and int(gen_num) < 5:
            newer_gen = str(int(gen_num) + 2)  # Suggest 2 generations newer
            old_prefix = worker_node_type.split(".")[0] if "." in worker_node_type else worker_node_type[:2]
            new_type_suggestion = f"{old_prefix[0]}{newer_gen}i.{worker_spec.size}" if worker_spec.size else f"{old_prefix[0]}{newer_gen}i"

            recommendations.append(NodeTypeRecommendation(
                cluster_id=cluster_id,
                cluster_name=cluster_name,
                issue_type=NodeTypeIssueType.LEGACY_INSTANCE,
                current_config=f"Instance generation: {worker_spec.generation} ({worker_node_type})",
                recommended_config=f"Upgrade to newer generation (e.g., {new_type_suggestion})",
                estimated_savings_percent=15.0,
                severity=NodeTypeSeverity.LOW,
                reason=f"Using older instance generation ({worker_spec.generation}). Newer generations (6th, 7th gen) often provide better price/performance and include improvements like faster networking and better CPU performance at similar or lower prices.",
                implementation_steps=[
                    "Check AWS/Azure/GCP pricing for newer instance types",
                    f"Consider upgrading from {worker_node_type} to {new_type_suggestion}",
                    "Newer generations often cost the same but perform better",
                    "Test workload on new instance type before full migration"
                ],
            ))

    # --- Issue 4: Mismatched Driver/Worker Categories ---
    if not uses_same_driver_worker:
        if driver_spec.category != worker_spec.category and \
           driver_spec.category != NodeTypeCategory.UNKNOWN and \
           worker_spec.category != NodeTypeCategory.UNKNOWN:
            recommendations.append(NodeTypeRecommendation(
                cluster_id=cluster_id,
                cluster_name=cluster_name,
                issue_type=NodeTypeIssueType.MISMATCHED_DRIVER_WORKER,
                current_config=f"Driver: {driver_node_type} ({driver_spec.category.value}), Workers: {worker_node_type} ({worker_spec.category.value})",
                recommended_config="Use consistent instance families for driver and workers",
                estimated_savings_percent=5.0,
                severity=NodeTypeSeverity.LOW,
                reason=f"Driver ({driver_spec.category.value}) and workers ({worker_spec.category.value}) use different instance families. While this can be intentional, using the same family often simplifies management and ensures consistent behavior. Consider if the mixed configuration is necessary.",
                implementation_steps=[
                    "Review why different families are used",
                    "For most workloads, matching families simplifies tuning",
                    "Exception: memory-heavy collect() may justify larger driver"
                ],
            ))

    # --- Issue 5: Overprovisioned for Small Clusters ---
    if num_workers <= 2 and worker_spec.vcpus and worker_spec.vcpus >= 32:
        recommendations.append(NodeTypeRecommendation(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            issue_type=NodeTypeIssueType.OVERPROVISIONED,
            current_config=f"{num_workers} workers × {worker_spec.vcpus} vCPUs = {num_workers * worker_spec.vcpus} total vCPUs",
            recommended_config=f"Use smaller instances with more workers for better parallelism",
            estimated_savings_percent=20.0,
            severity=NodeTypeSeverity.MEDIUM,
            reason=f"Few workers ({num_workers}) with very large instances ({worker_spec.vcpus} vCPUs each). For distributed workloads, more smaller workers often outperform fewer large workers due to better parallelism and fault tolerance. Consider 4-8 workers with 8-16 vCPU instances.",
            implementation_steps=[
                "Calculate total vCPUs needed: current = " + str(num_workers * worker_spec.vcpus if worker_spec.vcpus else "unknown"),
                "Redistribute across more workers: e.g., 4x r5.2xlarge instead of 2x r5.8xlarge",
                "Enable autoscaling to handle variable workloads",
                "More workers = better parallelism and fault isolation"
            ],
        ))

    # --- Issue 6: Wrong Category for Workload Type ---
    if cluster_type == ClusterType.SQL and worker_spec.category == NodeTypeCategory.COMPUTE_OPTIMIZED:
        recommendations.append(NodeTypeRecommendation(
            cluster_id=cluster_id,
            cluster_name=cluster_name,
            issue_type=NodeTypeIssueType.WRONG_CATEGORY,
            current_config=f"SQL cluster using {worker_spec.category.value} instances",
            recommended_config="Use memory-optimized instances for SQL workloads",
            estimated_savings_percent=10.0,
            severity=NodeTypeSeverity.LOW,
            reason="SQL workloads typically benefit from memory-optimized instances (r-series) for caching and join operations. Compute-optimized instances (c-series) are better for CPU-intensive transformations.",
            implementation_steps=[
                "For SQL/analytics: consider r5/r6i instances",
                "Memory-optimized instances improve query cache hit rates",
                "If using Photon, it can run on any instance type"
            ],
        ))

    # Calculate total potential savings (cap at 80%)
    total_savings = sum(r.estimated_savings_percent for r in recommendations)
    total_savings = min(80.0, total_savings)

    return ClusterNodeTypeAnalysis(
        cluster_id=cluster_id,
        cluster_name=cluster_name,
        cluster_type=cluster_type,
        cloud_provider=cloud_provider,
        worker_node_type=worker_node_type,
        worker_node_category=worker_spec.category,
        worker_spec=worker_spec,
        driver_node_type=driver_node_type,
        driver_node_category=driver_spec.category,
        driver_spec=driver_spec,
        num_workers=num_workers,
        uses_same_driver_worker=uses_same_driver_worker,
        total_issues=len(recommendations),
        total_potential_savings_percent=round(total_savings, 1),
        recommendations=recommendations,
    )


@router.get("/node-type-recommendations", response_model=list[ClusterNodeTypeAnalysis])
def get_node_type_recommendations(
    ws: Dependency.Client,
    config: Dependency.Config,
    include_no_issues: Annotated[bool, Query()] = False,
) -> list[ClusterNodeTypeAnalysis]:
    """Analyze node type configurations across all clusters and provide recommendations.

    Checks for:
    - Oversized driver instances
    - GPU instances on non-ML workloads
    - Legacy instance generations
    - Mismatched driver/worker instance families
    - Overprovisioned small clusters
    - Wrong instance category for workload type

    Args:
        include_no_issues: If True, include clusters with no node type issues.
    """
    logger.info("Analyzing node type configurations for all clusters")

    clusters = _list_clusters_limited(ws, limit=100)
    analyses = []

    for cluster in clusters:
        try:
            analysis = _analyze_cluster_node_type(cluster)

            # Only include if there are issues or user wants all clusters
            if analysis.total_issues > 0 or include_no_issues:
                analyses.append(analysis)

        except Exception as e:
            logger.warning(f"Could not analyze cluster {cluster.cluster_id} for node types: {e}")
            continue

    # Sort by potential savings (highest first)
    analyses.sort(key=lambda x: x.total_potential_savings_percent, reverse=True)

    logger.info(f"Analyzed {len(clusters)} clusters, {len(analyses)} have node type recommendations")
    return analyses
