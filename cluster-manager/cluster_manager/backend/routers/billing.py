"""Billing API endpoints using Unity Catalog system tables."""

from datetime import datetime, timedelta, timezone
from typing import Annotated

from databricks.sdk.service.sql import (
    Disposition,
    Format,
    StatementState,
)
from fastapi import APIRouter, HTTPException, Query

from ..core import Dependency, logger
from ..models import (
    BillingSummary,
    BillingTrend,
    ClusterBillingUsage,
    TopConsumer,
)

router = APIRouter(prefix="/api/billing", tags=["billing"])


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

    # Get column names from manifest
    columns = [col.name for col in response.manifest.schema.columns] if response.manifest else []

    # Convert to list of dicts
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

    # Try to find a running warehouse
    warehouses = list(ws.warehouses.list())
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            logger.info(f"Using warehouse: {wh.name} ({wh.id})")
            return wh.id

    # Fall back to first available
    if warehouses:
        logger.info(f"Using warehouse: {warehouses[0].name} ({warehouses[0].id})")
        return warehouses[0].id

    raise HTTPException(
        status_code=500,
        detail="No SQL warehouse available. Configure CLUSTER_MANAGER_SQL_WAREHOUSE_ID"
    )


@router.get("/summary", response_model=BillingSummary)
def get_billing_summary(
    ws: Dependency.Client,
    config: Dependency.Config,
    days: Annotated[int, Query(ge=1, le=90)] = 30,
) -> BillingSummary:
    """Get summary of DBU usage and estimated costs.

    Queries Unity Catalog system.billing.usage table for actual usage data.
    """
    logger.info(f"Getting billing summary for last {days} days")

    warehouse_id = _get_warehouse_id(ws, config)

    sql = f"""
    SELECT
        COALESCE(SUM(usage_quantity), 0) as total_dbu,
        MIN(usage_date) as period_start,
        MAX(usage_date) as period_end
    FROM system.billing.usage
    WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
        AND usage_metadata.cluster_id IS NOT NULL
    """

    try:
        results = _execute_sql(ws, warehouse_id, sql)

        if not results:
            now = datetime.now(timezone.utc)
            return BillingSummary(
                total_dbu=0.0,
                estimated_cost_usd=0.0,
                period_start=now - timedelta(days=days),
                period_end=now,
            )

        row = results[0]
        total_dbu = float(row.get("total_dbu") or 0)

        # Get price estimate (average $0.15 per DBU)
        estimated_cost = total_dbu * 0.15

        # Parse dates
        period_start = row.get("period_start")
        period_end = row.get("period_end")

        if isinstance(period_start, str):
            period_start = datetime.fromisoformat(period_start.replace("Z", "+00:00"))
        if isinstance(period_end, str):
            period_end = datetime.fromisoformat(period_end.replace("Z", "+00:00"))

        now = datetime.now(timezone.utc)
        if not period_start:
            period_start = now - timedelta(days=days)
        if not period_end:
            period_end = now

        return BillingSummary(
            total_dbu=total_dbu,
            estimated_cost_usd=round(estimated_cost, 2),
            period_start=period_start,
            period_end=period_end,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get billing summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/by-cluster", response_model=list[ClusterBillingUsage])
def get_billing_by_cluster(
    ws: Dependency.Client,
    config: Dependency.Config,
    days: Annotated[int, Query(ge=1, le=90)] = 30,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> list[ClusterBillingUsage]:
    """Get DBU usage breakdown by cluster.

    Returns top clusters by DBU consumption.
    """
    logger.info(f"Getting billing by cluster for last {days} days")

    warehouse_id = _get_warehouse_id(ws, config)

    sql = f"""
    SELECT
        usage_metadata.cluster_id as cluster_id,
        COALESCE(SUM(usage_quantity), 0) as total_dbu,
        MIN(usage_date) as usage_start,
        MAX(usage_date) as usage_end
    FROM system.billing.usage
    WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
        AND usage_metadata.cluster_id IS NOT NULL
    GROUP BY usage_metadata.cluster_id
    ORDER BY total_dbu DESC
    LIMIT {limit}
    """

    try:
        results = _execute_sql(ws, warehouse_id, sql)

        # Get cluster names
        cluster_names = {}
        try:
            clusters = list(ws.clusters.list())
            cluster_names = {c.cluster_id: c.cluster_name for c in clusters}
        except Exception as e:
            logger.warning(f"Failed to get cluster names: {e}")

        usage_list = []
        for row in results:
            cluster_id = row.get("cluster_id")
            total_dbu = float(row.get("total_dbu") or 0)
            estimated_cost = total_dbu * 0.15

            usage_start = row.get("usage_start")
            usage_end = row.get("usage_end")

            if isinstance(usage_start, str):
                usage_start = datetime.fromisoformat(usage_start.replace("Z", "+00:00"))
            if isinstance(usage_end, str):
                usage_end = datetime.fromisoformat(usage_end.replace("Z", "+00:00"))

            now = datetime.now(timezone.utc)
            if not usage_start:
                usage_start = now - timedelta(days=days)
            if not usage_end:
                usage_end = now

            usage_list.append(ClusterBillingUsage(
                cluster_id=cluster_id,
                cluster_name=cluster_names.get(cluster_id),
                total_dbu=total_dbu,
                estimated_cost_usd=round(estimated_cost, 2),
                usage_date_start=usage_start,
                usage_date_end=usage_end,
            ))

        return usage_list

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get billing by cluster: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trend", response_model=list[BillingTrend])
def get_billing_trend(
    ws: Dependency.Client,
    config: Dependency.Config,
    days: Annotated[int, Query(ge=1, le=90)] = 30,
) -> list[BillingTrend]:
    """Get daily DBU usage trend.

    Returns daily aggregated DBU usage for charting.
    """
    logger.info(f"Getting billing trend for last {days} days")

    warehouse_id = _get_warehouse_id(ws, config)

    sql = f"""
    SELECT
        usage_date as date,
        COALESCE(SUM(usage_quantity), 0) as dbu
    FROM system.billing.usage
    WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
        AND usage_metadata.cluster_id IS NOT NULL
    GROUP BY usage_date
    ORDER BY usage_date ASC
    """

    try:
        results = _execute_sql(ws, warehouse_id, sql)

        trend_list = []
        for row in results:
            date_val = row.get("date")
            dbu = float(row.get("dbu") or 0)
            estimated_cost = dbu * 0.15

            if isinstance(date_val, str):
                date_val = datetime.fromisoformat(date_val.replace("Z", "+00:00"))
            elif not isinstance(date_val, datetime):
                continue

            trend_list.append(BillingTrend(
                date=date_val,
                dbu=dbu,
                estimated_cost_usd=round(estimated_cost, 2),
            ))

        return trend_list

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get billing trend: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-consumers", response_model=list[TopConsumer])
def get_top_consumers(
    ws: Dependency.Client,
    config: Dependency.Config,
    days: Annotated[int, Query(ge=1, le=90)] = 30,
    limit: Annotated[int, Query(ge=1, le=20)] = 10,
) -> list[TopConsumer]:
    """Get top DBU consuming clusters.

    Returns clusters sorted by DBU consumption with percentage of total.
    """
    logger.info(f"Getting top {limit} consumers for last {days} days")

    warehouse_id = _get_warehouse_id(ws, config)

    # First get total
    total_sql = f"""
    SELECT COALESCE(SUM(usage_quantity), 0) as total_dbu
    FROM system.billing.usage
    WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
        AND usage_metadata.cluster_id IS NOT NULL
    """

    # Then get by cluster
    cluster_sql = f"""
    SELECT
        usage_metadata.cluster_id as cluster_id,
        COALESCE(SUM(usage_quantity), 0) as total_dbu
    FROM system.billing.usage
    WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
        AND usage_metadata.cluster_id IS NOT NULL
    GROUP BY usage_metadata.cluster_id
    ORDER BY total_dbu DESC
    LIMIT {limit}
    """

    try:
        # Get total
        total_results = _execute_sql(ws, warehouse_id, total_sql)
        total_dbu = float(total_results[0].get("total_dbu") or 0) if total_results else 0

        # Get cluster breakdown
        cluster_results = _execute_sql(ws, warehouse_id, cluster_sql)

        # Get cluster names
        cluster_names = {}
        try:
            clusters = list(ws.clusters.list())
            cluster_names = {c.cluster_id: c.cluster_name for c in clusters}
        except Exception as e:
            logger.warning(f"Failed to get cluster names: {e}")

        consumers = []
        for row in cluster_results:
            cluster_id = row.get("cluster_id")
            dbu = float(row.get("total_dbu") or 0)
            estimated_cost = dbu * 0.15
            percentage = (dbu / total_dbu * 100) if total_dbu > 0 else 0

            consumers.append(TopConsumer(
                cluster_id=cluster_id,
                cluster_name=cluster_names.get(cluster_id),
                total_dbu=dbu,
                estimated_cost_usd=round(estimated_cost, 2),
                percentage_of_total=round(percentage, 1),
            ))

        return consumers

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get top consumers: {e}")
        raise HTTPException(status_code=500, detail=str(e))
