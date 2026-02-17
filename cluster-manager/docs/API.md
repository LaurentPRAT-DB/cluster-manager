# API Reference

## Overview

The Cluster Manager API provides RESTful endpoints for managing Databricks clusters, viewing billing data, and receiving optimization recommendations.

**Base URL**: `https://{app-name}.{region}.databricksapps.com/api`

**Authentication**: OAuth via Databricks Apps (automatic with logged-in user)

---

## Clusters API

### List Clusters

Returns a summary of all clusters in the workspace.

```http
GET /api/clusters
```

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `state` | string | - | Filter by cluster state (RUNNING, TERMINATED, etc.) |
| `limit` | integer | 100 | Maximum clusters to return (1-500) |

**Response**: `ClusterSummary[]`

```json
[
  {
    "cluster_id": "1234-567890-abc123",
    "cluster_name": "my-cluster",
    "state": "RUNNING",
    "creator_user_name": "user@example.com",
    "node_type_id": "i3.xlarge",
    "driver_node_type_id": "i3.xlarge",
    "num_workers": 4,
    "autoscale": {
      "min_workers": 2,
      "max_workers": 8
    },
    "spark_version": "14.3.x-scala2.12",
    "cluster_source": "UI",
    "start_time": "2024-01-15T10:00:00Z",
    "last_activity_time": "2024-01-15T14:30:00Z",
    "uptime_minutes": 270,
    "estimated_dbu_per_hour": 5.0,
    "policy_id": "E0123456789ABCDEF"
  }
]
```

---

### Get Cluster Details

Returns detailed information about a specific cluster.

```http
GET /api/clusters/{cluster_id}
```

**Path Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| `cluster_id` | string | Unique cluster identifier |

**Response**: `ClusterDetail`

```json
{
  "cluster_id": "1234-567890-abc123",
  "cluster_name": "my-cluster",
  "state": "RUNNING",
  "terminated_time": null,
  "termination_reason": null,
  "state_message": "Cluster is running",
  "default_tags": {
    "Vendor": "Databricks",
    "Creator": "user@example.com"
  },
  "custom_tags": {
    "Environment": "development"
  },
  "spark_conf": {
    "spark.databricks.delta.preview.enabled": "true"
  },
  "spark_env_vars": {},
  "init_scripts": [],
  "enable_elastic_disk": true,
  "data_security_mode": "SINGLE_USER"
}
```

**Error Responses**

| Code | Description |
|------|-------------|
| 404 | Cluster not found |
| 500 | Internal server error |

---

### Start Cluster

Starts a terminated or stopped cluster.

```http
POST /api/clusters/{cluster_id}/start
```

**Response**: `ClusterActionResponse`

```json
{
  "success": true,
  "message": "Cluster start initiated",
  "cluster_id": "1234-567890-abc123"
}
```

**Error Responses**

| Code | Message | Description |
|------|---------|-------------|
| 200 | "Cluster is already running" | Cluster already in RUNNING state |
| 200 | "Cannot start cluster in state: PENDING" | Invalid current state |
| 500 | Error message | SDK/API error |

---

### Stop Cluster

Stops a running cluster (Safe Mode - preserves configuration).

```http
POST /api/clusters/{cluster_id}/stop
```

**Response**: `ClusterActionResponse`

```json
{
  "success": true,
  "message": "Cluster stop initiated",
  "cluster_id": "1234-567890-abc123"
}
```

**Note**: This is a non-destructive operation. The cluster configuration is preserved and can be started again later.

---

### Get Cluster Events

Returns recent events for a cluster.

```http
GET /api/clusters/{cluster_id}/events
```

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | 50 | Maximum events to return (1-100) |

**Response**: `ClusterEventsResponse`

```json
{
  "events": [
    {
      "cluster_id": "1234-567890-abc123",
      "timestamp": "2024-01-15T14:00:00Z",
      "event_type": "RUNNING",
      "details": {}
    }
  ],
  "next_page_token": null,
  "total_count": 25
}
```

---

## Billing API

### Get Billing Summary

Returns total DBU usage and estimated costs for the specified period.

```http
GET /api/billing/summary
```

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `days` | integer | 30 | Number of days to analyze (1-90) |

**Response**: `BillingSummary`

```json
{
  "total_dbu": 15234.5,
  "estimated_cost_usd": 2285.18,
  "period_start": "2023-12-15T00:00:00Z",
  "period_end": "2024-01-15T00:00:00Z",
  "currency": "USD"
}
```

**Data Source**: `system.billing.usage` (Unity Catalog)

**Note**: Estimated cost uses $0.15/DBU as a rough approximation. Actual rates vary by SKU.

---

### Get Billing by Cluster

Returns DBU usage breakdown by cluster.

```http
GET /api/billing/by-cluster
```

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `days` | integer | 30 | Number of days to analyze (1-90) |
| `limit` | integer | 50 | Maximum clusters to return (1-100) |

**Response**: `ClusterBillingUsage[]`

```json
[
  {
    "cluster_id": "1234-567890-abc123",
    "cluster_name": "data-science-prod",
    "total_dbu": 5234.5,
    "estimated_cost_usd": 785.18,
    "usage_date_start": "2023-12-15T00:00:00Z",
    "usage_date_end": "2024-01-15T00:00:00Z"
  }
]
```

---

### Get Billing Trend

Returns daily DBU usage for charting.

```http
GET /api/billing/trend
```

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `days` | integer | 30 | Number of days to analyze (1-90) |

**Response**: `BillingTrend[]`

```json
[
  {
    "date": "2024-01-14T00:00:00Z",
    "dbu": 523.5,
    "estimated_cost_usd": 78.53
  },
  {
    "date": "2024-01-15T00:00:00Z",
    "dbu": 612.3,
    "estimated_cost_usd": 91.85
  }
]
```

---

### Get Top Consumers

Returns top DBU-consuming clusters with percentage of total.

```http
GET /api/billing/top-consumers
```

**Query Parameters**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `days` | integer | 30 | Number of days to analyze (1-90) |
| `limit` | integer | 10 | Maximum clusters to return (1-20) |

**Response**: `TopConsumer[]`

```json
[
  {
    "cluster_id": "1234-567890-abc123",
    "cluster_name": "data-science-prod",
    "total_dbu": 5234.5,
    "estimated_cost_usd": 785.18,
    "percentage_of_total": 34.4
  }
]
```

---

## Metrics API

### Get Metrics Summary

Returns workspace-wide cluster metrics.

```http
GET /api/metrics/summary
```

**Response**: `ClusterMetricsSummary`

```json
{
  "total_clusters": 45,
  "running_clusters": 12,
  "pending_clusters": 2,
  "terminated_clusters": 31,
  "total_running_workers": 156,
  "estimated_hourly_dbu": 78.5
}
```

---

### Get Idle Cluster Alerts

Returns alerts for clusters that have been idle for extended periods.

```http
GET /api/metrics/idle-clusters
```

**Response**: `IdleClusterAlert[]`

```json
[
  {
    "cluster_id": "1234-567890-abc123",
    "cluster_name": "forgotten-cluster",
    "idle_duration_minutes": 263,
    "estimated_wasted_dbu": 52.3,
    "recommendation": "Consider stopping this cluster to save ~$7.85"
  }
]
```

**Idle Detection**: Clusters running with no activity for 30+ minutes.

---

### Get Optimization Recommendations

Returns optimization recommendations for all clusters.

```http
GET /api/metrics/recommendations
```

**Response**: `OptimizationRecommendation[]`

```json
[
  {
    "cluster_id": "1234-567890-abc123",
    "cluster_name": "my-cluster",
    "issue": "No auto-termination configured",
    "recommendation": "Enable auto-termination with 60-120 minutes timeout",
    "potential_savings": "40-60% reduction in idle costs",
    "priority": "high"
  }
]
```

---

## Policies API

### List Policies

Returns all cluster policies in the workspace.

```http
GET /api/policies
```

**Response**: `ClusterPolicySummary[]`

```json
[
  {
    "policy_id": "E0123456789ABCDEF",
    "name": "Data Science Policy",
    "definition": "{...}",
    "description": "Standard policy for data science workloads",
    "creator_user_name": "admin@example.com",
    "created_at_timestamp": "2023-06-01T00:00:00Z",
    "is_default": false
  }
]
```

---

### Get Policy Details

Returns detailed information about a specific policy.

```http
GET /api/policies/{policy_id}
```

**Response**: `ClusterPolicyDetail`

```json
{
  "policy_id": "E0123456789ABCDEF",
  "name": "Data Science Policy",
  "definition_json": {
    "spark_version": {
      "type": "fixed",
      "value": "14.3.x-scala2.12"
    },
    "autoscale.max_workers": {
      "type": "range",
      "maxValue": 10
    }
  },
  "max_clusters_per_user": 3,
  "policy_family_id": "personal-vm"
}
```

---

### Get Policy Usage

Returns clusters using a specific policy.

```http
GET /api/policies/{policy_id}/usage
```

**Response**: `PolicyUsage`

```json
{
  "policy_id": "E0123456789ABCDEF",
  "policy_name": "Data Science Policy",
  "cluster_count": 8,
  "clusters": [...]
}
```

---

## Optimization API

### Get Optimization Summary

Returns an overview of all optimization opportunities.

```http
GET /api/optimization/summary
```

**Response**: `OptimizationSummary`

```json
{
  "total_clusters_analyzed": 45,
  "oversized_clusters": 5,
  "underutilized_clusters": 12,
  "total_potential_monthly_savings": 1250.50,
  "recommendations_count": 28,
  "last_analysis_time": "2024-01-15T14:00:00Z"
}
```

---

### Get Spark Config Recommendations

Returns Spark configuration optimization recommendations.

```http
GET /api/optimization/spark-config-recommendations
```

**Response**: `ClusterSparkConfigAnalysis[]`

```json
[
  {
    "cluster_id": "1234-567890-abc123",
    "cluster_name": "my-cluster",
    "spark_version": "14.3.x-scala2.12",
    "is_photon_enabled": false,
    "aqe_enabled": true,
    "total_issues": 2,
    "recommendations": [
      {
        "setting": "spark.databricks.photon.enabled",
        "current_value": "false",
        "recommended_value": "true",
        "impact": "performance",
        "severity": "medium",
        "reason": "Photon can provide 2-8x speedup for SQL workloads"
      }
    ]
  }
]
```

---

### Get Cost Recommendations

Returns cost optimization recommendations (Spot instances, storage, etc.).

```http
GET /api/optimization/cost-recommendations
```

**Response**: `ClusterCostAnalysis[]`

---

### Get Autoscaling Recommendations

Returns autoscaling configuration recommendations.

```http
GET /api/optimization/autoscaling-recommendations
```

**Response**: `ClusterAutoscalingAnalysis[]`

---

### Get Node Type Recommendations

Returns node type right-sizing recommendations.

```http
GET /api/optimization/node-type-recommendations
```

**Response**: `ClusterNodeTypeAnalysis[]`

---

## Workspace API

### Get Workspace Info

Returns information about the connected workspace.

```http
GET /api/workspace/info
```

**Response**:

```json
{
  "workspace_url": "https://e2-demo-field-eng.cloud.databricks.com",
  "cloud_provider": "aws",
  "workspace_id": "1234567890123456"
}
```

---

## Common Response Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Bad request (invalid parameters) |
| 404 | Resource not found |
| 500 | Internal server error |
| 503 | Service unavailable (SQL warehouse starting) |

---

## Rate Limiting

The API inherits rate limits from the Databricks platform. For high-volume usage, consider:

- Caching responses on the client side
- Using the `limit` parameter to reduce response size
- Batching requests where possible

---

## SDK Usage Example

```python
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()

# List clusters
for cluster in ws.clusters.list():
    print(f"{cluster.cluster_name}: {cluster.state}")

# Start a cluster
ws.clusters.start("1234-567890-abc123")

# Query billing data
results = ws.statement_execution.execute_statement(
    warehouse_id="abc123",
    statement="SELECT * FROM system.billing.usage LIMIT 10"
)
```
