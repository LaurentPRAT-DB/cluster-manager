# Data Dictionary

## Overview

This document describes all data models used in the Cluster Manager application, including API response schemas and data sources.

---

## External Data Sources

### system.billing.usage (Unity Catalog)

**Description**: Databricks billing usage data for all compute resources.

**Refresh Frequency**: Near real-time (within hours)

**Key Columns Used**:

| Column | Type | Description |
|--------|------|-------------|
| `usage_date` | DATE | Date of usage |
| `usage_quantity` | DOUBLE | DBU consumption |
| `usage_metadata.cluster_id` | STRING | Cluster identifier |

**Sample Query**:
```sql
SELECT
    usage_metadata.cluster_id as cluster_id,
    SUM(usage_quantity) as total_dbu
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAY
    AND usage_metadata.cluster_id IS NOT NULL
GROUP BY usage_metadata.cluster_id
ORDER BY total_dbu DESC
```

---

## API Response Models

### Cluster Models

#### ClusterState (Enum)

| Value | Description |
|-------|-------------|
| `PENDING` | Cluster is starting |
| `RUNNING` | Cluster is running and ready |
| `RESTARTING` | Cluster is restarting |
| `RESIZING` | Cluster is adding/removing workers |
| `TERMINATING` | Cluster is shutting down |
| `TERMINATED` | Cluster is stopped |
| `ERROR` | Cluster encountered an error |
| `UNKNOWN` | State could not be determined |

#### ClusterSource (Enum)

| Value | Description |
|-------|-------------|
| `UI` | Created via Databricks UI |
| `API` | Created via API/SDK |
| `JOB` | Job cluster |
| `MODELS` | Model serving cluster |
| `PIPELINE` | DLT pipeline cluster |
| `PIPELINE_MAINTENANCE` | DLT maintenance cluster |
| `SQL` | SQL warehouse cluster |

#### ClusterSummary

**Description**: Summary view of a cluster for list displays.

| Field | Type | Nullable | Description | Example |
|-------|------|----------|-------------|---------|
| `cluster_id` | string | No | Unique cluster identifier | `1234-567890-abc123` |
| `cluster_name` | string | No | Human-readable cluster name | `my-data-cluster` |
| `state` | ClusterState | No | Current cluster state | `RUNNING` |
| `creator_user_name` | string | Yes | Email of cluster creator | `user@example.com` |
| `node_type_id` | string | Yes | Worker instance type | `i3.xlarge` |
| `driver_node_type_id` | string | Yes | Driver instance type | `i3.xlarge` |
| `num_workers` | integer | Yes | Fixed number of workers (if not autoscaling) | `4` |
| `autoscale` | AutoScaleConfig | Yes | Autoscaling configuration | `{"min_workers": 2, "max_workers": 8}` |
| `spark_version` | string | Yes | Databricks Runtime version | `14.3.x-scala2.12` |
| `cluster_source` | ClusterSource | Yes | How cluster was created | `UI` |
| `start_time` | datetime | Yes | When cluster started | `2024-01-15T10:00:00Z` |
| `last_activity_time` | datetime | Yes | Last activity timestamp | `2024-01-15T14:30:00Z` |
| `uptime_minutes` | integer | No | Current uptime in minutes | `270` |
| `estimated_dbu_per_hour` | float | No | Estimated hourly DBU usage | `5.0` |
| `policy_id` | string | Yes | Associated cluster policy ID | `E0123456789ABCDEF` |

#### ClusterDetail (extends ClusterSummary)

**Description**: Full cluster details including configuration.

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `terminated_time` | datetime | Yes | When cluster was terminated |
| `termination_reason` | string | Yes | Reason for termination |
| `state_message` | string | Yes | Current state description |
| `default_tags` | dict | No | System-assigned tags |
| `custom_tags` | dict | No | User-defined tags |
| `aws_attributes` | dict | Yes | AWS-specific configuration |
| `azure_attributes` | dict | Yes | Azure-specific configuration |
| `gcp_attributes` | dict | Yes | GCP-specific configuration |
| `spark_conf` | dict | No | Spark configuration |
| `spark_env_vars` | dict | No | Environment variables |
| `init_scripts` | list | No | Initialization scripts |
| `cluster_log_conf` | dict | Yes | Log delivery configuration |
| `enable_elastic_disk` | boolean | Yes | Auto-expand disk enabled |
| `disk_spec` | dict | Yes | Disk configuration |
| `single_user_name` | string | Yes | Single user access mode user |
| `data_security_mode` | string | Yes | Data access security mode |

#### AutoScaleConfig

| Field | Type | Description |
|-------|------|-------------|
| `min_workers` | integer | Minimum number of workers |
| `max_workers` | integer | Maximum number of workers |

---

### Billing Models

#### BillingSummary

**Description**: Aggregate billing summary for a time period.

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `total_dbu` | float | Total DBU consumption | `15234.5` |
| `estimated_cost_usd` | float | Estimated cost at $0.15/DBU | `2285.18` |
| `period_start` | datetime | Start of billing period | `2023-12-15T00:00:00Z` |
| `period_end` | datetime | End of billing period | `2024-01-15T00:00:00Z` |
| `currency` | string | Currency code (always USD) | `USD` |

#### ClusterBillingUsage

**Description**: DBU usage for a specific cluster.

| Field | Type | Description |
|-------|------|-------------|
| `cluster_id` | string | Cluster identifier |
| `cluster_name` | string | Cluster name (if available) |
| `total_dbu` | float | Total DBU consumption |
| `estimated_cost_usd` | float | Estimated cost |
| `usage_date_start` | datetime | First usage date |
| `usage_date_end` | datetime | Last usage date |

#### BillingTrend

**Description**: Daily billing data point for trending.

| Field | Type | Description |
|-------|------|-------------|
| `date` | datetime | Usage date |
| `dbu` | float | DBU consumed that day |
| `estimated_cost_usd` | float | Estimated cost for the day |

#### TopConsumer

**Description**: Top DBU-consuming cluster with percentage.

| Field | Type | Description |
|-------|------|-------------|
| `cluster_id` | string | Cluster identifier |
| `cluster_name` | string | Cluster name |
| `total_dbu` | float | Total DBU consumption |
| `estimated_cost_usd` | float | Estimated cost |
| `percentage_of_total` | float | Percentage of workspace total |

---

### Metrics Models

#### ClusterMetricsSummary

**Description**: Workspace-wide cluster metrics snapshot.

| Field | Type | Description |
|-------|------|-------------|
| `total_clusters` | integer | Total cluster count |
| `running_clusters` | integer | Currently running clusters |
| `pending_clusters` | integer | Clusters starting up |
| `terminated_clusters` | integer | Stopped clusters |
| `total_running_workers` | integer | Sum of all running workers |
| `estimated_hourly_dbu` | float | Estimated DBU/hour for running clusters |

#### IdleClusterAlert

**Description**: Alert for a cluster that has been idle.

| Field | Type | Description |
|-------|------|-------------|
| `cluster_id` | string | Cluster identifier |
| `cluster_name` | string | Cluster name |
| `idle_duration_minutes` | integer | How long cluster has been idle |
| `estimated_wasted_dbu` | float | DBU consumed while idle |
| `recommendation` | string | Action recommendation |

**Business Rule**: Clusters are considered idle if running with no activity for 30+ minutes.

---

### Policy Models

#### ClusterPolicySummary

**Description**: Summary view of a cluster policy.

| Field | Type | Description |
|-------|------|-------------|
| `policy_id` | string | Unique policy identifier |
| `name` | string | Policy name |
| `definition` | string | JSON policy definition (string) |
| `description` | string | Policy description |
| `creator_user_name` | string | Policy creator |
| `created_at_timestamp` | datetime | Creation timestamp |
| `is_default` | boolean | Whether this is the default policy |

#### ClusterPolicyDetail (extends ClusterPolicySummary)

**Description**: Full policy details with parsed definition.

| Field | Type | Description |
|-------|------|-------------|
| `definition_json` | dict | Parsed JSON policy definition |
| `max_clusters_per_user` | integer | Cluster limit per user |
| `policy_family_id` | string | Policy family identifier |
| `policy_family_definition_overrides` | string | Family overrides |

---

### Optimization Models

#### ClusterType (Enum)

| Value | Description |
|-------|-------------|
| `JOB` | Job cluster |
| `INTERACTIVE` | Interactive/all-purpose cluster |
| `SQL` | SQL warehouse cluster |
| `PIPELINE` | DLT pipeline cluster |
| `MODELS` | Model serving cluster |

#### OptimizationRecommendation

**Description**: Single optimization recommendation.

| Field | Type | Description |
|-------|------|-------------|
| `cluster_id` | string | Target cluster |
| `cluster_name` | string | Cluster name |
| `issue` | string | Identified issue |
| `recommendation` | string | Recommended action |
| `potential_savings` | string | Expected cost savings |
| `priority` | string | `low`, `medium`, or `high` |

#### OptimizationSummary

**Description**: Overview of all optimization opportunities.

| Field | Type | Description |
|-------|------|-------------|
| `total_clusters_analyzed` | integer | Clusters reviewed |
| `oversized_clusters` | integer | Count with excess capacity |
| `underutilized_clusters` | integer | Count with low usage |
| `total_potential_monthly_savings` | float | Estimated monthly savings |
| `recommendations_count` | integer | Total recommendations |
| `last_analysis_time` | datetime | When analysis was performed |

---

### Spark Configuration Models

#### SparkConfigImpact (Enum)

| Value | Description |
|-------|-------------|
| `performance` | Affects query/job performance |
| `cost` | Affects resource costs |
| `reliability` | Affects stability |
| `memory` | Affects memory usage |

#### SparkConfigSeverity (Enum)

| Value | Description |
|-------|-------------|
| `high` | Significant impact, fix immediately |
| `medium` | Moderate impact, should address |
| `low` | Minor impact, optional fix |

#### SparkConfigRecommendation

**Description**: Single Spark configuration recommendation.

| Field | Type | Description |
|-------|------|-------------|
| `cluster_id` | string | Target cluster |
| `cluster_name` | string | Cluster name |
| `setting` | string | Spark config key |
| `current_value` | string | Current setting value |
| `recommended_value` | string | Recommended value |
| `impact` | SparkConfigImpact | Impact category |
| `severity` | SparkConfigSeverity | Issue severity |
| `reason` | string | Why this change helps |
| `documentation_link` | string | Reference documentation |

---

### Cost Optimization Models

#### CostOptimizationCategory (Enum)

| Value | Description |
|-------|-------------|
| `spot_instances` | Spot/preemptible instance usage |
| `node_type` | Instance type selection |
| `storage` | Storage configuration |
| `autoscaling` | Autoscaling settings |
| `serverless` | Serverless compute options |

#### NodeTypeCategory (Enum)

| Value | Description | Examples |
|-------|-------------|----------|
| `memory_optimized` | High memory per vCPU | r5, r6i, E-series |
| `compute_optimized` | High vCPU per memory | c5, c6i, F-series |
| `general_purpose` | Balanced resources | m5, m6i, D-series |
| `gpu` | GPU-enabled instances | p3, p4, g4, NC-series |
| `storage_optimized` | High local storage | i3, d2, L-series |
| `unknown` | Could not determine | - |

---

## Computed Metrics

### Efficiency Score

**Formula**: `(actual_dbu / potential_dbu) * 100`

- `actual_dbu`: DBUs consumed from billing data
- `potential_dbu`: `worker_count * hours_running * dbu_per_worker_hour`

**Thresholds**:
- `< 30%`: Oversized cluster
- `< 50%`: Underutilized cluster
- `>= 50%`: Healthy utilization

### Estimated Cost

**Formula**: `total_dbu * $0.15`

**Note**: This is a rough estimate. Actual DBU rates vary by SKU and cloud provider.

### Idle Duration

**Formula**: `current_time - last_activity_time` (when cluster is RUNNING)

**Idle Threshold**: 30 minutes
