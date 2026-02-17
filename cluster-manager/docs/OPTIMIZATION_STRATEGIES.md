# Cluster Optimization Strategies for Cost Reduction & Better Utilization

## Executive Summary

The Cluster Manager implements **42+ optimization checks** across **12 categories** to reduce Databricks Total Cost of Ownership (TCO). This document organizes all strategies by audience: Platform Administrators and Cost Controllers.

---

## For Administrators

### Category 1: Idle Resource Management

| Strategy | What It Does | Expected Savings |
|----------|--------------|------------------|
| **Idle Cluster Detection** | Identifies clusters running with no activity for 30+ minutes | Up to $200/month per cluster |
| **Auto-termination Enforcement** | Flags clusters without auto-termination configured | 40-60% reduction in idle costs |
| **Long Auto-termination Values** | Detects values >120 minutes that could be reduced | 10-20% additional savings |
| **Wasted DBU Calculation** | Shows exact cost of idle time per cluster | Visibility for prioritization |

**Action**: Stop idle clusters immediately; configure 60-120 minute auto-termination on all clusters.

---

### Category 2: Autoscaling Configuration

| Strategy | What It Does | Expected Savings |
|----------|--------------|------------------|
| **Enable Autoscaling** | Detects fixed-size clusters (4+ workers) that should autoscale | 30-35% cost reduction |
| **High Minimum Workers** | Flags min_workers ≥8 that keep capacity running during idle | 25-50% during low usage |
| **Wide Autoscale Range** | Detects ranges >5x ratio suggesting uncertainty | Better predictability |
| **Narrow Autoscale Range** | Flags ranges ≤2 workers (consider fixed-size instead) | Simpler management |
| **Scale-to-Zero for Jobs** | Recommends min_workers=0 for job clusters | Eliminates idle job cluster costs |
| **Combined Auto-termination** | Suggests auto-termination + autoscaling together | 40-60% total savings |

**Action**: Set min_workers=1-2 for interactive clusters; min_workers=0 for job clusters; always enable auto-termination.

---

### Category 3: Instance Type Optimization

| Strategy | What It Does | Expected Savings |
|----------|--------------|------------------|
| **GPU on Non-ML Workloads** | Flags expensive GPU instances for SQL/ETL work | 60-70% cost reduction |
| **Oversized Driver** | Detects driver instances larger than 2x workers | 10-15% savings |
| **Legacy Instance Generations** | Recommends newer generations (6th, 7th gen) | 10-20% better price/performance |
| **Very Large Instances** | Flags 16xlarge/24xlarge with few workers | 20% savings via scaling out |
| **Wrong Category for Workload** | Detects compute-optimized for SQL (should be memory-optimized) | 10% performance improvement |
| **Overprovisioned Small Clusters** | Flags 2 workers with 32+ vCPUs each | Better parallelism |

**Action**: Match instance type to workload; avoid GPU for non-ML; consider r-series for SQL.

---

### Category 4: Spot/Preemptible Instances

| Strategy | What It Does | Expected Savings |
|----------|--------------|------------------|
| **AWS Spot Instances** | Recommends SPOT_WITH_FALLBACK for fault-tolerant workloads | 60-70% compute cost |
| **Azure Spot VMs** | Same as above for Azure | Up to 90% compute cost |
| **GCP Preemptible VMs** | Same as above for GCP | Up to 80% compute cost |
| **First-on-Demand Ratio** | Flags when >50% nodes are on-demand in Spot mode | 30% additional savings |
| **EBS Volume Type** | Recommends Throughput-Optimized HDD for batch jobs | 15% storage savings |

**Action**: Enable Spot/Preemptible with fallback; keep only driver on on-demand (first_on_demand=1).

---

### Category 5: Spark Configuration Tuning

| Strategy | What It Does | Expected Savings |
|----------|--------------|------------------|
| **AQE Disabled** | Flags disabled Adaptive Query Execution | 20-30% query speedup |
| **AQE Coalesce Disabled** | Partition coalescing not enabled | Faster small queries |
| **AQE Skew Join Disabled** | Skew handling not enabled | Prevents slow skewed joins |
| **High Shuffle Partitions** | Flags >2000 partitions (too much overhead) | Faster execution |
| **Broadcast Join Disabled** | Broadcast threshold set to -1 or 0 | Faster small-table joins |
| **Photon Not Enabled** | SQL/analytics clusters not using Photon | 2-8x speedup |
| **Delta Auto-Optimize Disabled** | Small file compaction not automatic | Better read performance |
| **Imbalanced Driver Memory** | Driver memory < 50% of executor memory | Prevents OOM errors |

**Action**: Don't disable AQE defaults; enable Photon for SQL; let Delta auto-optimize.

---

### Category 6: Cluster Consolidation

| Strategy | What It Does | Expected Savings |
|----------|--------------|------------------|
| **User Cluster Proliferation** | Flags users with 3+ clusters | $100-500/month by sharing |
| **Multiple Running Clusters** | Same user running 2+ clusters simultaneously | Consolidation opportunity |
| **Similar Configurations** | Clusters with same node type + runtime could share | $50-300/month |
| **Always-On Interactive** | Long-running interactive → consider job clusters | 70% cost reduction |

**Action**: Review per-user cluster counts; share clusters for similar workloads; use job clusters for batch.

---

### Category 7: Runtime & Version Management

| Strategy | What It Does | Expected Savings |
|----------|--------------|------------------|
| **Old Databricks Runtime** | Flags versions <13.x | 20% performance improvement |
| **Photon Recommendation** | Suggests Photon for SQL workloads | 2-8x faster queries |

**Action**: Upgrade to DBR 14.x or latest LTS; enable Photon for analytics.

---

## For Cost Controllers

### Financial Impact Summary

| Category | Typical Monthly Savings | Implementation Effort |
|----------|------------------------|----------------------|
| Spot/Preemptible Instances | 60-70% of compute | Low (configuration change) |
| Auto-termination | 40-60% of idle costs | Low |
| Autoscaling | 25-35% during low usage | Low |
| GPU → CPU for non-ML | 60-70% for affected clusters | Medium |
| Instance Right-sizing | 15-25% | Medium |
| Cluster Consolidation | $100-500 per redundant cluster | Medium |
| Spark Config Tuning | 20-30% faster = lower cost per job | Low |

---

### Key Metrics Tracked

| Metric | Purpose | Data Source |
|--------|---------|-------------|
| **Total DBU Usage** | Overall consumption | system.billing.usage |
| **DBU by Cluster** | Identify top consumers | system.billing.usage |
| **Daily Usage Trend** | Spot anomalies | system.billing.usage |
| **Efficiency Score** | Actual vs. potential DBU utilization | Calculated |
| **Idle Time** | Wasted compute hours | Cluster activity timestamps |
| **Top 10 Consumers** | Focus optimization efforts | Ranked by DBU |

---

### Cost Allocation Visibility

The system provides:

1. **Per-cluster cost breakdown** - Know exactly which clusters drive costs
2. **User-level attribution** - Track costs by cluster creator
3. **Cluster type segmentation** - Interactive vs. Job vs. SQL vs. Pipeline
4. **Trend analysis** - 7/30/90 day views with moving averages
5. **Percentage of total** - See cost distribution across resources

---

### Recommended Cost Governance Actions

| Priority | Action | Expected Impact |
|----------|--------|-----------------|
| **Critical** | Enable auto-termination on all clusters | 40-60% idle cost reduction |
| **Critical** | Switch to Spot instances (with fallback) | 60-70% compute savings |
| **High** | Reduce min_workers to 1-2 | 25-40% during low usage |
| **High** | Remove GPU instances from non-ML workloads | 60-70% on affected clusters |
| **Medium** | Consolidate user clusters | $100-500/user/month |
| **Medium** | Upgrade to newer instance generations | 10-20% better price/performance |
| **Low** | Enable Photon for SQL workloads | Faster queries = lower cost per job |

---

### Cost Monitoring Alerts

The system can detect and alert on:

- Clusters running 24+ hours continuously
- Clusters with no auto-termination
- Idle clusters wasting > $50/day
- Top 5 cost-growing clusters week-over-week
- New clusters using expensive GPU instances

---

### Savings Estimation Formula

```
Monthly Savings =
  (Spot Savings: 60% × non-spot compute) +
  (Idle Reduction: estimated_idle_hours × DBU_rate × $0.15) +
  (Autoscale Savings: 30% × peak_to_average_ratio × base_cost) +
  (Consolidation: redundant_clusters × avg_cluster_cost)
```

---

## Summary: Top 10 Quick Wins

| # | Strategy | Savings | Effort |
|---|----------|---------|--------|
| 1 | Enable Spot instances with fallback | 60-70% | 5 min/cluster |
| 2 | Set auto-termination to 60-90 min | 40-60% idle | 2 min/cluster |
| 3 | Reduce min_workers to 1-2 | 25-40% low usage | 2 min/cluster |
| 4 | Remove GPU from non-ML workloads | 60-70% | Review required |
| 5 | Enable autoscaling on fixed clusters | 30-35% | 5 min/cluster |
| 6 | Stop idle clusters immediately | Instant savings | Dashboard action |
| 7 | Consolidate similar clusters | $50-500/month | Planning required |
| 8 | Upgrade to newer instance generations | 10-20% | Testing required |
| 9 | Enable Photon for SQL workloads | 2-8x faster | Config change |
| 10 | Enable AQE (don't disable defaults) | 20-30% faster | Don't touch! |

---

## API Endpoints for Optimization

| Endpoint | Description |
|----------|-------------|
| `GET /api/optimization/summary` | Overview of all optimization opportunities |
| `GET /api/optimization/oversized-clusters` | Clusters with excess capacity |
| `GET /api/optimization/job-recommendations` | Cluster consolidation suggestions |
| `GET /api/optimization/schedule-recommendations` | Auto-termination improvements |
| `GET /api/optimization/spark-config-recommendations` | Spark tuning opportunities |
| `GET /api/optimization/cost-recommendations` | Spot/instance type savings |
| `GET /api/optimization/autoscaling-recommendations` | Autoscale configuration issues |
| `GET /api/optimization/node-type-recommendations` | Instance sizing analysis |
| `GET /api/optimization/trends` | Historical utilization with moving averages |
| `POST /api/optimization/collect-metrics` | Persist daily metrics for trend analysis |

---

## Related Documentation

- [README.md](../README.md) - Administrator's guide and deployment instructions
- [Databricks Cost Management](https://docs.databricks.com/administration-guide/account-settings/billable-usage.html)
- [Cluster Best Practices](https://docs.databricks.com/clusters/cluster-config-best-practices.html)
