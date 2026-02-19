# Databricks Cluster Manager

## Administrator's Guide

A centralized management console for Databricks workspace administrators to **control costs**, **optimize resource utilization**, and **maintain governance** over compute clusters.

---

## Why This Tool?

As a Databricks administrator, you face several challenges:

| Challenge | Impact | How Cluster Manager Helps |
|-----------|--------|---------------------------|
| **Uncontrolled costs** | DBU expenses can spiral without visibility | Real-time cost tracking with Unity Catalog billing data |
| **Idle resources** | Running clusters with no activity waste money | Automated idle detection with wasted DBU calculations |
| **No single pane of glass** | Switching between UI, CLI, and notebooks | Unified dashboard for all cluster operations |
| **Risky operations** | Accidental cluster deletion causes disruption | Safe Mode prevents permanent cluster termination |
| **Configuration drift** | Clusters without auto-termination or autoscaling | Actionable optimization recommendations |

---

## Key Objectives

### 1. Cost Visibility & Control

**Problem**: DBU costs accumulate across dozens of clusters with no easy way to identify cost drivers.

**Solution**:
- **Billing Dashboard** pulls data from `system.billing.usage` Unity Catalog table
- View total DBU consumption over configurable time periods (7/30/90 days)
- Identify **top consuming clusters** with percentage breakdown
- Track **daily usage trends** to spot anomalies
- Estimated cost calculations (configurable DBU rate)

```
Example insight: "Cluster 'data-science-dev' consumed 2,450 DBUs (35% of total)
in the last 30 days, estimated cost: $367.50"
```

### 2. Idle Resource Detection

**Problem**: Clusters left running overnight or over weekends drain budget.

**Solution**:
- Automatic detection of clusters running with **no activity for 30+ minutes**
- **Wasted DBU calculation** showing exactly how much idle time costs
- Prioritized alerts sorted by cost impact
- Direct action: stop idle clusters with one click

```
Example alert: "Cluster 'analytics-prod' has been idle for 4 hours 23 minutes.
Estimated wasted DBUs: 52.3 (~$7.85)"
```

### 3. Optimization Recommendations

**Problem**: Sub-optimal cluster configurations increase costs without improving performance.

**Solution**: Automated analysis generates actionable recommendations:

| Issue Detected | Recommendation | Priority |
|----------------|----------------|----------|
| No auto-termination | Set 30-120 min timeout | High |
| Large fixed-size cluster (10+ workers) | Enable autoscaling | Medium |
| Running 24+ hours continuously | Review if needed; use job clusters | Medium/High |
| Wide autoscale range (>20 workers) | Consider tighter bounds | Low |
| Old Databricks Runtime (<13.x) | Upgrade for 20%+ performance gains | Low |

> **Deep Dive**: See [Optimization Strategies](docs/OPTIMIZATION_STRATEGIES.md) for all 42+ optimization checks across 12 categories, with detailed savings estimates for administrators and cost controllers.

### 4. Safe Cluster Operations

**Problem**: Administrative accidents (wrong cluster terminated) cause production outages.

**Solution**: **Safe Mode** by design:
- **Start** clusters - Enabled
- **Stop** clusters (preserves configuration) - Enabled
- **Terminate/Delete** clusters - Disabled

This ensures no permanent cluster loss through the UI. Configuration remains intact for restart.

### 5. Policy Compliance Monitoring

**Problem**: Clusters created outside of approved policies bypass governance controls.

**Solution**:
- View all cluster policies in the workspace
- See which clusters use which policies
- Identify clusters running without any policy (potential governance gap)

---

## Dashboard Views

### Clusters Overview
- **Real-time status**: Running, Pending, Terminated, Error
- **Resource allocation**: Workers, node types, autoscale settings
- **Uptime tracking**: How long each cluster has been running
- **Quick actions**: Start/Stop with confirmation

### Analytics
- **Cost summary cards**: Total DBUs, estimated cost, time period
- **Trend charts**: Daily DBU consumption visualization
- **Top consumers**: Ranked list with percentage of total
- **Per-cluster breakdown**: Drill-down into individual cluster costs

### Policies
- **Policy inventory**: All cluster policies with definitions
- **Usage mapping**: Which clusters are using each policy
- **Compliance gaps**: Clusters without policy assignment

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Databricks App                           │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐         ┌─────────────────────────┐   │
│  │   React UI      │  HTTP   │   FastAPI Backend       │   │
│  │   (TypeScript)  │◄───────►│   (Python)              │   │
│  └─────────────────┘         └───────────┬─────────────┘   │
│                                          │                  │
│                              ┌───────────▼─────────────┐   │
│                              │   Databricks SDK        │   │
│                              └───────────┬─────────────┘   │
└──────────────────────────────────────────│──────────────────┘
                                           │
           ┌───────────────────────────────┼───────────────────────────────┐
           │                               │                               │
           ▼                               ▼                               ▼
   ┌───────────────┐            ┌───────────────────┐           ┌───────────────┐
   │ Clusters API  │            │ SQL Warehouse     │           │ Policies API  │
   │ (start/stop)  │            │ (billing queries) │           │ (governance)  │
   └───────────────┘            └───────────────────┘           └───────────────┘
                                         │
                                         ▼
                              ┌───────────────────────┐
                              │ system.billing.usage  │
                              │ (Unity Catalog)       │
                              └───────────────────────┘
```

### MCP Server Integration

The app also exposes a **Managed MCP Server** for AI agent integration via Databricks AI Playground:

```
┌──────────────────────┐     UC HTTP Connection     ┌──────────────────────┐
│   AI Playground      │ ◄─────────────────────────►│   Cluster Manager    │
│   (or AI Agent)      │      JSON-RPC 2.0          │   /api/mcp/*         │
└──────────────────────┘                            └──────────────────────┘
                                                              │
                                                    ┌─────────▼─────────┐
                                                    │   7 MCP Tools     │
                                                    │   list_clusters   │
                                                    │   get_cluster     │
                                                    │   start_cluster   │
                                                    │   stop_cluster    │
                                                    │   get_events      │
                                                    │   list_policies   │
                                                    │   get_policy      │
                                                    └───────────────────┘
```

See [MCP Server Guide](docs/MCP_SERVER.md) for setup instructions.

---

## Prerequisites

Before deploying, ensure you have:

| Requirement | Purpose | How to Verify |
|-------------|---------|---------------|
| **Unity Catalog** | Access to `system.billing.usage` table | `SELECT * FROM system.billing.usage LIMIT 1` |
| **SQL Warehouse** | Execute billing queries | Check SQL Warehouses in workspace |
| **Cluster Admin permissions** | Start/stop clusters | `CAN_MANAGE` on clusters |
| **Databricks CLI** | Deploy the app | `databricks auth login` |

---

## Deployment

### Quick Start

```bash
# Clone and navigate
cd cluster-manager

# Deploy to your workspace
databricks bundle deploy -t dev

# Access the app
databricks bundle open -t dev
```

### Configuration

Set the SQL Warehouse for billing queries:

```bash
# Option 1: Via deployment variable
databricks bundle deploy -t dev -var="sql_warehouse_id=abc123def456"

# Option 2: Via environment variable
export CLUSTER_MANAGER_SQL_WAREHOUSE_ID=abc123def456
```

---

## API Reference

### Cluster Operations
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/clusters` | GET | List all clusters with state and metrics |
| `/api/clusters/{id}` | GET | Detailed cluster configuration |
| `/api/clusters/{id}/start` | POST | Start a terminated cluster |
| `/api/clusters/{id}/stop` | POST | Stop a running cluster (Safe Mode) |
| `/api/clusters/{id}/events` | GET | Recent cluster events |

### Billing & Analytics
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/billing/summary` | GET | Total DBU usage and estimated cost |
| `/api/billing/by-cluster` | GET | DBU breakdown per cluster |
| `/api/billing/trend` | GET | Daily usage for charting |
| `/api/billing/top-consumers` | GET | Ranked list of cost drivers |

### Metrics & Recommendations
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/metrics/summary` | GET | Workspace-wide cluster metrics |
| `/api/metrics/idle-clusters` | GET | Idle cluster alerts with wasted DBU |
| `/api/metrics/recommendations` | GET | Optimization suggestions |

### Policies
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/policies` | GET | List all cluster policies |
| `/api/policies/{id}` | GET | Policy details |
| `/api/policies/{id}/usage` | GET | Clusters using this policy |

### MCP Server (AI Agent Integration)
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/mcp/health` | GET | Server health and capabilities |
| `/api/mcp/tools` | GET | List available MCP tools |
| `/api/mcp` | POST | JSON-RPC 2.0 endpoint for tool execution |

---

## Security Considerations

- **Authentication**: Uses Databricks App OAuth (inherits user permissions)
- **Authorization**: Operations are limited to what the logged-in user can perform
- **Safe Mode**: No permanent cluster deletion through this UI
- **Audit**: All actions are logged through standard Databricks audit logs

---

## Roadmap

Future enhancements for administrators:

- [ ] **Scheduled reports**: Weekly cost summary emails
- [ ] **Budget alerts**: Notifications when DBU thresholds are exceeded
- [ ] **Auto-stop policies**: Automatically stop idle clusters
- [ ] **Tag-based cost allocation**: Group costs by team/project tags
- [ ] **Comparison views**: Month-over-month cost trends

---

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/ARCHITECTURE.md) | System architecture, data flows, and technology stack |
| [API Reference](docs/API.md) | Complete API documentation with examples |
| [Deployment Guide](docs/DEPLOYMENT.md) | Local development, deployment, and operations |
| [Contributing Guide](docs/CONTRIBUTING.md) | Development workflow and coding standards |
| [Data Dictionary](docs/DATA_DICTIONARY.md) | All data models and schemas |
| [Optimization Strategies](docs/OPTIMIZATION_STRATEGIES.md) | Complete guide to all 42+ cost optimization checks |
| [MCP Server Guide](docs/MCP_SERVER.md) | Transform your app into a managed MCP server for AI agents |

### External References

| Resource | Description |
|----------|-------------|
| [Databricks Cost Management](https://docs.databricks.com/administration-guide/account-settings/billable-usage.html) | Official Databricks billing documentation |
| [Cluster Best Practices](https://docs.databricks.com/clusters/cluster-config-best-practices.html) | Databricks cluster configuration guide |
| [Databricks Apps](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html) | Databricks Apps platform documentation |

---

## Support

For issues or feature requests, please contact your platform team or open an issue in the repository.

---

## License

MIT
