# Deployment Guide

## Overview

The Cluster Manager is deployed as a Databricks App using Databricks Asset Bundles (DABs). This guide covers local development, deployment, and production operations.

---

## Prerequisites

### Required Tools

| Tool | Version | Installation |
|------|---------|--------------|
| Python | 3.10+ | `brew install python@3.10` |
| Node.js | 18+ | `brew install node` |
| Databricks CLI | Latest | `brew install databricks/tap/databricks` |
| uv | Latest | `brew install uv` |

### Required Permissions

| Resource | Permission | Purpose |
|----------|------------|---------|
| Databricks Apps | `CAN_MANAGE` | Deploy and manage app |
| Clusters | `CAN_MANAGE` | Start/stop clusters |
| SQL Warehouses | `CAN_USE` | Execute billing queries |
| Unity Catalog | `USE CATALOG` | Access system.billing.usage |

### Authenticate with Databricks

```bash
# Configure profile
databricks configure --profile my-workspace

# Verify authentication
databricks auth env --profile my-workspace
```

---

## Local Development

### 1. Clone and Setup

```bash
# Clone repository
git clone https://github.com/your-org/cluster-manager.git
cd cluster-manager

# Create Python virtual environment
uv venv
source .venv/bin/activate

# Install Python dependencies
uv pip install -e ".[dev]"

# Install Node dependencies
cd cluster_manager/ui
npm install
cd ../..
```

### 2. Configure Environment

Create a `.env` file in the project root:

```bash
# .env
CLUSTER_MANAGER_APP_NAME="Cluster Manager (Dev)"
CLUSTER_MANAGER_SQL_WAREHOUSE_ID="your-warehouse-id"
DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
```

### 3. Start Development Servers

```bash
# Terminal 1: Start backend
cd cluster_manager/backend
uvicorn app:app --reload --port 8000

# Terminal 2: Start frontend
cd cluster_manager/ui
npm run dev
```

Access the app at `http://localhost:5173` (Vite dev server proxies API calls to port 8000).

---

## Building for Deployment

### Build Frontend

**CRITICAL**: TSX changes require rebuilding before deployment!

```bash
cd cluster_manager/ui

# Build frontend assets
npm run build

# Verify build output
ls -la ../__dist__/
# Should contain: index.html, assets/
```

The build output goes to `cluster_manager/__dist__/` which is served by FastAPI in production.

### Verify .gitignore

Ensure `__dist__/` is **NOT** in `.gitignore`:

```bash
# This should return nothing
grep -n "__dist__" .gitignore

# If found, remove the line!
```

---

## Deployment with DABs

### Configuration Files

#### databricks.yml

```yaml
bundle:
  name: cluster-manager

variables:
  app_name:
    description: Name of the Databricks App
    default: cluster-manager
  sql_warehouse_id:
    description: SQL Warehouse ID for billing queries (optional)
    default: ""

targets:
  dev:
    mode: development
    default: true
    workspace:
      root_path: /Users/${workspace.current_user.userName}/.bundle/${bundle.name}/dev

  prod:
    mode: production
    workspace:
      root_path: /Shared/.bundle/${bundle.name}/prod

resources:
  apps:
    cluster_manager:
      name: ${var.app_name}
      description: "Databricks Cluster Management & Cost Optimization Dashboard"
      source_code_path: .
      permissions:
        - user_name: ${workspace.current_user.userName}
          level: CAN_MANAGE
```

#### app.yaml

```yaml
command:
  - uvicorn
  - cluster_manager.backend.app:app
  - --host
  - 0.0.0.0
  - --port
  - "8000"

env:
  - name: CLUSTER_MANAGER_APP_NAME
    value: "Cluster Manager"
  - name: DATABRICKS_HOST
    value: "https://your-workspace.cloud.databricks.com"  # REQUIRED!

# OAuth scopes for user token
user_api_scopes:
  - compute.clusters:read
  - compute.clusters:manage
  - compute.cluster-policies:read
  - sql.statement-execution:execute
```

**Important**: `DATABRICKS_HOST` must be set explicitly as the WorkspaceClient may not auto-detect it in the Databricks Apps environment.

### Deploy Commands

```bash
# Validate bundle configuration
databricks bundle validate -t dev

# Deploy to development
databricks bundle deploy -t dev

# Deploy with custom warehouse
databricks bundle deploy -t dev -var="sql_warehouse_id=abc123def456"

# Deploy to production
databricks bundle deploy -t prod
```

---

## App Lifecycle Management

### Start/Stop/Restart

```bash
# Stop the app
databricks apps stop cluster-manager

# Start the app
databricks apps start cluster-manager

# Check app status
databricks apps get cluster-manager

# View app URL
databricks apps get cluster-manager | jq -r '.url'
```

### Viewing Logs

```bash
# View recent logs
databricks apps logs cluster-manager

# Stream logs
databricks apps logs cluster-manager --follow

# Filter app logs (exclude system logs)
databricks apps logs cluster-manager | grep "\[APP\]"
```

### Full Redeployment

When making changes, follow this sequence:

```bash
# 1. Build frontend
cd cluster_manager/ui
npm run build
cd ../..

# 2. Deploy bundle
databricks bundle deploy -t dev

# 3. Restart app to pick up changes
databricks apps stop cluster-manager
databricks apps start cluster-manager

# 4. Verify deployment
databricks apps get cluster-manager
```

---

## Service Principal Setup

For cluster management operations, the app uses Service Principal authentication.

### 1. Get Service Principal ID

```bash
# Get the app's service principal ID
databricks apps get cluster-manager | jq -r '.service_principal_id'
# Example output: 77802625334619
```

### 2. Add SP to Admins Group

```bash
# Get admins group ID
databricks groups list | jq '.[] | select(.display_name=="admins") | .id'
# Example output: 2545746352486952

# Add SP to admins group
databricks groups patch 2545746352486952 --json '{
  "Operations": [{
    "op": "add",
    "path": "members",
    "value": [{"value": "77802625334619"}]
  }],
  "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"]
}'
```

---

## Version Management

### Tracking Deployments

Add a visible version number in the UI sidebar to verify deployments:

```tsx
// In _sidebar.tsx footer
<span className="text-xs text-muted-foreground">v1.5.0</span>
```

Bump the version after each deployment to visually confirm updates are live.

### Deployment Checklist

- [ ] Frontend built (`npm run build`)
- [ ] Version number updated
- [ ] `__dist__/` not in `.gitignore`
- [ ] Bundle deployed (`databricks bundle deploy`)
- [ ] App restarted (`databricks apps stop/start`)
- [ ] Version visible in UI footer
- [ ] Test cluster operations work

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| UI not updating | Frontend not rebuilt | Run `npm run build` |
| `__dist__` not deploying | In `.gitignore` | Remove from `.gitignore` |
| API returns 404 | `DATABRICKS_HOST` not set | Set in `app.yaml` |
| Permission denied | SP not in admins | Add SP to admins group |
| Cluster operations fail | Using OBO instead of SP | Check `core.py` uses SP auth |
| SQL queries timeout | Warehouse starting | Wait and retry, use serverless |

### Debug Endpoints

```bash
# Check workspace client configuration
curl https://cluster-manager-xxx.aws.databricksapps.com/api/debug/ws-info

# Test cluster access
curl https://cluster-manager-xxx.aws.databricksapps.com/api/debug/clusters-count

# Check health
curl https://cluster-manager-xxx.aws.databricksapps.com/api/health
```

### Viewing App Logs

```bash
# All logs
databricks apps logs cluster-manager

# Filter for errors
databricks apps logs cluster-manager | grep -i error

# Filter for specific component
databricks apps logs cluster-manager | grep "clusters"
```

---

## Production Considerations

### High Availability

- Databricks Apps automatically handles scaling
- Multiple app instances may run concurrently
- API is stateless, no session affinity required

### Monitoring

- Use Databricks audit logs for access tracking
- Monitor SQL warehouse usage for billing queries
- Set up alerts for app errors in logs

### Security

- App inherits user permissions via OAuth
- SP credentials stored securely by Databricks
- No secrets stored in code or config files

### Backup

- No persistent state to backup
- Code stored in bundle deployment
- Configuration in `databricks.yml` and `app.yaml`
