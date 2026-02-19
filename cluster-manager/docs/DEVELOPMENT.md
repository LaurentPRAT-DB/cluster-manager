# Development Guide

Development notes, patterns, and gotchas for the Cluster Manager project.

## Project Overview

- **Framework**: APX (FastAPI + React with TanStack Router/Query)
- **Deployment**: Databricks Asset Bundles (DABS) to serverless compute
- **App URL**: https://cluster-manager-1444828305810485.aws.databricksapps.com
- **GitHub**: https://github.com/LaurentPRAT-DB/cluster-manager

## Project Structure

```
cluster-manager/
├── docs/
│   ├── MCP_SERVER.md      # Comprehensive MCP guide
│   ├── DEVELOPMENT.md     # This file
│   └── images/            # Screenshots for documentation
├── skills/
│   └── databricks-apx-mcp-server/  # MCP skill documentation
├── cluster_manager/
│   ├── ui/
│   │   ├── routes/_sidebar/
│   │   │   ├── clusters/          # Cluster list and detail pages
│   │   │   ├── optimization.tsx   # Optimization with filter tabs
│   │   │   └── policies.tsx       # Policy management
│   │   ├── vite.config.ts         # Build config with version injection
│   │   └── vite-env.d.ts          # TypeScript declarations
│   └── backend/
│       └── routers/
│           ├── mcp.py             # MCP JSON-RPC 2.0 router
│           ├── clusters.py        # Cluster CRUD operations
│           ├── policies.py        # Policy operations
│           └── optimization.py    # Optimization analysis
```

## Deployment Commands

```bash
# Build frontend (REQUIRED before deploy)
cd cluster-manager/cluster_manager/ui && npm run build

# Deploy bundle files
cd ../.. && databricks bundle deploy -t dev

# Trigger new deployment (IMPORTANT: needed after bundle deploy)
databricks apps deploy cluster-manager \
  --source-code-path "/Workspace/Users/<your-email>/.bundle/cluster-manager/dev/files"

# Check status
databricks apps get cluster-manager
```

## Critical Gotchas

### SDK Methods Return Generators

Many Databricks SDK methods return generators, not objects with attributes:

```python
# WRONG - clusters.events() returns a generator, not an object
events_response = ws.clusters.events(cluster_id=cluster_id, limit=limit)
for event in events_response.events:  # AttributeError: 'generator' has no attribute 'events'

# CORRECT - iterate directly over the generator
events = []
for i, event in enumerate(ws.clusters.events(cluster_id=cluster_id)):
    if i >= limit:
        break
    events.append(event)
```

Common SDK methods that return generators:
- `ws.clusters.events()` - yields ClusterEvent objects
- `ws.clusters.list()` - yields ClusterDetails objects
- `ws.jobs.list()` - yields BaseJob objects

### SDK Object Attributes

Don't assume SDK objects have intuitive attributes. Check the actual structure:

```python
# WRONG - TerminationReason has no .message attribute
cluster.termination_reason.message

# CORRECT - TerminationReason has code, type, parameters
def _format_termination_reason(reason):
    parts = []
    if reason.code:
        parts.append(str(reason.code.value))
    if reason.type:
        parts.append(str(reason.type.value))
    if reason.parameters:
        for k, v in reason.parameters.items():
            parts.append(f"{k}={v}")
    return " - ".join(parts)
```

### Back Button Navigation (React/TanStack Router)

Don't hardcode back button destinations - use browser history:

```tsx
// WRONG - Always goes to /clusters regardless of where user came from
<Link to="/clusters">
  <ArrowLeft />
</Link>

// CORRECT - Returns to previous page (Optimization, Policies, etc.)
import { useRouter } from "@tanstack/react-router";

function DetailPage() {
  const router = useRouter();
  return (
    <button onClick={() => router.history.back()} title="Go back">
      <ArrowLeft />
    </button>
  );
}
```

### Falsy Value Checks for Filters

When filtering on numeric fields that can be 0 or null:

```tsx
// WRONG - 0 !== null is true, so 0 counts as "has value"
filtered.filter(r => r.auto_terminate_minutes !== null)

// CORRECT - treats both 0 and null as "no value"
filtered.filter(r => !!r.auto_terminate_minutes)
```

### Filter Scope UX

Place filters **inside the section they affect**, not at a global level. Global-looking filters that only affect one tab confuse users. Each tab should have its own filter chips.

### Deployment Not Reflecting Changes

After code changes, you need BOTH commands:

```bash
# 1. Upload bundle files
databricks bundle deploy -t dev

# 2. Trigger actual app deployment (REQUIRED!)
databricks apps deploy <app-name> \
  --source-code-path "/Workspace/Users/<email>/.bundle/<app>/dev/files"
```

The first command uploads files; the second triggers the actual deployment. Missing the second command is a common mistake.

### MCP Endpoint Testing

MCP endpoints require authentication - `curl` will return empty responses. Test via:
- Browser (if logged in)
- SQL `http_request()` with UC connection
- AI Playground with MCP server configured

## MCP Server

See [MCP_SERVER.md](MCP_SERVER.md) for comprehensive documentation on:
- Adding MCP capabilities to APX apps
- Tool definition with 4-section description format
- Unity Catalog HTTP Connection setup
- AI Playground integration

### 7 MCP Tools Exposed

1. `list_clusters` - List/search clusters with state filter
2. `get_cluster` - Get detailed cluster config
3. `start_cluster` - Start a stopped cluster
4. `stop_cluster` - Stop a running cluster
5. `get_cluster_events` - Get cluster event history
6. `list_policies` - List cluster policies
7. `get_policy` - Get policy details

## Testing

### Browser Testing with Chrome DevTools MCP

Use Chrome DevTools MCP for automated UI testing:
- Navigate pages
- Click elements by uid
- Check console for errors
- Verify network requests

### Test Checklist

| Test | Expected |
|------|----------|
| App loads | Shows version in footer |
| Navigation sidebar | All links work |
| Cluster list | Shows clusters with state badges |
| Cluster detail | Shows config, timing, tags, events |
| Back button | Returns to previous page (not hardcoded) |
| Terminated cluster | Shows termination reason |
| Optimization tabs | All 7 tabs load with data |
| Console errors | None |

**Note**: Start/Stop cluster buttons require a dedicated test cluster to avoid affecting production workloads.
