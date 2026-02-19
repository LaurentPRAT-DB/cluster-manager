---
name: databricks-apx-mcp-server
description: "Transform a Databricks APX app (FastAPI + React) into a managed MCP server for AI Playground integration. Creates JSON-RPC 2.0 endpoint, Unity Catalog HTTP Connection, and enables Supervisor Agent usage. Use when adding MCP capabilities to existing APX apps."
---

# APX to Databricks Managed MCP Server

Transform an existing APX (FastAPI + React) Databricks App into a managed MCP server that can be used from the Databricks AI Playground via Supervisor Agents.

## Overview

This skill adds Model Context Protocol (MCP) capabilities to an APX app, enabling:
- AI agents to call your app's functionality as tools
- Integration with Databricks AI Playground
- Integration with Supervisor Agents (MAS)
- Registration in Unity Catalog for governance

## Prerequisites

- Existing APX Databricks App deployed and running
- Unity Catalog enabled workspace
- App's service principal ID (from `databricks apps get <app-name>`)

## Quick Start Checklist

```
- [ ] Identify REST endpoints to expose as MCP tools
- [ ] Create MCP router with JSON-RPC 2.0 endpoint
- [ ] Register router in app.py
- [ ] Deploy updated app with `databricks bundle deploy` then `databricks apps deploy`
- [ ] Create OAuth secret for app's service principal
- [ ] Create Unity Catalog HTTP Connection with is_mcp_connection='true'
- [ ] Test with http_request() SQL function
- [ ] Test interactively in AI Playground
- [ ] Create Supervisor Agent (optional)
```

---

## Part 1: Developer Implementation

### Dependencies

No additional dependencies beyond standard APX setup:

```toml
# pyproject.toml
[project]
dependencies = [
    "fastapi>=0.115.0",
    "pydantic>=2.0.0",
    "databricks-sdk>=0.40.0",
]
```

### Step 1: Create MCP Router

Create `routers/mcp.py` with the MCP JSON-RPC 2.0 endpoint:

```python
"""MCP (Model Context Protocol) JSON-RPC 2.0 endpoint."""

import json
from typing import Any
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..core import Dependency, logger

router = APIRouter(prefix="/api/mcp", tags=["mcp"])

# --- MCP Protocol Models ---

class JsonRpcRequest(BaseModel):
    """JSON-RPC 2.0 request."""
    jsonrpc: str = Field(default="2.0", description="JSON-RPC version")
    method: str = Field(..., description="Method to call")
    params: dict[str, Any] | None = Field(default=None, description="Method parameters")
    id: int | str | None = Field(default=None, description="Request ID")


class JsonRpcError(BaseModel):
    """JSON-RPC 2.0 error object."""
    code: int
    message: str
    data: Any | None = None


class JsonRpcResponse(BaseModel):
    """JSON-RPC 2.0 response."""
    jsonrpc: str = "2.0"
    result: Any | None = None
    error: JsonRpcError | None = None
    id: int | str | None = None
```

### Step 2: Define MCP Tools

**CRITICAL**: Tool descriptions determine how well AI routes user requests. Use the 4-section format:

```python
MCP_TOOLS = [
    {
        "name": "list_items",
        "description": (
            "ACTION VERB + PURPOSE - Brief summary of what this tool does. "
            "\n\n"
            "RETURNS: List specific fields returned (item_id, name, status, etc.). "
            "\n\n"
            "USE THIS TOOL WHEN USER ASKS: "
            "'Example 1', 'Example 2', 'Example 3' (include 10+ variations). "
            "\n\n"
            "DO NOT USE FOR: Getting detailed info about ONE item (use get_item), "
            "or for taking actions (use respective action tools). "
            "\n\n"
            "TIP: Optional usage hints for better results."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "description": "Filter by status. ACTIVE=current, PENDING=waiting.",
                    "enum": ["ACTIVE", "PENDING", "COMPLETED"]
                },
                "limit": {
                    "type": "integer",
                    "description": "Max items to return.",
                    "default": 100,
                    "minimum": 1,
                    "maximum": 500
                }
            }
        }
    },
    # Add more tools...
]

SERVER_INFO = {
    "name": "your-app-mcp",
    "version": "1.0.0",
    "description": "Your app description"
}
```

### Step 3: Implement Tool Execution

Connect MCP tools to your existing router functions:

```python
async def execute_tool(
    tool_name: str,
    arguments: dict[str, Any],
    ws: Dependency.Client,
) -> dict[str, Any]:
    """Execute an MCP tool by calling existing functions."""
    logger.info(f"MCP executing tool: {tool_name} with args: {arguments}")

    if tool_name == "list_items":
        from .items import list_items
        result = list_items(ws, arguments.get("status"), arguments.get("limit", 100))
        return {"items": [r.model_dump(mode="json") for r in result], "count": len(result)}

    elif tool_name == "get_item":
        from .items import get_item
        item = get_item(arguments["item_id"], ws)
        return item.model_dump(mode="json")

    raise ValueError(f"Unknown tool: {tool_name}")
```

### Step 4: MCP Protocol Handlers

```python
def _handle_initialize(request: JsonRpcRequest) -> JsonRpcResponse:
    """Handle MCP initialize method."""
    return JsonRpcResponse(
        id=request.id,
        result={
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": SERVER_INFO,
        },
    )


def _handle_tools_list(request: JsonRpcRequest) -> JsonRpcResponse:
    """Handle MCP tools/list method."""
    return JsonRpcResponse(id=request.id, result={"tools": MCP_TOOLS})


async def _handle_tools_call(request: JsonRpcRequest, ws) -> JsonRpcResponse:
    """Handle MCP tools/call method."""
    params = request.params or {}
    tool_name = params.get("name")
    arguments = params.get("arguments", {})

    if not tool_name:
        return JsonRpcResponse(
            id=request.id,
            error=JsonRpcError(code=-32602, message="Invalid params: 'name' is required"),
        )

    # Validate tool exists
    tool_names = [t["name"] for t in MCP_TOOLS]
    if tool_name not in tool_names:
        return JsonRpcResponse(
            id=request.id,
            error=JsonRpcError(code=-32602, message=f"Unknown tool: {tool_name}"),
        )

    try:
        result = await execute_tool(tool_name, arguments, ws)
        return JsonRpcResponse(
            id=request.id,
            result={"content": [{"type": "text", "text": json.dumps(result, indent=2, default=str)}]},
        )
    except HTTPException as e:
        return JsonRpcResponse(
            id=request.id,
            error=JsonRpcError(code=-32000, message=f"Tool execution failed: {e.detail}"),
        )
    except Exception as e:
        logger.exception(f"MCP tool call failed: {tool_name}")
        return JsonRpcResponse(
            id=request.id,
            error=JsonRpcError(code=-32000, message=str(e)),
        )


@router.post("", response_model=JsonRpcResponse)
async def mcp_handler(request: JsonRpcRequest, ws: Dependency.Client) -> JsonRpcResponse:
    """MCP JSON-RPC 2.0 endpoint."""
    logger.info(f"MCP request: method={request.method}, id={request.id}")

    if request.jsonrpc != "2.0":
        return JsonRpcResponse(
            id=request.id,
            error=JsonRpcError(code=-32600, message=f"Invalid JSON-RPC version: {request.jsonrpc}"),
        )

    if request.method == "initialize":
        return _handle_initialize(request)
    elif request.method == "tools/list":
        return _handle_tools_list(request)
    elif request.method == "tools/call":
        return await _handle_tools_call(request, ws)

    return JsonRpcResponse(
        id=request.id,
        error=JsonRpcError(code=-32601, message=f"Method not found: {request.method}"),
    )


# REST endpoints for debugging
@router.get("/tools")
async def list_tools():
    """List available MCP tools (debugging endpoint)."""
    return {"tools": MCP_TOOLS, "server": SERVER_INFO}


@router.get("/health")
async def mcp_health():
    """MCP endpoint health check."""
    return {"status": "healthy", "server": SERVER_INFO, "protocol_version": "2024-11-05"}
```

### Step 5: Register the Router

Update `routers/__init__.py`:

```python
from .mcp import router as mcp_router

__all__ = [
    # ... existing routers
    "mcp_router",
]
```

Update `app.py`:

```python
from .routers import mcp_router, ...

app = create_app(
    routers=[
        # ... existing routers
        mcp_router,
    ]
)
```

### Step 6: Deploy the App

```bash
# Build frontend (REQUIRED before deploy)
cd your_app/ui
npm run build

# Deploy bundle files
cd ../..
databricks bundle deploy -t dev

# Trigger new deployment to pick up changes
databricks apps deploy <your-app-name> \
  --source-code-path "/Workspace/Users/<your-email>/.bundle/<app-name>/dev/files"

# Verify app is running
databricks apps get <your-app-name>
```

---

## Part 2: Administrator Configuration

### Step 1: Get App Service Principal Info

```bash
databricks apps get <your-app-name> --output json
```

Note these values:
- `service_principal_client_id` - Used as client_id in connection
- `service_principal_id` - Numeric ID for creating secrets
- `url` - App URL for the connection host

### Step 2: Create OAuth Secret

```bash
# Create OAuth secret for the app's service principal
databricks api post /api/2.0/accounts/servicePrincipals/<SERVICE_PRINCIPAL_ID>/credentials/secrets \
  --json '{}'
```

**IMPORTANT**: Save the returned `secret` value securely - it cannot be retrieved again.

### Step 3: Create Unity Catalog HTTP Connection

Run this SQL in Databricks SQL Editor:

```sql
CREATE OR REPLACE CONNECTION <app_name>_mcp TYPE HTTP
OPTIONS (
  host 'https://<app-url>.aws.databricksapps.com',
  port '443',
  base_path '/api/mcp',
  client_id '<SERVICE_PRINCIPAL_CLIENT_ID>',
  client_secret '<OAUTH_SECRET>',
  oauth_scope 'all-apis',
  token_endpoint 'https://<workspace>.cloud.databricks.com/oidc/v1/token',
  is_mcp_connection 'true'
);

-- Grant access to users (optional)
GRANT USE CONNECTION ON <app_name>_mcp TO `user@company.com`;
```

### Step 4: Test the Connection with SQL

```sql
-- Test tools/list
SELECT http_request(
  conn => '<app_name>_mcp',
  method => 'POST',
  path => '',
  json => '{"jsonrpc":"2.0","method":"tools/list","id":1}'
);

-- Test initialize
SELECT http_request(
  conn => '<app_name>_mcp',
  method => 'POST',
  path => '',
  json => '{"jsonrpc":"2.0","method":"initialize","id":2}'
);

-- Test a specific tool
SELECT http_request(
  conn => '<app_name>_mcp',
  method => 'POST',
  path => '',
  json => '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"list_items","arguments":{"limit":5}},"id":3}'
);
```

### Step 5: Test with AI Playground

1. Navigate to **AI/ML** > **Playground** in Databricks workspace
2. Click **Tools** button in toolbar
3. Click **MCP Servers** tab
4. Under **External MCP Servers** > **Unity Catalog Connection**, select your connection
5. Click **Save**
6. Verify tools appear in the Tools panel
7. Test with natural language queries like:
   - "Show me all items"
   - "Get details for item abc-123"
   - "What items are active?"

### Step 6: Create Supervisor Agent (Optional)

```python
manage_mas(
    action="create_or_update",
    name="Your Assistant",
    agents=[
        {
            "name": "your_app",
            "connection_name": "<app_name>_mcp",
            "description": "Detailed description of capabilities..."
        }
    ],
    description="Assistant for your domain",
    instructions="Routing instructions..."
)
```

---

## Tool Description Best Practices

### The 4-Section Format

1. **HEADER**: `ACTION VERB + PURPOSE` (e.g., "LIST AND SEARCH ITEMS")
2. **RETURNS**: Specific fields returned - helps AI understand output
3. **USE THIS TOOL WHEN**: Example user requests (include 10+ variations)
4. **DO NOT USE FOR**: Clarify boundaries with other tools
5. **TIP** (optional): Usage hints

### Bad vs Good Descriptions

```python
# BAD - Too vague, AI won't know when to use it
{"name": "list_items", "description": "Lists items"}

# GOOD - Specific, AI can route correctly
{"name": "list_items", "description": (
    "LIST AND SEARCH ITEMS - Get an overview of all items. "
    "\n\n"
    "RETURNS: Array with: item_id, name, status (ACTIVE/PENDING), owner_email. "
    "\n\n"
    "USE THIS TOOL WHEN USER ASKS: "
    "'Show me all items', 'What items are active?', 'List pending items', "
    "'How many items?', 'Find items owned by X'. "
    "\n\n"
    "DO NOT USE FOR: Getting ONE item's details (use get_item). "
)}
```

---

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| 401 Unauthorized | Invalid OAuth | Verify client_id and client_secret |
| Connection refused | App not running | `databricks apps start <app>` |
| Empty response from curl | Auth required | Test via browser or SQL instead |
| MCP server not in Playground | Missing is_mcp_connection | Verify `is_mcp_connection = 'true'` in SQL |
| No tools listed | App not deployed | Run `databricks apps deploy` after bundle deploy |
| Wrong tool selected | Vague descriptions | Improve tool descriptions with 4-section format |
| Tool execution error | Check app logs | `databricks apps get <app>` |

---

## Common Debugging Issues

### SDK Object Attributes

Don't assume Databricks SDK objects have intuitive attributes. Always check the actual structure:

```python
# WRONG - TerminationReason has no .message attribute
termination_reason=cluster.termination_reason.message

# CORRECT - TerminationReason has code, type, parameters
def _format_termination_reason(reason) -> str | None:
    """Format TerminationReason object to a readable string."""
    if reason is None:
        return None
    parts = []
    if hasattr(reason, 'code') and reason.code:
        parts.append(str(reason.code.value) if hasattr(reason.code, 'value') else str(reason.code))
    if hasattr(reason, 'type') and reason.type:
        parts.append(str(reason.type.value) if hasattr(reason.type, 'value') else str(reason.type))
    if hasattr(reason, 'parameters') and reason.parameters:
        for key, value in reason.parameters.items():
            parts.append(f"{key}={value}")
    return " - ".join(parts) if parts else None
```

### SDK Methods Return Generators

Many Databricks SDK methods return generators (iterators), not objects with attributes:

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

### Back Button Navigation (React/TanStack Router)

Don't hardcode back button destinations - use browser history for proper navigation:

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

---

## Reference Implementation

See the Cluster Manager project for a complete example:
- **MCP Router**: `cluster_manager/backend/routers/mcp.py`
- **Tool Definitions**: 7 tools with comprehensive descriptions
- **Documentation**: `docs/MCP_SERVER.md`

## References

- [Model Context Protocol](https://modelcontextprotocol.io)
- [Databricks Supervisor Agents](https://docs.databricks.com/en/generative-ai/agent-framework/supervisor-agents.html)
- [Unity Catalog Connections](https://docs.databricks.com/en/connect/unity-catalog/index.html)
- [Databricks Apps](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html)

## Related Skills

- **[databricks-app-apx](../databricks-app-apx/SKILL.md)** - Build APX apps from scratch
- **[databricks-agent-bricks](../databricks-agent-bricks/SKILL.md)** - Create Supervisor Agents
