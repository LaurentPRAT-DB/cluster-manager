# Cluster Manager MCP Server

This document explains how to use the Cluster Manager as a Databricks managed MCP (Model Context Protocol) server, enabling AI agents in the Databricks Playground to manage clusters.

## Overview

The Cluster Manager exposes a JSON-RPC 2.0 endpoint at `/api/mcp` that implements the Model Context Protocol. This allows Databricks AI agents (Supervisor Agents) to:

- List clusters in the workspace
- Get detailed cluster information
- Start and stop clusters
- View cluster events
- List and inspect cluster policies

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Databricks AI Playground                      │
│                    (Supervisor Agent / MAS)                      │
└─────────────────────────────────────┬───────────────────────────┘
                                      │ JSON-RPC 2.0
                                      ▼
┌─────────────────────────────────────────────────────────────────┐
│               Unity Catalog HTTP Connection                      │
│         cluster_manager_mcp (is_mcp_connection: 'true')         │
└─────────────────────────────────────┬───────────────────────────┘
                                      │ OAuth M2M
                                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Cluster Manager App                          │
│                    /api/mcp endpoint                             │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  MCP Tools: list_clusters, get_cluster, start_cluster,      ││
│  │             stop_cluster, get_cluster_events,               ││
│  │             list_policies, get_policy                       ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

## Setup

### Prerequisites

1. Cluster Manager app deployed and running
2. Service Principal with cluster management permissions
3. Unity Catalog enabled workspace

### Step 1: Create Unity Catalog HTTP Connection

Run this SQL in your Databricks workspace to create the MCP connection:

```sql
-- Create the MCP connection
CREATE CONNECTION cluster_manager_mcp TYPE HTTP
OPTIONS (
  host 'https://cluster-manager-1444828305810485.aws.databricksapps.com',
  port '443',
  base_path '/api/mcp',
  -- OAuth M2M authentication using service principal
  client_id '<YOUR_SERVICE_PRINCIPAL_APPLICATION_ID>',
  client_secret '<YOUR_SERVICE_PRINCIPAL_SECRET>',
  oauth_scope 'all-apis',
  token_endpoint 'https://e2-demo-field-eng.cloud.databricks.com/oidc/v1/token',
  -- CRITICAL: This marks the connection as an MCP server
  is_mcp_connection 'true'
);
```

Replace:
- `<YOUR_SERVICE_PRINCIPAL_APPLICATION_ID>` - The Application (client) ID of your service principal
- `<YOUR_SERVICE_PRINCIPAL_SECRET>` - A secret for the service principal

### Step 2: Grant Permissions

Grant the agent service principal access to use the connection:

```sql
-- Grant access to the connection
GRANT USE CONNECTION ON cluster_manager_mcp TO `<AGENT_SERVICE_PRINCIPAL>`;
```

### Step 3: Test the Connection

Verify the MCP connection works:

```sql
-- Test tools/list method
SELECT http_request(
  conn => 'cluster_manager_mcp',
  method => 'POST',
  path => '',
  json => '{"jsonrpc":"2.0","method":"tools/list","id":1}'
);

-- Test initialize method
SELECT http_request(
  conn => 'cluster_manager_mcp',
  method => 'POST',
  path => '',
  json => '{"jsonrpc":"2.0","method":"initialize","id":2}'
);
```

### Step 4: Create a Supervisor Agent

Create a Supervisor Agent that uses the MCP connection:

```python
# Using the manage_mas MCP tool
manage_mas(
    action="create_or_update",
    name="Infrastructure Assistant",
    agents=[
        {
            "name": "cluster_manager",
            "connection_name": "cluster_manager_mcp",
            "description": (
                "Manage Databricks clusters: list all clusters, get cluster details, "
                "start stopped clusters, stop running clusters, view cluster events, "
                "list cluster policies, and get policy details. "
                "Use for any cluster-related operations or questions."
            )
        }
    ],
    description="AI assistant for managing Databricks compute infrastructure",
    instructions="""
    You are an infrastructure assistant that helps users manage Databricks clusters.

    When users ask about clusters, use the cluster_manager agent to:
    - List clusters: "list_clusters" tool
    - Get cluster details: "get_cluster" tool with cluster_id
    - Start a cluster: "start_cluster" tool with cluster_id
    - Stop a cluster: "stop_cluster" tool with cluster_id
    - View cluster history: "get_cluster_events" tool with cluster_id
    - List policies: "list_policies" tool
    - Get policy details: "get_policy" tool with policy_id

    Always confirm destructive actions (start/stop) with the user before executing.
    """
)
```

## Available MCP Tools

| Tool | Description | Required Parameters |
|------|-------------|---------------------|
| `list_clusters` | List all clusters with state and metrics | `state` (optional), `limit` (optional) |
| `get_cluster` | Get detailed cluster information | `cluster_id` |
| `start_cluster` | Start a terminated cluster | `cluster_id` |
| `stop_cluster` | Stop a running cluster (preserves config) | `cluster_id` |
| `get_cluster_events` | Get cluster event history | `cluster_id`, `limit` (optional) |
| `list_policies` | List all cluster policies | None |
| `get_policy` | Get policy details and definition | `policy_id` |

## REST API Endpoints

The MCP endpoint also provides convenience REST endpoints for debugging:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/mcp` | POST | Main MCP JSON-RPC 2.0 endpoint |
| `/api/mcp/tools` | GET | List available tools (debugging) |
| `/api/mcp/health` | GET | Health check |

## Example Conversations

Once set up, users can interact with the Supervisor Agent:

**User**: "Show me all running clusters"
**Agent**: *Calls list_clusters with state="RUNNING"*
**Response**: "Here are the 3 running clusters: cluster-1 (2 workers), cluster-2 (4 workers)..."

**User**: "Stop cluster-1, it's been idle for 2 hours"
**Agent**: "I'll stop cluster-1 for you. Are you sure?"
**User**: "Yes"
**Agent**: *Calls stop_cluster with cluster_id*
**Response**: "Cluster-1 is now stopping. It will transition to TERMINATED state."

## Troubleshooting

### Connection Test Fails

1. Verify the app is running: Check the app URL in a browser
2. Check service principal permissions
3. Verify the token endpoint URL matches your workspace

### Tool Execution Errors

1. Check app logs: `databricks apps logs cluster-manager`
2. Verify the cluster_id or policy_id is correct
3. Check service principal has cluster management permissions

### Agent Not Routing to MCP

1. Improve the agent description in the Supervisor Agent config
2. Make descriptions specific about what the agent can do
3. Add example questions in the Supervisor Agent

## Security Considerations

1. **Service Principal Permissions**: The MCP server uses the app's service principal, which should have minimal required permissions
2. **Safe Mode**: The app only exposes Start/Stop operations, not Terminate (which permanently deletes)
3. **Audit Trail**: All operations are logged in the app and Databricks audit logs

## References

- [Model Context Protocol](https://modelcontextprotocol.io)
- [Databricks Supervisor Agents](https://docs.databricks.com/en/generative-ai/agent-framework/supervisor-agents.html)
- [Unity Catalog Connections](https://docs.databricks.com/en/connect/unity-catalog/index.html)
