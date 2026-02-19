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

### Step 4: Test with AI Playground (Interactive)

The fastest way to validate your MCP tools work correctly is through the Databricks AI Playground. This lets you test tool selection and execution interactively before creating a Supervisor Agent.

#### 4.1: Navigate to AI Playground

1. Open your Databricks workspace in a browser
2. In the left sidebar, click **Machine Learning**
3. Click **AI Playground** from the submenu
4. Wait for the Playground interface to load

#### 4.2: Add the MCP Server

1. Look at the **right sidebar** of the Playground
2. Click the **MCP Servers** tab (icon looks like a plug/connection)
3. Click the **+ Add MCP Server** button
4. From the dropdown, select **Unity Catalog Connection**
5. In the connection picker:
   - Browse or search for `cluster_manager_mcp`
   - Select it from the list
6. Click **Add**
7. **Verify**: The MCP server should now appear in the sidebar with a green status indicator

#### 4.3: Verify Tools Are Loaded

1. Click on `cluster_manager_mcp` in the sidebar to expand it
2. You should see a list of available tools:
   - `list_clusters`
   - `get_cluster`
   - `start_cluster`
   - `stop_cluster`
   - `get_cluster_events`
   - `list_policies`
   - `get_policy`

3. **If no tools appear**:
   - Check that the app is running: `databricks apps get cluster-manager`
   - Verify the connection is correctly configured
   - Try removing and re-adding the MCP server

#### 4.4: Test Tool Selection with Natural Language

Type a natural language question in the chat input that should trigger one of the tools:

**Example questions to test:**
```
"What clusters are currently running in the workspace?"
"Show me all terminated clusters"
"How many clusters do we have?"
"Get details for cluster 0123-456789-abcdef"
"Show me clusters owned by alice@company.com"
"What policies are available?"
"Which clusters have been running for more than 24 hours?"
```

#### 4.5: Verify the AI's Tool Selection

After submitting your question, observe the AI's response:

1. **Check tool selection**: The AI should display which tool it chose to call
   - Look for text like "Calling list_clusters..." or a tool indicator

2. **Check arguments**: The AI should populate correct arguments
   - For "running clusters" → `state: "RUNNING"`
   - For "owned by alice" → filter in results

3. **Check results**: The response should contain actual data
   - Clusters should have real names, IDs, and states
   - Data should match what you see in the Databricks Clusters UI

#### 4.6: Example Successful Interaction

Here's what a successful test looks like:

```
YOU: "What clusters are currently running in the workspace?"

AI RESPONSE:
┌─────────────────────────────────────────────────────────────┐
│ Using tool: list_clusters                                   │
│ Arguments: {"state": "RUNNING"}                             │
└─────────────────────────────────────────────────────────────┘

There are 21 clusters currently running in the workspace:

| Cluster Name       | Owner              | Uptime   | Node Type  |
|--------------------|--------------------|----------|------------|
| analytics-prod     | alice@company.com  | 4h 32m   | i3.xlarge  |
| ml-training-01     | bob@company.com    | 12h 15m  | p3.2xlarge |
| data-pipeline      | etl@company.com    | 2h 45m   | r5.4xlarge |
| ...                | ...                | ...      | ...        |

The clusters are sorted by uptime. The ml-training-01 cluster has been
running the longest at 12 hours and 15 minutes.
```

#### 4.7: Test Each Tool

Test all MCP tools to ensure they work correctly:

| Question | Expected Tool | Expected Arguments |
|----------|---------------|-------------------|
| "Show me all clusters" | `list_clusters` | `{}` |
| "Show running clusters" | `list_clusters` | `{"state": "RUNNING"}` |
| "Get details for cluster abc-123" | `get_cluster` | `{"cluster_id": "abc-123"}` |
| "What happened to cluster abc-123?" | `get_cluster_events` | `{"cluster_id": "abc-123"}` |
| "Start the dev-cluster" | `start_cluster` | `{"cluster_id": "..."}` |
| "Stop cluster abc-123" | `stop_cluster` | `{"cluster_id": "abc-123"}` |
| "List all policies" | `list_policies` | `{}` |
| "Show policy xyz-456" | `get_policy` | `{"policy_id": "xyz-456"}` |

#### 4.8: Troubleshooting in AI Playground

| Issue | Cause | Solution |
|-------|-------|----------|
| MCP Server doesn't appear | Connection not added | Click + Add MCP Server → Unity Catalog Connection |
| No tools listed | App not running | Run `databricks apps start cluster-manager` |
| "Tool not found" error | Tool name mismatch | Verify tool names in app match expected names |
| Wrong tool selected | Ambiguous request | Be more specific in your question |
| "Authentication failed" | OAuth secret expired | Create new secret, update UC connection |
| "Connection refused" | App crashed | Check logs: `databricks apps get cluster-manager` |
| Empty results | Tool returned no data | Check app logs for errors |
| Timeout | App too slow | Check app performance |

#### Validation Checklist

Complete this checklist to confirm MCP tools are working:

```
- [ ] AI Playground is accessible
- [ ] MCP Server added successfully (shows green status)
- [ ] All 7 tools appear in the sidebar
- [ ] list_clusters returns expected data
- [ ] Filtered query (state=RUNNING) applies correctly
- [ ] get_cluster works with valid cluster_id
- [ ] get_cluster_events returns event history
- [ ] list_policies returns available policies
- [ ] get_policy returns policy details
- [ ] Error handling works for invalid IDs
```

---

### Step 5: Create a Supervisor Agent (Optional)

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
