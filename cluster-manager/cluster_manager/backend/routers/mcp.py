"""MCP (Model Context Protocol) JSON-RPC 2.0 endpoint.

This module exposes cluster management operations as MCP tools that can be
consumed by Databricks AI agents (Supervisor Agents) via Unity Catalog
HTTP Connections.

MCP Protocol Reference: https://modelcontextprotocol.io
"""

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


# --- MCP Tool Definitions ---

# These tools expose cluster management operations to AI agents.
# IMPORTANT: Descriptions are critical for AI routing - be specific about:
# - What the tool does and returns
# - When to use it vs other tools
# - Example user requests that should trigger this tool

MCP_TOOLS = [
    {
        "name": "list_clusters",
        "description": (
            "LIST AND SEARCH CLUSTERS - Get an overview of all Databricks clusters in the workspace. "
            "\n\n"
            "RETURNS: Array of clusters with: cluster_id, cluster_name, state (RUNNING/TERMINATED/PENDING/ERROR), "
            "creator_user_name, node_type_id, num_workers (or autoscale min/max), spark_version, "
            "uptime_minutes (for running clusters), estimated_dbu_per_hour, policy_id. "
            "\n\n"
            "USE THIS TOOL WHEN USER ASKS: "
            "'Show me all clusters', 'What clusters are running?', 'List terminated clusters', "
            "'How many clusters do we have?', 'Which clusters are using the most DBUs?', "
            "'Find clusters owned by X', 'Show idle clusters', 'What's the cluster status?', "
            "'Are there any clusters in error state?', 'Show me clusters with high uptime'. "
            "\n\n"
            "DO NOT USE FOR: Getting detailed config of ONE specific cluster (use get_cluster instead), "
            "or for taking actions like start/stop (use start_cluster/stop_cluster). "
            "\n\n"
            "TIP: Filter by state='RUNNING' to find active clusters, or state='TERMINATED' for stopped ones. "
            "The uptime_minutes field helps identify long-running clusters that may need attention."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "state": {
                    "type": "string",
                    "description": (
                        "Filter clusters by state. RUNNING=active clusters, TERMINATED=stopped clusters, "
                        "PENDING=starting up, ERROR=failed clusters. Omit to get all clusters."
                    ),
                    "enum": ["RUNNING", "TERMINATED", "PENDING", "RESTARTING", "RESIZING", "TERMINATING", "ERROR"],
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum clusters to return. Use smaller limits (10-20) for quick overviews, larger (100+) for full inventory.",
                    "default": 100,
                    "minimum": 1,
                    "maximum": 500,
                },
            },
        },
    },
    {
        "name": "get_cluster",
        "description": (
            "GET DETAILED CLUSTER CONFIGURATION - Retrieve complete information about ONE specific cluster. "
            "\n\n"
            "RETURNS: Full cluster details including: cluster_id, cluster_name, state, state_message, "
            "all Spark configuration (spark_conf), environment variables (spark_env_vars), "
            "init scripts, custom tags, cloud-specific attributes (AWS/Azure/GCP), "
            "termination_reason (why it stopped), security settings (data_security_mode, single_user_name), "
            "disk configuration, and policy_id. "
            "\n\n"
            "USE THIS TOOL WHEN USER ASKS: "
            "'Show me details for cluster X', 'What's the configuration of cluster Y?', "
            "'Why did cluster Z terminate?', 'What Spark version is cluster X using?', "
            "'Show me the Spark config for this cluster', 'What tags are on cluster X?', "
            "'Is cluster X using spot instances?', 'What's the termination reason?', "
            "'Show me the init scripts for cluster X', 'What security mode is this cluster using?'. "
            "\n\n"
            "DO NOT USE FOR: Listing multiple clusters (use list_clusters), "
            "or taking actions (use start_cluster/stop_cluster). "
            "\n\n"
            "REQUIRES: cluster_id - get this from list_clusters first if you don't have it."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "cluster_id": {
                    "type": "string",
                    "description": "The unique cluster ID (format: '0123-456789-abcdef12'). Get this from list_clusters if unknown.",
                },
            },
            "required": ["cluster_id"],
        },
    },
    {
        "name": "start_cluster",
        "description": (
            "START A STOPPED CLUSTER - Boot up a terminated cluster to make it available for use. "
            "\n\n"
            "RETURNS: Success/failure status with message. On success, cluster begins transitioning "
            "from TERMINATED -> PENDING -> RUNNING 'state. This typically takes 2-5 minutes. "
            "\n\n"
            "USE THIS TOOL WHEN USER ASKS: "
            "'Start cluster X', 'Boot up the data team cluster', 'Turn on cluster Y', "
            "'Spin up the ML cluster', 'I need cluster X running', 'Bring cluster X online', "
            "'Wake up cluster X', 'Resume cluster X'. "
            "\n\n"
            "PRECONDITIONS: Cluster must be in TERMINATED or ERROR state. "
            "If cluster is already RUNNING, the tool returns success with 'already running' message. "
            "If cluster is in PENDING/RESTARTING state, the tool will fail - wait and retry. "
            "\n\n"
            "DO NOT USE FOR: Stopping clusters (use stop_cluster), getting info (use get_cluster/list_clusters). "
            "\n\n"
            "IMPORTANT: Always confirm with user before starting - this incurs compute costs. "
            "After starting, use list_clusters with state='RUNNING' to verify it came up."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "cluster_id": {
                    "type": "string",
                    "description": "The cluster ID to start. Get from list_clusters if unknown.",
                },
            },
            "required": ["cluster_id"],
        },
    },
    {
        "name": "stop_cluster",
        "description": (
            "STOP A RUNNING CLUSTER - Safely shut down a cluster to save costs. Configuration is preserved. "
            "\n\n"
            "RETURNS: Success/failure status with message. On success, cluster transitions "
            "from RUNNING -> TERMINATING -> TERMINATED. This typically takes 1-2 minutes. "
            "\n\n"
            "USE THIS TOOL WHEN USER ASKS: "
            "'Stop cluster X', 'Shut down the dev cluster', 'Turn off cluster Y', "
            "'Terminate cluster X', 'Kill cluster X', 'Take cluster X offline', "
            "'Stop all idle clusters', 'Shut down clusters not in use'. "
            "\n\n"
            "SAFE OPERATION: This is NOT destructive - the cluster configuration is preserved "
            "and can be started again later with start_cluster. The cluster definition remains "
            "in the workspace. "
            "\n\n"
            "PRECONDITIONS: Cluster should be in RUNNING, PENDING, or RESIZING state. "
            "If already TERMINATED, returns success with 'already stopped' message. "
            "\n\n"
            "WARNING: Any running jobs on the cluster will be interrupted! "
            "Always confirm with user before stopping, especially during business hours. "
            "\n\n"
            "DO NOT USE FOR: Starting clusters (use start_cluster), getting info (use get_cluster)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "cluster_id": {
                    "type": "string",
                    "description": "The cluster ID to stop. Get from list_clusters if unknown.",
                },
            },
            "required": ["cluster_id"],
        },
    },
    {
        "name": "get_cluster_events",
        "description": (
            "GET CLUSTER EVENT HISTORY - View recent lifecycle events and state changes for a cluster. "
            "\n\n"
            "RETURNS: Array of events with: timestamp, event_type (STARTING, RUNNING, TERMINATING, etc.), "
            "and details about each event (resize info, error messages, user actions). "
            "\n\n"
            "USE THIS TOOL WHEN USER ASKS: "
            "'What happened to cluster X?', 'Show me cluster X event history', "
            "'Why did cluster X fail?', 'When was cluster X last started?', "
            "'Show me the cluster activity log', 'Debug cluster X issues', "
            "'What caused cluster X to terminate?', 'Show recent cluster X events', "
            "'Has anyone restarted cluster X recently?', 'Cluster X timeline'. "
            "\n\n"
            "USEFUL FOR: Debugging cluster problems, understanding why a cluster terminated unexpectedly, "
            "auditing who started/stopped a cluster, tracking resize operations, "
            "investigating error states, understanding cluster lifecycle. "
            "\n\n"
            "DO NOT USE FOR: Current cluster state (use get_cluster), listing clusters (use list_clusters), "
            "or taking actions (use start_cluster/stop_cluster). "
            "\n\n"
            "TIP: Start with limit=20 for recent events, increase to 50-100 for deeper history."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "cluster_id": {
                    "type": "string",
                    "description": "The cluster ID to get events for.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of events to return. Use 10-20 for quick look, 50+ for detailed investigation.",
                    "default": 50,
                    "minimum": 1,
                    "maximum": 100,
                },
            },
            "required": ["cluster_id"],
        },
    },
    {
        "name": "list_policies",
        "description": (
            "LIST CLUSTER POLICIES - View all cluster policies that govern how clusters can be created. "
            "\n\n"
            "RETURNS: Array of policies with: policy_id, name, description, definition (JSON rules), "
            "creator_user_name, created_at_timestamp, is_default flag. "
            "\n\n"
            "USE THIS TOOL WHEN USER ASKS: "
            "'What cluster policies do we have?', 'Show me all policies', "
            "'List available cluster policies', 'What policies can I use?', "
            "'Find the policy for data team', 'Show me job cluster policies', "
            "'What are our cluster governance rules?', 'Policy inventory'. "
            "\n\n"
            "CLUSTER POLICIES DEFINE: Which instance types are allowed, min/max workers, "
            "auto-termination settings, Spark versions, required tags, and other constraints. "
            "They help enforce cost controls and compliance. "
            "\n\n"
            "DO NOT USE FOR: Getting full details of one policy (use get_policy with policy_id), "
            "or anything related to clusters themselves (use cluster tools). "
            "\n\n"
            "TIP: After listing, use get_policy to see the full definition/constraints of a specific policy."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_policy",
        "description": (
            "GET POLICY DETAILS - Retrieve the complete definition and constraints of ONE cluster policy. "
            "\n\n"
            "RETURNS: Full policy including: policy_id, name, description, definition_json (parsed rules), "
            "max_clusters_per_user, creator, policy_family info. The definition_json contains "
            "all constraints like allowed instance types, worker limits, required tags, etc. "
            "\n\n"
            "USE THIS TOOL WHEN USER ASKS: "
            "'What does policy X allow?', 'Show me the rules for policy Y', "
            "'What instance types can I use with policy X?', 'What are the limits in policy Y?', "
            "'Explain policy X constraints', 'What tags are required by policy X?', "
            "'Can I use spot instances with policy Y?', 'What's the max workers for policy X?'. "
            "\n\n"
            "USEFUL FOR: Understanding what a policy allows/restricts before creating a cluster, "
            "troubleshooting why cluster creation failed, auditing governance rules, "
            "comparing policies. "
            "\n\n"
            "DO NOT USE FOR: Listing all policies (use list_policies first to find policy_id). "
            "\n\n"
            "REQUIRES: policy_id - get this from list_policies or from a cluster's policy_id field."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "policy_id": {
                    "type": "string",
                    "description": "The policy ID to retrieve. Get from list_policies or from a cluster's policy_id.",
                },
            },
            "required": ["policy_id"],
        },
    },
]

# Server metadata
SERVER_INFO = {
    "name": "cluster-manager-mcp",
    "version": "1.0.0",
    "description": "Databricks Cluster Manager MCP Server - manage clusters via AI agents",
}


# --- Tool Execution ---


async def execute_tool(
    tool_name: str,
    arguments: dict[str, Any],
    ws: Dependency.Client,
) -> dict[str, Any]:
    """Execute an MCP tool and return the result.

    Args:
        tool_name: Name of the tool to execute
        arguments: Tool arguments
        ws: Databricks WorkspaceClient

    Returns:
        Tool execution result as a dictionary
    """
    logger.info(f"MCP executing tool: {tool_name} with args: {arguments}")

    try:
        if tool_name == "list_clusters":
            return await _list_clusters(ws, arguments)
        elif tool_name == "get_cluster":
            return await _get_cluster(ws, arguments)
        elif tool_name == "start_cluster":
            return await _start_cluster(ws, arguments)
        elif tool_name == "stop_cluster":
            return await _stop_cluster(ws, arguments)
        elif tool_name == "get_cluster_events":
            return await _get_cluster_events(ws, arguments)
        elif tool_name == "list_policies":
            return await _list_policies(ws, arguments)
        elif tool_name == "get_policy":
            return await _get_policy(ws, arguments)
        else:
            raise ValueError(f"Unknown tool: {tool_name}")
    except Exception as e:
        logger.error(f"MCP tool execution failed: {tool_name} - {e}")
        raise


async def _list_clusters(ws, args: dict) -> dict:
    """List clusters with optional state filter."""
    from ..models import ClusterState
    from .clusters import list_clusters

    state_str = args.get("state")
    state = ClusterState(state_str) if state_str else None
    limit = args.get("limit", 100)

    clusters = list_clusters(ws, state, limit)
    return {
        "clusters": [c.model_dump(mode="json") for c in clusters],
        "count": len(clusters),
    }


async def _get_cluster(ws, args: dict) -> dict:
    """Get cluster details."""
    from .clusters import get_cluster

    cluster_id = args["cluster_id"]
    cluster = get_cluster(cluster_id, ws)
    return cluster.model_dump(mode="json")


async def _start_cluster(ws, args: dict) -> dict:
    """Start a cluster."""
    from .clusters import start_cluster

    cluster_id = args["cluster_id"]
    result = start_cluster(cluster_id, ws)
    return result.model_dump(mode="json")


async def _stop_cluster(ws, args: dict) -> dict:
    """Stop a cluster."""
    from .clusters import stop_cluster

    cluster_id = args["cluster_id"]
    result = stop_cluster(cluster_id, ws)
    return result.model_dump(mode="json")


async def _get_cluster_events(ws, args: dict) -> dict:
    """Get cluster events."""
    from .clusters import get_cluster_events

    cluster_id = args["cluster_id"]
    limit = args.get("limit", 50)
    result = get_cluster_events(cluster_id, ws, limit)
    return result.model_dump(mode="json")


async def _list_policies(ws, args: dict) -> dict:
    """List cluster policies."""
    from .policies import list_policies

    policies = list_policies(ws)
    return {
        "policies": [p.model_dump(mode="json") for p in policies],
        "count": len(policies),
    }


async def _get_policy(ws, args: dict) -> dict:
    """Get policy details."""
    from .policies import get_policy

    policy_id = args["policy_id"]
    policy = get_policy(policy_id, ws)
    return policy.model_dump(mode="json")


# --- MCP Protocol Handlers ---


def _handle_initialize(request: JsonRpcRequest) -> JsonRpcResponse:
    """Handle MCP initialize method."""
    return JsonRpcResponse(
        id=request.id,
        result={
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {},
            },
            "serverInfo": SERVER_INFO,
        },
    )


def _handle_tools_list(request: JsonRpcRequest) -> JsonRpcResponse:
    """Handle MCP tools/list method."""
    return JsonRpcResponse(
        id=request.id,
        result={"tools": MCP_TOOLS},
    )


async def _handle_tools_call(
    request: JsonRpcRequest,
    ws: Dependency.Client,
) -> JsonRpcResponse:
    """Handle MCP tools/call method."""
    params = request.params or {}
    tool_name = params.get("name")
    arguments = params.get("arguments", {})

    if not tool_name:
        return JsonRpcResponse(
            id=request.id,
            error=JsonRpcError(
                code=-32602,
                message="Invalid params: 'name' is required",
            ),
        )

    # Check if tool exists
    tool_names = [t["name"] for t in MCP_TOOLS]
    if tool_name not in tool_names:
        return JsonRpcResponse(
            id=request.id,
            error=JsonRpcError(
                code=-32602,
                message=f"Unknown tool: {tool_name}. Available tools: {tool_names}",
            ),
        )

    try:
        result = await execute_tool(tool_name, arguments, ws)

        # Format result as MCP content
        return JsonRpcResponse(
            id=request.id,
            result={
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(result, indent=2, default=str),
                    }
                ],
            },
        )
    except HTTPException as e:
        return JsonRpcResponse(
            id=request.id,
            error=JsonRpcError(
                code=-32000,
                message=f"Tool execution failed: {e.detail}",
            ),
        )
    except Exception as e:
        logger.exception(f"MCP tool call failed: {tool_name}")
        return JsonRpcResponse(
            id=request.id,
            error=JsonRpcError(
                code=-32000,
                message=f"Tool execution failed: {str(e)}",
            ),
        )


# --- FastAPI Endpoint ---


@router.post("", response_model=JsonRpcResponse)
async def mcp_handler(
    request: JsonRpcRequest,
    ws: Dependency.Client,
) -> JsonRpcResponse:
    """MCP JSON-RPC 2.0 endpoint.

    This endpoint implements the Model Context Protocol for AI agent integration.
    It supports the following methods:
    - initialize: Initialize the MCP connection
    - tools/list: List available tools
    - tools/call: Execute a tool

    To use this endpoint from Databricks:
    1. Create a Unity Catalog HTTP Connection with is_mcp_connection='true'
    2. Reference the connection in a Supervisor Agent configuration
    """
    logger.info(f"MCP request: method={request.method}, id={request.id}")

    if request.jsonrpc != "2.0":
        return JsonRpcResponse(
            id=request.id,
            error=JsonRpcError(
                code=-32600,
                message=f"Invalid JSON-RPC version: {request.jsonrpc}. Expected '2.0'",
            ),
        )

    # Route to appropriate handler
    if request.method == "initialize":
        return _handle_initialize(request)

    elif request.method == "tools/list":
        return _handle_tools_list(request)

    elif request.method == "tools/call":
        return await _handle_tools_call(request, ws)

    else:
        return JsonRpcResponse(
            id=request.id,
            error=JsonRpcError(
                code=-32601,
                message=f"Method not found: {request.method}",
            ),
        )


@router.get("/tools", response_model=dict)
async def list_tools() -> dict:
    """List available MCP tools (convenience endpoint for debugging).

    This is a REST endpoint for easy tool discovery. The actual MCP protocol
    uses the POST endpoint with tools/list method.
    """
    return {
        "tools": MCP_TOOLS,
        "server": SERVER_INFO,
    }


@router.get("/health", response_model=dict)
async def mcp_health() -> dict:
    """MCP endpoint health check."""
    return {
        "status": "healthy",
        "server": SERVER_INFO,
        "protocol_version": "2024-11-05",
    }
