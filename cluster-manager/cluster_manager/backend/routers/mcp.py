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

# These tools expose cluster management operations to AI agents
MCP_TOOLS = [
    {
        "name": "list_clusters",
        "description": (
            "List all clusters in the Databricks workspace. Returns cluster ID, name, "
            "state (RUNNING, TERMINATED, PENDING, etc.), creator, node types, worker count, "
            "Spark version, uptime in minutes, and estimated DBU per hour. "
            "Use this to get an overview of all clusters or find specific clusters by state."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "state": {
                    "type": "string",
                    "description": "Optional filter by cluster state",
                    "enum": ["RUNNING", "TERMINATED", "PENDING", "RESTARTING", "RESIZING", "TERMINATING", "ERROR"],
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of clusters to return (default: 100, max: 500)",
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
            "Get detailed information about a specific cluster by ID. Returns full configuration "
            "including Spark settings, environment variables, init scripts, tags, cloud attributes, "
            "termination reason (if terminated), and security settings. "
            "Use this when you need complete details about a single cluster."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "cluster_id": {
                    "type": "string",
                    "description": "The unique cluster ID (e.g., '0123-456789-abcdef12')",
                },
            },
            "required": ["cluster_id"],
        },
    },
    {
        "name": "start_cluster",
        "description": (
            "Start a terminated or stopped cluster. The cluster will transition through PENDING "
            "state before becoming RUNNING. Only works on clusters in TERMINATED or ERROR state. "
            "Use this to spin up a cluster that was previously stopped."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "cluster_id": {
                    "type": "string",
                    "description": "The unique cluster ID to start",
                },
            },
            "required": ["cluster_id"],
        },
    },
    {
        "name": "stop_cluster",
        "description": (
            "Stop a running cluster. This is a SAFE operation - the cluster configuration is "
            "preserved and can be started again later. The cluster will transition through "
            "TERMINATING state before becoming TERMINATED. Any running jobs will be interrupted. "
            "Use this to save costs by stopping idle clusters."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "cluster_id": {
                    "type": "string",
                    "description": "The unique cluster ID to stop",
                },
            },
            "required": ["cluster_id"],
        },
    },
    {
        "name": "get_cluster_events",
        "description": (
            "Get recent events for a cluster. Returns event history including state changes, "
            "resize operations, errors, and other cluster lifecycle events. "
            "Use this to debug cluster issues or understand recent cluster activity."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "cluster_id": {
                    "type": "string",
                    "description": "The unique cluster ID",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of events to return (default: 50, max: 100)",
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
            "List all cluster policies in the workspace. Returns policy ID, name, description, "
            "creator, and whether the policy is used for jobs. Cluster policies define "
            "constraints and defaults for cluster creation. "
            "Use this to see available policies or find a policy by name."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_policy",
        "description": (
            "Get detailed information about a specific cluster policy. Returns the full policy "
            "definition including all constraints, defaults, and overrides. "
            "Use this to understand what a policy allows or restricts."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "policy_id": {
                    "type": "string",
                    "description": "The unique policy ID",
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
