"""
MCP (Model Context Protocol) JSON-RPC 2.0 Router Template

This template provides a complete MCP server implementation for APX apps.
Copy this file to your app's routers/ directory and customize:
1. Update MCP_TOOLS with your tool definitions
2. Update SERVER_INFO with your app details
3. Implement execute_tool() to call your existing functions

Reference: https://modelcontextprotocol.io
"""

import json
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..core import Dependency, logger

router = APIRouter(prefix="/api/mcp", tags=["mcp"])

# =============================================================================
# MCP Protocol Models (do not modify)
# =============================================================================


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


# =============================================================================
# CUSTOMIZE: Your MCP Tools
# =============================================================================

# Define your tools here. Each tool should have:
# - name: Unique identifier
# - description: CRITICAL for AI routing - use structured format below
# - inputSchema: JSON Schema for parameters
#
# DESCRIPTION FORMAT (4 sections):
# 1. HEADER: Action verb + what it does
# 2. RETURNS: Specific fields returned
# 3. USE THIS TOOL WHEN: Example user requests (10+ variations)
# 4. DO NOT USE FOR: Clarify boundaries with other tools
# 5. TIP (optional): Usage hints

MCP_TOOLS = [
    # Example tool - replace with your actual tools
    {
        "name": "list_items",
        "description": (
            "LIST AND SEARCH ITEMS - Get an overview of all items in the system. "
            "\n\n"
            "RETURNS: Array of items with: item_id, name, status (ACTIVE/PENDING/COMPLETED), "
            "created_date, owner_email, category, and last_modified timestamp. "
            "\n\n"
            "USE THIS TOOL WHEN USER ASKS: "
            "'Show me all items', 'What items are active?', 'List pending items', "
            "'How many items do we have?', 'Find items owned by X', "
            "'What's the status of our items?', 'Show items in category Y', "
            "'Which items were recently modified?', 'Give me an overview of items'. "
            "\n\n"
            "DO NOT USE FOR: Getting detailed info about ONE specific item (use get_item), "
            "or for taking actions like create/update/delete (use respective tools). "
            "\n\n"
            "TIP: Filter by status='ACTIVE' for current items. Use last_modified to find recent changes."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "description": (
                        "Filter by status. ACTIVE=current items, PENDING=awaiting action, "
                        "COMPLETED=finished items. Omit to get all items."
                    ),
                    "enum": ["ACTIVE", "PENDING", "COMPLETED"]
                },
                "limit": {
                    "type": "integer",
                    "description": "Max items to return. Use 10-20 for quick overview, 100+ for full list.",
                    "default": 100,
                    "minimum": 1,
                    "maximum": 500
                },
                "category": {
                    "type": "string",
                    "description": "Filter by category name (optional)"
                }
            }
        }
    },
    {
        "name": "get_item",
        "description": (
            "GET DETAILED ITEM INFO - Retrieve complete information about ONE specific item. "
            "\n\n"
            "RETURNS: Full item details including: item_id, name, description, status, "
            "all metadata fields, history/audit trail, related items, and configuration. "
            "\n\n"
            "USE THIS TOOL WHEN USER ASKS: "
            "'Show me details for item X', 'What's the configuration of item Y?', "
            "'Tell me about item Z', 'Get item X info', 'What's the history of item X?'. "
            "\n\n"
            "DO NOT USE FOR: Listing multiple items (use list_items), "
            "or taking actions (use update_item/delete_item). "
            "\n\n"
            "REQUIRES: item_id - get this from list_items first if you don't have it."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "item_id": {
                    "type": "string",
                    "description": "The unique item ID. Get from list_items if unknown."
                }
            },
            "required": ["item_id"]
        }
    },
    # Add your tools here...
]

# Server metadata
SERVER_INFO = {
    "name": "your-app-mcp",  # CUSTOMIZE: Your app name
    "version": "1.0.0",
    "description": "Your app description",  # CUSTOMIZE: Your app description
}


# =============================================================================
# CUSTOMIZE: Tool Execution
# =============================================================================


async def execute_tool(
    tool_name: str,
    arguments: dict[str, Any],
    ws: Dependency.Client,
) -> dict[str, Any]:
    """
    Execute an MCP tool and return the result.

    CUSTOMIZE: Add handlers for each of your tools.

    Args:
        tool_name: Name of the tool to execute
        arguments: Tool arguments from the request
        ws: Databricks WorkspaceClient

    Returns:
        Tool execution result as a dictionary

    Example:
        if tool_name == "list_items":
            from .items import list_items
            result = list_items(ws, arguments.get("status"), arguments.get("limit", 100))
            return {"items": [r.model_dump(mode="json") for r in result]}
    """
    logger.info(f"MCP executing tool: {tool_name} with args: {arguments}")

    if tool_name == "example_tool":
        # Replace with your actual implementation
        return {
            "message": "Example tool executed",
            "params_received": arguments
        }

    # Add more tool handlers here...

    raise ValueError(f"Unknown tool: {tool_name}")


# =============================================================================
# MCP Protocol Handlers (do not modify unless extending)
# =============================================================================


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

    # Validate tool exists
    tool_names = [t["name"] for t in MCP_TOOLS]
    if tool_name not in tool_names:
        return JsonRpcResponse(
            id=request.id,
            error=JsonRpcError(
                code=-32602,
                message=f"Unknown tool: {tool_name}. Available: {tool_names}",
            ),
        )

    try:
        result = await execute_tool(tool_name, arguments, ws)

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


# =============================================================================
# FastAPI Endpoints
# =============================================================================


@router.post("", response_model=JsonRpcResponse)
async def mcp_handler(
    request: JsonRpcRequest,
    ws: Dependency.Client,
) -> JsonRpcResponse:
    """
    MCP JSON-RPC 2.0 endpoint.

    Implements the Model Context Protocol for AI agent integration.
    Supports methods: initialize, tools/list, tools/call
    """
    logger.info(f"MCP request: method={request.method}, id={request.id}")

    if request.jsonrpc != "2.0":
        return JsonRpcResponse(
            id=request.id,
            error=JsonRpcError(
                code=-32600,
                message=f"Invalid JSON-RPC version: {request.jsonrpc}",
            ),
        )

    if request.method == "initialize":
        return _handle_initialize(request)

    elif request.method == "tools/list":
        return _handle_tools_list(request)

    elif request.method == "tools/call":
        return await _handle_tools_call(request, ws)

    return JsonRpcResponse(
        id=request.id,
        error=JsonRpcError(
            code=-32601,
            message=f"Method not found: {request.method}",
        ),
    )


@router.get("/tools", response_model=dict)
async def list_tools() -> dict:
    """List available MCP tools (REST endpoint for debugging)."""
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
