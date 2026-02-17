"""Workspace information API router."""

import os

from pydantic import BaseModel

from ..core import Dependency, create_router, logger

router = create_router()


class WorkspaceInfo(BaseModel):
    """Workspace information."""
    host: str
    org_id: str | None = None


@router.get("/workspace/debug")
def debug_workspace_env(ws: Dependency.Client) -> dict:
    """Debug endpoint to show workspace-related environment variables."""
    env_vars = {}
    for key, value in os.environ.items():
        if "DATABRICKS" in key or "DB_" in key or "WORKSPACE" in key:
            # Mask sensitive values
            if "TOKEN" in key or "SECRET" in key or "PASSWORD" in key:
                env_vars[key] = "***MASKED***"
            else:
                env_vars[key] = value
    return {
        "env_vars": env_vars,
        "config_host": ws.config.host,
        "config_auth_type": ws.config.auth_type,
    }


@router.get("/workspace/info", response_model=WorkspaceInfo)
def get_workspace_info(ws: Dependency.Client) -> WorkspaceInfo:
    """Get workspace information including host URL."""
    # Try multiple sources for workspace host
    host = None

    # 1. Try WorkspaceClient config
    if ws.config.host:
        host = ws.config.host
        logger.debug(f"Got host from WorkspaceClient config: {host}")

    # 2. Try DATABRICKS_HOST environment variable
    if not host:
        host = os.environ.get("DATABRICKS_HOST", "")
        logger.debug(f"Got host from DATABRICKS_HOST env: {host}")

    # 3. Try to get from workspace API
    if not host:
        try:
            # Get current workspace info
            workspace_id = os.environ.get("DATABRICKS_WORKSPACE_ID")
            if workspace_id:
                # Construct host from workspace ID (AWS pattern)
                host = f"https://dbc-{workspace_id}.cloud.databricks.com"
                logger.debug(f"Constructed host from workspace ID: {host}")
        except Exception as e:
            logger.debug(f"Failed to construct host from workspace ID: {e}")

    # Ensure we have a valid host
    if not host:
        host = ""
        logger.warning("Could not determine workspace host")

    # Remove trailing slash if present
    host = host.rstrip("/")

    # Try to get org_id from workspace
    org_id = None
    try:
        # The org_id is typically in the URL path or can be extracted
        # For AWS workspaces, it's in the URL like /o/{org_id}/
        if "/o/" in host:
            org_id = host.split("/o/")[1].split("/")[0]
    except Exception:
        pass

    logger.info(f"Returning workspace info: host={host}, org_id={org_id}")
    return WorkspaceInfo(host=host, org_id=org_id)
