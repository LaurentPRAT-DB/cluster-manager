"""
Core application infrastructure: config, logging, utilities, dependencies, and bootstrap.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path
from typing import Annotated, ClassVar, TypeAlias

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, FastAPI, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from starlette.exceptions import HTTPException as StarletteHTTPException

from .._metadata import api_prefix, app_name, app_slug, dist_dir

# --- Config ---

project_root = Path(__file__).parent.parent.parent
env_file = project_root / ".env"

# Load .env file if it exists
try:
    if env_file.exists():
        load_dotenv(dotenv_path=env_file)
except Exception:
    pass  # Ignore .env errors in production


class AppConfig(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_prefix=f"{app_slug.upper()}_",
        extra="ignore",
        env_nested_delimiter="__",
    )
    app_name: str = Field(default=app_name)
    sql_warehouse_id: str | None = Field(
        default=None,
        description="SQL Warehouse ID for billing queries"
    )

    # Optimization settings
    metrics_catalog: str = Field(
        default="main",
        description="Unity Catalog for metrics storage"
    )
    metrics_schema: str = Field(
        default="cluster_manager_app",
        description="Schema for metrics tables"
    )
    oversized_threshold: float = Field(
        default=30.0,
        description="Efficiency % below which cluster is considered oversized"
    )
    underutilized_threshold: float = Field(
        default=50.0,
        description="Efficiency % below which cluster is underutilized"
    )

    def __hash__(self) -> int:
        return hash(self.app_name)


# --- Logger ---

# Configure logging for both local and remote debugging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Console output
    ]
)
logger = logging.getLogger(app_name)
logger.setLevel(logging.DEBUG)

# Log startup info
try:
    logger.info(f"Initializing {app_name} backend")
    logger.info(f"Project root: {project_root}")
    logger.info(f"Environment file exists: {env_file.exists()}")
except Exception as e:
    logger.warning(f"Startup logging failed: {e}")


# --- Utils ---


def _add_exception_handler(app: FastAPI) -> None:
    """Register a global exception handler."""

    async def http_exception_handler(request: Request, exc: StarletteHTTPException):
        logger.info(
            f"HTTP exception handler called for request {request.url.path} "
            f"with status code {exc.status_code}"
        )
        return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)

    app.exception_handler(StarletteHTTPException)(http_exception_handler)


# --- Lifespan ---


@asynccontextmanager
async def _default_lifespan(app: FastAPI):
    """Default lifespan that initializes config and workspace client."""
    config = AppConfig()
    logger.info(f"Starting app with configuration:\n{config}")
    ws = WorkspaceClient()

    # Log workspace client configuration for debugging
    logger.info(f"WorkspaceClient config.host: {ws.config.host}")
    logger.info(f"WorkspaceClient config.auth_type: {ws.config.auth_type}")

    # Log DATABRICKS environment variables
    databricks_env = {k: v for k, v in os.environ.items() if "DATABRICKS" in k and "TOKEN" not in k and "SECRET" not in k}
    logger.info(f"DATABRICKS environment variables: {databricks_env}")

    app.state.config = config
    app.state.workspace_client = ws

    yield


# --- Factory ---


def create_app(
    *,
    routers: list[APIRouter] | None = None,
    lifespan: Callable[[FastAPI], AbstractAsyncContextManager[None]] | None = None,
) -> FastAPI:
    """Create and configure a FastAPI application.

    Args:
        routers: List of APIRouter instances to include in the app.
        lifespan: Optional async context manager for custom startup/shutdown logic.
                  When provided, `app.state.config` and `app.state.workspace_client`
                  are already available.

    Returns:
        Configured FastAPI application instance.
    """

    @asynccontextmanager
    async def _composed_lifespan(app: FastAPI):
        async with _default_lifespan(app):
            if lifespan:
                async with lifespan(app):
                    yield
            else:
                yield

    app = FastAPI(title=app_name, lifespan=_composed_lifespan)

    # Add CORS middleware for development
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    for router in routers or []:
        app.include_router(router)

    _add_exception_handler(app)

    # Serve frontend static files if dist directory exists
    if dist_dir.exists():
        # Serve static assets
        app.mount("/assets", StaticFiles(directory=dist_dir / "assets"), name="assets")

        # Serve index.html for all non-API routes (SPA routing)
        @app.get("/{full_path:path}")
        async def serve_spa(full_path: str):
            # Don't intercept API routes
            if full_path.startswith("api/"):
                return JSONResponse({"detail": "Not Found"}, status_code=404)
            # Serve index.html for SPA routing
            return FileResponse(dist_dir / "index.html")

    return app


def create_router() -> APIRouter:
    """Create an APIRouter with the application's API prefix."""
    return APIRouter(prefix=api_prefix)


# --- Dependencies ---


def get_config(request: Request) -> AppConfig:
    """
    Returns the AppConfig instance from app.state.
    The config is initialized during application lifespan startup.
    """
    if not hasattr(request.app.state, "config"):
        raise RuntimeError(
            "AppConfig not initialized. "
            "Ensure app.state.config is set during application lifespan startup."
        )
    return request.app.state.config


def get_ws(request: Request) -> WorkspaceClient:
    """
    Returns the WorkspaceClient instance from app.state.
    The client is initialized during application lifespan startup.
    """
    if not hasattr(request.app.state, "workspace_client"):
        raise RuntimeError(
            "WorkspaceClient not initialized. "
            "Ensure app.state.workspace_client is set during application lifespan startup."
        )
    return request.app.state.workspace_client


def get_user_ws(
    request: Request,
    token: Annotated[str | None, Header(alias="X-Forwarded-Access-Token")] = None,
) -> WorkspaceClient:
    """
    Returns a Databricks Workspace client with authentication on behalf of user.
    If the request contains an X-Forwarded-Access-Token header, OBO auth is used.
    Falls back to service principal client if OBO token is not available.

    Example usage: `user_ws: Dependency.UserClient`
    """
    if token:
        logger.debug("Using OBO token for user authentication")
        return WorkspaceClient(
            token=token, auth_type="pat"
        )  # set pat explicitly to avoid issues with SP client

    # Fall back to service principal client
    logger.debug("OBO token not available, using service principal client")
    if not hasattr(request.app.state, "workspace_client"):
        raise RuntimeError(
            "WorkspaceClient not initialized. "
            "Ensure app.state.workspace_client is set during application lifespan startup."
        )
    return request.app.state.workspace_client


class Dependency:
    """FastAPI dependency injection shorthand for route handler parameters."""

    Client: TypeAlias = Annotated[WorkspaceClient, Depends(get_ws)]
    """Databricks WorkspaceClient using app-level service principal credentials.
    Recommended usage: `ws: Dependency.Client`"""

    UserClient: TypeAlias = Annotated[WorkspaceClient, Depends(get_user_ws)]
    """WorkspaceClient authenticated on behalf of the current user via OBO token.
    Requires the X-Forwarded-Access-Token header.
    Recommended usage: `user_ws: Dependency.UserClient`"""

    Config: TypeAlias = Annotated[AppConfig, Depends(get_config)]
    """Application configuration loaded from environment variables.
    Recommended usage: `config: Dependency.Config`"""
