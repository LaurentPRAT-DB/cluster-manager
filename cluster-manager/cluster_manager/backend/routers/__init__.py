"""API routers package."""

from .billing import router as billing_router
from .clusters import router as clusters_router
from .mcp import router as mcp_router
from .metrics import router as metrics_router
from .optimization import router as optimization_router
from .policies import router as policies_router
from .workspace import router as workspace_router

__all__ = [
    "clusters_router",
    "mcp_router",
    "metrics_router",
    "billing_router",
    "policies_router",
    "optimization_router",
    "workspace_router",
]
