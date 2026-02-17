"""Main FastAPI application."""

from databricks.sdk.service.iam import User as UserOut

from .core import Dependency, create_app, create_router
from .models import VersionOut
from .routers import billing_router, clusters_router, metrics_router, policies_router

# Create main router for basic endpoints
main_router = create_router()


@main_router.get("/version", response_model=VersionOut, operation_id="version")
async def version():
    """Get the application version."""
    return VersionOut.from_metadata()


@main_router.get("/current-user", response_model=UserOut, operation_id="currentUser")
def current_user(user_ws: Dependency.UserClient):
    """Get the current authenticated user."""
    return user_ws.current_user.me()


@main_router.get("/health", operation_id="health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


@main_router.get("/debug/ws-info", operation_id="wsInfo")
def ws_info(ws: Dependency.Client):
    """Debug endpoint to check workspace client."""
    try:
        # Try a simple API call that should work with SP auth
        me = ws.current_user.me()
        return {
            "status": "ok",
            "user": me.user_name if me else "unknown",
            "user_id": str(me.id) if me else None
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@main_router.get("/debug/clusters-count", operation_id="clustersCount")
def clusters_count(ws: Dependency.Client):
    """Debug endpoint to count clusters."""
    try:
        count = 0
        for cluster in ws.clusters.list():
            count += 1
            if count == 1:
                return {
                    "status": "ok",
                    "first_cluster": cluster.cluster_name,
                    "message": "at least 1 cluster found"
                }
        return {"status": "ok", "count": count}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# Create the app with all routers
app = create_app(
    routers=[
        main_router,
        clusters_router,
        metrics_router,
        billing_router,
        policies_router,
    ]
)
